package clientapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/dealprod"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/pelletier/go-toml/v2"
)

const (
	BBroadcastSuiteVersion             = "BBroadcast/1.0"
	BBroadcastProtocolName             = "Bitcast Broadcast Protocol"
	ProtoHealth            protocol.ID = "/bsv-transfer/healthz/1.0.0"

	ProtoArbHealth         protocol.ID = "/bsv-transfer/arbiter/healthz/1.0.0"
	ProtoSeedGet           protocol.ID = "/bsv-transfer/client/seed/get/1.0.0"
	ProtoQuoteDirectSubmit protocol.ID = "/bsv-transfer/client/quote/direct_submit/1.0.0"
	ProtoLiveQuoteSubmit   protocol.ID = "/bsv-transfer/client/live_quote/submit/1.0.0"
	ProtoDirectDealAccept  protocol.ID = "/bsv-transfer/client/deal/accept/1.0.0"
	ProtoTransferPoolOpen  protocol.ID = "/bsv-transfer/client/transfer-pool/open/1.0.0"
	ProtoTransferPoolPay   protocol.ID = "/bsv-transfer/client/transfer-pool/pay/1.0.0"
	ProtoTransferPoolClose protocol.ID = "/bsv-transfer/client/transfer-pool/close/1.0.0"
	ProtoLiveSubscribe     protocol.ID = "/bsv-transfer/live/subscribe/1.0.0"
	ProtoLiveHeadPush      protocol.ID = "/bsv-transfer/live/head-push/1.0.0"

	bootPeerConnectTimeout = 5 * time.Second
	bootPeerHealthTimeout  = 5 * time.Second

	defaultIndexRelPath = "data/client-index.sqlite"
	seedBlockSize       = 65536
)

type healthReq struct{}
type healthResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}
type seedGetReq struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	SeedHash  string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
}
type seedGetResp struct {
	Seed []byte `protobuf:"bytes,1,opt,name=seed,proto3" json:"seed"`
}
type directQuoteSubmitReq struct {
	DemandID             string   `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	SellerPeerID         string   `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	SeedPrice            uint64   `protobuf:"varint,3,opt,name=seed_price,json=seedPrice,proto3" json:"seed_price"`
	ChunkPrice           uint64   `protobuf:"varint,4,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	ExpiresAtUnix        int64    `protobuf:"varint,5,opt,name=expires_at_unix,json=expiresAtUnix,proto3" json:"expires_at_unix"`
	RecommendedFileName  string   `protobuf:"bytes,6,opt,name=recommended_file_name,json=recommendedFileName,proto3" json:"recommended_file_name,omitempty"`
	ArbiterPeerIDs       []string `protobuf:"bytes,7,rep,name=arbiter_pubkey_hexes,json=arbiterPeerIds,proto3" json:"arbiter_pubkey_hexes,omitempty"`
	AvailableChunkBitmap []byte   `protobuf:"bytes,8,opt,name=available_chunk_bitmap,json=availableChunkBitmap,proto3" json:"available_chunk_bitmap,omitempty"`
	ChunkCount           uint32   `protobuf:"varint,9,opt,name=chunk_count,json=chunkCount,proto3" json:"chunk_count,omitempty"`
	FileSize             uint64   `protobuf:"varint,10,opt,name=file_size,json=fileSize,proto3" json:"file_size,omitempty"`
	MIMEHint             string   `protobuf:"bytes,11,opt,name=mime_hint,json=mimeHint,proto3" json:"mime_hint,omitempty"`
}
type directQuoteSubmitResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}
type directDealAcceptReq struct {
	DemandID      string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	BuyerPeerID   string `protobuf:"bytes,2,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	SeedHash      string `protobuf:"bytes,3,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	SeedPrice     uint64 `protobuf:"varint,4,opt,name=seed_price,json=seedPrice,proto3" json:"seed_price"`
	ChunkPrice    uint64 `protobuf:"varint,5,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	ExpiresAtUnix int64  `protobuf:"varint,6,opt,name=expires_at_unix,json=expiresAtUnix,proto3" json:"expires_at_unix"`
	ArbiterPeerID string `protobuf:"bytes,7,opt,name=arbiter_pubkey_hex,json=arbiterPeerId,proto3" json:"arbiter_pubkey_hex,omitempty"`
}
type directDealAcceptResp struct {
	DealID       string `protobuf:"bytes,1,opt,name=deal_id,json=dealId,proto3" json:"deal_id"`
	SellerPeerID string `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	ChunkPrice   uint64 `protobuf:"varint,3,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	Status       string `protobuf:"bytes,4,opt,name=status,proto3" json:"status"`
}

type directTransferPoolOpenReq struct {
	SessionID      string  `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	DealID         string  `protobuf:"bytes,2,opt,name=deal_id,json=dealId,proto3" json:"deal_id"`
	BuyerPeerID    string  `protobuf:"bytes,3,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	ArbiterPeerID  string  `protobuf:"bytes,4,opt,name=arbiter_pubkey_hex,json=arbiterPeerId,proto3" json:"arbiter_pubkey_hex"`
	ArbiterPubKey  string  `protobuf:"bytes,5,opt,name=arbiter_pubkey,json=arbiterPubkey,proto3" json:"arbiter_pubkey_hex"`
	PoolAmount     uint64  `protobuf:"varint,6,opt,name=pool_amount,json=poolAmount,proto3" json:"pool_amount"`
	SpendTxFee     uint64  `protobuf:"varint,7,opt,name=spend_tx_fee,json=spendTxFee,proto3" json:"spend_tx_fee"`
	Sequence       uint32  `protobuf:"varint,8,opt,name=sequence,proto3" json:"sequence"`
	SellerAmount   uint64  `protobuf:"varint,9,opt,name=seller_amount,json=sellerAmount,proto3" json:"seller_amount"`
	BuyerAmount    uint64  `protobuf:"varint,10,opt,name=buyer_amount,json=buyerAmount,proto3" json:"buyer_amount"`
	CurrentTx      []byte  `protobuf:"bytes,11,opt,name=current_tx,json=currentTx,proto3" json:"current_tx"`
	BuyerSig       []byte  `protobuf:"bytes,12,opt,name=buyer_sig,json=buyerSig,proto3" json:"buyer_sig"`
	BaseTx         []byte  `protobuf:"bytes,13,opt,name=base_tx,json=baseTx,proto3" json:"base_tx"`
	BaseTxID       string  `protobuf:"bytes,14,opt,name=base_txid,json=baseTxid,proto3" json:"base_txid"`
	FeeRateSatByte float64 `protobuf:"fixed64,15,opt,name=fee_rate_sat_byte,json=feeRateSatByte,proto3" json:"fee_rate_sat_byte"`
	LockBlocks     uint32  `protobuf:"varint,16,opt,name=lock_blocks,json=lockBlocks,proto3" json:"lock_blocks"`
}

type directTransferPoolOpenResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
	SellerSig []byte `protobuf:"bytes,3,opt,name=seller_sig,json=sellerSig,proto3" json:"seller_sig,omitempty"`
	Error     string `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
}

type directTransferPoolPayReq struct {
	SessionID    string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	SeedHash     string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	ChunkHash    string `protobuf:"bytes,3,opt,name=chunk_hash,json=chunkHash,proto3" json:"chunk_hash"`
	ChunkIndex   uint32 `protobuf:"varint,4,opt,name=chunk_index,json=chunkIndex,proto3" json:"chunk_index"`
	Sequence     uint32 `protobuf:"varint,5,opt,name=sequence,proto3" json:"sequence"`
	SellerAmount uint64 `protobuf:"varint,6,opt,name=seller_amount,json=sellerAmount,proto3" json:"seller_amount"`
	BuyerAmount  uint64 `protobuf:"varint,7,opt,name=buyer_amount,json=buyerAmount,proto3" json:"buyer_amount"`
	CurrentTx    []byte `protobuf:"bytes,8,opt,name=current_tx,json=currentTx,proto3" json:"current_tx"`
	BuyerSig     []byte `protobuf:"bytes,9,opt,name=buyer_sig,json=buyerSig,proto3" json:"buyer_sig"`
}

type directTransferPoolPayResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
	SellerSig []byte `protobuf:"bytes,3,opt,name=seller_sig,json=sellerSig,proto3" json:"seller_sig,omitempty"`
	Chunk     []byte `protobuf:"bytes,4,opt,name=chunk,proto3" json:"chunk,omitempty"`
	Error     string `protobuf:"bytes,5,opt,name=error,proto3" json:"error,omitempty"`
}

type directTransferPoolCloseReq struct {
	SessionID    string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Sequence     uint32 `protobuf:"varint,2,opt,name=sequence,proto3" json:"sequence"`
	SellerAmount uint64 `protobuf:"varint,3,opt,name=seller_amount,json=sellerAmount,proto3" json:"seller_amount"`
	BuyerAmount  uint64 `protobuf:"varint,4,opt,name=buyer_amount,json=buyerAmount,proto3" json:"buyer_amount"`
	CurrentTx    []byte `protobuf:"bytes,5,opt,name=current_tx,json=currentTx,proto3" json:"current_tx"`
	BuyerSig     []byte `protobuf:"bytes,6,opt,name=buyer_sig,json=buyerSig,proto3" json:"buyer_sig"`
}

type directTransferPoolCloseResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
	SellerSig []byte `protobuf:"bytes,3,opt,name=seller_sig,json=sellerSig,proto3" json:"seller_sig,omitempty"`
	Error     string `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
}

type liveSegmentDataPB struct {
	Version           uint32 `protobuf:"varint,1,opt,name=version,proto3" json:"version"`
	StreamID          string `protobuf:"bytes,2,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	SegmentIndex      uint64 `protobuf:"varint,3,opt,name=segment_index,json=segmentIndex,proto3" json:"segment_index"`
	PrevSeedHash      string `protobuf:"bytes,4,opt,name=prev_seed_hash,json=prevSeedHash,proto3" json:"prev_seed_hash,omitempty"`
	PublisherPubKey   string `protobuf:"bytes,5,opt,name=publisher_pubkey,json=publisherPubkey,proto3" json:"publisher_pubkey"`
	MediaHash         []byte `protobuf:"bytes,6,opt,name=media_hash,json=mediaHash,proto3" json:"media_hash"`
	DurationMs        uint64 `protobuf:"varint,7,opt,name=duration_ms,json=durationMs,proto3" json:"duration_ms,omitempty"`
	PublishedAtUnixMs int64  `protobuf:"varint,8,opt,name=published_at_unix_ms,json=publishedAtUnixMs,proto3" json:"published_at_unix_ms,omitempty"`
	IsDiscontinuity   bool   `protobuf:"varint,9,opt,name=is_discontinuity,json=isDiscontinuity,proto3" json:"is_discontinuity,omitempty"`
	MIMEType          string `protobuf:"bytes,10,opt,name=mime_type,json=mimeType,proto3" json:"mime_type,omitempty"`
	InitSeedHash      string `protobuf:"bytes,11,opt,name=init_seed_hash,json=initSeedHash,proto3" json:"init_seed_hash,omitempty"`
	PlaylistURIHint   string `protobuf:"bytes,12,opt,name=playlist_uri_hint,json=playlistUriHint,proto3" json:"playlist_uri_hint,omitempty"`
	MediaSequence     uint64 `protobuf:"varint,13,opt,name=media_sequence,json=mediaSequence,proto3" json:"media_sequence,omitempty"`
	IsEnd             bool   `protobuf:"varint,14,opt,name=is_end,json=isEnd,proto3" json:"is_end,omitempty"`
}

type liveSegmentPB struct {
	Data       []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data"`
	MediaBytes []byte `protobuf:"bytes,2,opt,name=media_bytes,json=mediaBytes,proto3" json:"media_bytes"`
	Signature  []byte `protobuf:"bytes,3,opt,name=signature,proto3" json:"signature"`
}

type liveSegmentRefPB struct {
	SegmentIndex    uint64 `protobuf:"varint,1,opt,name=segment_index,json=segmentIndex,proto3" json:"segment_index"`
	SeedHash        string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	PublishedAtUnix int64  `protobuf:"varint,3,opt,name=published_at_unix,json=publishedAtUnix,proto3" json:"published_at_unix,omitempty"`
}

type liveSubscribeReq struct {
	StreamURI        string   `protobuf:"bytes,1,opt,name=stream_uri,json=streamUri,proto3" json:"stream_uri"`
	StreamID         string   `protobuf:"bytes,2,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	Window           uint32   `protobuf:"varint,3,opt,name=window,proto3" json:"window"`
	SubscriberPeerID string   `protobuf:"bytes,4,opt,name=subscriber_pubkey_hex,json=subscriberPeerId,proto3" json:"subscriber_pubkey_hex"`
	SubscriberAddrs  []string `protobuf:"bytes,5,rep,name=subscriber_addrs,json=subscriberAddrs,proto3" json:"subscriber_addrs,omitempty"`
}

type liveSubscribeResp struct {
	Status          string              `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
	StreamID        string              `protobuf:"bytes,2,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	PublisherPubKey string              `protobuf:"bytes,3,opt,name=publisher_pubkey,json=publisherPubkey,proto3" json:"publisher_pubkey"`
	RecentSegments  []*liveSegmentRefPB `protobuf:"bytes,4,rep,name=recent_segments,json=recentSegments,proto3" json:"recent_segments,omitempty"`
}

type liveHeadPushReq struct {
	StreamID        string              `protobuf:"bytes,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	PublisherPubKey string              `protobuf:"bytes,2,opt,name=publisher_pubkey,json=publisherPubkey,proto3" json:"publisher_pubkey"`
	RecentSegments  []*liveSegmentRefPB `protobuf:"bytes,3,rep,name=recent_segments,json=recentSegments,proto3" json:"recent_segments,omitempty"`
	SentAtUnix      int64               `protobuf:"varint,4,opt,name=sent_at_unix,json=sentAtUnix,proto3" json:"sent_at_unix"`
}

type liveHeadPushResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}

type liveQuoteSegmentPB struct {
	SegmentIndex uint64 `protobuf:"varint,1,opt,name=segment_index,json=segmentIndex,proto3" json:"segment_index"`
	SeedHash     string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
}

type liveQuoteSubmitReq struct {
	DemandID           string                `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	SellerPeerID       string                `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	StreamID           string                `protobuf:"bytes,3,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	LatestSegmentIndex uint64                `protobuf:"varint,4,opt,name=latest_segment_index,json=latestSegmentIndex,proto3" json:"latest_segment_index"`
	RecentSegments     []*liveQuoteSegmentPB `protobuf:"bytes,5,rep,name=recent_segments,json=recentSegments,proto3" json:"recent_segments,omitempty"`
	ExpiresAtUnix      int64                 `protobuf:"varint,6,opt,name=expires_at_unix,json=expiresAtUnix,proto3" json:"expires_at_unix"`
}

type liveQuoteSubmitResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}

type Config struct {
	ClientID string `yaml:"-" toml:"-"`
	// Debug 打开后，client 会把 pproto 收发原文落盘到本地 raw 抓包目录。
	// 注意：这里只抓 pproto 控制层原文，不抓更底层 libp2p/TCP 线包。
	Debug bool `yaml:"debug" toml:"debug"`
	Keys  struct {
		PrivkeyHex string `yaml:"-" toml:"-"`
	} `yaml:"keys" toml:"keys"`
	BSV struct {
		Network string `yaml:"network" toml:"network"` // "test" 或 "main"（默认 "test"）
	} `yaml:"bsv" toml:"bsv"`
	Network struct {
		Gateways []PeerNode `yaml:"gateways" toml:"gateways"`
		Arbiters []PeerNode `yaml:"arbiters" toml:"arbiters"`
	} `yaml:"network" toml:"network"`
	Storage struct {
		WorkspaceDir string `yaml:"workspace_dir" toml:"workspace_dir"`
		DataDir      string `yaml:"data_dir" toml:"data_dir"`
		MinFreeBytes uint64 `yaml:"min_free_bytes" toml:"min_free_bytes"`
	} `yaml:"storage" toml:"storage"`
	Seller struct {
		Enabled bool `yaml:"enabled" toml:"enabled"`
		Pricing struct {
			FloorPriceSatPer64K     uint64 `yaml:"floor_price_sat_per_64k" toml:"floor_price_sat_per_64k"`
			ResaleDiscountBPS       uint64 `yaml:"resale_discount_bps" toml:"resale_discount_bps"`
			LiveBasePriceSatPer64K  uint64 `yaml:"live_base_price_sat_per_64k" toml:"live_base_price_sat_per_64k"`
			LiveFloorPriceSatPer64K uint64 `yaml:"live_floor_price_sat_per_64k" toml:"live_floor_price_sat_per_64k"`
			LiveDecayPerMinuteBPS   uint64 `yaml:"live_decay_per_minute_bps" toml:"live_decay_per_minute_bps"`
		} `yaml:"pricing" toml:"pricing"`
	} `yaml:"seller" toml:"seller"`
	Live struct {
		CacheMaxBytes uint64 `yaml:"cache_max_bytes" toml:"cache_max_bytes"`
		Buyer         struct {
			TargetLagSegments   uint32 `yaml:"target_lag_segments" toml:"target_lag_segments"`
			MaxBudgetPerMinute  uint64 `yaml:"max_budget_per_minute" toml:"max_budget_per_minute"`
			PreferOlderSegments bool   `yaml:"prefer_older_segments" toml:"prefer_older_segments"`
		} `yaml:"buyer" toml:"buyer"`
		Publish struct {
			BroadcastWindow      uint32 `yaml:"broadcast_window" toml:"broadcast_window"`
			BroadcastIntervalSec uint32 `yaml:"broadcast_interval_seconds" toml:"broadcast_interval_seconds"`
		} `yaml:"publish" toml:"publish"`
	} `yaml:"live" toml:"live"`
	Listen struct {
		Enabled               *bool  `yaml:"enabled" toml:"enabled"`
		RenewThresholdSeconds uint32 `yaml:"renew_threshold_seconds" toml:"renew_threshold_seconds"`
		AutoRenewRounds       uint64 `yaml:"auto_renew_rounds" toml:"auto_renew_rounds"`
		OfferPaymentSatoshi   uint64 `yaml:"offer_payment_satoshi" toml:"offer_payment_satoshi"`
		TickSeconds           uint32 `yaml:"tick_seconds" toml:"tick_seconds"`
	} `yaml:"listen" toml:"listen"`
	Payment struct {
		// PreferredScheme 是“系统默认优先支付通道”。
		// 设计说明：
		// - 这里只表达默认优先级，不引入“强制只走某一条”的第二层语义；
		// - 显式 quote/pay 仍可带 payment_scheme 覆盖；
		// - 缺省保持旧行为，优先走 pool_2of2_v1。
		PreferredScheme string `yaml:"preferred_scheme" toml:"preferred_scheme"`
	} `yaml:"payment" toml:"payment"`
	Reachability struct {
		// AutoAnnounceEnabled 控制“客户端自动把自己当前可达地址发布到 gateway 目录”。
		// 设计说明：
		// - 这是节点侧行为开关，不是 gateway 侧策略；
		// - 缺省开启，让 node locator 在大多数情况下开箱即用；
		// - 关闭后仍保留手动触发口子，方便后续 e2e 与精细调试。
		AutoAnnounceEnabled *bool  `yaml:"auto_announce_enabled" toml:"auto_announce_enabled"`
		AnnounceTTLSeconds  uint32 `yaml:"announce_ttl_seconds" toml:"announce_ttl_seconds"`
	} `yaml:"reachability" toml:"reachability"`
	Scan struct {
		StartupFullScan       bool   `yaml:"startup_full_scan" toml:"startup_full_scan"`
		FSWatchEnabled        bool   `yaml:"fs_watch_enabled" toml:"fs_watch_enabled"`
		RescanIntervalSeconds uint32 `yaml:"rescan_interval_seconds" toml:"rescan_interval_seconds"`
	} `yaml:"scan" toml:"scan"`
	Index struct {
		Backend    string `yaml:"backend" toml:"backend"`
		SQLitePath string `yaml:"sqlite_path" toml:"sqlite_path"`
	} `yaml:"index" toml:"index"`
	HTTP struct {
		Enabled    bool   `yaml:"enabled" toml:"enabled"`
		ListenAddr string `yaml:"listen_addr" toml:"listen_addr"`
	} `yaml:"http" toml:"http"`
	FSHTTP struct {
		Enabled                    bool   `yaml:"enabled" toml:"enabled"`
		ListenAddr                 string `yaml:"listen_addr" toml:"listen_addr"`
		DownloadWaitTimeoutSeconds uint32 `yaml:"download_wait_timeout_seconds" toml:"download_wait_timeout_seconds"`
		MaxConcurrentSessions      uint32 `yaml:"max_concurrent_sessions" toml:"max_concurrent_sessions"`
		MaxChunkPriceSatPer64K     uint64 `yaml:"max_chunk_price_sat_per_64k" toml:"max_chunk_price_sat_per_64k"`
		QuoteWaitSeconds           uint32 `yaml:"quote_wait_seconds" toml:"quote_wait_seconds"`
		QuotePollSeconds           uint32 `yaml:"quote_poll_seconds" toml:"quote_poll_seconds"`
		PrefetchDistanceChunks     uint32 `yaml:"prefetch_distance_chunks" toml:"prefetch_distance_chunks"`
		StrategyDebugLogEnabled    bool   `yaml:"strategy_debug_log_enabled" toml:"strategy_debug_log_enabled"`
	} `yaml:"fs_http" toml:"fs_http"`
	ExternalAPI struct {
		WOC struct {
			APIKey        string `yaml:"api_key" toml:"api_key"`
			MinIntervalMS uint32 `yaml:"min_interval_ms" toml:"min_interval_ms"`
		} `yaml:"woc" toml:"woc"`
	} `yaml:"external_api" toml:"external_api"`
	// WOCAPIKey 只保留作旧配置读入迁移。
	// 运行态和新配置统一走 external_api.woc.api_key。
	WOCAPIKey string `yaml:"woc_api_key,omitempty" toml:"woc_api_key"`
	Log       struct {
		File            string `yaml:"file" toml:"file"`
		ConsoleMinLevel string `yaml:"console_min_level" toml:"console_min_level"`
	} `yaml:"log" toml:"log"`
}

type PeerNode struct {
	Enabled                   bool   `yaml:"enabled" toml:"enabled"`
	Addr                      string `yaml:"addr" toml:"addr"`
	Pubkey                    string `yaml:"pubkey" toml:"pubkey"`
	ListenOfferPaymentSatoshi uint64 `yaml:"listen_offer_payment_satoshi" toml:"listen_offer_payment_satoshi"`
}

type sellerSeed struct {
	SeedHash            string
	FileSize            uint64
	ChunkCount          uint32
	SeedPrice           uint64
	ChunkPrice          uint64
	RecommendedFileName string
	MIMEHint            string
}

type sellerCatalog struct {
	mu    sync.RWMutex
	seeds map[string]sellerSeed
}

type RunInput struct {
	ClientID   string
	ConfigPath string
	Debug      bool
	BSV        struct {
		Network string
	}
	Network struct {
		Gateways []PeerNode
		Arbiters []PeerNode
	}
	Storage struct {
		WorkspaceDir string
		DataDir      string
		MinFreeBytes uint64
	}
	Seller struct {
		Enabled bool
		Pricing struct {
			FloorPriceSatPer64K     uint64
			ResaleDiscountBPS       uint64
			LiveBasePriceSatPer64K  uint64
			LiveFloorPriceSatPer64K uint64
			LiveDecayPerMinuteBPS   uint64
		}
	}
	Live struct {
		CacheMaxBytes uint64
		Buyer         struct {
			TargetLagSegments   uint32
			MaxBudgetPerMinute  uint64
			PreferOlderSegments bool
		}
		Publish struct {
			BroadcastWindow      uint32
			BroadcastIntervalSec uint32
		}
	}
	Listen struct {
		Enabled               *bool
		RenewThresholdSeconds uint32
		AutoRenewRounds       uint64
		OfferPaymentSatoshi   uint64
		TickSeconds           uint32
	}
	Payment struct {
		PreferredScheme string
	}
	Reachability struct {
		AutoAnnounceEnabled *bool
		AnnounceTTLSeconds  uint32
	}
	Scan struct {
		StartupFullScan       bool
		FSWatchEnabled        bool
		RescanIntervalSeconds uint32
	}
	Index struct {
		Backend    string
		SQLitePath string
	}
	HTTP struct {
		Enabled    bool
		ListenAddr string
	}
	FSHTTP struct {
		Enabled                    bool
		ListenAddr                 string
		DownloadWaitTimeoutSeconds uint32
		MaxConcurrentSessions      uint32
		MaxChunkPriceSatPer64K     uint64
		QuoteWaitSeconds           uint32
		QuotePollSeconds           uint32
		PrefetchDistanceChunks     uint32
		StrategyDebugLogEnabled    bool
	}
	ExternalAPI struct {
		WOC struct {
			APIKey        string
			MinIntervalMS uint32
		}
	}
	Log struct {
		File            string
		ConsoleMinLevel string
	}

	// DisableHTTPServer 仅影响本次 Run 的启动行为，不会持久化到配置。
	// managed 模式使用单入口时设为 true，避免 runtime 内部再开启 HTTP 监听。
	DisableHTTPServer bool

	// FSHTTPListener 仅用于桌面托管模式：
	// - 解锁前主进程先把 fs_http 端口真实监听住，提前暴露端口占用错误；
	// - 解锁后把同一个 listener 交给真正文件服务，避免“先说可启动，解锁时才 bind 失败”。
	FSHTTPListener net.Listener

	// EffectivePrivKeyHex 是启动前已确定的“唯一运行时私钥”。
	// 设计约束：Host 身份与费用池签名必须都来自这把私钥。
	EffectivePrivKeyHex string
	ObsSink             obs.Sink

	// ActionChain 承载真实上链动作与费用池最小读能力。
	// 生产环境默认使用 chainbridge 的嵌入式费用池客户端。
	ActionChain poolcore.ChainClient

	// WalletChain 只服务钱包同步与历史扫描。
	// 设计约束：
	// - 钱包同步明确接受 whatsonchain 语义，不再复制一层同构接口；
	// - 运行时只持有已装配好的最小 WOC 客户端，不再依赖历史中间层。
	WalletChain walletChainClient

	// RPCTrace 仅用于集成测试：记录 client 自己的 pproto 收发报文（JSONL）。
	// 正常运行默认不启用（nil）。
	RPCTrace pproto.TraceSink

	// PostWorkspaceBootstrap 在 workspace 初始化完成后、运行期后台协程启动前执行。
	// 设计说明：
	// - 这里给上层产品壳预留“本次启动专属”的 DB 引导钩子；
	// - 典型用途是桌面版把系统首页的元信息/默认价格固化进索引；
	// - 放在这个时机可以复用 startup_full_scan 的结果，同时避开运行期并发写数据库带来的锁竞争。
	PostWorkspaceBootstrap func(db *sql.DB) error
}

// NewRunInputFromConfig 在 Run 外完成配置复制，避免 RunInput 直接携带 Config。
func NewRunInputFromConfig(cfg Config, effectivePrivKeyHex string) RunInput {
	in := RunInput{
		ClientID:            cfg.ClientID,
		EffectivePrivKeyHex: effectivePrivKeyHex,
	}
	in.Debug = cfg.Debug
	in.BSV.Network = cfg.BSV.Network
	in.Network.Gateways = append([]PeerNode(nil), cfg.Network.Gateways...)
	in.Network.Arbiters = append([]PeerNode(nil), cfg.Network.Arbiters...)
	in.Storage.WorkspaceDir = cfg.Storage.WorkspaceDir
	in.Storage.DataDir = cfg.Storage.DataDir
	in.Storage.MinFreeBytes = cfg.Storage.MinFreeBytes
	in.Seller.Enabled = cfg.Seller.Enabled
	in.Seller.Pricing.FloorPriceSatPer64K = cfg.Seller.Pricing.FloorPriceSatPer64K
	in.Seller.Pricing.ResaleDiscountBPS = cfg.Seller.Pricing.ResaleDiscountBPS
	in.Seller.Pricing.LiveBasePriceSatPer64K = cfg.Seller.Pricing.LiveBasePriceSatPer64K
	in.Seller.Pricing.LiveFloorPriceSatPer64K = cfg.Seller.Pricing.LiveFloorPriceSatPer64K
	in.Seller.Pricing.LiveDecayPerMinuteBPS = cfg.Seller.Pricing.LiveDecayPerMinuteBPS
	in.Live.CacheMaxBytes = cfg.Live.CacheMaxBytes
	in.Live.Buyer.TargetLagSegments = cfg.Live.Buyer.TargetLagSegments
	in.Live.Buyer.MaxBudgetPerMinute = cfg.Live.Buyer.MaxBudgetPerMinute
	in.Live.Buyer.PreferOlderSegments = cfg.Live.Buyer.PreferOlderSegments
	in.Live.Publish.BroadcastWindow = cfg.Live.Publish.BroadcastWindow
	in.Live.Publish.BroadcastIntervalSec = cfg.Live.Publish.BroadcastIntervalSec
	if cfg.Listen.Enabled != nil {
		v := *cfg.Listen.Enabled
		in.Listen.Enabled = &v
	}
	in.Listen.RenewThresholdSeconds = cfg.Listen.RenewThresholdSeconds
	in.Listen.AutoRenewRounds = cfg.Listen.AutoRenewRounds
	in.Listen.OfferPaymentSatoshi = cfg.Listen.OfferPaymentSatoshi
	in.Listen.TickSeconds = cfg.Listen.TickSeconds
	in.Payment.PreferredScheme = cfg.Payment.PreferredScheme
	if cfg.Reachability.AutoAnnounceEnabled != nil {
		v := *cfg.Reachability.AutoAnnounceEnabled
		in.Reachability.AutoAnnounceEnabled = &v
	}
	in.Reachability.AnnounceTTLSeconds = cfg.Reachability.AnnounceTTLSeconds
	in.Scan.StartupFullScan = cfg.Scan.StartupFullScan
	in.Scan.FSWatchEnabled = cfg.Scan.FSWatchEnabled
	in.Scan.RescanIntervalSeconds = cfg.Scan.RescanIntervalSeconds
	in.Index.Backend = cfg.Index.Backend
	in.Index.SQLitePath = cfg.Index.SQLitePath
	in.HTTP.Enabled = cfg.HTTP.Enabled
	in.HTTP.ListenAddr = cfg.HTTP.ListenAddr
	in.FSHTTP.Enabled = cfg.FSHTTP.Enabled
	in.FSHTTP.ListenAddr = cfg.FSHTTP.ListenAddr
	in.FSHTTP.DownloadWaitTimeoutSeconds = cfg.FSHTTP.DownloadWaitTimeoutSeconds
	in.FSHTTP.MaxConcurrentSessions = cfg.FSHTTP.MaxConcurrentSessions
	in.FSHTTP.MaxChunkPriceSatPer64K = cfg.FSHTTP.MaxChunkPriceSatPer64K
	in.FSHTTP.QuoteWaitSeconds = cfg.FSHTTP.QuoteWaitSeconds
	in.FSHTTP.QuotePollSeconds = cfg.FSHTTP.QuotePollSeconds
	in.FSHTTP.PrefetchDistanceChunks = cfg.FSHTTP.PrefetchDistanceChunks
	in.FSHTTP.StrategyDebugLogEnabled = cfg.FSHTTP.StrategyDebugLogEnabled
	in.ExternalAPI.WOC.APIKey = cfg.ExternalAPI.WOC.APIKey
	in.ExternalAPI.WOC.MinIntervalMS = cfg.ExternalAPI.WOC.MinIntervalMS
	in.Log.File = cfg.Log.File
	in.Log.ConsoleMinLevel = cfg.Log.ConsoleMinLevel
	return in
}

func (in *RunInput) applyConfig(cfg Config) {
	if in == nil {
		return
	}
	disableHTTPServer := in.DisableHTTPServer
	next := NewRunInputFromConfig(cfg, in.EffectivePrivKeyHex)
	next.ConfigPath = in.ConfigPath
	next.ObsSink = in.ObsSink
	next.ActionChain = in.ActionChain
	next.WalletChain = in.WalletChain
	next.RPCTrace = in.RPCTrace
	next.DisableHTTPServer = disableHTTPServer
	next.FSHTTPListener = in.FSHTTPListener
	*in = next
}

func (in RunInput) toConfig() Config {
	cfg := Config{
		ClientID: in.ClientID,
	}
	cfg.Debug = in.Debug
	cfg.BSV.Network = in.BSV.Network
	cfg.Network.Gateways = append([]PeerNode(nil), in.Network.Gateways...)
	cfg.Network.Arbiters = append([]PeerNode(nil), in.Network.Arbiters...)
	cfg.Storage.WorkspaceDir = in.Storage.WorkspaceDir
	cfg.Storage.DataDir = in.Storage.DataDir
	cfg.Storage.MinFreeBytes = in.Storage.MinFreeBytes
	cfg.Seller.Enabled = in.Seller.Enabled
	cfg.Seller.Pricing.FloorPriceSatPer64K = in.Seller.Pricing.FloorPriceSatPer64K
	cfg.Seller.Pricing.ResaleDiscountBPS = in.Seller.Pricing.ResaleDiscountBPS
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = in.Seller.Pricing.LiveBasePriceSatPer64K
	cfg.Seller.Pricing.LiveFloorPriceSatPer64K = in.Seller.Pricing.LiveFloorPriceSatPer64K
	cfg.Seller.Pricing.LiveDecayPerMinuteBPS = in.Seller.Pricing.LiveDecayPerMinuteBPS
	cfg.Live.CacheMaxBytes = in.Live.CacheMaxBytes
	cfg.Live.Buyer.TargetLagSegments = in.Live.Buyer.TargetLagSegments
	cfg.Live.Buyer.MaxBudgetPerMinute = in.Live.Buyer.MaxBudgetPerMinute
	cfg.Live.Buyer.PreferOlderSegments = in.Live.Buyer.PreferOlderSegments
	cfg.Live.Publish.BroadcastWindow = in.Live.Publish.BroadcastWindow
	cfg.Live.Publish.BroadcastIntervalSec = in.Live.Publish.BroadcastIntervalSec
	if in.Listen.Enabled != nil {
		v := *in.Listen.Enabled
		cfg.Listen.Enabled = &v
	}
	cfg.Listen.RenewThresholdSeconds = in.Listen.RenewThresholdSeconds
	cfg.Listen.AutoRenewRounds = in.Listen.AutoRenewRounds
	cfg.Listen.OfferPaymentSatoshi = in.Listen.OfferPaymentSatoshi
	cfg.Listen.TickSeconds = in.Listen.TickSeconds
	cfg.Payment.PreferredScheme = in.Payment.PreferredScheme
	if in.Reachability.AutoAnnounceEnabled != nil {
		v := *in.Reachability.AutoAnnounceEnabled
		cfg.Reachability.AutoAnnounceEnabled = &v
	}
	cfg.Reachability.AnnounceTTLSeconds = in.Reachability.AnnounceTTLSeconds
	cfg.Scan.StartupFullScan = in.Scan.StartupFullScan
	cfg.Scan.FSWatchEnabled = in.Scan.FSWatchEnabled
	cfg.Scan.RescanIntervalSeconds = in.Scan.RescanIntervalSeconds
	cfg.Index.Backend = in.Index.Backend
	cfg.Index.SQLitePath = in.Index.SQLitePath
	cfg.HTTP.Enabled = in.HTTP.Enabled
	cfg.HTTP.ListenAddr = in.HTTP.ListenAddr
	cfg.FSHTTP.Enabled = in.FSHTTP.Enabled
	cfg.FSHTTP.ListenAddr = in.FSHTTP.ListenAddr
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = in.FSHTTP.DownloadWaitTimeoutSeconds
	cfg.FSHTTP.MaxConcurrentSessions = in.FSHTTP.MaxConcurrentSessions
	cfg.FSHTTP.MaxChunkPriceSatPer64K = in.FSHTTP.MaxChunkPriceSatPer64K
	cfg.FSHTTP.QuoteWaitSeconds = in.FSHTTP.QuoteWaitSeconds
	cfg.FSHTTP.QuotePollSeconds = in.FSHTTP.QuotePollSeconds
	cfg.FSHTTP.PrefetchDistanceChunks = in.FSHTTP.PrefetchDistanceChunks
	cfg.FSHTTP.StrategyDebugLogEnabled = in.FSHTTP.StrategyDebugLogEnabled
	cfg.ExternalAPI.WOC.APIKey = in.ExternalAPI.WOC.APIKey
	cfg.ExternalAPI.WOC.MinIntervalMS = in.ExternalAPI.WOC.MinIntervalMS
	cfg.Log.File = in.Log.File
	cfg.Log.ConsoleMinLevel = in.Log.ConsoleMinLevel
	cfg.Keys.PrivkeyHex = strings.TrimSpace(in.EffectivePrivKeyHex)
	return cfg
}

type Runtime struct {
	Host host.Host
	// DB              *sql.DB
	// DBActor         *sqliteactor.Actor
	// store           *clientDB
	runIn           RunInput
	StartedAtUnix   int64
	HealthyGWs      []peer.AddrInfo
	HealthyArbiters []peer.AddrInfo
	Workspace       *workspaceManager
	Catalog         *sellerCatalog
	HTTP            *httpAPIServer
	FSHTTP          *fileHTTPServer

	ActionChain poolcore.ChainClient
	WalletChain walletChainClient
	feePoolsMu  sync.RWMutex
	feePools    map[string]*feePoolSession
	// feePoolPayLocks 按 gateway 串行化费用池扣费路径（listen cycle / publish demand / publish live demand）。
	// 设计约束：同一 gateway 只能有一个扣费请求在飞，避免 sequence/server_amount 并发竞争。
	feePoolPayLocksMu sync.Mutex
	feePoolPayLocks   map[string]*sync.Mutex
	tripleMu          sync.RWMutex
	triplePool        map[string]*triplePoolSession

	// 设计说明：
	// - open 阶段涉及 deal/session 建立与钱包输入准备，仍用全局锁保证顺序；
	// - pay/close 阶段按 session 串行，不同 session 允许并发，支撑多卖家并行下载。
	transferPoolOpenMu         sync.Mutex
	transferPoolSessionLocksMu sync.Mutex
	transferPoolSessionLocks   map[string]*sync.Mutex
	// walletAllocMu 保证“钱包 UTXO 分配”串行执行，避免并发选中同一输入导致冲突。
	// 分配完成后，基于专属 UTXO 的后续池内操作可并行。
	walletAllocMu sync.Mutex

	rpcTrace pproto.TraceSink
	live     *liveRuntime

	// 运行时状态
	gwManager   *gatewayManager
	masterGW    peer.ID
	masterGWMu  sync.RWMutex
	kernel      *clientKernel
	orch        *orchestrator
	chainMaint  *chainMaintainer
	taskSched   *taskScheduler
	taskSchedMu sync.Mutex

	bgCancel context.CancelFunc

	closeOnce sync.Once
	closeFn   func() error
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			err = r.closeFn()
		}
	})
	return err
}

func (r *Runtime) ClientID() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.runIn.ClientID)
}

func (r *Runtime) HTTPListenAddr() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.runIn.HTTP.ListenAddr)
}

func (r *Runtime) FSHTTPListenAddr() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.runIn.FSHTTP.ListenAddr)
}

func (r *Runtime) WorkspaceDir() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.runIn.Storage.WorkspaceDir)
}

func (r *Runtime) BSVNetwork() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.runIn.BSV.Network)
}

func (r *Runtime) NetworkArbiters() []PeerNode {
	if r == nil {
		return nil
	}
	out := make([]PeerNode, len(r.runIn.Network.Arbiters))
	copy(out, r.runIn.Network.Arbiters)
	return out
}

func (r *Runtime) SellerEnabled() bool {
	if r == nil {
		return false
	}
	return r.runIn.Seller.Enabled
}

func Run(ctx context.Context, in RunInput) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	cfg := in.toConfig()
	if err := ApplyConfigDefaults(&cfg); err != nil {
		return nil, err
	}

	var removeObs func()
	if in.ObsSink != nil {
		removeObs = obs.AddListener(func(ev obs.Event) {
			if ev.Service != "bitcast-client" {
				return
			}
			in.ObsSink.Handle(ev)
		})
	}

	if err := validateConfig(&cfg); err != nil {
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := initDataDirs(&cfg); err != nil {
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	dbPath := cfg.Index.SQLitePath
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(cfg.Storage.DataDir, dbPath)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	openedDB, err := sqliteactor.Open(dbPath)
	if err != nil {
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	db := openedDB.DB
	dbActor := openedDB.Actor
	store := newClientDB(db, dbActor)
	// 设计说明：
	// - BitFS 正式运行时的 sqlite 从这里开始统一进入单 owner 模型；
	// - 初始化阶段仍有一些旧 helper 直接吃 `*sql.DB`，所以当前同时保留 `db` 和 `dbActor`；
	// - 但运行期并发最重的路径必须优先走 actor，避免再长出新的直连写库逻辑。
	if err := ensureClientDBSchema(store); err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := dbSyncSystemSeedPricingPolicies(db, cfg.Seller.Pricing.FloorPriceSatPer64K, cfg.Seller.Pricing.ResaleDiscountBPS); err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	catalog := &sellerCatalog{seeds: map[string]sellerSeed{}}
	workspaceMgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		store:   store,
		catalog: catalog,
	}
	if err := workspaceMgr.EnsureDefaultWorkspace(); err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := workspaceMgr.ValidateLiveCacheCapacity(cfg.Live.CacheMaxBytes); err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if cfg.Scan.StartupFullScan {
		if _, err := workspaceMgr.SyncOnce(ctx); err != nil {
			_ = dbActor.Close()
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
	}
	if err := workspaceMgr.EnforceLiveCacheLimit(cfg.Live.CacheMaxBytes); err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if in.PostWorkspaceBootstrap != nil {
		if err := in.PostWorkspaceBootstrap(db); err != nil {
			_ = dbActor.Close()
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
	}

	// 强制仅启用 TCP 传输，规避 QUIC 在当前工具链环境下的 TLS session ticket panic。
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	}
	effectivePrivHex, err := normalizeRawSecp256k1PrivKeyHex(in.EffectivePrivKeyHex)
	if err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if strings.TrimSpace(effectivePrivHex) == "" {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, fmt.Errorf("effective private key is required")
	}
	// 设计约束：client_pubkey_hex 与费用池签名必须来自同一私钥。
	// 运行时有效私钥作为唯一真源，后续签名路径统一读取 cfg.Keys.PrivkeyHex。
	cfg.Keys.PrivkeyHex = effectivePrivHex
	in.EffectivePrivKeyHex = effectivePrivHex
	priv, err := parsePrivHex(effectivePrivHex)
	if err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	opts = append(opts, libp2p.Identity(priv))
	h, err := libp2p.New(opts...)
	if err != nil {
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	clientPubHex, err := localPubKeyHex(h)
	if err != nil {
		_ = h.Close()
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if cfg.ClientID != "" && !strings.EqualFold(strings.TrimSpace(cfg.ClientID), clientPubHex) {
		obs.Info("bitcast-client", "client_pubkey_hex_overridden_by_pubkey", map[string]any{"configured_client_pubkey_hex": cfg.ClientID, "effective_client_pubkey_hex": clientPubHex})
	}
	cfg.ClientID = clientPubHex
	if err := validateClientIdentityConsistency(cfg); err != nil {
		_ = h.Close()
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	activeGWs, err := connectGateways(ctx, h, cfg.Network.Gateways)
	if err != nil {
		_ = h.Close()
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if len(activeGWs) == 0 {
		obs.Business("bitcast-client", "waiting_gateway_config", map[string]any{"message": "no enabled gateway, waiting for HTTP API configuration"})
	}
	arbInfo, err := connectArbiters(ctx, h, cfg.Network.Arbiters)
	if err != nil {
		_ = h.Close()
		_ = dbActor.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	logFile, logConsoleMinLevel := ResolveLogConfig(&cfg)
	trace := in.RPCTrace
	var closeTrace func() error
	if trace == nil && cfg.Debug {
		localTrace, err := pproto.NewLocalRawTraceSink(logFile)
		if err != nil {
			_ = h.Close()
			_ = dbActor.Close()
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
		trace = localTrace
		closeTrace = localTrace.Close
	}
	healthyGWs := checkPeerHealth(ctx, h, activeGWs, ProtoHealth, gwSec(trace), "gateway")
	if len(healthyGWs) == 0 {
		obs.Error("bitcast-client", "gateway_health_all_failed", map[string]any{
			"configured_gateway_count": len(activeGWs),
			"fallback":                 "use_connected_gateways_and_retry_in_listen_loop",
		})
		healthyGWs = activeGWs
	}
	healthyArbiters := checkPeerHealth(ctx, h, arbInfo, ProtoArbHealth, arbSec(trace), "arbiter")
	if len(healthyArbiters) == 0 && len(arbInfo) > 0 {
		obs.Error("bitcast-client", "arbiter_health_all_failed", map[string]any{
			"configured_arbiter_count": len(arbInfo),
		})
	}

	in.applyConfig(cfg)
	obs.Important("bitcast-client", "started", map[string]any{
		"transport_peer_id": h.ID().String(),
		"pubkey_hex":        clientPubHex,
		"client_pubkey_hex": cfg.ClientID,
		"seller_enabled":    cfg.Seller.Enabled,
		"listen_enabled":    cfgBool(cfg.Listen.Enabled, true),
		"gateway_count":     len(healthyGWs),
		"arbiter_count":     len(healthyArbiters),
		"db":                dbPath,
		"log_file":          logFile,
		"log_console":       logConsoleMinLevel,
		"protocol_suite":    BBroadcastSuiteVersion,
		"protocol_doc_name": BBroadcastProtocolName,
	})
	rt := &Runtime{
		Host: h,
		// DB:                       db,
		// DBActor:                  dbActor,
		// store:                    store,
		runIn:                    in,
		StartedAtUnix:            time.Now().Unix(),
		HealthyGWs:               healthyGWs,
		HealthyArbiters:          healthyArbiters,
		Workspace:                workspaceMgr,
		Catalog:                  catalog,
		ActionChain:              in.ActionChain,
		WalletChain:              in.WalletChain,
		live:                     newLiveRuntime(),
		feePools:                 map[string]*feePoolSession{},
		feePoolPayLocks:          map[string]*sync.Mutex{},
		triplePool:               map[string]*triplePoolSession{},
		transferPoolSessionLocks: map[string]*sync.Mutex{},
		rpcTrace:                 trace,
	}
	rtCtx, rtCancel := context.WithCancel(ctx)
	rt.bgCancel = rtCancel
	rt.taskSched = newTaskScheduler(store, "bitcast-client")
	rt.kernel = newClientKernel(rt, store)
	rt.orch = newOrchestrator(rt, store)
	registerLiveHandlers(store, rt)
	registerNodeRouteHandlers(rt, store)
	registerResolverHandlers(rt)
	registerDirectQuoteSubmitHandler(h, store, trace)
	if cfg.Seller.Enabled {
		registerSellerHandlers(h, store, rt.live, trace, cfg)
	}
	if rt.ActionChain == nil {
		actionChain, err := chainbridge.NewDefaultFeePoolChain(chainbridge.RouteConfig{
			Provider: chainbridge.WhatsOnChainProvider,
			Network:  in.BSV.Network,
		})
		if err != nil {
			_ = dbActor.Close()
			if closeTrace != nil {
				_ = closeTrace()
			}
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
		rt.ActionChain = actionChain
	}
	if rt.WalletChain == nil {
		walletChain, err := NewWalletChainClient(chainbridge.Route{
			Provider: chainbridge.WhatsOnChainProvider,
			Network:  in.BSV.Network,
		})
		if err != nil {
			_ = dbActor.Close()
			if closeTrace != nil {
				_ = closeTrace()
			}
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
		rt.WalletChain = walletChain
	}

	// 初始化网关管理器
	rt.gwManager = newGatewayManager(rt, h)
	_ = rt.gwManager.InitFromConfig(rtCtx, cfg.Network.Gateways)
	// 更新 HealthyGWs 为已连接的网关
	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()

	var wg sync.WaitGroup
	if rt.orch != nil {
		rt.orch.Start(rtCtx)
	}
	// 链维护进程：统一串行调度链 API 查询，业务侧只读本地快照。
	startChainMaintainer(rtCtx, rt, store)
	// listen 费用池自动 loop（按周期扣费/续费，网关联通后自动触发）。
	startListenLoops(rtCtx, rt, store)
	// 自动地址声明发布与 listen loop 并列存在：
	// - listen 解决“我是否持续监听网关广播”；
	// - reachability announce 解决“别人是否能通过 gateway 目录找到我”。
	startAutoNodeReachabilityAnnounceLoop(rtCtx, rt, store)
	if cfg.HTTP.Enabled && !in.DisableHTTPServer {
		rt.HTTP = newHTTPAPIServer(rt, &cfg, db, dbActor, h, healthyGWs, workspaceMgr, trace)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rt.HTTP.Start(); err != nil {
				obs.Error("bitcast-client", "http_api_stopped", map[string]any{"error": err.Error()})
			}
		}()
	}
	if cfg.FSHTTP.Enabled {
		rt.FSHTTP = newFileHTTPServer(rt, &cfg, store, workspaceMgr)
		fsHTTPListener := in.FSHTTPListener
		in.FSHTTPListener = nil
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if fsHTTPListener != nil {
				err = rt.FSHTTP.StartOnListener(fsHTTPListener)
			} else {
				err = rt.FSHTTP.Start()
			}
			if err != nil {
				obs.Error("bitcast-client", "fs_http_stopped", map[string]any{"error": err.Error()})
			}
		}()
	}

	go restorePersistedLiveFollows(rtCtx, store, rt)

	rt.closeFn = func() error {
		if rt.bgCancel != nil {
			rt.bgCancel()
		}
		if rt.taskSched != nil {
			if err := rt.taskSched.Shutdown(); err != nil {
				obs.Error("bitcast-client", "task_scheduler_shutdown_failed", map[string]any{"error": err.Error()})
			}
		}
		if rt.chainMaint != nil {
			rt.chainMaint.Wait()
		}
		if rt.HTTP != nil {
			_ = rt.HTTP.Shutdown(context.Background())
		}
		if rt.FSHTTP != nil {
			_ = rt.FSHTTP.Shutdown(context.Background())
		}
		wg.Wait()
		if removeObs != nil {
			removeObs()
		}
		var firstErr error
		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := dbActor.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if closeTrace != nil {
			if err := closeTrace(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	go func() {
		<-ctx.Done()
		_ = rt.Close()
	}()
	return rt, nil
}

func ApplyConfigDefaults(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	// BSV：仅支持 test/main 两种网络；默认 test。
	{
		n, err := NormalizeBSVNetwork(cfg.BSV.Network)
		if err != nil {
			return err
		}
		cfg.BSV.Network = n
	}
	networkDefaults, err := networkInitDefaults(cfg.BSV.Network)
	if err != nil {
		return err
	}
	if len(cfg.Network.Gateways) == 0 {
		cfg.Network.Gateways = initPeerNodesToPeerNodes(networkDefaults.DefaultGateways)
	}
	if len(cfg.Network.Arbiters) == 0 {
		cfg.Network.Arbiters = initPeerNodesToPeerNodes(networkDefaults.DefaultArbiters)
	}
	if cfg.Index.Backend == "" {
		cfg.Index.Backend = networkDefaults.IndexBackend
	}
	if cfg.Index.SQLitePath == "" {
		cfg.Index.SQLitePath = networkDefaults.IndexSQLitePath
	}
	if cfg.Seller.Pricing.FloorPriceSatPer64K == 0 {
		cfg.Seller.Pricing.FloorPriceSatPer64K = networkDefaults.SellerFloorPriceSatPer64K
	}
	if cfg.Seller.Pricing.ResaleDiscountBPS == 0 {
		cfg.Seller.Pricing.ResaleDiscountBPS = networkDefaults.SellerResaleDiscountBPS
	}
	if cfg.Seller.Pricing.LiveBasePriceSatPer64K == 0 {
		cfg.Seller.Pricing.LiveBasePriceSatPer64K = cfg.Seller.Pricing.FloorPriceSatPer64K * networkDefaults.SellerLiveBaseMultiplier
		if cfg.Seller.Pricing.LiveBasePriceSatPer64K == 0 {
			cfg.Seller.Pricing.LiveBasePriceSatPer64K = networkDefaults.SellerFloorPriceSatPer64K * networkDefaults.SellerLiveBaseMultiplier
		}
	}
	if cfg.Seller.Pricing.LiveFloorPriceSatPer64K == 0 {
		cfg.Seller.Pricing.LiveFloorPriceSatPer64K = cfg.Seller.Pricing.FloorPriceSatPer64K
	}
	if cfg.Seller.Pricing.LiveDecayPerMinuteBPS == 0 {
		cfg.Seller.Pricing.LiveDecayPerMinuteBPS = networkDefaults.SellerLiveDecayPerMinuteBPS
	}
	if cfg.Live.Buyer.TargetLagSegments == 0 {
		cfg.Live.Buyer.TargetLagSegments = networkDefaults.LiveBuyerTargetLagSegments
	}
	if cfg.Live.Publish.BroadcastWindow == 0 {
		cfg.Live.Publish.BroadcastWindow = networkDefaults.LivePublishBroadcastWindow
	}
	if cfg.Live.Publish.BroadcastIntervalSec == 0 {
		cfg.Live.Publish.BroadcastIntervalSec = networkDefaults.LivePublishIntervalSeconds
	}
	if cfg.Listen.Enabled == nil {
		v := networkDefaults.ListenEnabled
		cfg.Listen.Enabled = &v
	}
	if cfg.Listen.RenewThresholdSeconds == 0 {
		cfg.Listen.RenewThresholdSeconds = networkDefaults.ListenRenewThresholdSeconds
	}
	if cfg.Listen.AutoRenewRounds == 0 {
		cfg.Listen.AutoRenewRounds = networkDefaults.ListenAutoRenewRounds
	}
	if cfg.Listen.TickSeconds == 0 {
		cfg.Listen.TickSeconds = networkDefaults.ListenTickSeconds
	}
	scheme, err := normalizePreferredPaymentScheme(cfg.Payment.PreferredScheme)
	if err != nil {
		return err
	}
	cfg.Payment.PreferredScheme = scheme
	if cfg.Reachability.AutoAnnounceEnabled == nil {
		v := true
		cfg.Reachability.AutoAnnounceEnabled = &v
	}
	if cfg.Reachability.AnnounceTTLSeconds == 0 {
		cfg.Reachability.AnnounceTTLSeconds = 3600
	}
	if cfg.Scan.RescanIntervalSeconds == 0 {
		cfg.Scan.RescanIntervalSeconds = networkDefaults.ScanRescanIntervalSeconds
	}
	if cfg.Storage.MinFreeBytes == 0 {
		cfg.Storage.MinFreeBytes = networkDefaults.StorageMinFreeBytes
	}
	if strings.TrimSpace(cfg.HTTP.ListenAddr) == "" {
		cfg.HTTP.ListenAddr = networkDefaults.HTTPListenAddr
	}
	if strings.TrimSpace(cfg.FSHTTP.ListenAddr) == "" {
		cfg.FSHTTP.ListenAddr = networkDefaults.FSHTTPListenAddr
	}
	if cfg.FSHTTP.DownloadWaitTimeoutSeconds == 0 {
		cfg.FSHTTP.DownloadWaitTimeoutSeconds = networkDefaults.FSHTTPDownloadWaitSeconds
	}
	if cfg.FSHTTP.MaxConcurrentSessions == 0 {
		cfg.FSHTTP.MaxConcurrentSessions = networkDefaults.FSHTTPMaxConcurrentSessions
	}
	if cfg.FSHTTP.QuoteWaitSeconds == 0 {
		cfg.FSHTTP.QuoteWaitSeconds = networkDefaults.FSHTTPQuoteWaitSeconds
	}
	if cfg.FSHTTP.QuotePollSeconds == 0 {
		cfg.FSHTTP.QuotePollSeconds = networkDefaults.FSHTTPQuotePollSeconds
	}
	if cfg.FSHTTP.PrefetchDistanceChunks == 0 {
		cfg.FSHTTP.PrefetchDistanceChunks = networkDefaults.FSHTTPPrefetchDistanceChunks
	}
	// 设计说明：
	// - 外部 API 保护统一放在 external_api 下做透明管理；
	// - 但每个 provider 的频率策略独立，不能共用一个桶；
	// - 当前默认让 WOC 更保守，1sat 兼容资产索引稍快一些。
	if strings.TrimSpace(cfg.ExternalAPI.WOC.APIKey) == "" && strings.TrimSpace(cfg.WOCAPIKey) != "" {
		cfg.ExternalAPI.WOC.APIKey = strings.TrimSpace(cfg.WOCAPIKey)
		cfg.WOCAPIKey = ""
	}
	if cfg.ExternalAPI.WOC.MinIntervalMS == 0 {
		cfg.ExternalAPI.WOC.MinIntervalMS = 1000
	}
	if strings.TrimSpace(cfg.Log.ConsoleMinLevel) == "" {
		cfg.Log.ConsoleMinLevel = networkDefaults.LogConsoleMinLevel
	}
	return nil
}

func ParseConfigTOML(data []byte) (Config, error) {
	// 历史字段迁移：金额语义已下线，统一收敛到“续费轮数”。
	// 迁移后仍走严格模式，避免把未知字段静默吞掉。
	normalized := strings.ReplaceAll(string(data), "max_auto_renew_amount", "auto_renew_rounds")
	// 历史字段迁移：管理 API token 语义已移除。
	// 这里仅做读时清理，避免旧 DB 配置因严格解析失败。
	normalized = stripDeprecatedTOMLKeyLines(normalized, map[string]struct{}{
		"auth_token": {},
	})
	// 历史字段迁移：1Sat overlay 资产索引能力已整体下线。
	// 旧配置文件即使还留着该 section，也不应继续影响当前启动。
	normalized = stripTOMLSections(normalized, map[string]struct{}{
		"external_api.asset_index": {},
	})
	var cfg Config
	dec := toml.NewDecoder(strings.NewReader(normalized))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func stripDeprecatedTOMLKeyLines(src string, keys map[string]struct{}) string {
	if strings.TrimSpace(src) == "" || len(keys) == 0 {
		return src
	}
	lines := strings.Split(src, "\n")
	dst := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			dst = append(dst, line)
			continue
		}
		candidate := trimmed
		if i := strings.Index(candidate, "="); i >= 0 {
			candidate = strings.TrimSpace(candidate[:i])
		}
		if _, deprecated := keys[candidate]; deprecated {
			continue
		}
		dst = append(dst, line)
	}
	return strings.Join(dst, "\n")
}

func stripTOMLSections(src string, sections map[string]struct{}) string {
	if strings.TrimSpace(src) == "" || len(sections) == 0 {
		return src
	}
	lines := strings.Split(src, "\n")
	dst := make([]string, 0, len(lines))
	skip := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
			_, shouldSkip := sections[name]
			skip = shouldSkip
			if shouldSkip {
				continue
			}
		}
		if skip {
			continue
		}
		dst = append(dst, line)
	}
	return strings.Join(dst, "\n")
}

func EncodeConfigTOML(cfg Config) ([]byte, error) {
	return toml.Marshal(cfg)
}

func ResolveLogConfig(cfg *Config) (string, string) {
	logFile := strings.TrimSpace(cfg.Log.File)
	if logFile == "" {
		logFile = filepath.Join("logs", "bitfs.log")
		if strings.TrimSpace(cfg.Storage.DataDir) != "" {
			logFile = filepath.Join(cfg.Storage.DataDir, "logs", "bitfs.log")
		}
	}
	consoleMin := strings.TrimSpace(cfg.Log.ConsoleMinLevel)
	if consoleMin == "" {
		consoleMin = obs.LevelNone
	}
	return filepath.Clean(logFile), consoleMin
}

func validateConfig(cfg *Config) error {
	n, err := NormalizeBSVNetwork(cfg.BSV.Network)
	if err != nil {
		return err
	}
	cfg.BSV.Network = n

	cfg.Storage.WorkspaceDir = filepath.Clean(strings.TrimSpace(cfg.Storage.WorkspaceDir))
	cfg.Storage.DataDir = filepath.Clean(strings.TrimSpace(cfg.Storage.DataDir))
	if cfg.Storage.WorkspaceDir == "" || cfg.Storage.DataDir == "" {
		return errors.New("storage.workspace_dir and storage.data_dir are required")
	}
	if cfg.Storage.WorkspaceDir == cfg.Storage.DataDir {
		return errors.New("workspace_dir and data_dir must be different")
	}
	if overlaps(cfg.Storage.WorkspaceDir, cfg.Storage.DataDir) {
		return errors.New("workspace_dir and data_dir must not overlap")
	}
	// 允许零网关启动，client 会等待 HTTP API 配置
	if len(cfg.Network.Gateways) > 0 {
		if err := validateNetworkPeers(cfg.Network.Gateways, true); err != nil {
			return err
		}
	}
	// 允许零仲裁配置（仅用于测试）
	if len(cfg.Network.Arbiters) > 0 {
		if err := validateNetworkPeers(cfg.Network.Arbiters, false); err != nil {
			return err
		}
	}
	if cfg.Index.Backend != "sqlite" {
		return errors.New("index.backend must be sqlite in phase1")
	}
	cfg.Index.SQLitePath = filepath.Clean(strings.TrimSpace(cfg.Index.SQLitePath))
	if cfg.Index.SQLitePath == "" {
		return errors.New("index.sqlite_path is required")
	}
	if cfg.Seller.Pricing.ResaleDiscountBPS > 10000 {
		return errors.New("seller.pricing.resale_discount_bps must be <= 10000")
	}
	if cfg.Seller.Pricing.LiveFloorPriceSatPer64K > cfg.Seller.Pricing.LiveBasePriceSatPer64K {
		return errors.New("seller.pricing.live_floor_price_sat_per_64k must be <= seller.pricing.live_base_price_sat_per_64k")
	}
	if cfg.Seller.Pricing.LiveDecayPerMinuteBPS > 10000 {
		return errors.New("seller.pricing.live_decay_per_minute_bps must be <= 10000")
	}
	if cfg.Live.Publish.BroadcastWindow == 0 || cfg.Live.Publish.BroadcastWindow > maxLiveWindowSize {
		return fmt.Errorf("live.publish.broadcast_window must be between 1 and %d", maxLiveWindowSize)
	}
	if cfg.Live.Publish.BroadcastIntervalSec == 0 {
		return errors.New("live.publish.broadcast_interval_seconds must be > 0")
	}
	if strings.TrimSpace(cfg.FSHTTP.ListenAddr) == "" {
		return errors.New("fs_http.listen_addr is required")
	}
	if cfg.FSHTTP.MaxConcurrentSessions == 0 {
		return errors.New("fs_http.max_concurrent_sessions must be > 0")
	}
	if cfg.FSHTTP.DownloadWaitTimeoutSeconds == 0 {
		return errors.New("fs_http.download_wait_timeout_seconds must be > 0")
	}
	if strings.TrimSpace(cfg.HTTP.ListenAddr) == "" {
		return errors.New("http.listen_addr is required")
	}
	if cfg.Reachability.AnnounceTTLSeconds == 0 {
		return errors.New("reachability.announce_ttl_seconds must be > 0")
	}
	cfg.ExternalAPI.WOC.APIKey = strings.TrimSpace(cfg.ExternalAPI.WOC.APIKey)
	return nil
}

// ValidateConfig 对外提供启动前配置校验，失败即中止启动。
func ValidateConfig(cfg *Config) error {
	return validateConfig(cfg)
}

func validateNetworkPeers(items []PeerNode, requireEnabled bool) error {
	seenPub := map[string]struct{}{}
	for i, p := range items {
		if requireEnabled && strings.TrimSpace(p.Addr) == "" {
			return fmt.Errorf("network entry[%d] addr required", i)
		}
		if strings.TrimSpace(p.Addr) == "" || strings.TrimSpace(p.Pubkey) == "" {
			return fmt.Errorf("network entry[%d] addr/pubkey required", i)
		}
		pk := strings.ToLower(strings.TrimSpace(p.Pubkey))
		if _, ok := seenPub[pk]; ok {
			return fmt.Errorf("duplicate pubkey in network config: %s", pk)
		}
		seenPub[pk] = struct{}{}
		peerIDFromCfg, err := peerIDFromSecp256k1PubHex(pk)
		if err != nil {
			return err
		}
		addrInfo, err := parseAddr(p.Addr)
		if err != nil {
			return err
		}
		if addrInfo.ID != peerIDFromCfg {
			return fmt.Errorf("addr transport_peer_id mismatch for pubkey=%s", pk)
		}
	}
	return nil
}

func initDataDirs(cfg *Config) error {
	for _, d := range []string{
		cfg.Storage.WorkspaceDir,
		cfg.Storage.DataDir,
		filepath.Join(cfg.Storage.DataDir, "config"),
		filepath.Join(cfg.Storage.DataDir, "seeds"),
		filepath.Join(cfg.Storage.DataDir, "db"),
		filepath.Join(cfg.Storage.DataDir, "keys"),
		filepath.Join(cfg.Storage.DataDir, "logs"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	if err := ensureReadableDir(cfg.Storage.WorkspaceDir); err != nil {
		return err
	}
	if err := ensureWritableDir(cfg.Storage.DataDir); err != nil {
		return err
	}
	if err := ensureMinFreeSpace(cfg.Storage.DataDir, cfg.Storage.MinFreeBytes); err != nil {
		return err
	}
	return nil
}

func ensureReadableDir(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !s.IsDir() {
		return fmt.Errorf("not a directory: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, _ = f.Readdirnames(1)
	return nil
}

func ensureWritableDir(path string) error {
	f, err := os.CreateTemp(path, ".write-check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	return os.Remove(name)
}

func ensureMinFreeSpace(path string, minBytes uint64) error {
	free, err := freeBytesUnderPath(path)
	if err != nil {
		return err
	}
	if free < minBytes {
		return fmt.Errorf("insufficient free space under %s: have=%d need=%d", path, free, minBytes)
	}
	return nil
}

func applySQLitePragmas(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	// 设计说明：
	// - 运行时正式入口统一走 infra/sqliteactor.Open；
	// - 这里保留给直接 sql.Open("sqlite", ...) 的测试库做最小 WAL 初始化；
	// - 不再在这里叠加 busy_timeout 之类并发补丁，避免测试口径和正式口径分裂。
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("sqlite pragma journal_mode: %w", err)
	}
	return nil
}

// 注意：数据库初始化与迁移逻辑已统一移至 db_init.go，通过 ensureClientDBSchema 入口调用。
// 该文件不再包含 initIndexDB 及其相关迁移函数，以保持 run.go 只保留启动顺序。

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func cfgBool(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// registerDirectQuoteSubmitHandler 注册买方接收报价入口。
// 设计说明：
// - direct quote 是“卖方 -> 买方”回推路径，买方即便不是 seller 模式也必须可接收；
// - 该入口只负责落库 demand_quotes 和 demand_quote_arbiters，不涉及卖方资源读取，因此可全端默认启用。
func registerDirectQuoteSubmitHandler(h host.Host, store *clientDB, trace pproto.TraceSink) {
	pproto.HandleProto[directQuoteSubmitReq, directQuoteSubmitResp](h, ProtoQuoteDirectSubmit, clientSec(trace), func(ctx context.Context, req directQuoteSubmitReq) (directQuoteSubmitResp, error) {
		if strings.TrimSpace(req.DemandID) == "" || strings.TrimSpace(req.SellerPeerID) == "" || req.SeedPrice == 0 || req.ChunkPrice == 0 {
			return directQuoteSubmitResp{}, fmt.Errorf("invalid direct quote")
		}
		sellerPubHex, err := normalizeCompressedPubKeyHex(req.SellerPeerID)
		if err != nil {
			return directQuoteSubmitResp{}, fmt.Errorf("invalid seller pubkey")
		}
		if req.ExpiresAtUnix > 0 && req.ExpiresAtUnix < time.Now().Unix() {
			return directQuoteSubmitResp{}, fmt.Errorf("direct quote expired")
		}
		if err := dbUpsertDirectQuote(ctx, store, req, sellerPubHex); err != nil {
			return directQuoteSubmitResp{}, err
		}
		return directQuoteSubmitResp{Status: "stored"}, nil
	})
}

func registerSellerHandlers(h host.Host, store *clientDB, live *liveRuntime, trace pproto.TraceSink, cfg Config) {
	if store == nil {
		return
	}
	pproto.HandleProto[dealprod.DemandAnnounceReq, dealprod.DemandAnnounceResp](h, protocol.ID(dealprod.ProtoDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.DemandAnnounceReq) (dealprod.DemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || seedHash == "" || buyerPeerID == "" || req.ChunkCount == 0 {
			return dealprod.DemandAnnounceResp{}, fmt.Errorf("invalid demand announce")
		}
		if err := dbRecordDemand(ctx, store, demandID, seedHash); err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		seed, ok, err := dbLoadSellerSeedSnapshot(ctx, store, seedHash)
		if err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		if !ok {
			obs.Business("bitcast-client", "demand_announce_ignored_no_seed", map[string]any{
				"demand_id":   demandID,
				"seed_hash":   seedHash,
				"buyer_peer":  buyerPeerID,
				"chunk_count": req.ChunkCount,
			})
			return dealprod.DemandAnnounceResp{Status: "ignored_no_seed"}, nil
		}
		if seed.ChunkPrice == 0 {
			seed.ChunkPrice = cfg.Seller.Pricing.FloorPriceSatPer64K
		}
		if seed.SeedPrice == 0 {
			seed.SeedPrice = seed.ChunkPrice * uint64(seed.ChunkCount)
		}
		if liveMeta, ok := live.segment(seedHash); ok {
			seed = ComputeLiveQuotePrices(seed, liveMeta, LiveSellerPricing{
				BasePriceSatPer64K:  cfg.Seller.Pricing.LiveBasePriceSatPer64K,
				FloorPriceSatPer64K: cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
				DecayPerMinuteBPS:   cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
			}, time.Now())
		}
		availableChunks, err := dbListSeedChunkSupply(ctx, store, seedHash)
		if err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		if len(availableChunks) == 0 {
			obs.Business("bitcast-client", "demand_announce_ignored_no_chunks", map[string]any{
				"demand_id":  demandID,
				"seed_hash":  seedHash,
				"buyer_peer": buyerPeerID,
			})
			return dealprod.DemandAnnounceResp{Status: "ignored_no_chunks"}, nil
		}
		if err := submitDirectQuote(ctx, h, trace, DirectQuoteParams{
			DemandID:            demandID,
			BuyerPubHex:         buyerPeerID,
			BuyerAddrs:          req.BuyerAddrs,
			SeedPrice:           seed.SeedPrice,
			ChunkPrice:          seed.ChunkPrice,
			ChunkCount:          seed.ChunkCount,
			FileSizeBytes:       seed.FileSize,
			ExpiresAtUnix:       time.Now().Add(10 * time.Minute).Unix(),
			RecommendedFileName: seed.RecommendedFileName,
			MimeType:            seed.MIMEHint,
			ArbiterPubHexes:     configuredArbiterPubHexes(cfg),
			AvailableChunkBitmapHex: chunkBitmapHexFromIndexes(
				availableChunks,
				seed.ChunkCount,
			),
		}); err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		obs.Business("bitcast-client", "demand_announce_quote_submitted", map[string]any{
			"demand_id":   demandID,
			"seed_hash":   seedHash,
			"buyer_peer":  buyerPeerID,
			"seed_price":  seed.SeedPrice,
			"chunk_price": seed.ChunkPrice,
			"chunk_have":  len(availableChunks),
		})
		return dealprod.DemandAnnounceResp{Status: "quoted"}, nil
	})
	pproto.HandleProto[dealprod.LiveDemandAnnounceReq, dealprod.LiveDemandAnnounceResp](h, protocol.ID(dealprod.ProtoLiveDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.LiveDemandAnnounceReq) (dealprod.LiveDemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || !isSeedHashHex(streamID) || buyerPeerID == "" || req.Window == 0 {
			return dealprod.LiveDemandAnnounceResp{}, fmt.Errorf("invalid live demand announce")
		}
		recentSegments, latestIndex, err := listLocalLiveQuoteSegments(store, streamID, int(req.Window))
		if err != nil {
			return dealprod.LiveDemandAnnounceResp{}, err
		}
		if len(recentSegments) == 0 {
			obs.Business("bitcast-client", "live_demand_announce_ignored_no_stream", map[string]any{
				"demand_id":  demandID,
				"stream_id":  streamID,
				"buyer_peer": buyerPeerID,
			})
			return dealprod.LiveDemandAnnounceResp{Status: "ignored_no_stream"}, nil
		}
		if err := submitLiveQuote(ctx, h, trace, LiveQuoteParams{
			DemandID:           demandID,
			BuyerPeerID:        buyerPeerID,
			BuyerAddrs:         req.BuyerAddrs,
			StreamID:           streamID,
			LatestSegmentIndex: latestIndex,
			RecentSegments:     recentSegments,
			ExpiresAtUnix:      time.Now().Add(2 * time.Minute).Unix(),
		}); err != nil {
			return dealprod.LiveDemandAnnounceResp{}, err
		}
		obs.Business("bitcast-client", "live_demand_announce_quote_submitted", map[string]any{
			"demand_id":            demandID,
			"stream_id":            streamID,
			"buyer_peer":           buyerPeerID,
			"latest_segment_index": latestIndex,
			"segment_count":        len(recentSegments),
		})
		return dealprod.LiveDemandAnnounceResp{Status: "quoted"}, nil
	})

	pproto.HandleProto[seedGetReq, seedGetResp](h, ProtoSeedGet, clientSec(trace), func(ctx context.Context, req seedGetReq) (seedGetResp, error) {
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		if strings.TrimSpace(req.SessionID) == "" || seedHash == "" {
			return seedGetResp{}, fmt.Errorf("invalid params")
		}
		row, err := dbLoadDirectTransferPoolRow(ctx, store, strings.TrimSpace(req.SessionID))
		if err != nil {
			return seedGetResp{}, fmt.Errorf("session not found")
		}
		dealSeedHash, err := dbLoadDirectDealSeedHash(ctx, store, strings.TrimSpace(row.DealID))
		if err != nil {
			return seedGetResp{}, fmt.Errorf("deal not found")
		}
		if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
			return seedGetResp{}, fmt.Errorf("seed_hash mismatch")
		}
		seedBytes, err := dbLoadSeedBytesBySeedHash(ctx, store, seedHash)
		if err != nil {
			return seedGetResp{}, err
		}
		return seedGetResp{Seed: append([]byte(nil), seedBytes...)}, nil
	})
	pproto.HandleProto[directDealAcceptReq, directDealAcceptResp](h, ProtoDirectDealAccept, clientSec(trace), func(ctx context.Context, req directDealAcceptReq) (directDealAcceptResp, error) {
		if strings.TrimSpace(req.DemandID) == "" || strings.TrimSpace(req.BuyerPeerID) == "" || strings.TrimSpace(req.SeedHash) == "" || req.SeedPrice == 0 || req.ChunkPrice == 0 {
			return directDealAcceptResp{}, fmt.Errorf("invalid direct deal accept")
		}
		buyerPubHex, err := normalizeCompressedPubKeyHex(req.BuyerPeerID)
		if err != nil {
			return directDealAcceptResp{}, fmt.Errorf("invalid buyer pubkey")
		}
		sellerPubHex, err := normalizeCompressedPubKeyHex(localPubHex(h))
		if err != nil {
			return directDealAcceptResp{}, fmt.Errorf("invalid seller pubkey")
		}
		if req.ExpiresAtUnix > 0 && req.ExpiresAtUnix < time.Now().Unix() {
			return directDealAcceptResp{}, fmt.Errorf("direct quote expired")
		}
		dealID := "ddeal_" + randHex(8)
		if err := dbInsertDirectDeal(ctx, store, dealID, req, buyerPubHex, sellerPubHex); err != nil {
			return directDealAcceptResp{}, err
		}
		return directDealAcceptResp{
			DealID:       dealID,
			SellerPeerID: sellerPubHex,
			ChunkPrice:   req.ChunkPrice,
			Status:       "accepted",
		}, nil
	})
	pproto.HandleProto[directTransferPoolOpenReq, directTransferPoolOpenResp](h, ProtoTransferPoolOpen, clientSec(trace), func(_ context.Context, req directTransferPoolOpenReq) (directTransferPoolOpenResp, error) {
		return handleDirectTransferPoolOpen(h, store, cfg, req)
	})
	pproto.HandleProto[directTransferPoolPayReq, directTransferPoolPayResp](h, ProtoTransferPoolPay, clientSec(trace), func(_ context.Context, req directTransferPoolPayReq) (directTransferPoolPayResp, error) {
		return handleDirectTransferPoolPay(h, store, cfg, req)
	})
	pproto.HandleProto[directTransferPoolCloseReq, directTransferPoolCloseResp](h, ProtoTransferPoolClose, clientSec(trace), func(_ context.Context, req directTransferPoolCloseReq) (directTransferPoolCloseResp, error) {
		return handleDirectTransferPoolClose(h, store, cfg, req)
	})
}

func submitDirectQuote(ctx context.Context, h host.Host, trace pproto.TraceSink, p DirectQuoteParams) error {
	if h == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(p.DemandID) == "" || strings.TrimSpace(p.BuyerPubHex) == "" || p.SeedPrice == 0 || p.ChunkPrice == 0 {
		return fmt.Errorf("invalid params")
	}
	if p.ExpiresAtUnix == 0 {
		p.ExpiresAtUnix = time.Now().Add(10 * time.Minute).Unix()
	}
	buyerID, err := peerIDFromClientID(strings.TrimSpace(p.BuyerPubHex))
	if err != nil {
		return err
	}
	for _, raw := range p.BuyerAddrs {
		ai, err := parseAddr(strings.TrimSpace(raw))
		if err != nil || ai == nil {
			continue
		}
		h.Peerstore().AddAddrs(ai.ID, ai.Addrs, 10*time.Minute)
	}
	if err := h.Connect(ctx, peer.AddrInfo{ID: buyerID}); err != nil {
		return err
	}
	sellerClientID, err := localPubKeyHex(h)
	if err != nil {
		return err
	}
	var resp directQuoteSubmitResp
	bitmapHex, err := normalizeChunkBitmapHex(p.AvailableChunkBitmapHex)
	if err != nil {
		return err
	}
	var bitmapBytes []byte
	if bitmapHex != "" {
		bitmapBytes, err = hex.DecodeString(bitmapHex)
		if err != nil {
			return fmt.Errorf("invalid available_chunk_bitmap_hex")
		}
	}
	arbiterPubHexes, err := normalizePubHexList(p.ArbiterPubHexes)
	if err != nil {
		return err
	}
	if err := pproto.CallProto(ctx, h, buyerID, ProtoQuoteDirectSubmit, clientSec(trace), directQuoteSubmitReq{
		DemandID:             strings.TrimSpace(p.DemandID),
		SellerPeerID:         strings.ToLower(strings.TrimSpace(sellerClientID)),
		SeedPrice:            p.SeedPrice,
		ChunkPrice:           p.ChunkPrice,
		ChunkCount:           p.ChunkCount,
		FileSize:             p.FileSizeBytes,
		ExpiresAtUnix:        p.ExpiresAtUnix,
		RecommendedFileName:  sanitizeRecommendedFileName(p.RecommendedFileName),
		MIMEHint:             sanitizeMIMEHint(p.MimeType),
		ArbiterPeerIDs:       arbiterPubHexes,
		AvailableChunkBitmap: bitmapBytes,
	}, &resp); err != nil {
		return err
	}
	if strings.TrimSpace(resp.Status) != "stored" {
		return fmt.Errorf("direct quote not stored")
	}
	return nil
}

func submitLiveQuote(ctx context.Context, h host.Host, trace pproto.TraceSink, p LiveQuoteParams) error {
	if h == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(p.DemandID) == "" || strings.TrimSpace(p.BuyerPeerID) == "" || !isSeedHashHex(strings.ToLower(strings.TrimSpace(p.StreamID))) || len(p.RecentSegments) == 0 {
		return fmt.Errorf("invalid params")
	}
	if p.ExpiresAtUnix == 0 {
		p.ExpiresAtUnix = time.Now().Add(2 * time.Minute).Unix()
	}
	buyerID, err := peerIDFromClientID(strings.TrimSpace(p.BuyerPeerID))
	if err != nil {
		return err
	}
	for _, raw := range p.BuyerAddrs {
		ai, err := parseAddr(strings.TrimSpace(raw))
		if err != nil || ai == nil {
			continue
		}
		h.Peerstore().AddAddrs(ai.ID, ai.Addrs, 10*time.Minute)
	}
	if err := h.Connect(ctx, peer.AddrInfo{ID: buyerID}); err != nil {
		return err
	}
	sellerClientID, err := localPubKeyHex(h)
	if err != nil {
		return err
	}
	recent := make([]*liveQuoteSegmentPB, 0, len(p.RecentSegments))
	for _, seg := range p.RecentSegments {
		seedHash := strings.ToLower(strings.TrimSpace(seg.SeedHash))
		if !isSeedHashHex(seedHash) {
			continue
		}
		recent = append(recent, &liveQuoteSegmentPB{SegmentIndex: seg.SegmentIndex, SeedHash: seedHash})
	}
	if len(recent) == 0 {
		return fmt.Errorf("empty recent segments")
	}
	var resp liveQuoteSubmitResp
	if err := pproto.CallProto(ctx, h, buyerID, ProtoLiveQuoteSubmit, clientSec(trace), liveQuoteSubmitReq{
		DemandID:           strings.TrimSpace(p.DemandID),
		SellerPeerID:       strings.ToLower(strings.TrimSpace(sellerClientID)),
		StreamID:           strings.ToLower(strings.TrimSpace(p.StreamID)),
		LatestSegmentIndex: p.LatestSegmentIndex,
		RecentSegments:     recent,
		ExpiresAtUnix:      p.ExpiresAtUnix,
	}, &resp); err != nil {
		return err
	}
	if strings.TrimSpace(resp.Status) != "stored" {
		return fmt.Errorf("live quote not stored")
	}
	return nil
}

func sanitizeRecommendedFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

func sanitizeMIMEHint(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return ""
	}
	mediaType = strings.TrimSpace(strings.ToLower(mediaType))
	if mediaType == "" || !strings.Contains(mediaType, "/") {
		return ""
	}
	return mediaType
}

func recommendedFileNameBySeedHash(store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	return dbRecommendedFileNameBySeedHash(context.Background(), store, seedHash)
}

func mimeHintBySeedHash(store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	return dbMimeHintBySeedHash(context.Background(), store, seedHash)
}

func configuredArbiterPubHexes(cfg Config) []string {
	out := make([]string, 0, len(cfg.Network.Arbiters))
	for _, a := range cfg.Network.Arbiters {
		if !a.Enabled {
			continue
		}
		pubHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(a.Pubkey))
		if err != nil {
			continue
		}
		out = append(out, pubHex)
	}
	normalized, err := normalizePubHexList(out)
	if err != nil {
		return nil
	}
	return normalized
}

func normalizePeerIDList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, err := peer.Decode(s); err != nil {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func localPubHex(h host.Host) string {
	if h == nil {
		return ""
	}
	pub := h.Peerstore().PubKey(h.ID())
	if pub == nil {
		return ""
	}
	raw, err := pub.Raw()
	if err != nil {
		return ""
	}
	out, err := normalizeCompressedPubKeyHex(hex.EncodeToString(raw))
	if err != nil {
		return ""
	}
	return out
}

func connectGateways(ctx context.Context, h host.Host, gateways []PeerNode) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0)
	for _, g := range gateways {
		if !g.Enabled {
			continue
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			obs.Error("bitcast-client", "gateway_addr_invalid", map[string]any{"addr": g.Addr, "error": err.Error()})
			continue
		}
		if err := h.Connect(ctx, *ai); err != nil {
			obs.Error("bitcast-client", "gateway_connect_failed", map[string]any{"transport_peer_id": ai.ID.String(), "error": err.Error()})
			continue
		}
		obs.Business("bitcast-client", "gateway_connected", map[string]any{"transport_peer_id": ai.ID.String(), "addr_count": len(ai.Addrs)})
		out = append(out, *ai)
	}
	// 允许零网关，返回空列表
	return out, nil
}

func connectArbiters(ctx context.Context, h host.Host, arbiters []PeerNode) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(arbiters))
	for i, a := range arbiters {
		if strings.TrimSpace(a.Addr) == "" {
			continue
		}
		ai, err := parseAddr(a.Addr)
		if err != nil {
			obs.Error("bitcast-client", "arbiter_addr_invalid", map[string]any{
				"index": i,
				"addr":  a.Addr,
				"error": err.Error(),
			})
			continue
		}
		if err := h.Connect(ctx, *ai); err != nil {
			obs.Error("bitcast-client", "arbiter_connect_failed", map[string]any{
				"index":             i,
				"transport_peer_id": ai.ID.String(),
				"error":             err.Error(),
			})
			continue
		}
		obs.Business("bitcast-client", "arbiter_connected", map[string]any{"transport_peer_id": ai.ID.String(), "addr_count": len(ai.Addrs)})
		out = append(out, *ai)
	}
	return out, nil
}

func checkPeerHealth(ctx context.Context, h host.Host, peers []peer.AddrInfo, protoID protocol.ID, sec pproto.SecurityConfig, kind string) []peer.AddrInfo {
	const maxAttempts = 3
	out := make([]peer.AddrInfo, 0, len(peers))
	for _, p := range peers {
		var lastErr error
		ok := false
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			var health healthResp
			err := pproto.CallProto(ctx, h, p.ID, protoID, sec, healthReq{}, &health)
			if err == nil {
				obs.Business("bitcast-client", kind+"_health_ok", map[string]any{
					"transport_peer_id": p.ID.String(),
					"status":            health.Status,
					"attempt":           attempt,
				})
				ok = true
				break
			}
			lastErr = err
			obs.Error("bitcast-client", kind+"_health_failed", map[string]any{
				"transport_peer_id": p.ID.String(),
				"attempt":           attempt,
				"error":             err.Error(),
			})
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if ok {
			out = append(out, p)
			continue
		}
		obs.Error("bitcast-client", kind+"_unhealthy", map[string]any{
			"transport_peer_id": p.ID.String(),
			"error":             errString(lastErr),
		})
	}
	return out
}

func buildSeedV1(path string) ([]byte, string, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, "", 0, err
	}
	fileSize := st.Size()
	if fileSize < 0 {
		return nil, "", 0, fmt.Errorf("negative file size")
	}
	chunkCount := uint32(ceilDiv(uint64(fileSize), seedBlockSize))

	buf := &bytes.Buffer{}
	buf.WriteString("BSE1")
	buf.WriteByte(0x01)
	buf.WriteByte(0x01)
	_ = binary.Write(buf, binary.BigEndian, uint32(seedBlockSize))
	_ = binary.Write(buf, binary.BigEndian, uint64(fileSize))
	_ = binary.Write(buf, binary.BigEndian, chunkCount)

	chunk := make([]byte, seedBlockSize)
	for i := uint32(0); i < chunkCount; i++ {
		n, err := io.ReadFull(f, chunk)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, "", 0, err
			}
		}
		if n < seedBlockSize {
			for j := n; j < seedBlockSize; j++ {
				chunk[j] = 0
			}
		}
		h := sha256.Sum256(chunk)
		buf.Write(h[:])
	}

	seedBytes := buf.Bytes()
	h := sha256.Sum256(seedBytes)
	return seedBytes, hex.EncodeToString(h[:]), chunkCount, nil
}

func writeIfChanged(path string, data []byte) error {
	old, err := os.ReadFile(path)
	if err == nil && bytes.Equal(old, data) {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func ceilDiv(v uint64, d uint64) uint64 {
	if v == 0 {
		return 0
	}
	return (v + d - 1) / d
}

func overlaps(a, b string) bool {
	aAbs, _ := filepath.Abs(a)
	bAbs, _ := filepath.Abs(b)
	return isParentOrSame(aAbs, bAbs) || isParentOrSame(bAbs, aAbs)
}

func isParentOrSame(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != "" && !strings.HasPrefix(rel, "..") && rel != ".."
}

func peerIDFromSecp256k1PubHex(pubHex string) (peer.ID, error) {
	b, err := hex.DecodeString(strings.TrimSpace(pubHex))
	if err != nil {
		return "", err
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(b)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(pub)
}

func normalizeChunkIndexes(in []uint32, maxExclusive uint32) []uint32 {
	if len(in) == 0 {
		return nil
	}
	tmp := make([]uint32, 0, len(in))
	seen := make(map[uint32]struct{}, len(in))
	for _, idx := range in {
		if maxExclusive > 0 && idx >= maxExclusive {
			continue
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		tmp = append(tmp, idx)
	}
	if len(tmp) == 0 {
		return nil
	}
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	return tmp
}

func normalizeChunkBitmapHex(bitmapHex string) (string, error) {
	bitmapHex = strings.ToLower(strings.TrimSpace(bitmapHex))
	if bitmapHex == "" {
		return "", nil
	}
	raw, err := hex.DecodeString(bitmapHex)
	if err != nil {
		return "", fmt.Errorf("invalid available_chunk_bitmap_hex")
	}
	return hex.EncodeToString(raw), nil
}

func normalizeChunkBitmapBytes(bitmap []byte) string {
	if len(bitmap) == 0 {
		return ""
	}
	return strings.ToLower(hex.EncodeToString(bitmap))
}

func chunkBitmapHexFromIndexes(indexes []uint32, chunkCount uint32) string {
	indexes = normalizeChunkIndexes(indexes, chunkCount)
	if len(indexes) == 0 {
		return ""
	}
	if chunkCount == 0 {
		chunkCount = indexes[len(indexes)-1] + 1
	}
	byteLen := int((chunkCount + 7) / 8)
	bits := make([]byte, byteLen)
	for _, idx := range indexes {
		if idx >= chunkCount {
			continue
		}
		byteIdx := idx / 8
		// 采用 BT 风格位序：块 0 对应字节最高位(bit7)。
		bit := 7 - (idx % 8)
		bits[byteIdx] |= byte(1 << bit)
	}
	return hex.EncodeToString(bits)
}

func chunkIndexesFromBitmapHex(bitmapHex string, maxExclusive uint32) ([]uint32, error) {
	bitmapHex, err := normalizeChunkBitmapHex(bitmapHex)
	if err != nil {
		return nil, err
	}
	if bitmapHex == "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(bitmapHex)
	if err != nil {
		return nil, err
	}
	out := make([]uint32, 0, len(raw)*4)
	for bi, b := range raw {
		if b == 0 {
			continue
		}
		for bit := uint32(0); bit < 8; bit++ {
			mask := byte(1 << (7 - bit))
			if b&mask == 0 {
				continue
			}
			idx := uint32(bi)*8 + bit
			if maxExclusive > 0 && idx >= maxExclusive {
				continue
			}
			out = append(out, idx)
		}
	}
	return normalizeChunkIndexes(out, maxExclusive), nil
}

func contiguousChunkIndexes(chunkCount uint32) []uint32 {
	if chunkCount == 0 {
		return nil
	}
	out := make([]uint32, 0, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		out = append(out, i)
	}
	return out
}

func (c *sellerCatalog) Replace(seeds map[string]sellerSeed) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seeds = seeds
}

func (c *sellerCatalog) Upsert(seed sellerSeed) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seeds == nil {
		c.seeds = map[string]sellerSeed{}
	}
	key := strings.ToLower(strings.TrimSpace(seed.SeedHash))
	if key == "" {
		return
	}
	c.seeds[key] = seed
}

func (c *sellerCatalog) Get(seedHash string) (sellerSeed, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.seeds[seedHash]
	return s, ok
}

func gwSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "bitcast-gateway", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func arbSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "arbiter-mr", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func clientSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "bitcast-client", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func nodeSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "bitfs-node", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func nodeSecForRuntime(rt *Runtime) pproto.SecurityConfig {
	network := "test"
	trace := pproto.TraceSink(nil)
	if rt != nil {
		trace = rt.rpcTrace
		if strings.TrimSpace(rt.runIn.BSV.Network) != "" {
			network = strings.ToLower(strings.TrimSpace(rt.runIn.BSV.Network))
		}
	}
	return pproto.SecurityConfig{Domain: "bitfs-node", Network: network, TTL: 30 * time.Second, Trace: trace}
}

func parseAddr(full string) (*peer.AddrInfo, error) {
	ma, err := multiaddr.NewMultiaddr(full)
	if err != nil {
		return nil, err
	}
	return peer.AddrInfoFromP2pAddr(ma)
}

func resolvePrivKeyHex(cfg Config, cliPrivHex string) (string, error) {
	if s := strings.TrimSpace(cliPrivHex); s != "" {
		return normalizeRawSecp256k1PrivKeyHex(s)
	}
	if s := strings.TrimSpace(cfg.Keys.PrivkeyHex); s != "" {
		return normalizeRawSecp256k1PrivKeyHex(s)
	}
	return "", nil
}

// ResolveEffectivePrivKeyHex 在启动前统一解析“唯一运行时私钥”。
// 调用方应在进入 Run 之前完成该解析，再通过 RunInput.EffectivePrivKeyHex 传入。
func ResolveEffectivePrivKeyHex(cfg Config, overridePrivHex string) (string, error) {
	return resolvePrivKeyHex(cfg, overridePrivHex)
}

func normalizeRawSecp256k1PrivKeyHex(s string) (string, error) {
	hexKey := strings.ToLower(strings.TrimSpace(s))
	if len(hexKey) != 64 {
		return "", fmt.Errorf("invalid private key format: expect 32-byte secp256k1 hex (len=64)")
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return "", fmt.Errorf("invalid private key hex: %w", err)
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(b)
	if err != nil {
		return "", fmt.Errorf("invalid secp256k1 private key: %w", err)
	}
	raw, err := priv.Raw()
	if err != nil {
		return "", fmt.Errorf("read private key raw bytes: %w", err)
	}
	if len(raw) != 32 {
		return "", fmt.Errorf("invalid secp256k1 private key length: got=%d want=32", len(raw))
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func buildClientActorFromConfig(cfg Config) (*poolcore.Actor, error) {
	privHex, err := normalizeRawSecp256k1PrivKeyHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		return nil, err
	}
	if cid := strings.TrimSpace(cfg.ClientID); cid != "" {
		derivedID, err := clientIDFromPrivHex(privHex)
		if err != nil {
			return nil, fmt.Errorf("derive client_pubkey_hex from signing key failed: %w", err)
		}
		if !strings.EqualFold(cid, derivedID) {
			return nil, fmt.Errorf("client_pubkey_hex and signing key mismatch")
		}
	}
	isMainnet := strings.EqualFold(strings.TrimSpace(cfg.BSV.Network), "main")
	return poolcore.BuildActor("client", privHex, isMainnet)
}

func buildClientActorFromRunInput(in RunInput) (*poolcore.Actor, error) {
	privHex, err := normalizeRawSecp256k1PrivKeyHex(in.EffectivePrivKeyHex)
	if err != nil {
		return nil, err
	}
	if cid := strings.TrimSpace(in.ClientID); cid != "" {
		derivedID, err := clientIDFromPrivHex(privHex)
		if err != nil {
			return nil, fmt.Errorf("derive client_pubkey_hex from signing key failed: %w", err)
		}
		if !strings.EqualFold(cid, derivedID) {
			return nil, fmt.Errorf("client_pubkey_hex and signing key mismatch")
		}
	}
	isMainnet := strings.EqualFold(strings.TrimSpace(in.BSV.Network), "main")
	return poolcore.BuildActor("client", privHex, isMainnet)
}

func validateClientIdentityConsistency(cfg Config) error {
	privHex := strings.TrimSpace(cfg.Keys.PrivkeyHex)
	clientID := strings.TrimSpace(cfg.ClientID)
	if privHex == "" || clientID == "" {
		return nil
	}
	derivedID, err := clientIDFromPrivHex(privHex)
	if err != nil {
		return fmt.Errorf("derive client_pubkey_hex from signing key failed: %w", err)
	}
	if !strings.EqualFold(clientID, derivedID) {
		return fmt.Errorf("client_pubkey_hex and signing key mismatch")
	}
	return nil
}

func clientIDFromPrivHex(privHex string) (string, error) {
	priv, err := parsePrivHex(privHex)
	if err != nil {
		return "", err
	}
	pubRaw, err := priv.GetPublic().Raw()
	if err != nil {
		return "", fmt.Errorf("read public key raw bytes: %w", err)
	}
	return normalizeCompressedPubKeyHex(hex.EncodeToString(pubRaw))
}

func parsePrivHex(s string) (crypto.PrivKey, error) {
	hexKey, err := normalizeRawSecp256k1PrivKeyHex(s)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalSecp256k1PrivateKey(b)
}

func localPubKeyHex(h host.Host) (string, error) {
	pub := h.Peerstore().PubKey(h.ID())
	if pub == nil {
		return "", fmt.Errorf("missing host public key")
	}
	raw, err := pub.Raw()
	if err != nil {
		return "", fmt.Errorf("read host public key raw bytes: %w", err)
	}
	return normalizeCompressedPubKeyHex(hex.EncodeToString(raw))
}

// must 已移除：库代码不应 panic。
