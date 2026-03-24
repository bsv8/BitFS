package clientapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/pelletier/go-toml/v2"
	_ "modernc.org/sqlite"
)

const (
	BBroadcastSuiteVersion             = "BBroadcast/1.0"
	BBroadcastProtocolName             = "Bitcast Broadcast Protocol"
	ProtoHealth            protocol.ID = "/bsv-transfer/healthz/1.0.0"

	ProtoArbHealth          protocol.ID = "/bsv-transfer/arbiter/healthz/1.0.0"
	ProtoSeedGet            protocol.ID = "/bsv-transfer/client/seed/get/1.0.0"
	ProtoQuoteDirectSubmit  protocol.ID = "/bsv-transfer/client/quote/direct_submit/1.0.0"
	ProtoLiveQuoteSubmit    protocol.ID = "/bsv-transfer/client/live_quote/submit/1.0.0"
	ProtoDirectDealAccept   protocol.ID = "/bsv-transfer/client/deal/accept/1.0.0"
	ProtoDirectSessionOpen  protocol.ID = "/bsv-transfer/client/session/open/1.0.0"
	ProtoDirectSessionClose protocol.ID = "/bsv-transfer/client/session/close/1.0.0"
	ProtoTransferPoolOpen   protocol.ID = "/bsv-transfer/client/transfer-pool/open/1.0.0"
	ProtoTransferPoolPay    protocol.ID = "/bsv-transfer/client/transfer-pool/pay/1.0.0"
	ProtoTransferPoolClose  protocol.ID = "/bsv-transfer/client/transfer-pool/close/1.0.0"
	ProtoLiveSubscribe      protocol.ID = "/bsv-transfer/live/subscribe/1.0.0"
	ProtoLiveHeadPush       protocol.ID = "/bsv-transfer/live/head-push/1.0.0"

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
type directSessionOpenReq struct {
	DealID string `protobuf:"bytes,1,opt,name=deal_id,json=dealId,proto3" json:"deal_id"`
}
type directSessionOpenResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
}
type directSessionCloseReq struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
}
type directSessionCloseResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
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
	// Debug 打开后，client 会把 p2prpc 收发原文落盘到本地 raw 抓包目录。
	// 注意：这里只抓 p2prpc 控制层原文，不抓更底层 libp2p/TCP 线包。
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
		TickSeconds           uint32 `yaml:"tick_seconds" toml:"tick_seconds"`
	} `yaml:"listen" toml:"listen"`
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
	WOCAPIKey string `yaml:"woc_api_key" toml:"woc_api_key"`
	Log       struct {
		File            string `yaml:"file" toml:"file"`
		ConsoleMinLevel string `yaml:"console_min_level" toml:"console_min_level"`
	} `yaml:"log" toml:"log"`
}

type PeerNode struct {
	Enabled bool   `yaml:"enabled" toml:"enabled"`
	Addr    string `yaml:"addr" toml:"addr"`
	Pubkey  string `yaml:"pubkey" toml:"pubkey"`
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
		TickSeconds           uint32
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
	WOCAPIKey string
	Log       struct {
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
	ActionChain dual2of2.ChainClient

	// WalletChain 只服务钱包同步与历史扫描。
	// 设计约束：
	// - 钱包同步明确接受 whatsonchain 语义，不再复制一层同构接口；
	// - 运行时只持有已装配好的最小 WOC 客户端，不再依赖历史中间层。
	WalletChain walletChainClient

	// RPCTrace 仅用于集成测试：记录 client 自己的 p2prpc 收发报文（JSONL）。
	// 正常运行默认不启用（nil）。
	RPCTrace p2prpc.TraceSink

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
	in.Listen.TickSeconds = cfg.Listen.TickSeconds
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
	in.WOCAPIKey = cfg.WOCAPIKey
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
	cfg.Listen.TickSeconds = in.Listen.TickSeconds
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
	cfg.WOCAPIKey = in.WOCAPIKey
	cfg.Log.File = in.Log.File
	cfg.Log.ConsoleMinLevel = in.Log.ConsoleMinLevel
	cfg.Keys.PrivkeyHex = strings.TrimSpace(in.EffectivePrivKeyHex)
	return cfg
}

type Runtime struct {
	Host            host.Host
	DB              *sql.DB
	runIn           RunInput
	StartedAtUnix   int64
	HealthyGWs      []peer.AddrInfo
	HealthyArbiters []peer.AddrInfo
	Workspace       *workspaceManager
	Catalog         *sellerCatalog
	HTTP            *httpAPIServer
	FSHTTP          *fileHTTPServer

	ActionChain dual2of2.ChainClient
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

	rpcTrace p2prpc.TraceSink
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
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := applySQLitePragmas(db); err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := initIndexDB(db); err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	catalog := &sellerCatalog{seeds: map[string]sellerSeed{}}
	workspaceMgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		catalog: catalog,
	}
	if err := workspaceMgr.EnsureDefaultWorkspace(); err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if err := workspaceMgr.ValidateLiveCacheCapacity(cfg.Live.CacheMaxBytes); err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if cfg.Scan.StartupFullScan {
		if _, err := workspaceMgr.SyncOnce(ctx); err != nil {
			_ = db.Close()
			if removeObs != nil {
				removeObs()
			}
			return nil, err
		}
	}
	if err := workspaceMgr.EnforceLiveCacheLimit(cfg.Live.CacheMaxBytes); err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if in.PostWorkspaceBootstrap != nil {
		if err := in.PostWorkspaceBootstrap(db); err != nil {
			_ = db.Close()
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
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	if strings.TrimSpace(effectivePrivHex) == "" {
		_ = db.Close()
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
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	opts = append(opts, libp2p.Identity(priv))
	h, err := libp2p.New(opts...)
	if err != nil {
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}
	clientPubHex, err := localPubKeyHex(h)
	if err != nil {
		_ = h.Close()
		_ = db.Close()
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
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	activeGWs, err := connectGateways(ctx, h, cfg.Network.Gateways)
	if err != nil {
		_ = h.Close()
		_ = db.Close()
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
		_ = db.Close()
		if removeObs != nil {
			removeObs()
		}
		return nil, err
	}

	logFile, logConsoleMinLevel := ResolveLogConfig(&cfg)
	trace := in.RPCTrace
	var closeTrace func() error
	if trace == nil && cfg.Debug {
		localTrace, err := p2prpc.NewLocalRawTraceSink(logFile)
		if err != nil {
			_ = h.Close()
			_ = db.Close()
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
		Host:                     h,
		DB:                       db,
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
	rt.taskSched = newTaskScheduler(db, "bitcast-client")
	rt.kernel = newClientKernel(rt)
	rt.orch = newOrchestrator(rt)
	registerLiveHandlers(rt)
	registerDirectQuoteSubmitHandler(h, db, trace)
	if cfg.Seller.Enabled {
		registerSellerHandlers(h, db, rt.live, trace, cfg)
	}
	if rt.ActionChain == nil {
		actionChain, err := chainbridge.NewDefaultFeePoolChain(chainbridge.RouteConfig{
			Provider: chainbridge.WhatsOnChainProvider,
			Network:  in.BSV.Network,
		})
		if err != nil {
			_ = db.Close()
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
			_ = db.Close()
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
	_ = rt.gwManager.InitFromConfig(ctx, cfg.Network.Gateways)
	// 更新 HealthyGWs 为已连接的网关
	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()

	var wg sync.WaitGroup
	if rt.orch != nil {
		rt.orch.Start(ctx)
	}
	// 链维护进程：统一串行调度链 API 查询，业务侧只读本地快照。
	startChainMaintainer(ctx, rt)
	// listen 费用池自动 loop（按周期扣费/续费，网关联通后自动触发）。
	startListenLoops(ctx, rt)
	if cfg.HTTP.Enabled && !in.DisableHTTPServer {
		rt.HTTP = newHTTPAPIServer(rt, &cfg, db, h, healthyGWs, workspaceMgr, trace)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rt.HTTP.Start(); err != nil {
				obs.Error("bitcast-client", "http_api_stopped", map[string]any{"error": err.Error()})
			}
		}()
	}
	if cfg.FSHTTP.Enabled {
		rt.FSHTTP = newFileHTTPServer(rt, &cfg, db, workspaceMgr)
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

	go restorePersistedLiveFollows(ctx, rt)

	rt.closeFn = func() error {
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
		if err := db.Close(); err != nil && firstErr == nil {
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
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("sqlite pragma journal_mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		return fmt.Errorf("sqlite pragma busy_timeout: %w", err)
	}
	return nil
}

func initIndexDB(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS workspace_files(path TEXT PRIMARY KEY, file_size INTEGER, mtime_unix INTEGER, seed_hash TEXT NOT NULL, seed_locked INTEGER NOT NULL DEFAULT 0, updated_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS workspaces(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			max_bytes INTEGER NOT NULL,
			enabled INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS seeds(seed_hash TEXT PRIMARY KEY, seed_file_path TEXT NOT NULL, chunk_count INTEGER, file_size INTEGER, recommended_file_name TEXT NOT NULL DEFAULT '', mime_hint TEXT NOT NULL DEFAULT '', created_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS seed_available_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`,
		`CREATE TABLE IF NOT EXISTS seed_price_state(seed_hash TEXT PRIMARY KEY, last_buy_unit_price_sat_per_64k INTEGER, floor_unit_price_sat_per_64k INTEGER, resale_discount_bps INTEGER, unit_price_sat_per_64k INTEGER, updated_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS demand_dedup(demand_id TEXT PRIMARY KEY, seed_hash TEXT, created_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS tx_history(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			event_type TEXT NOT NULL,
			direction TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			purpose TEXT NOT NULL,
			note TEXT NOT NULL,
			pool_id TEXT NOT NULL,
			msg_id TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			cycle_index INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sale_records(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			unit_price_sat_per_64k INTEGER NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			buyer_gateway_pubkey_hex TEXT NOT NULL,
			release_token TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS gateway_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			action TEXT NOT NULL,
			msg_id TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			pool_id TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS direct_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0,
			file_size INTEGER NOT NULL DEFAULT 0,
			expires_at_unix INTEGER NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT '',
			available_chunk_bitmap_hex TEXT NOT NULL DEFAULT '',
			seller_arbiter_pubkey_hexes_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id, seller_pubkey_hex)
		)`,
		`CREATE TABLE IF NOT EXISTS direct_deals(
			deal_id TEXT PRIMARY KEY,
			demand_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			arbiter_pubkey_hex TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS live_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			latest_segment_index INTEGER NOT NULL,
			recent_segments_json TEXT NOT NULL,
			expires_at_unix INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id, seller_pubkey_hex)
		)`,
		`CREATE TABLE IF NOT EXISTS direct_sessions(
			session_id TEXT PRIMARY KEY,
			deal_id TEXT NOT NULL,
			chunk_price INTEGER NOT NULL,
			paid_chunks INTEGER NOT NULL,
			paid_amount INTEGER NOT NULL,
			released_chunks INTEGER NOT NULL,
			released_amount INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS direct_transfer_pools(
			session_id TEXT PRIMARY KEY,
			deal_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			arbiter_pubkey_hex TEXT NOT NULL,
			pool_amount INTEGER NOT NULL,
			spend_tx_fee INTEGER NOT NULL,
			sequence_num INTEGER NOT NULL,
			seller_amount INTEGER NOT NULL,
			buyer_amount INTEGER NOT NULL,
			current_tx_hex TEXT NOT NULL,
			base_tx_hex TEXT NOT NULL,
			base_txid TEXT NOT NULL,
			status TEXT NOT NULL,
			fee_rate_sat_byte REAL NOT NULL,
			lock_blocks INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_fund_flows(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			flow_id TEXT NOT NULL,
			flow_type TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			direction TEXT NOT NULL,
			purpose TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			used_satoshi INTEGER NOT NULL,
			returned_satoshi INTEGER NOT NULL,
			related_txid TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS command_journal(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			requested_at_unix INTEGER NOT NULL,
			accepted INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL,
			error_message TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			duration_ms INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS state_snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			state TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS effect_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			effect_type TEXT NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS orchestrator_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			source TEXT NOT NULL,
			signal_type TEXT NOT NULL,
			aggregate_key TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			task_status TEXT NOT NULL,
			retry_count INTEGER NOT NULL,
			queue_length INTEGER NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_ledger_entries(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			txid TEXT NOT NULL,
			direction TEXT NOT NULL,
			category TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			counterparty_label TEXT NOT NULL,
			status TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			raw_ref_id TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo(
			utxo_id TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			value_satoshi INTEGER NOT NULL,
			state TEXT NOT NULL,
			created_txid TEXT NOT NULL,
			spent_txid TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			spent_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			utxo_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			ref_txid TEXT NOT NULL,
			ref_business_id TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_sync_state(
			address TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			utxo_count INTEGER NOT NULL,
			balance_satoshi INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			last_updated_by TEXT NOT NULL,
			last_trigger TEXT NOT NULL,
			last_duration_ms INTEGER NOT NULL,
			last_sync_round_id TEXT NOT NULL DEFAULT '',
			last_failed_step TEXT NOT NULL DEFAULT '',
			last_upstream_path TEXT NOT NULL DEFAULT '',
			last_http_status INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_history_cursor(
			address TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			next_confirmed_height INTEGER NOT NULL,
			next_page_token TEXT NOT NULL,
			anchor_height INTEGER NOT NULL,
			round_tip_height INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_business(
			business_id TEXT PRIMARY KEY,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_process_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			process_id TEXT NOT NULL,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			event_type TEXT NOT NULL,
			status TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_tx_breakdown(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			gross_input_satoshi INTEGER NOT NULL,
			change_back_satoshi INTEGER NOT NULL,
			external_in_satoshi INTEGER NOT NULL,
			counterparty_out_satoshi INTEGER NOT NULL,
			miner_fee_satoshi INTEGER NOT NULL,
			net_out_satoshi INTEGER NOT NULL,
			net_in_satoshi INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_business_txs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			tx_role TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			UNIQUE(business_id,txid)
		)`,
		`CREATE TABLE IF NOT EXISTS fin_tx_utxo_links(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			utxo_id TEXT NOT NULL,
			io_side TEXT NOT NULL,
			utxo_role TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			UNIQUE(business_id,txid,utxo_id,io_side,utxo_role)
		)`,
		`CREATE TABLE IF NOT EXISTS chain_tip_state(
			id INTEGER PRIMARY KEY CHECK(id=1),
			tip_height INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			last_updated_by TEXT NOT NULL,
			last_trigger TEXT NOT NULL,
			last_duration_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chain_tip_worker_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			triggered_at_unix INTEGER NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_source TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chain_utxo_worker_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			triggered_at_unix INTEGER NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_source TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS scheduler_tasks(
			task_name TEXT PRIMARY KEY,
			owner TEXT NOT NULL,
			mode TEXT NOT NULL,
			status TEXT NOT NULL,
			interval_seconds INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			closed_at_unix INTEGER NOT NULL,
			last_trigger TEXT NOT NULL,
			last_started_at_unix INTEGER NOT NULL,
			last_ended_at_unix INTEGER NOT NULL,
			last_duration_ms INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			in_flight INTEGER NOT NULL,
			run_count INTEGER NOT NULL,
			success_count INTEGER NOT NULL,
			failure_count INTEGER NOT NULL,
			last_summary_json TEXT NOT NULL,
			meta_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS scheduler_task_runs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			task_name TEXT NOT NULL,
			owner TEXT NOT NULL,
			mode TEXT NOT NULL,
			trigger TEXT NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			summary_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS static_file_prices(
			path TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS live_follows(
			stream_id TEXT PRIMARY KEY,
			stream_uri TEXT NOT NULL,
			publisher_pubkey TEXT NOT NULL,
			have_segment_index INTEGER NOT NULL,
			last_bought_segment_index INTEGER NOT NULL,
			last_bought_seed_hash TEXT NOT NULL,
			last_output_file_path TEXT NOT NULL,
			last_quote_seller_pubkey_hex TEXT NOT NULL,
			last_decision_json TEXT NOT NULL,
			status TEXT NOT NULL,
			last_error TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS file_downloads(
			seed_hash TEXT PRIMARY KEY,
			file_path TEXT NOT NULL,
			file_size INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL,
			completed_chunks INTEGER NOT NULL,
			paid_sats INTEGER NOT NULL,
			status TEXT NOT NULL,
			demand_id TEXT NOT NULL,
			last_error TEXT NOT NULL,
			status_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS file_download_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			status TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			price_sats INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_workspace_seed_hash ON workspace_files(seed_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_seed_available_chunks_seed ON seed_available_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_workspaces_path ON workspaces(path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_downloads_updated ON file_downloads(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_file_download_chunks_seed ON file_download_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_live_quotes_demand ON live_quotes(demand_id, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_tx_history_created_at ON tx_history(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_sale_records_created_at ON sale_records(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_created_at ON gateway_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_fund_flows_created_at ON wallet_fund_flows(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_fund_flows_flow_id ON wallet_fund_flows(flow_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_created_at ON command_journal(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_cmd_id ON command_journal(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_gateway ON command_journal(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_created_at ON domain_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_cmd_id ON domain_events(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_gateway ON domain_events(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_state_snapshots_created_at ON state_snapshots(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_state_snapshots_gateway ON state_snapshots(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_created_at ON effect_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_cmd_id ON effect_logs(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_gateway ON effect_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_created_at ON orchestrator_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_event_type ON orchestrator_logs(event_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_signal_type ON orchestrator_logs(signal_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_gateway ON orchestrator_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_idempotency ON orchestrator_logs(idempotency_key, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_created_at ON wallet_ledger_entries(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_occurred_at ON wallet_ledger_entries(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_txid ON wallet_ledger_entries(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_direction_category ON wallet_ledger_entries(direction, category, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_events_utxo ON wallet_utxo_events(utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_events_business ON wallet_utxo_events(ref_business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_history_cursor_round_tip ON wallet_utxo_history_cursor(round_tip_height DESC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_scene ON fin_business(scene_type, scene_subtype, occurred_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fin_business_idempotency ON fin_business(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_process_events_scene ON fin_process_events(scene_type, scene_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_process_events_process ON fin_process_events(process_id, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fin_process_events_idempotency ON fin_process_events(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_breakdown_business ON fin_tx_breakdown(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_breakdown_txid ON fin_tx_breakdown(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_txs_business ON fin_business_txs(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_txs_txid ON fin_business_txs(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_business ON fin_tx_utxo_links(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_utxo ON fin_tx_utxo_links(utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_txid ON fin_tx_utxo_links(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_tip_worker_logs_started ON chain_tip_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_tip_worker_logs_status ON chain_tip_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_utxo_worker_logs_started ON chain_utxo_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_utxo_worker_logs_status ON chain_utxo_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_status ON scheduler_tasks(status, updated_at_unix DESC, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_owner_mode ON scheduler_tasks(owner, mode, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_task ON scheduler_task_runs(task_name, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_status ON scheduler_task_runs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_started ON scheduler_task_runs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_static_file_prices_updated ON static_file_prices(updated_at_unix DESC)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}
	if err := ensureDirectQuotesSchema(db); err != nil {
		return err
	}
	if err := ensureSeedsSchema(db); err != nil {
		return err
	}
	if err := ensureWorkspaceFilesSchema(db); err != nil {
		return err
	}
	if err := ensureFileDownloadsSchema(db); err != nil {
		return err
	}
	if err := ensureLiveFollowsSchema(db); err != nil {
		return err
	}
	if err := migrateLegacyChainTables(db); err != nil {
		return err
	}
	if err := ensureWalletUTXOSchema(db); err != nil {
		return err
	}
	if err := ensureWalletUTXOSyncStateSchema(db); err != nil {
		return err
	}
	if err := migrateLegacyBizUTXOLinks(db); err != nil {
		return err
	}
	if err := normalizeClientPubKeyColumns(db); err != nil {
		return err
	}
	// 口径纠偏：cycle_pay 是过程事件，不应存在于财务主表。
	if err := cleanupLegacyCyclePayFinanceRows(db); err != nil {
		return err
	}
	return nil
}

// normalizeClientPubKeyColumns 把历史库里的旧格式公钥统一迁移为压缩公钥 hex（02/03）。
func normalizeClientPubKeyColumns(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	targets := []struct {
		table      string
		column     string
		allowEmpty bool
	}{
		{table: "direct_quotes", column: "seller_pubkey_hex"},
		{table: "direct_deals", column: "buyer_pubkey_hex"},
		{table: "direct_deals", column: "seller_pubkey_hex"},
		{table: "direct_transfer_pools", column: "buyer_pubkey_hex"},
		{table: "direct_transfer_pools", column: "seller_pubkey_hex"},
		{table: "live_quotes", column: "seller_pubkey_hex"},
		{table: "live_follows", column: "last_quote_seller_pubkey_hex", allowEmpty: true},
		{table: "file_download_chunks", column: "seller_pubkey_hex", allowEmpty: true},
	}
	for _, t := range targets {
		if err := normalizeClientPubKeyColumn(db, t.table, t.column, t.allowEmpty); err != nil {
			return fmt.Errorf("normalize %s.%s failed: %w", t.table, t.column, err)
		}
	}
	return nil
}

func normalizeClientPubKeyColumn(db *sql.DB, table, column string, allowEmpty bool) error {
	rows, err := db.Query(fmt.Sprintf("SELECT rowid,%s FROM %s", strings.TrimSpace(column), strings.TrimSpace(table)))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var rowID int64
		var raw string
		if err := rows.Scan(&rowID, &raw); err != nil {
			return err
		}
		raw = strings.TrimSpace(raw)
		if raw == "" && allowEmpty {
			continue
		}
		norm, err := normalizeCompressedPubKeyHexLegacyAware(raw)
		if err != nil {
			if allowEmpty && raw == "" {
				continue
			}
			return err
		}
		if strings.EqualFold(raw, norm) {
			continue
		}
		_, err = db.Exec(
			fmt.Sprintf("UPDATE %s SET %s=? WHERE rowid=?", strings.TrimSpace(table), strings.TrimSpace(column)),
			norm,
			rowID,
		)
		if err == nil {
			continue
		}
		// 处理唯一键冲突：同一业务行已存在新格式时，删除旧格式重复行。
		if strings.Contains(strings.ToLower(err.Error()), "unique constraint failed") {
			if _, delErr := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE rowid=?", strings.TrimSpace(table)), rowID); delErr != nil {
				return delErr
			}
			continue
		}
		return err
	}
	return rows.Err()
}

func cleanupLegacyCyclePayFinanceRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(
		`DELETE FROM fin_tx_breakdown
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM fin_tx_utxo_links
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM fin_business_txs
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	// 兼容旧库：如果历史表仍存在，也一起清掉，避免误导后续迁移逻辑。
	legacyExists, legacyErr := hasTable(db, "biz_utxo_links")
	if legacyErr != nil {
		return legacyErr
	}
	if legacyExists {
		if _, err = tx.Exec(
			`DELETE FROM biz_utxo_links
			 WHERE business_id IN (
				 SELECT business_id FROM fin_business
				 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
			 )`,
		); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM fin_business WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'`); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func hasTable(db *sql.DB, name string) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(name)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func migrateLegacyChainTables(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	exists, err := hasTable(db, "chain_tip_snapshot")
	if err != nil {
		return err
	}
	if exists {
		if _, err := db.Exec(
			`INSERT OR REPLACE INTO chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
			 SELECT id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms
			 FROM chain_tip_snapshot`,
		); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS chain_tip_snapshot`); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_utxo_snapshot`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_utxo_items`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_chain_tx_raw`); err != nil {
		return err
	}
	return nil
}

func ensureWalletUTXOSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "wallet_utxo")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, hasOrigin := cols["origin_type"]; !hasOrigin {
		if _, err := db.Exec(`DROP INDEX IF EXISTS idx_wallet_utxo_origin`); err != nil {
			return err
		}
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(`ALTER TABLE wallet_utxo RENAME TO wallet_utxo_legacy_v2`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE TABLE wallet_utxo(
		utxo_id TEXT PRIMARY KEY,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		txid TEXT NOT NULL,
		vout INTEGER NOT NULL,
		value_satoshi INTEGER NOT NULL,
		state TEXT NOT NULL,
		created_txid TEXT NOT NULL,
		spent_txid TEXT NOT NULL,
		created_at_unix INTEGER NOT NULL,
		updated_at_unix INTEGER NOT NULL,
		spent_at_unix INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		)
		SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,
			CASE WHEN lower(trim(state))='reserved' THEN 'unspent' ELSE state END,
			created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		FROM wallet_utxo_legacy_v2`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(`DROP TABLE wallet_utxo_legacy_v2`); err != nil {
		return err
	}
	if _, err = tx.Exec(`DROP INDEX IF EXISTS idx_wallet_utxo_origin`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`); err != nil {
		return err
	}
	return tx.Commit()
}

func ensureWalletUTXOSyncStateSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "wallet_utxo_sync_state")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["last_sync_round_id"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_sync_round_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if _, ok := cols["last_failed_step"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_failed_step TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if _, ok := cols["last_upstream_path"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_upstream_path TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if _, ok := cols["last_http_status"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_http_status INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	return nil
}

func migrateLegacyBizUTXOLinks(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	exists, err := hasTable(db, "biz_utxo_links")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	rows, err := db.Query(
		`SELECT l.business_id,l.txid,l.utxo_id,l.role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json,
		        COALESCE(b.scene_type,''),COALESCE(b.scene_subtype,'')
		   FROM biz_utxo_links l
		   LEFT JOIN fin_business b ON b.business_id=l.business_id
		   ORDER BY l.id ASC`,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var businessID string
		var txid string
		var utxoID string
		var role string
		var amount int64
		var createdAtUnix int64
		var note string
		var payload string
		var sceneType string
		var sceneSubtype string
		if err := rows.Scan(&businessID, &txid, &utxoID, &role, &amount, &createdAtUnix, &note, &payload, &sceneType, &sceneSubtype); err != nil {
			return err
		}
		txRole, ioSide, utxoRole := mapLegacyBizUTXORole(sceneType, sceneSubtype, role)
		if err := appendFinBusinessTxIfAbsent(db, finBusinessTxEntry{
			BusinessID:    businessID,
			TxID:          txid,
			TxRole:        txRole,
			CreatedAtUnix: createdAtUnix,
			Note:          note,
			Payload:       rawJSONPayload(payload),
		}); err != nil {
			return err
		}
		if err := appendFinTxUTXOLinkIfAbsent(db, finTxUTXOLinkEntry{
			BusinessID:    businessID,
			TxID:          txid,
			UTXOID:        utxoID,
			IOSide:        ioSide,
			UTXORole:      utxoRole,
			AmountSatoshi: amount,
			CreatedAtUnix: createdAtUnix,
			Note:          note,
			Payload:       rawJSONPayload(payload),
		}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS biz_utxo_links`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_biz_utxo_links_business`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_biz_utxo_links_utxo`); err != nil {
		return err
	}
	return nil
}

func mapLegacyBizUTXORole(sceneType string, sceneSubtype string, legacyRole string) (string, string, string) {
	sceneType = strings.TrimSpace(strings.ToLower(sceneType))
	sceneSubtype = strings.TrimSpace(strings.ToLower(sceneSubtype))
	legacyRole = strings.TrimSpace(strings.ToLower(legacyRole))
	txRole := "business_tx"
	switch {
	case sceneType == "fee_pool" && sceneSubtype == "open":
		txRole = "open_base"
	case sceneType == "c2c_transfer" && sceneSubtype == "open":
		txRole = "open_base"
	case sceneType == "c2c_transfer" && sceneSubtype == "close":
		txRole = "close_final"
	}
	switch legacyRole {
	case "input":
		return txRole, "input", "wallet_input"
	case "lock":
		return txRole, "output", "pool_lock"
	case "change":
		return txRole, "output", "wallet_change"
	case "settle_input":
		return txRole, "input", "pool_input"
	case "settle_to_seller":
		return txRole, "output", "settle_to_seller"
	case "settle_to_buyer":
		return txRole, "output", "settle_to_buyer"
	default:
		if strings.HasPrefix(legacyRole, "settle_") {
			return txRole, "output", legacyRole
		}
		return txRole, "output", legacyRole
	}
}

type rawJSONPayload string

func tableColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", strings.TrimSpace(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	return out, rows.Err()
}

func ensureDirectQuotesSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(direct_quotes)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasRecommendedFileName := false
	hasMIMEHint := false
	hasAvailableChunkBitmapHex := false
	hasAvailableChunksJSON := false
	hasChunkCount := false
	hasFileSize := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "recommended_file_name") {
			hasRecommendedFileName = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "mime_hint") {
			hasMIMEHint = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "available_chunk_bitmap_hex") {
			hasAvailableChunkBitmapHex = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "available_chunk_indexes_json") {
			hasAvailableChunksJSON = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "chunk_count") {
			hasChunkCount = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "file_size") {
			hasFileSize = true
		}
	}
	if !hasChunkCount {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !hasFileSize {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !hasRecommendedFileName {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN recommended_file_name TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasMIMEHint {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN mime_hint TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasAvailableChunkBitmapHex {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN available_chunk_bitmap_hex TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
		hasAvailableChunkBitmapHex = true
	}
	if hasAvailableChunkBitmapHex && hasAvailableChunksJSON {
		rows, err := db.Query(`SELECT id,available_chunk_indexes_json FROM direct_quotes`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var rawJSON string
			if err := rows.Scan(&id, &rawJSON); err != nil {
				return err
			}
			if strings.TrimSpace(rawJSON) == "" {
				continue
			}
			var indexes []uint32
			if err := json.Unmarshal([]byte(rawJSON), &indexes); err != nil {
				continue
			}
			bitmap := chunkBitmapHexFromIndexes(indexes, 0)
			if _, err := db.Exec(`UPDATE direct_quotes SET available_chunk_bitmap_hex=? WHERE id=?`, bitmap, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureSeedsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(seeds)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasRecommendedFileName := false
	hasMIMEHint := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "recommended_file_name") {
			hasRecommendedFileName = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "mime_hint") {
			hasMIMEHint = true
		}
	}
	if !hasRecommendedFileName {
		if _, err := db.Exec(`ALTER TABLE seeds ADD COLUMN recommended_file_name TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasMIMEHint {
		if _, err := db.Exec(`ALTER TABLE seeds ADD COLUMN mime_hint TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	return nil
}

func ensureWorkspaceFilesSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(workspace_files)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasSeedLocked := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "seed_locked") {
			hasSeedLocked = true
			break
		}
	}
	if hasSeedLocked {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE workspace_files ADD COLUMN seed_locked INTEGER NOT NULL DEFAULT 0`)
	return err
}

func ensureFileDownloadsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(file_downloads)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasStatusJSON := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "status_json") {
			hasStatusJSON = true
			break
		}
	}
	if hasStatusJSON {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE file_downloads ADD COLUMN status_json TEXT NOT NULL DEFAULT '{}'`)
	return err
}

func ensureLiveFollowsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(live_follows)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasLastQuoteSellerPeerID := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "last_quote_seller_pubkey_hex") {
			hasLastQuoteSellerPeerID = true
			break
		}
	}
	if hasLastQuoteSellerPeerID {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE live_follows ADD COLUMN last_quote_seller_pubkey_hex TEXT NOT NULL DEFAULT ''`)
	return err
}

func scanAndSyncWorkspace(ctx context.Context, cfg *Config, db *sql.DB) (map[string]sellerSeed, error) {
	now := time.Now().Unix()
	seenPaths := map[string]struct{}{}
	catalog := map[string]sellerSeed{}
	seedsDir := filepath.Join(cfg.Storage.DataDir, "seeds")
	type workspaceFileRef struct {
		SeedHash string
		Locked   bool
	}
	existing := map[string]workspaceFileRef{}
	rowsExists, err := db.Query(`SELECT path,seed_hash,seed_locked FROM workspace_files`)
	if err != nil {
		return nil, err
	}
	defer rowsExists.Close()
	for rowsExists.Next() {
		var path, seedHash string
		var locked int64
		if err := rowsExists.Scan(&path, &seedHash, &locked); err != nil {
			return nil, err
		}
		existing[filepath.Clean(strings.TrimSpace(path))] = workspaceFileRef{
			SeedHash: strings.ToLower(strings.TrimSpace(seedHash)),
			Locked:   locked != 0,
		}
	}

	workspaces, err := listEnabledWorkspacePaths(db, cfg.Storage.WorkspaceDir)
	if err != nil {
		return nil, err
	}

	for _, workspace := range workspaces {
		err = filepath.WalkDir(workspace, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			abs, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			abs = filepath.Clean(abs)
			seenPaths[abs] = struct{}{}
			st, err := os.Stat(abs)
			if err != nil {
				return err
			}
			if ref, ok := existing[abs]; ok && ref.Locked && ref.SeedHash != "" {
				if _, err := db.Exec(`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,seed_locked,updated_at_unix) VALUES(?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET file_size=excluded.file_size,mtime_unix=excluded.mtime_unix,seed_hash=excluded.seed_hash,seed_locked=excluded.seed_locked,updated_at_unix=excluded.updated_at_unix`, abs, st.Size(), st.ModTime().Unix(), ref.SeedHash, 1, now); err != nil {
					return err
				}
				var chunkCount uint32
				var seedPath string
				var recommendedName string
				var mimeHint string
				if err := db.QueryRow(`SELECT chunk_count,seed_file_path,recommended_file_name,mime_hint FROM seeds WHERE seed_hash=?`, ref.SeedHash).Scan(&chunkCount, &seedPath, &recommendedName, &mimeHint); err == nil {
					unit, total, err := upsertSeedPriceState(db, ref.SeedHash, cfg.Seller.Pricing.FloorPriceSatPer64K, cfg.Seller.Pricing.ResaleDiscountBPS, seedPath)
					if err != nil {
						return err
					}
					catalog[ref.SeedHash] = sellerSeed{
						SeedHash:            ref.SeedHash,
						ChunkCount:          chunkCount,
						ChunkPrice:          unit,
						SeedPrice:           total,
						RecommendedFileName: sanitizeRecommendedFileName(recommendedName),
						MIMEHint:            sanitizeMIMEHint(mimeHint),
					}
					return nil
				} else if !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				// 数据兜底：若锁定路径缺失 seed 行，回退为普通扫描重建映射。
			}
			seedBytes, seedHash, chunkCount, err := buildSeedV1(abs)
			if err != nil {
				return err
			}
			seedPath := filepath.Join(seedsDir, strings.ToLower(seedHash)+".bse")
			if err := writeIfChanged(seedPath, seedBytes); err != nil {
				return err
			}
			if _, err := db.Exec(`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,seed_locked,updated_at_unix) VALUES(?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET file_size=excluded.file_size,mtime_unix=excluded.mtime_unix,seed_hash=excluded.seed_hash,seed_locked=excluded.seed_locked,updated_at_unix=excluded.updated_at_unix`, abs, st.Size(), st.ModTime().Unix(), seedHash, 0, now); err != nil {
				return err
			}
			recommendedName := sanitizeRecommendedFileName(filepath.Base(abs))
			mimeHint := sanitizeMIMEHint(guessContentType(abs, nil))
			if _, err := db.Exec(`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?) ON CONFLICT(seed_hash) DO UPDATE SET seed_file_path=excluded.seed_file_path,chunk_count=excluded.chunk_count,file_size=excluded.file_size,recommended_file_name=excluded.recommended_file_name,mime_hint=excluded.mime_hint`, seedHash, seedPath, chunkCount, st.Size(), recommendedName, mimeHint, now); err != nil {
				return err
			}
			if err := replaceSeedAvailableChunks(db, seedHash, contiguousChunkIndexes(chunkCount)); err != nil {
				return err
			}
			unit, total, err := upsertSeedPriceState(db, seedHash, cfg.Seller.Pricing.FloorPriceSatPer64K, cfg.Seller.Pricing.ResaleDiscountBPS, seedPath)
			if err != nil {
				return err
			}
			catalog[seedHash] = sellerSeed{
				SeedHash:            seedHash,
				ChunkCount:          chunkCount,
				ChunkPrice:          unit,
				SeedPrice:           total,
				RecommendedFileName: recommendedName,
				MIMEHint:            mimeHint,
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	rows, err := db.Query(`SELECT path FROM workspace_files`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		if _, ok := seenPaths[p]; ok {
			continue
		}
		if _, err := db.Exec(`DELETE FROM workspace_files WHERE path=?`, p); err != nil {
			return nil, err
		}
	}

	orphanRows, err := db.Query(`SELECT s.seed_hash,s.seed_file_path FROM seeds s LEFT JOIN workspace_files wf ON wf.seed_hash=s.seed_hash GROUP BY s.seed_hash,s.seed_file_path HAVING COUNT(wf.path)=0`)
	if err != nil {
		return nil, err
	}
	defer orphanRows.Close()
	for orphanRows.Next() {
		var seedHash, seedPath string
		if err := orphanRows.Scan(&seedHash, &seedPath); err != nil {
			return nil, err
		}
		_ = os.Remove(seedPath)
		if _, err := db.Exec(`DELETE FROM seeds WHERE seed_hash=?`, seedHash); err != nil {
			return nil, err
		}
		if _, err := db.Exec(`DELETE FROM seed_price_state WHERE seed_hash=?`, seedHash); err != nil {
			return nil, err
		}
		if _, err := db.Exec(`DELETE FROM seed_available_chunks WHERE seed_hash=?`, seedHash); err != nil {
			return nil, err
		}
		if _, err := db.Exec(`DELETE FROM file_downloads WHERE seed_hash=?`, seedHash); err != nil {
			return nil, err
		}
		if _, err := db.Exec(`DELETE FROM file_download_chunks WHERE seed_hash=?`, seedHash); err != nil {
			return nil, err
		}
		delete(catalog, seedHash)
	}
	obs.Business("bitcast-client", "workspace_scanned", map[string]any{"seed_count": len(catalog), "path_count": len(seenPaths)})
	return catalog, nil
}

func listEnabledWorkspacePaths(db *sql.DB, fallback string) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`SELECT path FROM workspaces WHERE enabled=1 ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		p = filepath.Clean(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) > 0 {
		return out, nil
	}
	fallback = filepath.Clean(strings.TrimSpace(fallback))
	if fallback == "" {
		return nil, fmt.Errorf("no workspace configured")
	}
	return []string{fallback}, nil
}

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
// - 该入口只负责落库 direct_quotes，不涉及卖方资源读取，因此可全端默认启用。
func registerDirectQuoteSubmitHandler(h host.Host, db *sql.DB, trace p2prpc.TraceSink) {
	p2prpc.HandleProto[directQuoteSubmitReq, directQuoteSubmitResp](h, ProtoQuoteDirectSubmit, clientSec(trace), func(_ context.Context, req directQuoteSubmitReq) (directQuoteSubmitResp, error) {
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
		arbIDs := normalizePeerIDList(req.ArbiterPeerIDs)
		arbIDsJSON, err := json.Marshal(arbIDs)
		if err != nil {
			return directQuoteSubmitResp{}, err
		}
		availableChunkBitmapHex := normalizeChunkBitmapBytes(req.AvailableChunkBitmap)
		recommendedName := sanitizeRecommendedFileName(req.RecommendedFileName)
		mimeHint := sanitizeMIMEHint(req.MIMEHint)
		if _, err := db.Exec(
			`INSERT INTO direct_quotes(demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(demand_id,seller_pubkey_hex) DO UPDATE SET
			 seed_price=excluded.seed_price,
			 chunk_price=excluded.chunk_price,
			 chunk_count=excluded.chunk_count,
			 file_size=excluded.file_size,
			 expires_at_unix=excluded.expires_at_unix,
			 recommended_file_name=excluded.recommended_file_name,
			 mime_hint=excluded.mime_hint,
			 available_chunk_bitmap_hex=excluded.available_chunk_bitmap_hex,
			 seller_arbiter_pubkey_hexes_json=excluded.seller_arbiter_pubkey_hexes_json,
			 created_at_unix=excluded.created_at_unix`,
			strings.TrimSpace(req.DemandID),
			sellerPubHex,
			req.SeedPrice,
			req.ChunkPrice,
			req.ChunkCount,
			req.FileSize,
			req.ExpiresAtUnix,
			recommendedName,
			mimeHint,
			availableChunkBitmapHex,
			string(arbIDsJSON),
			time.Now().Unix(),
		); err != nil {
			return directQuoteSubmitResp{}, err
		}
		return directQuoteSubmitResp{Status: "stored"}, nil
	})
}

func registerSellerHandlers(h host.Host, db *sql.DB, live *liveRuntime, trace p2prpc.TraceSink, cfg Config) {
	p2prpc.HandleProto[dealprod.DemandAnnounceReq, dealprod.DemandAnnounceResp](h, protocol.ID(dealprod.ProtoDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.DemandAnnounceReq) (dealprod.DemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || seedHash == "" || buyerPeerID == "" || req.ChunkCount == 0 {
			return dealprod.DemandAnnounceResp{}, fmt.Errorf("invalid demand announce")
		}
		now := time.Now().Unix()
		if _, err := db.Exec(
			`INSERT INTO demand_dedup(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)
			 ON CONFLICT(demand_id) DO NOTHING`,
			demandID, seedHash, now,
		); err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		seed, ok, err := loadSellerSeedFromDB(db, seedHash)
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
		availableChunks, err := listSeedAvailableChunks(db, seedHash)
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
			BuyerPeerID:         buyerPeerID,
			BuyerAddrs:          req.BuyerAddrs,
			SeedPrice:           seed.SeedPrice,
			ChunkPrice:          seed.ChunkPrice,
			ChunkCount:          seed.ChunkCount,
			FileSize:            seed.FileSize,
			ExpiresAtUnix:       time.Now().Add(10 * time.Minute).Unix(),
			RecommendedFileName: seed.RecommendedFileName,
			MIMEHint:            seed.MIMEHint,
			ArbiterPeerIDs:      configuredArbiterPeerIDs(cfg),
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
	p2prpc.HandleProto[dealprod.LiveDemandAnnounceReq, dealprod.LiveDemandAnnounceResp](h, protocol.ID(dealprod.ProtoLiveDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.LiveDemandAnnounceReq) (dealprod.LiveDemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || !isSeedHashHex(streamID) || buyerPeerID == "" || req.Window == 0 {
			return dealprod.LiveDemandAnnounceResp{}, fmt.Errorf("invalid live demand announce")
		}
		recentSegments, latestIndex, err := listLocalLiveQuoteSegments(db, streamID, int(req.Window))
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

	p2prpc.HandleProto[seedGetReq, seedGetResp](h, ProtoSeedGet, clientSec(trace), func(_ context.Context, req seedGetReq) (seedGetResp, error) {
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		if strings.TrimSpace(req.SessionID) == "" || seedHash == "" {
			return seedGetResp{}, fmt.Errorf("invalid params")
		}
		var dealID string
		if err := db.QueryRow(`SELECT deal_id FROM direct_sessions WHERE session_id=?`, strings.TrimSpace(req.SessionID)).Scan(&dealID); err != nil {
			return seedGetResp{}, fmt.Errorf("session not found")
		}
		var dealSeedHash string
		if err := db.QueryRow(`SELECT seed_hash FROM direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).Scan(&dealSeedHash); err != nil {
			return seedGetResp{}, fmt.Errorf("deal not found")
		}
		if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
			return seedGetResp{}, fmt.Errorf("seed_hash mismatch")
		}
		seedBytes, err := loadSeedBytesBySeedHash(db, seedHash)
		if err != nil {
			return seedGetResp{}, err
		}
		return seedGetResp{Seed: append([]byte(nil), seedBytes...)}, nil
	})
	p2prpc.HandleProto[directDealAcceptReq, directDealAcceptResp](h, ProtoDirectDealAccept, clientSec(trace), func(_ context.Context, req directDealAcceptReq) (directDealAcceptResp, error) {
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
		if _, err := db.Exec(
			`INSERT INTO direct_deals(deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?)`,
			dealID,
			strings.TrimSpace(req.DemandID),
			buyerPubHex,
			sellerPubHex,
			strings.ToLower(strings.TrimSpace(req.SeedHash)),
			req.SeedPrice,
			req.ChunkPrice,
			strings.TrimSpace(req.ArbiterPeerID),
			"accepted",
			time.Now().Unix(),
		); err != nil {
			return directDealAcceptResp{}, err
		}
		return directDealAcceptResp{
			DealID:       dealID,
			SellerPeerID: sellerPubHex,
			ChunkPrice:   req.ChunkPrice,
			Status:       "accepted",
		}, nil
	})
	p2prpc.HandleProto[directSessionOpenReq, directSessionOpenResp](h, ProtoDirectSessionOpen, clientSec(trace), func(_ context.Context, req directSessionOpenReq) (directSessionOpenResp, error) {
		if strings.TrimSpace(req.DealID) == "" {
			return directSessionOpenResp{}, fmt.Errorf("deal_id required")
		}
		var chunkPrice uint64
		if err := db.QueryRow(`SELECT chunk_price FROM direct_deals WHERE deal_id=?`, req.DealID).Scan(&chunkPrice); err != nil {
			return directSessionOpenResp{}, err
		}
		sessionID := "dsess_" + randHex(8)
		now := time.Now().Unix()
		if _, err := db.Exec(`INSERT INTO direct_sessions(session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			sessionID, strings.TrimSpace(req.DealID), chunkPrice, 0, 0, 0, 0, "active", now, now); err != nil {
			return directSessionOpenResp{}, err
		}
		return directSessionOpenResp{SessionID: sessionID, Status: "active"}, nil
	})
	p2prpc.HandleProto[directTransferPoolOpenReq, directTransferPoolOpenResp](h, ProtoTransferPoolOpen, clientSec(trace), func(_ context.Context, req directTransferPoolOpenReq) (directTransferPoolOpenResp, error) {
		return handleDirectTransferPoolOpen(h, db, cfg, req)
	})
	p2prpc.HandleProto[directTransferPoolPayReq, directTransferPoolPayResp](h, ProtoTransferPoolPay, clientSec(trace), func(_ context.Context, req directTransferPoolPayReq) (directTransferPoolPayResp, error) {
		return handleDirectTransferPoolPay(h, db, cfg, req)
	})
	p2prpc.HandleProto[directTransferPoolCloseReq, directTransferPoolCloseResp](h, ProtoTransferPoolClose, clientSec(trace), func(_ context.Context, req directTransferPoolCloseReq) (directTransferPoolCloseResp, error) {
		return handleDirectTransferPoolClose(h, db, cfg, req)
	})
	p2prpc.HandleProto[directSessionCloseReq, directSessionCloseResp](h, ProtoDirectSessionClose, clientSec(trace), func(_ context.Context, req directSessionCloseReq) (directSessionCloseResp, error) {
		if strings.TrimSpace(req.SessionID) == "" {
			return directSessionCloseResp{}, fmt.Errorf("session_id required")
		}
		if _, err := db.Exec(`UPDATE direct_sessions SET status='finalized',updated_at_unix=? WHERE session_id=?`, time.Now().Unix(), req.SessionID); err != nil {
			return directSessionCloseResp{}, err
		}
		return directSessionCloseResp{SessionID: req.SessionID, Status: "finalized"}, nil
	})
}

func submitDirectQuote(ctx context.Context, h host.Host, trace p2prpc.TraceSink, p DirectQuoteParams) error {
	if h == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(p.DemandID) == "" || strings.TrimSpace(p.BuyerPeerID) == "" || p.SeedPrice == 0 || p.ChunkPrice == 0 {
		return fmt.Errorf("invalid params")
	}
	if p.ExpiresAtUnix == 0 {
		p.ExpiresAtUnix = time.Now().Add(10 * time.Minute).Unix()
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
	if err := p2prpc.CallProto(ctx, h, buyerID, ProtoQuoteDirectSubmit, clientSec(trace), directQuoteSubmitReq{
		DemandID:             strings.TrimSpace(p.DemandID),
		SellerPeerID:         strings.ToLower(strings.TrimSpace(sellerClientID)),
		SeedPrice:            p.SeedPrice,
		ChunkPrice:           p.ChunkPrice,
		ChunkCount:           p.ChunkCount,
		FileSize:             p.FileSize,
		ExpiresAtUnix:        p.ExpiresAtUnix,
		RecommendedFileName:  sanitizeRecommendedFileName(p.RecommendedFileName),
		MIMEHint:             sanitizeMIMEHint(p.MIMEHint),
		ArbiterPeerIDs:       normalizePeerIDList(p.ArbiterPeerIDs),
		AvailableChunkBitmap: bitmapBytes,
	}, &resp); err != nil {
		return err
	}
	if strings.TrimSpace(resp.Status) != "stored" {
		return fmt.Errorf("direct quote not stored")
	}
	return nil
}

func submitLiveQuote(ctx context.Context, h host.Host, trace p2prpc.TraceSink, p LiveQuoteParams) error {
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
	if err := p2prpc.CallProto(ctx, h, buyerID, ProtoLiveQuoteSubmit, clientSec(trace), liveQuoteSubmitReq{
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

func recommendedFileNameBySeedHash(db *sql.DB, seedHash string) string {
	if db == nil {
		return ""
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return ""
	}
	var stored string
	if err := db.QueryRow(`SELECT recommended_file_name FROM seeds WHERE seed_hash=?`, seedHash).Scan(&stored); err == nil {
		if normalized := sanitizeRecommendedFileName(stored); normalized != "" {
			return normalized
		}
	}
	var p string
	if err := db.QueryRow(`SELECT path FROM workspace_files WHERE seed_hash=? ORDER BY updated_at_unix DESC, path ASC LIMIT 1`, seedHash).Scan(&p); err != nil {
		return ""
	}
	return sanitizeRecommendedFileName(filepath.Base(strings.TrimSpace(p)))
}

func mimeHintBySeedHash(db *sql.DB, seedHash string) string {
	if db == nil {
		return ""
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return ""
	}
	var stored string
	if err := db.QueryRow(`SELECT mime_hint FROM seeds WHERE seed_hash=?`, seedHash).Scan(&stored); err != nil {
		return ""
	}
	return sanitizeMIMEHint(stored)
}

func configuredArbiterPeerIDs(cfg Config) []string {
	out := make([]string, 0, len(cfg.Network.Arbiters))
	for _, a := range cfg.Network.Arbiters {
		if !a.Enabled {
			continue
		}
		ai, err := parseAddr(strings.TrimSpace(a.Addr))
		if err != nil || ai == nil {
			continue
		}
		out = append(out, ai.ID.String())
	}
	return normalizePeerIDList(out)
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

func checkPeerHealth(ctx context.Context, h host.Host, peers []peer.AddrInfo, protoID protocol.ID, sec p2prpc.SecurityConfig, kind string) []peer.AddrInfo {
	const maxAttempts = 3
	out := make([]peer.AddrInfo, 0, len(peers))
	for _, p := range peers {
		var lastErr error
		ok := false
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			var health healthResp
			err := p2prpc.CallProto(ctx, h, p.ID, protoID, sec, healthReq{}, &health)
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

func upsertSeedPriceState(db *sql.DB, seedHash string, floorUnit, discountBPS uint64, seedPath string) (uint64, uint64, error) {
	var lastBuy sql.NullInt64
	_ = db.QueryRow(`SELECT last_buy_unit_price_sat_per_64k FROM seed_price_state WHERE seed_hash=?`, seedHash).Scan(&lastBuy)
	resale := uint64(0)
	if lastBuy.Valid && lastBuy.Int64 > 0 {
		resale = uint64(lastBuy.Int64) * discountBPS / 10000
	}
	unit := floorUnit
	if resale > unit {
		unit = resale
	}
	seedInfo, err := os.Stat(seedPath)
	if err != nil {
		return 0, 0, err
	}
	seedChunks := ceilDiv(uint64(seedInfo.Size()), seedBlockSize)
	total := unit * seedChunks
	now := time.Now().Unix()
	_, err = db.Exec(`INSERT INTO seed_price_state(seed_hash,last_buy_unit_price_sat_per_64k,floor_unit_price_sat_per_64k,resale_discount_bps,unit_price_sat_per_64k,updated_at_unix) VALUES(?,?,?,?,?,?) ON CONFLICT(seed_hash) DO UPDATE SET floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,resale_discount_bps=excluded.resale_discount_bps,unit_price_sat_per_64k=excluded.unit_price_sat_per_64k,updated_at_unix=excluded.updated_at_unix`, seedHash, nullInt64Value(lastBuy), floorUnit, discountBPS, unit, now)
	return unit, total, err
}

func nullInt64Value(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
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

func loadSeedBytesBySeedHash(db *sql.DB, seedHash string) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	var seedPath string
	if err := db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&seedPath); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("seed not found")
		}
		return nil, err
	}
	b, err := os.ReadFile(seedPath)
	if err != nil {
		return nil, err
	}
	return b, nil
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

func replaceSeedAvailableChunks(db *sql.DB, seedHash string, indexes []uint32) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	indexes = normalizeChunkIndexes(indexes, 0)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := tx.Exec(`DELETE FROM seed_available_chunks WHERE seed_hash=?`, seedHash); err != nil {
		rollback()
		return err
	}
	if len(indexes) > 0 {
		stmt, err := tx.Prepare(`INSERT INTO seed_available_chunks(seed_hash,chunk_index,updated_at_unix) VALUES(?,?,?)`)
		if err != nil {
			rollback()
			return err
		}
		defer stmt.Close()
		now := time.Now().Unix()
		for _, idx := range indexes {
			if _, err := stmt.Exec(seedHash, idx, now); err != nil {
				rollback()
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func listSeedAvailableChunks(db *sql.DB, seedHash string) ([]uint32, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return nil, fmt.Errorf("seed_hash required")
	}
	rows, err := db.Query(`SELECT chunk_index FROM seed_available_chunks WHERE seed_hash=? ORDER BY chunk_index ASC`, seedHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]uint32, 0, 128)
	for rows.Next() {
		var idx uint32
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		out = append(out, idx)
	}
	if len(out) > 0 {
		return out, nil
	}
	// 兼容旧数据：若尚未写入块状态，按当前文件长度推导前缀可用块。
	var seedChunkCount uint32
	if err := db.QueryRow(`SELECT chunk_count FROM seeds WHERE seed_hash=?`, seedHash).Scan(&seedChunkCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var fileSize uint64
	if err := db.QueryRow(`SELECT file_size FROM workspace_files WHERE seed_hash=? ORDER BY updated_at_unix DESC LIMIT 1`, seedHash).Scan(&fileSize); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	have := uint32(ceilDiv(fileSize, seedBlockSize))
	if have > seedChunkCount {
		have = seedChunkCount
	}
	return contiguousChunkIndexes(have), nil
}

// loadSellerSeedFromDB 从数据库读取卖方报价所需 seed 快照。
// 设计约束：卖方侧是否可报价以 DB 为唯一真相，不依赖内存镜像状态。
func loadSellerSeedFromDB(db *sql.DB, seedHash string) (sellerSeed, bool, error) {
	if db == nil {
		return sellerSeed{}, false, fmt.Errorf("db is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return sellerSeed{}, false, nil
	}
	var out sellerSeed
	var unitPrice uint64
	err := db.QueryRow(
		`SELECT s.seed_hash,s.chunk_count,COALESCE(p.unit_price_sat_per_64k,0),s.file_size,s.recommended_file_name,s.mime_hint
		   FROM seeds s
		   LEFT JOIN seed_price_state p ON p.seed_hash=s.seed_hash
		  WHERE s.seed_hash=?`,
		seedHash,
	).Scan(&out.SeedHash, &out.ChunkCount, &unitPrice, &out.FileSize, &out.RecommendedFileName, &out.MIMEHint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sellerSeed{}, false, nil
		}
		return sellerSeed{}, false, err
	}
	out.SeedHash = seedHash
	out.ChunkPrice = unitPrice
	out.SeedPrice = unitPrice * uint64(out.ChunkCount)
	out.RecommendedFileName = sanitizeRecommendedFileName(out.RecommendedFileName)
	out.MIMEHint = sanitizeMIMEHint(out.MIMEHint)
	return out, true, nil
}

func isSeedChunkAvailable(db *sql.DB, seedHash string, chunkIndex uint32) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return false, fmt.Errorf("seed_hash required")
	}
	var one int
	err := db.QueryRow(`SELECT 1 FROM seed_available_chunks WHERE seed_hash=? AND chunk_index=? LIMIT 1`, seedHash, chunkIndex).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		var cnt int
		if err := db.QueryRow(`SELECT COUNT(1) FROM seed_available_chunks WHERE seed_hash=?`, seedHash).Scan(&cnt); err != nil {
			return false, err
		}
		if cnt > 0 {
			return false, nil
		}
		// 兼容旧数据：若还没有块状态表记录，按当前文件长度推导“前缀块可用”。
		var seedChunkCount uint32
		if err := db.QueryRow(`SELECT chunk_count FROM seeds WHERE seed_hash=?`, seedHash).Scan(&seedChunkCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		var fileSize uint64
		if err := db.QueryRow(`SELECT file_size FROM workspace_files WHERE seed_hash=? ORDER BY updated_at_unix DESC LIMIT 1`, seedHash).Scan(&fileSize); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		have := uint32(ceilDiv(fileSize, seedBlockSize))
		if have > seedChunkCount {
			have = seedChunkCount
		}
		return chunkIndex < have, nil
	}
	return false, err
}

func loadChunkBytesBySeedHash(db *sql.DB, seedHash string, chunkIndex uint32) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	var filePath string
	var chunkCount uint32
	if err := db.QueryRow(
		`SELECT s.seed_file_path, s.chunk_count FROM seeds s WHERE s.seed_hash=?`,
		strings.ToLower(strings.TrimSpace(seedHash)),
	).Scan(&filePath, &chunkCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("seed not found")
		}
		return nil, err
	}
	if chunkIndex >= chunkCount {
		return nil, fmt.Errorf("chunk_index out of range")
	}
	have, err := isSeedChunkAvailable(db, seedHash, chunkIndex)
	if err != nil {
		return nil, err
	}
	if !have {
		return nil, fmt.Errorf("chunk not available")
	}
	var workspacePath string
	if err := db.QueryRow(`SELECT path FROM workspace_files WHERE seed_hash=? ORDER BY updated_at_unix DESC LIMIT 1`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&workspacePath); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("workspace file not found")
		}
		return nil, err
	}
	f, err := os.Open(workspacePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	offset := int64(chunkIndex) * seedBlockSize
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	out := make([]byte, seedBlockSize)
	n, err := io.ReadFull(f, out)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n < seedBlockSize {
		for i := n; i < seedBlockSize; i++ {
			out[i] = 0
		}
	}
	return out, nil
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

func gwSec(trace p2prpc.TraceSink) p2prpc.SecurityConfig {
	return p2prpc.SecurityConfig{Domain: "bitcast-gateway", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func arbSec(trace p2prpc.TraceSink) p2prpc.SecurityConfig {
	return p2prpc.SecurityConfig{Domain: "arbiter-mr", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func clientSec(trace p2prpc.TraceSink) p2prpc.SecurityConfig {
	return p2prpc.SecurityConfig{Domain: "bitcast-client", Network: "test", TTL: 30 * time.Second, Trace: trace}
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

func buildClientActorFromConfig(cfg Config) (*dual2of2.Actor, error) {
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
	return dual2of2.BuildActor("client", privHex, isMainnet)
}

func buildClientActorFromRunInput(in RunInput) (*dual2of2.Actor, error) {
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
	return dual2of2.BuildActor("client", privHex, isMainnet)
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
