package dealprod

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/storeactor"
)

type Service struct {
	DB    *sql.DB
	Actor *sqliteactor.Actor
}

// 设计说明：
// - dealprod 现在作为“库内业务存储过程”样板；
// - 外层业务不应该自己拼事务、自己拿 Rows 在外面跑；
// - 若注入了 sqliteactor，就优先走单 owner 串行入口；
// - 这样后面迁移时，调用方不需要知道底层是不是 sqlite 直连还是 actor 串行。
func serviceDBValue[T any](s *Service, fn func(*sql.DB) (T, error)) (T, error) {
	if s == nil {
		var zero T
		return zero, fmt.Errorf("dealprod service is nil")
	}
	if s.Actor != nil {
		return sqliteactor.DoValue(context.Background(), s.Actor, fn)
	}
	if s.DB == nil {
		var zero T
		return zero, fmt.Errorf("dealprod db is nil")
	}
	return fn(s.DB)
}

func serviceDBTxValue[T any](s *Service, fn func(*sql.Tx) (T, error)) (T, error) {
	if s == nil {
		var zero T
		return zero, fmt.Errorf("dealprod service is nil")
	}
	if s.Actor != nil {
		return sqliteactor.TxValue(context.Background(), s.Actor, fn)
	}
	if s.DB == nil {
		var zero T
		return zero, fmt.Errorf("dealprod db is nil")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		var zero T
		return zero, err
	}
	defer func() { _ = tx.Rollback() }()
	value, err := fn(tx)
	if err != nil {
		var zero T
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

const ProtoDemandAnnounce = "/bsv-transfer/demand/announce/1.0.0"
const ProtoLiveDemandAnnounce = "/bsv-transfer/live/demand/announce/1.0.0"

type PublishDemandReq struct {
	SeedHash            string   `protobuf:"bytes,1,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	BuyerPeerID         string   `protobuf:"bytes,2,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	BuyerAddrs          []string `protobuf:"bytes,3,rep,name=buyer_addrs,json=buyerAddrs,proto3" json:"buyer_addrs,omitempty"`
	ChunkCount          uint32   `protobuf:"varint,4,opt,name=chunk_count,json=chunkCount,proto3" json:"chunk_count"`
	ChargeAmountSatoshi uint64   `protobuf:"varint,5,opt,name=charge_amount_satoshi,json=chargeAmountSatoshi,proto3" json:"charge_amount_satoshi,omitempty"`
}
type PublishDemandResp struct {
	DemandID             string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	Status               string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
	ChargedAmountSatoshi uint64 `protobuf:"varint,3,opt,name=charged_amount_satoshi,json=chargedAmountSatoshi,proto3" json:"charged_amount_satoshi,omitempty"`
}

type PublishDemandBatchItemReq struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
}

type PublishDemandBatchItemResp struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
	DemandID   string `json:"demand_id"`
	Status     string `json:"status"`
}

type PublishDemandBatchReq struct {
	Items               []PublishDemandBatchItemReq `json:"items,omitempty"`
	BuyerPeerID         string                      `json:"buyer_pubkey_hex"`
	BuyerAddrs          []string                    `json:"buyer_addrs,omitempty"`
	ChargeAmountSatoshi uint64                      `json:"charge_amount_satoshi,omitempty"`
}

type PublishDemandBatchResp struct {
	Items                []PublishDemandBatchItemResp `json:"items,omitempty"`
	Status               string                       `json:"status"`
	ChargedAmountSatoshi uint64                       `json:"charged_amount_satoshi,omitempty"`
}

type PublishLiveDemandReq struct {
	StreamID            string   `protobuf:"bytes,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	BuyerPeerID         string   `protobuf:"bytes,2,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	BuyerAddrs          []string `protobuf:"bytes,3,rep,name=buyer_addrs,json=buyerAddrs,proto3" json:"buyer_addrs,omitempty"`
	HaveSegmentIndex    int64    `protobuf:"varint,4,opt,name=have_segment_index,json=haveSegmentIndex,proto3" json:"have_segment_index"`
	Window              uint32   `protobuf:"varint,5,opt,name=window,proto3" json:"window"`
	ChargeAmountSatoshi uint64   `protobuf:"varint,6,opt,name=charge_amount_satoshi,json=chargeAmountSatoshi,proto3" json:"charge_amount_satoshi,omitempty"`
}
type PublishLiveDemandResp struct {
	DemandID             string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	Status               string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
	ChargedAmountSatoshi uint64 `protobuf:"varint,3,opt,name=charged_amount_satoshi,json=chargedAmountSatoshi,proto3" json:"charged_amount_satoshi,omitempty"`
}

type QuoteSubmitReq struct {
	DemandID     string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	SellerPeerID string `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	SeedPrice    uint64 `protobuf:"varint,3,opt,name=seed_price,json=seedPrice,proto3" json:"seed_price"`
	ChunkPrice   uint64 `protobuf:"varint,4,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	ExpiresAt    int64  `protobuf:"varint,5,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at"`
}
type QuoteSubmitResp struct {
	QuoteID string `protobuf:"bytes,1,opt,name=quote_id,json=quoteId,proto3" json:"quote_id"`
	Status  string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
}

type QuoteListReq struct {
	DemandID string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
}
type QuoteItem struct {
	QuoteID      string `protobuf:"bytes,1,opt,name=quote_id,json=quoteId,proto3" json:"quote_id"`
	SellerPeerID string `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	SeedPrice    uint64 `protobuf:"varint,3,opt,name=seed_price,json=seedPrice,proto3" json:"seed_price"`
	ChunkPrice   uint64 `protobuf:"varint,4,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	ExpiresAt    int64  `protobuf:"varint,5,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at"`
}
type QuoteListResp struct {
	Quotes []QuoteItem `protobuf:"bytes,1,rep,name=quotes,proto3" json:"quotes"`
}

type DemandAnnounceReq struct {
	DemandID    string   `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	SeedHash    string   `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	BuyerPeerID string   `protobuf:"bytes,3,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	BuyerAddrs  []string `protobuf:"bytes,4,rep,name=buyer_addrs,json=buyerAddrs,proto3" json:"buyer_addrs"`
	ChunkCount  uint32   `protobuf:"varint,5,opt,name=chunk_count,json=chunkCount,proto3" json:"chunk_count"`
	Status      string   `protobuf:"bytes,6,opt,name=status,proto3" json:"status"`
}
type DemandAnnounceResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}

type LiveDemandAnnounceReq struct {
	DemandID         string   `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	StreamID         string   `protobuf:"bytes,2,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	BuyerPeerID      string   `protobuf:"bytes,3,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	BuyerAddrs       []string `protobuf:"bytes,4,rep,name=buyer_addrs,json=buyerAddrs,proto3" json:"buyer_addrs"`
	HaveSegmentIndex int64    `protobuf:"varint,5,opt,name=have_segment_index,json=haveSegmentIndex,proto3" json:"have_segment_index"`
	Window           uint32   `protobuf:"varint,6,opt,name=window,proto3" json:"window"`
	Status           string   `protobuf:"bytes,7,opt,name=status,proto3" json:"status"`
}
type LiveDemandAnnounceResp struct {
	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status"`
}

type LiveQuoteSegment struct {
	SegmentIndex uint64 `protobuf:"varint,1,opt,name=segment_index,json=segmentIndex,proto3" json:"segment_index"`
	SeedHash     string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
}

type LiveQuoteSubmitReq struct {
	DemandID           string              `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	SellerPeerID       string              `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	StreamID           string              `protobuf:"bytes,3,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	LatestSegmentIndex uint64              `protobuf:"varint,4,opt,name=latest_segment_index,json=latestSegmentIndex,proto3" json:"latest_segment_index"`
	RecentSegments     []*LiveQuoteSegment `protobuf:"bytes,5,rep,name=recent_segments,json=recentSegments,proto3" json:"recent_segments,omitempty"`
	ExpiresAt          int64               `protobuf:"varint,6,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at"`
}
type LiveQuoteSubmitResp struct {
	QuoteID string `protobuf:"bytes,1,opt,name=quote_id,json=quoteId,proto3" json:"quote_id"`
	Status  string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
}

type LiveQuoteListReq struct {
	DemandID string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
}
type LiveQuoteItem struct {
	QuoteID            string              `protobuf:"bytes,1,opt,name=quote_id,json=quoteId,proto3" json:"quote_id"`
	SellerPeerID       string              `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	StreamID           string              `protobuf:"bytes,3,opt,name=stream_id,json=streamId,proto3" json:"stream_id"`
	LatestSegmentIndex uint64              `protobuf:"varint,4,opt,name=latest_segment_index,json=latestSegmentIndex,proto3" json:"latest_segment_index"`
	RecentSegments     []*LiveQuoteSegment `protobuf:"bytes,5,rep,name=recent_segments,json=recentSegments,proto3" json:"recent_segments,omitempty"`
	ExpiresAt          int64               `protobuf:"varint,6,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at"`
}
type LiveQuoteListResp struct {
	Quotes []*LiveQuoteItem `protobuf:"bytes,1,rep,name=quotes,proto3" json:"quotes"`
}

type DealAcceptReq struct {
	DemandID           string `protobuf:"bytes,1,opt,name=demand_id,json=demandId,proto3" json:"demand_id"`
	QuoteID            string `protobuf:"bytes,2,opt,name=quote_id,json=quoteId,proto3" json:"quote_id"`
	BuyerPeerID        string `protobuf:"bytes,3,opt,name=buyer_pubkey_hex,json=buyerPeerId,proto3" json:"buyer_pubkey_hex"`
	ArbiterPeerID      string `protobuf:"bytes,4,opt,name=arbiter_pubkey_hex,json=arbiterPeerId,proto3" json:"arbiter_pubkey_hex"`
	DirectSellerPeerID string `protobuf:"bytes,5,opt,name=direct_seller_pubkey_hex,json=directSellerPeerId,proto3" json:"direct_seller_pubkey_hex,omitempty"`
	DirectSeedPrice    uint64 `protobuf:"varint,6,opt,name=direct_seed_price,json=directSeedPrice,proto3" json:"direct_seed_price,omitempty"`
	DirectChunkPrice   uint64 `protobuf:"varint,7,opt,name=direct_chunk_price,json=directChunkPrice,proto3" json:"direct_chunk_price,omitempty"`
	DirectExpiresAt    int64  `protobuf:"varint,8,opt,name=direct_expires_at,json=directExpiresAt,proto3" json:"direct_expires_at,omitempty"`
}
type DealAcceptResp struct {
	DealID       string `protobuf:"bytes,1,opt,name=deal_id,json=dealId,proto3" json:"deal_id"`
	SellerPeerID string `protobuf:"bytes,2,opt,name=seller_pubkey_hex,json=sellerPeerId,proto3" json:"seller_pubkey_hex"`
	ChunkPrice   uint64 `protobuf:"varint,3,opt,name=chunk_price,json=chunkPrice,proto3" json:"chunk_price"`
	Status       string `protobuf:"bytes,4,opt,name=status,proto3" json:"status"`
}

type SessionOpenReq struct {
	DealID string `protobuf:"bytes,1,opt,name=deal_id,json=dealId,proto3" json:"deal_id"`
}
type SessionOpenResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
}

type ReleaseChunkReq struct {
	SessionID  string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	ChunkIndex uint32 `protobuf:"varint,2,opt,name=chunk_index,json=chunkIndex,proto3" json:"chunk_index"`
	Amount     uint64 `protobuf:"varint,3,opt,name=amount,proto3" json:"amount"`
}
type ReleaseChunkResp struct {
	SessionID      string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	ReleaseToken   string `protobuf:"bytes,2,opt,name=release_token,json=releaseToken,proto3" json:"release_token"`
	ReleasedChunks uint32 `protobuf:"varint,3,opt,name=released_chunks,json=releasedChunks,proto3" json:"released_chunks"`
	ReleasedAmount uint64 `protobuf:"varint,4,opt,name=released_amount,json=releasedAmount,proto3" json:"released_amount"`
	Status         string `protobuf:"bytes,5,opt,name=status,proto3" json:"status"`
}

type VerifyReleaseReq struct {
	SessionID    string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	ChunkIndex   uint32 `protobuf:"varint,2,opt,name=chunk_index,json=chunkIndex,proto3" json:"chunk_index"`
	ReleaseToken string `protobuf:"bytes,3,opt,name=release_token,json=releaseToken,proto3" json:"release_token"`
}
type VerifyReleaseResp struct {
	Valid bool `protobuf:"varint,1,opt,name=valid,proto3" json:"valid"`
}

type SessionCloseReq struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
}
type SessionCloseResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Status    string `protobuf:"bytes,2,opt,name=status,proto3" json:"status"`
}

type DisputeReq struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	Reason    string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason"`
}
type DisputeResp struct {
	SessionID string `protobuf:"bytes,1,opt,name=session_id,json=sessionId,proto3" json:"session_id"`
	CaseID    string `protobuf:"bytes,2,opt,name=case_id,json=caseId,proto3" json:"case_id"`
	Status    string `protobuf:"bytes,3,opt,name=status,proto3" json:"status"`
}

func InitDB(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS demands (
			demand_id TEXT PRIMARY KEY,
			seed_hash TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			buyer_addrs_json TEXT NOT NULL DEFAULT '[]',
			chunk_count INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS live_demands (
			demand_id TEXT PRIMARY KEY,
			stream_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			buyer_addrs_json TEXT NOT NULL DEFAULT '[]',
			have_segment_index INTEGER NOT NULL,
			window_size INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS quotes (
			quote_id TEXT PRIMARY KEY,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS live_quotes (
			quote_id TEXT PRIMARY KEY,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			latest_segment_index INTEGER NOT NULL,
			recent_segments_json TEXT NOT NULL DEFAULT '[]',
			expires_at INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS deals (
			deal_id TEXT PRIMARY KEY,
			demand_id TEXT NOT NULL,
			quote_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			arbiter_pubkey_hex TEXT NOT NULL,
			chunk_price INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			deal_id TEXT NOT NULL,
			chunk_price INTEGER NOT NULL,
			released_chunks INTEGER NOT NULL,
			released_amount INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS release_tokens (
			session_id TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			release_token TEXT NOT NULL,
			used INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (session_id, chunk_index)
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}
	// 兼容老库：补充 buyer_addrs_json 字段。
	if _, err := db.Exec(`ALTER TABLE demands ADD COLUMN buyer_addrs_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
			return err
		}
	}
	return nil
}

func (s *Service) PublishDemand(req PublishDemandReq) (PublishDemandResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (PublishDemandResp, error) {
		if req.SeedHash == "" || req.BuyerPeerID == "" || req.ChunkCount == 0 {
			return PublishDemandResp{}, fmt.Errorf("invalid publish demand")
		}
		addrsJSON := "[]"
		if len(req.BuyerAddrs) > 0 {
			b, err := json.Marshal(req.BuyerAddrs)
			if err != nil {
				return PublishDemandResp{}, err
			}
			addrsJSON = string(b)
		}
		id := "dmd_" + randHex(8)
		_, err := db.Exec(`INSERT INTO demands(demand_id,seed_hash,buyer_pubkey_hex,buyer_addrs_json,chunk_count,status,created_at) VALUES(?,?,?,?,?,?,?)`, id, req.SeedHash, req.BuyerPeerID, addrsJSON, req.ChunkCount, "open", time.Now().Unix())
		if err != nil {
			return PublishDemandResp{}, err
		}
		return PublishDemandResp{DemandID: id, Status: "open"}, nil
	})
}

func (s *Service) PublishDemandBatch(req PublishDemandBatchReq) (PublishDemandBatchResp, error) {
	return serviceDBTxValue(s, func(tx *sql.Tx) (PublishDemandBatchResp, error) {
		if strings.TrimSpace(req.BuyerPeerID) == "" || len(req.Items) == 0 {
			return PublishDemandBatchResp{}, fmt.Errorf("invalid publish demand batch")
		}
		addrsJSON := "[]"
		if len(req.BuyerAddrs) > 0 {
			b, err := json.Marshal(req.BuyerAddrs)
			if err != nil {
				return PublishDemandBatchResp{}, err
			}
			addrsJSON = string(b)
		}
		seen := make(map[string]struct{}, len(req.Items))
		items := make([]PublishDemandBatchItemResp, 0, len(req.Items))
		now := time.Now().Unix()
		for _, item := range req.Items {
			seedHash := strings.ToLower(strings.TrimSpace(item.SeedHash))
			if seedHash == "" || item.ChunkCount == 0 {
				return PublishDemandBatchResp{}, fmt.Errorf("invalid publish demand batch item")
			}
			if _, ok := seen[seedHash]; ok {
				return PublishDemandBatchResp{}, fmt.Errorf("duplicate seed hash in demand batch")
			}
			seen[seedHash] = struct{}{}
			id := "dmd_" + randHex(8)
			if _, err := tx.Exec(`INSERT INTO demands(demand_id,seed_hash,buyer_pubkey_hex,buyer_addrs_json,chunk_count,status,created_at) VALUES(?,?,?,?,?,?,?)`,
				id,
				seedHash,
				req.BuyerPeerID,
				addrsJSON,
				item.ChunkCount,
				"open",
				now,
			); err != nil {
				return PublishDemandBatchResp{}, err
			}
			items = append(items, PublishDemandBatchItemResp{
				SeedHash:   seedHash,
				ChunkCount: item.ChunkCount,
				DemandID:   id,
				Status:     "open",
			})
		}
		return PublishDemandBatchResp{
			Items:                items,
			Status:               "open",
			ChargedAmountSatoshi: req.ChargeAmountSatoshi,
		}, nil
	})
}

func (s *Service) PublishLiveDemand(req PublishLiveDemandReq) (PublishLiveDemandResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (PublishLiveDemandResp, error) {
		if strings.TrimSpace(req.StreamID) == "" || strings.TrimSpace(req.BuyerPeerID) == "" || req.Window == 0 {
			return PublishLiveDemandResp{}, fmt.Errorf("invalid publish live demand")
		}
		addrsJSON := "[]"
		if len(req.BuyerAddrs) > 0 {
			b, err := json.Marshal(req.BuyerAddrs)
			if err != nil {
				return PublishLiveDemandResp{}, err
			}
			addrsJSON = string(b)
		}
		id := "ldmd_" + randHex(8)
		_, err := db.Exec(`INSERT INTO live_demands(demand_id,stream_id,buyer_pubkey_hex,buyer_addrs_json,have_segment_index,window_size,status,created_at) VALUES(?,?,?,?,?,?,?,?)`,
			id,
			strings.ToLower(strings.TrimSpace(req.StreamID)),
			strings.TrimSpace(req.BuyerPeerID),
			addrsJSON,
			req.HaveSegmentIndex,
			req.Window,
			"open",
			time.Now().Unix(),
		)
		if err != nil {
			return PublishLiveDemandResp{}, err
		}
		return PublishLiveDemandResp{DemandID: id, Status: "open", ChargedAmountSatoshi: req.ChargeAmountSatoshi}, nil
	})
}

func (s *Service) SubmitQuote(req QuoteSubmitReq) (QuoteSubmitResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (QuoteSubmitResp, error) {
		if req.DemandID == "" || req.SellerPeerID == "" || req.SeedPrice == 0 || req.ChunkPrice == 0 {
			return QuoteSubmitResp{}, fmt.Errorf("invalid submit quote")
		}
		if req.ExpiresAt == 0 {
			req.ExpiresAt = time.Now().Add(10 * time.Minute).Unix()
		}
		id := "q_" + randHex(8)
		_, err := db.Exec(`INSERT INTO quotes(quote_id,demand_id,seller_pubkey_hex,seed_price,chunk_price,expires_at,created_at) VALUES(?,?,?,?,?,?,?)`, id, req.DemandID, req.SellerPeerID, req.SeedPrice, req.ChunkPrice, req.ExpiresAt, time.Now().Unix())
		if err != nil {
			return QuoteSubmitResp{}, err
		}
		return QuoteSubmitResp{QuoteID: id, Status: "quoted"}, nil
	})
}

func (s *Service) ListQuotes(req QuoteListReq) (QuoteListResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (QuoteListResp, error) {
		rows, err := db.Query(`SELECT quote_id,seller_pubkey_hex,seed_price,chunk_price,expires_at FROM quotes WHERE demand_id=? ORDER BY created_at ASC`, req.DemandID)
		if err != nil {
			return QuoteListResp{}, err
		}
		defer rows.Close()
		out := make([]QuoteItem, 0)
		for rows.Next() {
			var q QuoteItem
			if err := rows.Scan(&q.QuoteID, &q.SellerPeerID, &q.SeedPrice, &q.ChunkPrice, &q.ExpiresAt); err != nil {
				return QuoteListResp{}, err
			}
			if q.ExpiresAt > time.Now().Unix() {
				out = append(out, q)
			}
		}
		if err := rows.Err(); err != nil {
			return QuoteListResp{}, err
		}
		return QuoteListResp{Quotes: out}, nil
	})
}

func (s *Service) SubmitLiveQuote(req LiveQuoteSubmitReq) (LiveQuoteSubmitResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (LiveQuoteSubmitResp, error) {
		if strings.TrimSpace(req.DemandID) == "" || strings.TrimSpace(req.SellerPeerID) == "" || strings.TrimSpace(req.StreamID) == "" || len(req.RecentSegments) == 0 {
			return LiveQuoteSubmitResp{}, fmt.Errorf("invalid submit live quote")
		}
		if req.ExpiresAt == 0 {
			req.ExpiresAt = time.Now().Add(2 * time.Minute).Unix()
		}
		segmentsJSON, err := json.Marshal(req.RecentSegments)
		if err != nil {
			return LiveQuoteSubmitResp{}, err
		}
		id := "lq_" + randHex(8)
		_, err = db.Exec(`INSERT INTO live_quotes(quote_id,demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?)`,
			id,
			strings.TrimSpace(req.DemandID),
			strings.TrimSpace(req.SellerPeerID),
			strings.ToLower(strings.TrimSpace(req.StreamID)),
			req.LatestSegmentIndex,
			string(segmentsJSON),
			req.ExpiresAt,
			time.Now().Unix(),
		)
		if err != nil {
			return LiveQuoteSubmitResp{}, err
		}
		return LiveQuoteSubmitResp{QuoteID: id, Status: "quoted"}, nil
	})
}

func (s *Service) ListLiveQuotes(req LiveQuoteListReq) (LiveQuoteListResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (LiveQuoteListResp, error) {
		rows, err := db.Query(`SELECT quote_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at FROM live_quotes WHERE demand_id=? ORDER BY created_at ASC`, strings.TrimSpace(req.DemandID))
		if err != nil {
			return LiveQuoteListResp{}, err
		}
		defer rows.Close()
		out := make([]*LiveQuoteItem, 0)
		now := time.Now().Unix()
		for rows.Next() {
			var item LiveQuoteItem
			var segmentsJSON string
			if err := rows.Scan(&item.QuoteID, &item.SellerPeerID, &item.StreamID, &item.LatestSegmentIndex, &segmentsJSON, &item.ExpiresAt); err != nil {
				return LiveQuoteListResp{}, err
			}
			if item.ExpiresAt > 0 && item.ExpiresAt < now {
				continue
			}
			if strings.TrimSpace(segmentsJSON) != "" {
				var segments []*LiveQuoteSegment
				if err := json.Unmarshal([]byte(segmentsJSON), &segments); err == nil {
					item.RecentSegments = segments
				}
			}
			out = append(out, &item)
		}
		if err := rows.Err(); err != nil {
			return LiveQuoteListResp{}, err
		}
		return LiveQuoteListResp{Quotes: out}, nil
	})
}

func (s *Service) AcceptDeal(req DealAcceptReq) (DealAcceptResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (DealAcceptResp, error) {
		if req.DemandID == "" || req.BuyerPeerID == "" || req.ArbiterPeerID == "" {
			return DealAcceptResp{}, fmt.Errorf("invalid accept deal")
		}
		var demandBuyerID string
		if err := db.QueryRow(`SELECT buyer_pubkey_hex FROM demands WHERE demand_id=?`, req.DemandID).Scan(&demandBuyerID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return DealAcceptResp{}, fmt.Errorf("demand not found")
			}
			return DealAcceptResp{}, err
		}
		if demandBuyerID != req.BuyerPeerID {
			return DealAcceptResp{}, fmt.Errorf("buyer mismatch for demand")
		}
		var sellerID string
		var chunkPrice uint64
		if strings.TrimSpace(req.QuoteID) != "" {
			if err := db.QueryRow(`SELECT seller_pubkey_hex,chunk_price FROM quotes WHERE quote_id=? AND demand_id=?`, req.QuoteID, req.DemandID).Scan(&sellerID, &chunkPrice); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return DealAcceptResp{}, fmt.Errorf("quote not found")
				}
				return DealAcceptResp{}, err
			}
		} else {
			if strings.TrimSpace(req.DirectSellerPeerID) == "" || req.DirectChunkPrice == 0 || req.DirectSeedPrice == 0 {
				return DealAcceptResp{}, fmt.Errorf("direct quote fields required")
			}
			if req.DirectExpiresAt > 0 && req.DirectExpiresAt < time.Now().Unix() {
				return DealAcceptResp{}, fmt.Errorf("direct quote expired")
			}
			sellerID = strings.ToLower(strings.TrimSpace(req.DirectSellerPeerID))
			chunkPrice = req.DirectChunkPrice
		}
		id := "deal_" + randHex(8)
		quoteID := strings.TrimSpace(req.QuoteID)
		if quoteID == "" {
			quoteID = "direct"
		}
		_, err := db.Exec(`INSERT INTO deals(deal_id,demand_id,quote_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,chunk_price,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, id, req.DemandID, quoteID, req.BuyerPeerID, sellerID, req.ArbiterPeerID, chunkPrice, "channel_prepared", time.Now().Unix())
		if err != nil {
			return DealAcceptResp{}, err
		}
		return DealAcceptResp{DealID: id, SellerPeerID: sellerID, ChunkPrice: chunkPrice, Status: "channel_prepared"}, nil
	})
}

func (s *Service) OpenSession(req SessionOpenReq) (SessionOpenResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (SessionOpenResp, error) {
		if req.DealID == "" {
			return SessionOpenResp{}, fmt.Errorf("deal_id required")
		}
		var chunkPrice uint64
		if err := db.QueryRow(`SELECT chunk_price FROM deals WHERE deal_id=?`, req.DealID).Scan(&chunkPrice); err != nil {
			return SessionOpenResp{}, err
		}
		id := "sess_" + randHex(8)
		_, err := db.Exec(`INSERT INTO sessions(session_id,deal_id,chunk_price,released_chunks,released_amount,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`, id, req.DealID, chunkPrice, 0, 0, "active", time.Now().Unix(), time.Now().Unix())
		if err != nil {
			return SessionOpenResp{}, err
		}
		return SessionOpenResp{SessionID: id, Status: "active"}, nil
	})
}

func (s *Service) ReleaseChunk(req ReleaseChunkReq) (ReleaseChunkResp, error) {
	return serviceDBTxValue(s, func(tx *sql.Tx) (ReleaseChunkResp, error) {
		if req.SessionID == "" {
			return ReleaseChunkResp{}, fmt.Errorf("session_id required")
		}
		var chunkPrice uint64
		var releasedChunks uint32
		var releasedAmount uint64
		var status string
		if err := tx.QueryRow(`SELECT chunk_price,released_chunks,released_amount,status FROM sessions WHERE session_id=?`, req.SessionID).Scan(&chunkPrice, &releasedChunks, &releasedAmount, &status); err != nil {
			return ReleaseChunkResp{}, err
		}
		if status != "active" && status != "chunk_paying" {
			return ReleaseChunkResp{}, fmt.Errorf("session status %s", status)
		}
		if req.Amount != chunkPrice {
			return ReleaseChunkResp{}, fmt.Errorf("amount mismatch")
		}
		if req.ChunkIndex != releasedChunks {
			return ReleaseChunkResp{}, fmt.Errorf("chunk_index must be next (%d)", releasedChunks)
		}
		releasedChunks++
		releasedAmount += req.Amount
		token := "rt_" + randHex(12)
		if _, err := tx.Exec(`INSERT INTO release_tokens(session_id,chunk_index,release_token,used,created_at) VALUES(?,?,?,?,?)`, req.SessionID, req.ChunkIndex, token, 0, time.Now().Unix()); err != nil {
			return ReleaseChunkResp{}, err
		}
		if _, err := tx.Exec(`UPDATE sessions SET released_chunks=?,released_amount=?,status='chunk_paying',updated_at=? WHERE session_id=?`, releasedChunks, releasedAmount, time.Now().Unix(), req.SessionID); err != nil {
			return ReleaseChunkResp{}, err
		}
		return ReleaseChunkResp{SessionID: req.SessionID, ReleaseToken: token, ReleasedChunks: releasedChunks, ReleasedAmount: releasedAmount, Status: "chunk_paying"}, nil
	})
}

func (s *Service) VerifyRelease(req VerifyReleaseReq) (VerifyReleaseResp, error) {
	return serviceDBTxValue(s, func(tx *sql.Tx) (VerifyReleaseResp, error) {
		var token string
		var used int
		err := tx.QueryRow(`SELECT release_token,used FROM release_tokens WHERE session_id=? AND chunk_index=?`, req.SessionID, req.ChunkIndex).Scan(&token, &used)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return VerifyReleaseResp{Valid: false}, nil
			}
			return VerifyReleaseResp{}, err
		}
		if token != req.ReleaseToken || used == 1 {
			return VerifyReleaseResp{Valid: false}, nil
		}
		if _, err := tx.Exec(`UPDATE release_tokens SET used=1 WHERE session_id=? AND chunk_index=?`, req.SessionID, req.ChunkIndex); err != nil {
			return VerifyReleaseResp{}, err
		}
		return VerifyReleaseResp{Valid: true}, nil
	})
}

func (s *Service) CloseSession(req SessionCloseReq) (SessionCloseResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (SessionCloseResp, error) {
		if _, err := db.Exec(`UPDATE sessions SET status='finalized',updated_at=? WHERE session_id=?`, time.Now().Unix(), req.SessionID); err != nil {
			return SessionCloseResp{}, err
		}
		return SessionCloseResp{SessionID: req.SessionID, Status: "finalized"}, nil
	})
}

func (s *Service) Dispute(req DisputeReq) (DisputeResp, error) {
	return serviceDBValue(s, func(db *sql.DB) (DisputeResp, error) {
		if req.SessionID == "" {
			return DisputeResp{}, fmt.Errorf("session_id required")
		}
		caseID := "case_" + randHex(8)
		if _, err := db.Exec(`UPDATE sessions SET status='dispute',updated_at=? WHERE session_id=?`, time.Now().Unix(), req.SessionID); err != nil {
			return DisputeResp{}, err
		}
		return DisputeResp{SessionID: req.SessionID, CaseID: caseID, Status: "dispute"}, nil
	})
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
