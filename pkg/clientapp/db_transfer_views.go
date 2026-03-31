package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// 管理页和调试页的只读查询统一放在 db 内，handler 只负责参数和回包。

type directQuoteFilter struct {
	Limit        int
	Offset       int
	DemandID     string
	SellerPeerID string
}

type directQuotePage struct {
	Total int
	Items []directQuoteItem
}

type directQuoteItem struct {
	ID                      int64           `json:"id"`
	DemandID                string          `json:"demand_id"`
	SellerPeerID            string          `json:"seller_pubkey_hex"`
	SeedPrice               uint64          `json:"seed_price"`
	ChunkPrice              uint64          `json:"chunk_price"`
	ChunkCount              uint32          `json:"chunk_count"`
	FileSize                uint64          `json:"file_size"`
	ExpiresAtUnix           int64           `json:"expires_at_unix"`
	RecommendedFileName     string          `json:"recommended_file_name"`
	MIMEHint                string          `json:"mime_hint,omitempty"`
	AvailableChunkBitmapHex string          `json:"available_chunk_bitmap_hex"`
	SellerArbiterPeerIDs    json.RawMessage `json:"seller_arbiter_pubkey_hexes"`
	CreatedAtUnix           int64           `json:"created_at_unix"`
}

type directDealFilter struct {
	Limit        int
	Offset       int
	DemandID     string
	DealID       string
	SellerPeerID string
	BuyerPeerID  string
	Status       string
}

type directDealPage struct {
	Total int
	Items []directDealItem
}

type directDealItem struct {
	DealID        string `json:"deal_id"`
	DemandID      string `json:"demand_id"`
	BuyerPeerID   string `json:"buyer_pubkey_hex"`
	SellerPeerID  string `json:"seller_pubkey_hex"`
	SeedHash      string `json:"seed_hash"`
	SeedPrice     uint64 `json:"seed_price"`
	ChunkPrice    uint64 `json:"chunk_price"`
	ArbiterPeerID string `json:"arbiter_pubkey_hex"`
	Status        string `json:"status"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type directSessionFilter struct {
	Limit     int
	Offset    int
	SessionID string
	DealID    string
	Status    string
}

type directSessionPage struct {
	Total int
	Items []directSessionItem
}

type directSessionItem struct {
	SessionID      string `json:"session_id"`
	DealID         string `json:"deal_id"`
	ChunkPrice     uint64 `json:"chunk_price"`
	PaidChunks     uint32 `json:"paid_chunks"`
	PaidAmount     uint64 `json:"paid_amount"`
	ReleasedChunks uint32 `json:"released_chunks"`
	ReleasedAmount uint64 `json:"released_amount"`
	Status         string `json:"status"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
}

type directTransferPoolFilter struct {
	Limit         int
	Offset        int
	SessionID     string
	DealID        string
	Status        string
	SellerPeerID  string
	BuyerPeerID   string
	ArbiterPeerID string
}

type directTransferPoolPage struct {
	Total int
	Items []directTransferPoolItem
}

type directTransferPoolItem struct {
	SessionID        string  `json:"session_id"`
	DealID           string  `json:"deal_id"`
	BuyerPeerID      string  `json:"buyer_pubkey_hex"`
	SellerPeerID     string  `json:"seller_pubkey_hex"`
	ArbiterPeerID    string  `json:"arbiter_pubkey_hex"`
	BuyerPubkeyHex   string  `json:"buyer_pubkey_hex"`
	SellerPubkeyHex  string  `json:"seller_pubkey_hex"`
	ArbiterPubkeyHex string  `json:"arbiter_pubkey_hex"`
	PoolAmount       uint64  `json:"pool_amount"`
	SpendTxFee       uint64  `json:"spend_tx_fee"`
	SequenceNum      uint32  `json:"sequence_num"`
	SellerAmount     uint64  `json:"seller_amount"`
	BuyerAmount      uint64  `json:"buyer_amount"`
	CurrentTxHex     string  `json:"current_tx_hex"`
	BaseTxHex        string  `json:"base_tx_hex"`
	BaseTxID         string  `json:"base_txid"`
	Status           string  `json:"status"`
	FeeRateSatByte   float64 `json:"fee_rate_sat_byte"`
	LockBlocks       uint32  `json:"lock_blocks"`
	CreatedAtUnix    int64   `json:"created_at_unix"`
	UpdatedAtUnix    int64   `json:"updated_at_unix"`
}

type txHistoryFilter struct {
	Limit     int
	Offset    int
	EventType string
	Direction string
	Purpose   string
	Query     string
}

type txHistoryPage struct {
	Total int
	Items []txHistoryItem
}

type txHistoryItem struct {
	ID            int64  `json:"id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	GatewayPeerID string `json:"gateway_pubkey_hex"`
	EventType     string `json:"event_type"`
	Direction     string `json:"direction"`
	AmountSatoshi int64  `json:"amount_satoshi"`
	Purpose       string `json:"purpose"`
	Note          string `json:"note"`
	PoolID        string `json:"pool_id,omitempty"`
	MsgID         string `json:"msg_id,omitempty"`
	SequenceNum   uint32 `json:"sequence_num,omitempty"`
	CycleIndex    uint32 `json:"cycle_index,omitempty"`
}

type saleRecordFilter struct {
	Limit    int
	Offset   int
	SeedHash string
}

type saleRecordPage struct {
	Total int
	Items []saleRecordItem
}

type saleRecordItem struct {
	ID                 int64  `json:"id"`
	CreatedAtUnix      int64  `json:"created_at_unix"`
	SessionID          string `json:"session_id"`
	SeedHash           string `json:"seed_hash"`
	ChunkIndex         uint32 `json:"chunk_index"`
	UnitPriceSatPer64K uint64 `json:"unit_price_sat_per_64k"`
	AmountSatoshi      uint64 `json:"amount_satoshi"`
	BuyerGatewayPeerID string `json:"buyer_gateway_pubkey_hex"`
	ReleaseToken       string `json:"release_token"`
}

type gatewayEventFilter struct {
	Limit         int
	Offset        int
	GatewayPeerID string
	Action        string
}

type gatewayEventPage struct {
	Total int
	Items []gatewayEventItem
}

type gatewayEventItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	GatewayPeerID string          `json:"gateway_pubkey_hex"`
	Action        string          `json:"action"`
	MsgID         string          `json:"msg_id,omitempty"`
	SequenceNum   uint32          `json:"sequence_num,omitempty"`
	PoolID        string          `json:"pool_id,omitempty"`
	AmountSatoshi int64           `json:"amount_satoshi"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListDirectQuotes(ctx context.Context, store *clientDB, f directQuoteFilter) (directQuotePage, error) {
	if store == nil {
		return directQuotePage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directQuotePage, error) {
		where := ""
		args := make([]any, 0, 4)
		if f.DemandID != "" {
			where += " AND demand_id=?"
			args = append(args, f.DemandID)
		}
		if f.SellerPeerID != "" {
			where += " AND seller_pubkey_hex=?"
			args = append(args, f.SellerPeerID)
		}
		var out directQuotePage
		if err := db.QueryRow("SELECT COUNT(1) FROM direct_quotes WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return directQuotePage{}, err
		}
		rows, err := db.Query(`SELECT id,demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix FROM direct_quotes WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return directQuotePage{}, err
		}
		defer rows.Close()
		out.Items = make([]directQuoteItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDirectQuoteItem(rows)
			if err != nil {
				return directQuotePage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return directQuotePage{}, err
		}
		return out, nil
	})
}

func dbGetDirectQuoteItem(ctx context.Context, store *clientDB, id int64) (directQuoteItem, error) {
	if store == nil {
		return directQuoteItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directQuoteItem, error) {
		row := db.QueryRow(`SELECT id,demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix FROM direct_quotes WHERE id=?`, id)
		return scanDirectQuoteItem(row)
	})
}

func dbListDirectDeals(ctx context.Context, store *clientDB, f directDealFilter) (directDealPage, error) {
	if store == nil {
		return directDealPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directDealPage, error) {
		where := ""
		args := make([]any, 0, 8)
		if f.DemandID != "" {
			where += " AND demand_id=?"
			args = append(args, f.DemandID)
		}
		if f.DealID != "" {
			where += " AND deal_id=?"
			args = append(args, f.DealID)
		}
		if f.SellerPeerID != "" {
			where += " AND seller_pubkey_hex=?"
			args = append(args, f.SellerPeerID)
		}
		if f.BuyerPeerID != "" {
			where += " AND buyer_pubkey_hex=?"
			args = append(args, f.BuyerPeerID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		var out directDealPage
		if err := db.QueryRow("SELECT COUNT(1) FROM direct_deals WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return directDealPage{}, err
		}
		rows, err := db.Query(`SELECT deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix FROM direct_deals WHERE 1=1`+where+` ORDER BY created_at_unix DESC,deal_id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return directDealPage{}, err
		}
		defer rows.Close()
		out.Items = make([]directDealItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDirectDealItem(rows)
			if err != nil {
				return directDealPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return directDealPage{}, err
		}
		return out, nil
	})
}

func dbGetDirectDealItem(ctx context.Context, store *clientDB, dealID string) (directDealItem, error) {
	if store == nil {
		return directDealItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directDealItem, error) {
		row := db.QueryRow(`SELECT deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix FROM direct_deals WHERE deal_id=?`, dealID)
		return scanDirectDealItem(row)
	})
}

func dbListDirectSessions(ctx context.Context, store *clientDB, f directSessionFilter) (directSessionPage, error) {
	if store == nil {
		return directSessionPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directSessionPage, error) {
		where := ""
		args := make([]any, 0, 6)
		if f.SessionID != "" {
			where += " AND session_id=?"
			args = append(args, f.SessionID)
		}
		if f.DealID != "" {
			where += " AND deal_id=?"
			args = append(args, f.DealID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		var out directSessionPage
		if err := db.QueryRow("SELECT COUNT(1) FROM direct_sessions WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return directSessionPage{}, err
		}
		rows, err := db.Query(`SELECT session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix FROM direct_sessions WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,session_id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return directSessionPage{}, err
		}
		defer rows.Close()
		out.Items = make([]directSessionItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDirectSessionItem(rows)
			if err != nil {
				return directSessionPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return directSessionPage{}, err
		}
		return out, nil
	})
}

func dbGetDirectSessionItem(ctx context.Context, store *clientDB, sessionID string) (directSessionItem, error) {
	if store == nil {
		return directSessionItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directSessionItem, error) {
		row := db.QueryRow(`SELECT session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix FROM direct_sessions WHERE session_id=?`, sessionID)
		return scanDirectSessionItem(row)
	})
}

func dbListDirectTransferPools(ctx context.Context, store *clientDB, f directTransferPoolFilter) (directTransferPoolPage, error) {
	if store == nil {
		return directTransferPoolPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directTransferPoolPage, error) {
		where := ""
		args := make([]any, 0, 8)
		if f.SessionID != "" {
			where += " AND session_id=?"
			args = append(args, f.SessionID)
		}
		if f.DealID != "" {
			where += " AND deal_id=?"
			args = append(args, f.DealID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.SellerPeerID != "" {
			where += " AND seller_pubkey_hex=?"
			args = append(args, f.SellerPeerID)
		}
		if f.BuyerPeerID != "" {
			where += " AND buyer_pubkey_hex=?"
			args = append(args, f.BuyerPeerID)
		}
		if f.ArbiterPeerID != "" {
			where += " AND arbiter_pubkey_hex=?"
			args = append(args, f.ArbiterPeerID)
		}
		var out directTransferPoolPage
		if err := db.QueryRow("SELECT COUNT(1) FROM direct_transfer_pools WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return directTransferPoolPage{}, err
		}
		rows, err := db.Query(`SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			buyer_pubkey_hex AS buyer_pubkey_hex_alias,
			seller_pubkey_hex AS seller_pubkey_hex_alias,
			arbiter_pubkey_hex AS arbiter_pubkey_hex_alias,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
			FROM direct_transfer_pools WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,session_id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return directTransferPoolPage{}, err
		}
		defer rows.Close()
		out.Items = make([]directTransferPoolItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDirectTransferPoolItem(rows)
			if err != nil {
				return directTransferPoolPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return directTransferPoolPage{}, err
		}
		return out, nil
	})
}

func dbGetDirectTransferPoolItem(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolItem, error) {
	if store == nil {
		return directTransferPoolItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directTransferPoolItem, error) {
		row := db.QueryRow(`SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			buyer_pubkey_hex AS buyer_pubkey_hex_alias,
			seller_pubkey_hex AS seller_pubkey_hex_alias,
			arbiter_pubkey_hex AS arbiter_pubkey_hex_alias,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
			FROM direct_transfer_pools WHERE session_id=?`, sessionID)
		return scanDirectTransferPoolItem(row)
	})
}

func dbListTxHistory(ctx context.Context, store *clientDB, f txHistoryFilter) (txHistoryPage, error) {
	if store == nil {
		return txHistoryPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (txHistoryPage, error) {
		where := ""
		args := make([]any, 0, 8)
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.Direction != "" {
			where += " AND direction=?"
			args = append(args, f.Direction)
		}
		if f.Purpose != "" {
			where += " AND purpose=?"
			args = append(args, f.Purpose)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (note LIKE ? OR msg_id LIKE ? OR gateway_pubkey_hex LIKE ?)"
			args = append(args, like, like, like)
		}
		var out txHistoryPage
		if err := db.QueryRow("SELECT COUNT(1) FROM tx_history WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return txHistoryPage{}, err
		}
		rows, err := db.Query("SELECT id,created_at_unix,gateway_pubkey_hex,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index FROM tx_history WHERE 1=1"+where+" ORDER BY id DESC LIMIT ? OFFSET ?", append(args, f.Limit, f.Offset)...)
		if err != nil {
			return txHistoryPage{}, err
		}
		defer rows.Close()
		out.Items = make([]txHistoryItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanTxHistoryItem(rows)
			if err != nil {
				return txHistoryPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return txHistoryPage{}, err
		}
		return out, nil
	})
}

func dbGetTxHistoryItem(ctx context.Context, store *clientDB, id int64) (txHistoryItem, error) {
	if store == nil {
		return txHistoryItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (txHistoryItem, error) {
		row := db.QueryRow(`SELECT id,created_at_unix,gateway_pubkey_hex,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index FROM tx_history WHERE id=?`, id)
		return scanTxHistoryItem(row)
	})
}

func dbListSaleRecords(ctx context.Context, store *clientDB, f saleRecordFilter) (saleRecordPage, error) {
	if store == nil {
		return saleRecordPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (saleRecordPage, error) {
		where := ""
		args := make([]any, 0, 2)
		if f.SeedHash != "" {
			where = " WHERE seed_hash=?"
			args = append(args, f.SeedHash)
		}
		var out saleRecordPage
		if err := db.QueryRow("SELECT COUNT(1) FROM sale_records"+where, args...).Scan(&out.Total); err != nil {
			return saleRecordPage{}, err
		}
		rows, err := db.Query(`SELECT id,created_at_unix,session_id,seed_hash,chunk_index,unit_price_sat_per_64k,amount_satoshi,buyer_gateway_pubkey_hex,release_token
			 FROM sale_records`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return saleRecordPage{}, err
		}
		defer rows.Close()
		out.Items = make([]saleRecordItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanSaleRecordItem(rows)
			if err != nil {
				return saleRecordPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return saleRecordPage{}, err
		}
		return out, nil
	})
}

func dbGetSaleRecordItem(ctx context.Context, store *clientDB, id int64) (saleRecordItem, error) {
	if store == nil {
		return saleRecordItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (saleRecordItem, error) {
		row := db.QueryRow(`SELECT id,created_at_unix,session_id,seed_hash,chunk_index,unit_price_sat_per_64k,amount_satoshi,buyer_gateway_pubkey_hex,release_token FROM sale_records WHERE id=?`, id)
		return scanSaleRecordItem(row)
	})
}

func dbListGatewayEvents(ctx context.Context, store *clientDB, f gatewayEventFilter) (gatewayEventPage, error) {
	if store == nil {
		return gatewayEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (gatewayEventPage, error) {
		where := ""
		args := make([]any, 0, 4)
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.Action != "" {
			where += " AND action=?"
			args = append(args, f.Action)
		}
		var out gatewayEventPage
		if err := db.QueryRow("SELECT COUNT(1) FROM gateway_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return gatewayEventPage{}, err
		}
		rows, err := db.Query(`SELECT id,created_at_unix,gateway_pubkey_hex,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM gateway_events WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return gatewayEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]gatewayEventItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanGatewayEventItem(rows)
			if err != nil {
				return gatewayEventPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return gatewayEventPage{}, err
		}
		return out, nil
	})
}

func dbGetGatewayEventItem(ctx context.Context, store *clientDB, id int64) (gatewayEventItem, error) {
	if store == nil {
		return gatewayEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (gatewayEventItem, error) {
		row := db.QueryRow(`SELECT id,created_at_unix,gateway_pubkey_hex,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM gateway_events WHERE id=?`, id)
		return scanGatewayEventItem(row)
	})
}

type scanDirectQuote interface {
	Scan(dest ...any) error
}

func scanDirectQuoteItem(row scanDirectQuote) (directQuoteItem, error) {
	var out directQuoteItem
	var arbiterIDs string
	err := row.Scan(&out.ID, &out.DemandID, &out.SellerPeerID, &out.SeedPrice, &out.ChunkPrice, &out.ChunkCount, &out.FileSize, &out.ExpiresAtUnix, &out.RecommendedFileName, &out.MIMEHint, &out.AvailableChunkBitmapHex, &arbiterIDs, &out.CreatedAtUnix)
	if err != nil {
		return directQuoteItem{}, err
	}
	out.SellerArbiterPeerIDs = json.RawMessage(arbiterIDs)
	return out, nil
}

type scanDirectDeal interface {
	Scan(dest ...any) error
}

func scanDirectDealItem(row scanDirectDeal) (directDealItem, error) {
	var out directDealItem
	err := row.Scan(&out.DealID, &out.DemandID, &out.BuyerPeerID, &out.SellerPeerID, &out.SeedHash, &out.SeedPrice, &out.ChunkPrice, &out.ArbiterPeerID, &out.Status, &out.CreatedAtUnix)
	if err != nil {
		return directDealItem{}, err
	}
	return out, nil
}

type scanDirectSession interface {
	Scan(dest ...any) error
}

func scanDirectSessionItem(row scanDirectSession) (directSessionItem, error) {
	var out directSessionItem
	err := row.Scan(&out.SessionID, &out.DealID, &out.ChunkPrice, &out.PaidChunks, &out.PaidAmount, &out.ReleasedChunks, &out.ReleasedAmount, &out.Status, &out.CreatedAtUnix, &out.UpdatedAtUnix)
	if err != nil {
		return directSessionItem{}, err
	}
	return out, nil
}

type scanDirectTransferPool interface {
	Scan(dest ...any) error
}

func scanDirectTransferPoolItem(row scanDirectTransferPool) (directTransferPoolItem, error) {
	var out directTransferPoolItem
	err := row.Scan(
		&out.SessionID, &out.DealID, &out.BuyerPeerID, &out.SellerPeerID, &out.ArbiterPeerID,
		&out.BuyerPubkeyHex, &out.SellerPubkeyHex, &out.ArbiterPubkeyHex, &out.PoolAmount, &out.SpendTxFee,
		&out.SequenceNum, &out.SellerAmount, &out.BuyerAmount, &out.CurrentTxHex, &out.BaseTxHex, &out.BaseTxID,
		&out.Status, &out.FeeRateSatByte, &out.LockBlocks, &out.CreatedAtUnix, &out.UpdatedAtUnix,
	)
	if err != nil {
		return directTransferPoolItem{}, err
	}
	return out, nil
}

type scanTxHistory interface {
	Scan(dest ...any) error
}

func scanTxHistoryItem(row scanTxHistory) (txHistoryItem, error) {
	var out txHistoryItem
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.GatewayPeerID, &out.EventType, &out.Direction, &out.AmountSatoshi, &out.Purpose, &out.Note, &out.PoolID, &out.MsgID, &out.SequenceNum, &out.CycleIndex)
	if err != nil {
		return txHistoryItem{}, err
	}
	return out, nil
}

type scanSaleRecord interface {
	Scan(dest ...any) error
}

func scanSaleRecordItem(row scanSaleRecord) (saleRecordItem, error) {
	var out saleRecordItem
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.SessionID, &out.SeedHash, &out.ChunkIndex, &out.UnitPriceSatPer64K, &out.AmountSatoshi, &out.BuyerGatewayPeerID, &out.ReleaseToken)
	if err != nil {
		return saleRecordItem{}, err
	}
	return out, nil
}

type scanGatewayEvent interface {
	Scan(dest ...any) error
}

func scanGatewayEventItem(row scanGatewayEvent) (gatewayEventItem, error) {
	var out gatewayEventItem
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.GatewayPeerID, &out.Action, &out.MsgID, &out.SequenceNum, &out.PoolID, &out.AmountSatoshi, &payload)
	if err != nil {
		return gatewayEventItem{}, err
	}
	out.Payload = json.RawMessage(payload)
	return out, nil
}
