package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// 管理页和调试页的只读查询统一放在 db 内，handler 只负责参数和回包。

type demandQuoteFilter struct {
	Limit        int
	Offset       int
	DemandID     string
	SellerPubHex string
}

type demandQuotePage struct {
	Total int
	Items []demandQuoteItem
}

type demandQuoteItem struct {
	ID                      int64    `json:"id"`
	DemandID                string   `json:"demand_id"`
	SellerPubHex            string   `json:"seller_pubkey_hex"`
	SeedPriceSatoshi        uint64   `json:"seed_price"`
	ChunkPriceSatoshi       uint64   `json:"chunk_price"`
	ChunkCount              uint32   `json:"chunk_count"`
	FileSizeBytes           uint64   `json:"file_size"`
	RecommendedFileName     string   `json:"recommended_file_name"`
	MimeType                string   `json:"mime_hint,omitempty"`
	AvailableChunkBitmapHex string   `json:"available_chunk_bitmap_hex"`
	SellerArbiterPubHexes   []string `json:"seller_arbiter_pubkey_hexes,omitempty"`
	ExpiresAtUnix           int64    `json:"expires_at_unix"`
	CreatedAtUnix           int64    `json:"created_at_unix"`
}

type directTransferPoolFilter struct {
	Limit         int
	Offset        int
	SessionID     string
	DealID        string
	Status        string
	SellerPubHex  string
	BuyerPubHex   string
	ArbiterPubHex string
}

type directTransferPoolPage struct {
	Total int
	Items []directTransferPoolItem
}

type directTransferPoolItem struct {
	SessionID      string  `json:"session_id"`
	DealID         string  `json:"deal_id"`
	BuyerPubHex    string  `json:"buyer_pubkey_hex"`
	SellerPubHex   string  `json:"seller_pubkey_hex"`
	ArbiterPubHex  string  `json:"arbiter_pubkey_hex"`
	PoolAmount     uint64  `json:"pool_amount"`
	SpendTxFee     uint64  `json:"spend_tx_fee"`
	SequenceNum    uint32  `json:"sequence_num"`
	SellerAmount   uint64  `json:"seller_amount"`
	BuyerAmount    uint64  `json:"buyer_amount"`
	CurrentTxHex   string  `json:"current_tx_hex"`
	BaseTxHex      string  `json:"base_tx_hex"`
	BaseTxID       string  `json:"base_txid"`
	Status         string  `json:"status"`
	FeeRateSatByte float64 `json:"fee_rate_sat_byte"`
	LockBlocks     uint32  `json:"lock_blocks"`
	CreatedAtUnix  int64   `json:"created_at_unix"`
	UpdatedAtUnix  int64   `json:"updated_at_unix"`
}

type purchaseFilter struct {
	Limit         int
	Offset        int
	DemandID      string
	SellerPubHex  string
	ArbiterPubHex string
	Status        string
}

type purchasePage struct {
	Total int
	Items []purchaseItem
}

type purchaseItem struct {
	ID             int64  `json:"id"`
	DemandID       string `json:"demand_id"`
	SellerPubHex   string `json:"seller_pubkey_hex"`
	ArbiterPubHex  string `json:"arbiter_pubkey_hex"`
	ChunkIndex     uint32 `json:"chunk_index"`
	ObjectHash     string `json:"object_hash"`
	AmountSatoshi  uint64 `json:"amount_satoshi"`
	Status         string `json:"status"`
	ErrorMessage   string `json:"error_message"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
	FinishedAtUnix int64  `json:"finished_at_unix"`
}

type purchaseDemandSummary struct {
	DemandID               string `json:"demand_id"`
	SeedPurchaseCount      int64  `json:"seed_purchase_count"`
	ChunkPurchaseCount     int64  `json:"chunk_purchase_count"`
	TotalPurchaseCount     int64  `json:"total_purchase_count"`
	ChunkPurchaseAmountSat int64  `json:"chunk_purchase_amount_satoshi"`
	TotalPurchaseAmountSat int64  `json:"total_purchase_amount_satoshi"`
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

type gatewayEventFilter struct {
	Limit         int
	Offset        int
	GatewayPeerID string
	CommandID     string
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
	CommandID     string          `json:"command_id"`
	Action        string          `json:"action"`
	MsgID         string          `json:"msg_id,omitempty"`
	SequenceNum   uint32          `json:"sequence_num,omitempty"`
	PoolID        string          `json:"pool_id,omitempty"`
	AmountSatoshi int64           `json:"amount_satoshi"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListDemandQuotes(ctx context.Context, store *clientDB, f demandQuoteFilter) (demandQuotePage, error) {
	if store == nil {
		return demandQuotePage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (demandQuotePage, error) {
		where := ""
		args := make([]any, 0, 4)
		if f.DemandID != "" {
			where += " AND demand_id=?"
			args = append(args, f.DemandID)
		}
		if f.SellerPubHex != "" {
			where += " AND seller_pub_hex=?"
			args = append(args, f.SellerPubHex)
		}
		var out demandQuotePage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM biz_demand_quotes WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return demandQuotePage{}, err
		}
		rows, err := QueryContext(ctx, db, `SELECT id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix FROM biz_demand_quotes WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return demandQuotePage{}, err
		}
		defer rows.Close()
		out.Items = make([]demandQuoteItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDemandQuoteItem(rows)
			if err != nil {
				return demandQuotePage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return demandQuotePage{}, err
		}
		if err := hydrateDemandQuoteArbiters(ctx, db, out.Items); err != nil {
			return demandQuotePage{}, err
		}
		return out, nil
	})
}

func dbGetDemandQuoteItem(ctx context.Context, store *clientDB, id int64) (demandQuoteItem, error) {
	if store == nil {
		return demandQuoteItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (demandQuoteItem, error) {
		row := QueryRowContext(ctx, db, `SELECT id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix FROM biz_demand_quotes WHERE id=?`, id)
		it, err := scanDemandQuoteItem(row)
		if err != nil {
			return demandQuoteItem{}, err
		}
		items := []demandQuoteItem{it}
		if err := hydrateDemandQuoteArbiters(ctx, db, items); err != nil {
			return demandQuoteItem{}, err
		}
		return items[0], nil
	})
}

func hydrateDemandQuoteArbiters(ctx context.Context, db *sql.DB, items []demandQuoteItem) error {
	if db == nil || len(items) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(items))
	byID := make(map[int64]*demandQuoteItem, len(items))
	for i := range items {
		ids = append(ids, items[i].ID)
		byID[items[i].ID] = &items[i]
		items[i].SellerArbiterPubHexes = nil
	}
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	query := `SELECT quote_id,arbiter_pub_hex FROM biz_demand_quote_arbiters WHERE quote_id IN (` + strings.Join(placeholders, ",") + `) ORDER BY quote_id ASC, id ASC`
	rows, err := QueryContext(ctx, db, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var quoteID int64
		var arbiterPubHex string
		if err := rows.Scan(&quoteID, &arbiterPubHex); err != nil {
			return err
		}
		if it, ok := byID[quoteID]; ok {
			it.SellerArbiterPubHexes = append(it.SellerArbiterPubHexes, arbiterPubHex)
		}
	}
	return rows.Err()
}

// dbListDirectTransferPoolsDebug 只用于调试查询。
// - 业务状态统一走 GetFrontOrderSettlementSummary；
// - proc_direct_transfer_pools 只表达协议运行态，不代表业务结算结果；
// - 不要用它判断下载是否完成、是否已付费这类业务结论。
func dbListDirectTransferPoolsDebug(ctx context.Context, store *clientDB, f directTransferPoolFilter) (directTransferPoolPage, error) {
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
		if f.SellerPubHex != "" {
			where += " AND seller_pubkey_hex=?"
			args = append(args, f.SellerPubHex)
		}
		if f.BuyerPubHex != "" {
			where += " AND buyer_pubkey_hex=?"
			args = append(args, f.BuyerPubHex)
		}
		if f.ArbiterPubHex != "" {
			where += " AND arbiter_pubkey_hex=?"
			args = append(args, f.ArbiterPubHex)
		}
		var out directTransferPoolPage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM proc_direct_transfer_pools WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return directTransferPoolPage{}, err
		}
		rows, err := QueryContext(ctx, db, `SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
			FROM proc_direct_transfer_pools WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,session_id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
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

// dbGetDirectTransferPoolItemDebug 只用于调试查询。
// - 业务状态统一走 GetFrontOrderSettlementSummary；
// - proc_direct_transfer_pools 只表达协议运行态；
// - 不要用它判断下载是否完成这类业务结论。
func dbGetDirectTransferPoolItemDebug(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolItem, error) {
	if store == nil {
		return directTransferPoolItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (directTransferPoolItem, error) {
		row := QueryRowContext(ctx, db, `SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
			FROM proc_direct_transfer_pools WHERE session_id=?`, sessionID)
		return scanDirectTransferPoolItem(row)
	})
}

// Deprecated: 保留给历史查询。
// - biz_purchases 是历史过程表，新代码应走 biz_front_orders -> settle_records
func dbListPurchases(ctx context.Context, store *clientDB, f purchaseFilter) (purchasePage, error) {
	if store == nil {
		return purchasePage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (purchasePage, error) {
		where := ""
		args := make([]any, 0, 8)
		if f.DemandID != "" {
			where += " AND demand_id=?"
			args = append(args, f.DemandID)
		}
		if f.SellerPubHex != "" {
			where += " AND seller_pub_hex=?"
			args = append(args, f.SellerPubHex)
		}
		if f.ArbiterPubHex != "" {
			where += " AND arbiter_pub_hex=?"
			args = append(args, f.ArbiterPubHex)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, strings.ToLower(strings.TrimSpace(f.Status)))
		}
		var out purchasePage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM biz_purchases WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return purchasePage{}, err
		}
		rows, err := QueryContext(ctx, db, `SELECT id,demand_id,seller_pub_hex,arbiter_pub_hex,chunk_index,object_hash,amount_satoshi,status,error_message,created_at_unix,finished_at_unix
			FROM biz_purchases WHERE 1=1`+where+` ORDER BY created_at_unix DESC,id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return purchasePage{}, err
		}
		defer rows.Close()
		out.Items = make([]purchaseItem, 0, f.Limit)
		for rows.Next() {
			var it purchaseItem
			if err := rows.Scan(&it.ID, &it.DemandID, &it.SellerPubHex, &it.ArbiterPubHex, &it.ChunkIndex, &it.ObjectHash, &it.AmountSatoshi, &it.Status, &it.ErrorMessage, &it.CreatedAtUnix, &it.FinishedAtUnix); err != nil {
				return purchasePage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return purchasePage{}, err
		}
		return out, nil
	})
}

func dbGetPurchaseItem(ctx context.Context, store *clientDB, id int64) (purchaseItem, error) {
	if store == nil {
		return purchaseItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (purchaseItem, error) {
		row := QueryRowContext(ctx, db, `SELECT id,demand_id,seller_pub_hex,arbiter_pub_hex,chunk_index,object_hash,amount_satoshi,status,error_message,created_at_unix,finished_at_unix FROM biz_purchases WHERE id=?`, id)
		var it purchaseItem
		if err := row.Scan(&it.ID, &it.DemandID, &it.SellerPubHex, &it.ArbiterPubHex, &it.ChunkIndex, &it.ObjectHash, &it.AmountSatoshi, &it.Status, &it.ErrorMessage, &it.CreatedAtUnix, &it.FinishedAtUnix); err != nil {
			return purchaseItem{}, err
		}
		return it, nil
	})
}

// Deprecated: 保留给历史统计查询。
// - 新代码应使用 GetFrontOrderSettlementSummary 统计 settlement 状态
func dbSummarizeDemandPurchases(ctx context.Context, store *clientDB, demandID string) (purchaseDemandSummary, error) {
	if store == nil {
		return purchaseDemandSummary{}, fmt.Errorf("client db is nil")
	}
	demandID = strings.TrimSpace(demandID)
	if demandID == "" {
		return purchaseDemandSummary{}, fmt.Errorf("demand_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (purchaseDemandSummary, error) {
		var out purchaseDemandSummary
		out.DemandID = demandID
		err := QueryRowContext(ctx, db, `
			SELECT
				COALESCE(SUM(CASE WHEN status='done' AND chunk_index=0 THEN 1 ELSE 0 END),0),
				COALESCE(SUM(CASE WHEN status='done' AND chunk_index>=1 THEN 1 ELSE 0 END),0),
				COUNT(1),
				COALESCE(SUM(CASE WHEN status='done' AND chunk_index>=1 THEN amount_satoshi ELSE 0 END),0),
				COALESCE(SUM(CASE WHEN status='done' THEN amount_satoshi ELSE 0 END),0)
			FROM biz_purchases
			WHERE demand_id=?
		`, demandID).Scan(&out.SeedPurchaseCount, &out.ChunkPurchaseCount, &out.TotalPurchaseCount, &out.ChunkPurchaseAmountSat, &out.TotalPurchaseAmountSat)
		return out, err
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
			where += " AND payload_json LIKE ?"
			args = append(args, "%\"event_type\":\""+f.EventType+"\"%")
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
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM fact_pool_session_events WHERE event_kind=? AND 1=1"+where, append([]any{PoolFactEventKindTxHistory}, args...)...).Scan(&out.Total); err != nil {
			return txHistoryPage{}, err
		}
		rows, err := QueryContext(ctx, db, "SELECT id,created_at_unix,gateway_pubkey_hex,event_kind,direction,amount_satoshi,purpose,note,pool_session_id,msg_id,sequence_num,cycle_index,payload_json FROM fact_pool_session_events WHERE event_kind=? AND 1=1"+where+" ORDER BY id DESC LIMIT ? OFFSET ?", append([]any{PoolFactEventKindTxHistory}, append(args, f.Limit, f.Offset)...)...)
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
		row := QueryRowContext(ctx, db, `SELECT id,created_at_unix,gateway_pubkey_hex,event_kind,direction,amount_satoshi,purpose,note,pool_session_id,msg_id,sequence_num,cycle_index,payload_json FROM fact_pool_session_events WHERE id=? AND event_kind=?`, id, PoolFactEventKindTxHistory)
		return scanTxHistoryItem(row)
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
		if f.CommandID != "" {
			where += " AND command_id=?"
			args = append(args, f.CommandID)
		}
		if f.Action != "" {
			where += " AND action=?"
			args = append(args, f.Action)
		}
		var out gatewayEventPage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM proc_gateway_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return gatewayEventPage{}, err
		}
		rows, err := QueryContext(ctx, db, `SELECT id,created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM proc_gateway_events WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
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
		row := QueryRowContext(ctx, db, `SELECT id,created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM proc_gateway_events WHERE id=?`, id)
		return scanGatewayEventItem(row)
	})
}

type scanDemandQuote interface {
	Scan(dest ...any) error
}

func scanDemandQuoteItem(row scanDemandQuote) (demandQuoteItem, error) {
	var out demandQuoteItem
	err := row.Scan(&out.ID, &out.DemandID, &out.SellerPubHex, &out.SeedPriceSatoshi, &out.ChunkPriceSatoshi, &out.ChunkCount, &out.FileSizeBytes, &out.RecommendedFileName, &out.MimeType, &out.AvailableChunkBitmapHex, &out.ExpiresAtUnix, &out.CreatedAtUnix)
	if err != nil {
		return demandQuoteItem{}, err
	}
	return out, nil
}

type scanDirectTransferPool interface {
	Scan(dest ...any) error
}

func scanDirectTransferPoolItem(row scanDirectTransferPool) (directTransferPoolItem, error) {
	var out directTransferPoolItem
	err := row.Scan(
		&out.SessionID, &out.DealID, &out.BuyerPubHex, &out.SellerPubHex, &out.ArbiterPubHex,
		&out.PoolAmount, &out.SpendTxFee, &out.SequenceNum, &out.SellerAmount, &out.BuyerAmount,
		&out.CurrentTxHex, &out.BaseTxHex, &out.BaseTxID, &out.Status, &out.FeeRateSatByte, &out.LockBlocks,
		&out.CreatedAtUnix, &out.UpdatedAtUnix,
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
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.GatewayPeerID, &out.EventType, &out.Direction, &out.AmountSatoshi, &out.Purpose, &out.Note, &out.PoolID, &out.MsgID, &out.SequenceNum, &out.CycleIndex, &payload)
	if err != nil {
		return txHistoryItem{}, err
	}
	if strings.TrimSpace(out.EventType) == PoolFactEventKindTxHistory && strings.TrimSpace(payload) != "" {
		var body map[string]any
		if json.Unmarshal([]byte(payload), &body) == nil {
			if eventType, ok := body["event_type"].(string); ok && strings.TrimSpace(eventType) != "" {
				out.EventType = eventType
			}
		}
	}
	return out, nil
}

type scanGatewayEvent interface {
	Scan(dest ...any) error
}

func scanGatewayEventItem(row scanGatewayEvent) (gatewayEventItem, error) {
	var out gatewayEventItem
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.GatewayPeerID, &out.CommandID, &out.Action, &out.MsgID, &out.SequenceNum, &out.PoolID, &out.AmountSatoshi, &payload)
	if err != nil {
		return gatewayEventItem{}, err
	}
	out.Payload = json.RawMessage(payload)
	return out, nil
}
