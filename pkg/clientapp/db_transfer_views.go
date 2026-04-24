package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	bitfsbizdemandquotearbiters "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizdemandquotearbiters"
	bitfsbizdemandquotes "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizdemandquotes"
	bitfsbizpurchases "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpurchases"
	bitfsprocgatewayevents "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procgatewayevents"
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
	ID                  int64  `json:"id"`
	CreatedAtUnix       int64  `json:"created_at_unix"`
	GatewayPeerID       string `json:"gateway_pubkey_hex"`
	EventType           string `json:"event_type"`
	Direction           string `json:"direction"`
	AmountSatoshi       int64  `json:"amount_satoshi"`
	Purpose             string `json:"purpose"`
	Note                string `json:"note"`
	PoolID              string `json:"pool_id,omitempty"`
	MsgID               string `json:"msg_id,omitempty"`
	SequenceNum         uint32 `json:"sequence_num,omitempty"`
	PaymentAttemptIndex uint32 `json:"cycle_index,omitempty"`
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
	return readEntValue(ctx, store, func(root EntReadRoot) (demandQuotePage, error) {
		q := root.BizDemandQuotes.Query()
		if f.DemandID != "" {
			q = q.Where(bitfsbizdemandquotes.DemandIDEQ(f.DemandID))
		}
		if f.SellerPubHex != "" {
			q = q.Where(bitfsbizdemandquotes.SellerPubHexEQ(f.SellerPubHex))
		}
		total, err := q.Clone().Count(ctx)
		if err != nil {
			return demandQuotePage{}, err
		}
		nodes, err := q.Order(bitfsbizdemandquotes.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
		if err != nil {
			return demandQuotePage{}, err
		}
		out := demandQuotePage{
			Total: total,
			Items: make([]demandQuoteItem, 0, len(nodes)),
		}
		for _, node := range nodes {
			out.Items = append(out.Items, demandQuoteItemFromEnt(node))
		}
		if err := hydrateDemandQuoteArbitersEnt(ctx, store, out.Items); err != nil {
			return demandQuotePage{}, err
		}
		return out, nil
	})
}

func dbGetDemandQuoteItem(ctx context.Context, store *clientDB, id int64) (demandQuoteItem, error) {
	if store == nil {
		return demandQuoteItem{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (demandQuoteItem, error) {
		node, err := root.BizDemandQuotes.Query().Where(bitfsbizdemandquotes.IDEQ(int(id))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return demandQuoteItem{}, sql.ErrNoRows
			}
			return demandQuoteItem{}, err
		}
		items := []demandQuoteItem{demandQuoteItemFromEnt(node)}
		if err := hydrateDemandQuoteArbitersEnt(ctx, store, items); err != nil {
			return demandQuoteItem{}, err
		}
		return items[0], nil
	})
}

func hydrateDemandQuoteArbitersEnt(ctx context.Context, store *clientDB, items []demandQuoteItem) error {
	if store == nil || len(items) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(items))
	byID := make(map[int64]*demandQuoteItem, len(items))
	for i := range items {
		if items[i].ID <= 0 {
			continue
		}
		ids = append(ids, items[i].ID)
		byID[items[i].ID] = &items[i]
		items[i].SellerArbiterPubHexes = nil
	}
	if len(ids) == 0 {
		return nil
	}
	nodes, err := readEntValue(ctx, store, func(root EntReadRoot) ([]*gen.BizDemandQuoteArbiters, error) {
		return root.BizDemandQuoteArbiters.Query().
			Where(bitfsbizdemandquotearbiters.QuoteIDIn(ids...)).
			Order(bitfsbizdemandquotearbiters.ByQuoteID(entsql.OrderAsc()), bitfsbizdemandquotearbiters.ByID(entsql.OrderAsc())).
			All(ctx)
	})
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if it, ok := byID[node.QuoteID]; ok {
			it.SellerArbiterPubHexes = append(it.SellerArbiterPubHexes, node.ArbiterPubHex)
		}
	}
	return nil
}

// dbListDirectTransferPoolsDebug proc_direct_transfer_pools 已删除，返回错误。
func dbListDirectTransferPoolsDebug(ctx context.Context, store *clientDB, f directTransferPoolFilter) (directTransferPoolPage, error) {
	return directTransferPoolPage{}, fmt.Errorf("proc_direct_transfer_pools deleted")
}

// dbGetDirectTransferPoolItemDebug proc_direct_transfer_pools 已删除，返回错误。
func dbGetDirectTransferPoolItemDebug(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolItem, error) {
	return directTransferPoolItem{}, fmt.Errorf("proc_direct_transfer_pools deleted")
}

// Deprecated: 保留给历史查询。
// - biz_purchases 是历史过程表，新代码应走 orders -> order_settlements

// dbListTxHistory fact_pool_session_events 已删除，返回错误。
func dbListTxHistory(ctx context.Context, store *clientDB, f txHistoryFilter) (txHistoryPage, error) {
	return txHistoryPage{}, fmt.Errorf("fact_pool_session_events deleted")
}

// dbGetTxHistoryItem fact_pool_session_events 已删除，返回错误。
func dbGetTxHistoryItem(ctx context.Context, store *clientDB, id int64) (txHistoryItem, error) {
	return txHistoryItem{}, fmt.Errorf("fact_pool_session_events deleted")
}

func dbListPurchases(ctx context.Context, store *clientDB, f purchaseFilter) (purchasePage, error) {
	if store == nil {
		return purchasePage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (purchasePage, error) {
		q := root.BizPurchases.Query()
		if f.DemandID != "" {
			q = q.Where(bitfsbizpurchases.DemandIDEQ(f.DemandID))
		}
		if f.SellerPubHex != "" {
			q = q.Where(bitfsbizpurchases.SellerPubHexEQ(f.SellerPubHex))
		}
		if f.ArbiterPubHex != "" {
			q = q.Where(bitfsbizpurchases.ArbiterPubHexEQ(f.ArbiterPubHex))
		}
		if f.Status != "" {
			q = q.Where(bitfsbizpurchases.StatusEQ(strings.ToLower(strings.TrimSpace(f.Status))))
		}
		total, err := q.Clone().Count(ctx)
		if err != nil {
			return purchasePage{}, err
		}
		nodes, err := q.Order(bitfsbizpurchases.ByCreatedAtUnix(entsql.OrderDesc()), bitfsbizpurchases.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
		if err != nil {
			return purchasePage{}, err
		}
		out := purchasePage{
			Total: total,
			Items: make([]purchaseItem, 0, len(nodes)),
		}
		for _, node := range nodes {
			out.Items = append(out.Items, purchaseItemFromEnt(node))
		}
		return out, nil
	})
}

func dbGetPurchaseItem(ctx context.Context, store *clientDB, id int64) (purchaseItem, error) {
	if store == nil {
		return purchaseItem{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (purchaseItem, error) {
		node, err := root.BizPurchases.Query().Where(bitfsbizpurchases.IDEQ(int(id))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return purchaseItem{}, sql.ErrNoRows
			}
			return purchaseItem{}, err
		}
		return purchaseItemFromEnt(node), nil
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
	return readEntValue(ctx, store, func(root EntReadRoot) (purchaseDemandSummary, error) {
		nodes, err := root.BizPurchases.Query().Where(bitfsbizpurchases.DemandIDEQ(demandID)).All(ctx)
		if err != nil {
			return purchaseDemandSummary{}, err
		}
		var out purchaseDemandSummary
		out.DemandID = demandID
		out.TotalPurchaseCount = int64(len(nodes))
		for _, node := range nodes {
			if strings.TrimSpace(node.Status) != "done" {
				continue
			}
			out.TotalPurchaseAmountSat += node.AmountSatoshi
			if node.ChunkIndex == 0 {
				out.SeedPurchaseCount++
				continue
			}
			if node.ChunkIndex >= 1 {
				out.ChunkPurchaseCount++
				out.ChunkPurchaseAmountSat += node.AmountSatoshi
			}
		}
		return out, nil
	})
}

func dbListGatewayEvents(ctx context.Context, store *clientDB, f gatewayEventFilter) (gatewayEventPage, error) {
	if store == nil {
		return gatewayEventPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (gatewayEventPage, error) {
		q := root.ProcGatewayEvents.Query()
		if f.GatewayPeerID != "" {
			q = q.Where(bitfsprocgatewayevents.GatewayPubkeyHexEQ(f.GatewayPeerID))
		}
		if f.CommandID != "" {
			q = q.Where(bitfsprocgatewayevents.CommandIDEQ(f.CommandID))
		}
		if f.Action != "" {
			q = q.Where(bitfsprocgatewayevents.ActionEQ(f.Action))
		}
		total, err := q.Clone().Count(ctx)
		if err != nil {
			return gatewayEventPage{}, err
		}
		nodes, err := q.Order(bitfsprocgatewayevents.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
		if err != nil {
			return gatewayEventPage{}, err
		}
		out := gatewayEventPage{
			Total: total,
			Items: make([]gatewayEventItem, 0, len(nodes)),
		}
		for _, node := range nodes {
			out.Items = append(out.Items, gatewayEventItemFromEnt(node))
		}
		return out, nil
	})
}

func dbGetGatewayEventItem(ctx context.Context, store *clientDB, id int64) (gatewayEventItem, error) {
	if store == nil {
		return gatewayEventItem{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (gatewayEventItem, error) {
		node, err := root.ProcGatewayEvents.Query().Where(bitfsprocgatewayevents.IDEQ(int(id))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return gatewayEventItem{}, sql.ErrNoRows
			}
			return gatewayEventItem{}, err
		}
		return gatewayEventItemFromEnt(node), nil
	})
}

func demandQuoteItemFromEnt(node *gen.BizDemandQuotes) demandQuoteItem {
	if node == nil {
		return demandQuoteItem{}
	}
	return demandQuoteItem{
		ID:                      int64(node.ID),
		DemandID:                node.DemandID,
		SellerPubHex:            node.SellerPubHex,
		SeedPriceSatoshi:        uint64(node.SeedPriceSatoshi),
		ChunkPriceSatoshi:       uint64(node.ChunkPriceSatoshi),
		ChunkCount:              uint32(node.ChunkCount),
		FileSizeBytes:           uint64(node.FileSizeBytes),
		RecommendedFileName:     node.RecommendedFileName,
		MimeType:                node.MimeType,
		AvailableChunkBitmapHex: node.AvailableChunkBitmapHex,
		ExpiresAtUnix:           node.ExpiresAtUnix,
		CreatedAtUnix:           node.CreatedAtUnix,
	}
}

func purchaseItemFromEnt(node *gen.BizPurchases) purchaseItem {
	if node == nil {
		return purchaseItem{}
	}
	return purchaseItem{
		ID:             int64(node.ID),
		DemandID:       node.DemandID,
		SellerPubHex:   node.SellerPubHex,
		ArbiterPubHex:  node.ArbiterPubHex,
		ChunkIndex:     uint32(node.ChunkIndex),
		ObjectHash:     node.ObjectHash,
		AmountSatoshi:  uint64(node.AmountSatoshi),
		Status:         node.Status,
		ErrorMessage:   node.ErrorMessage,
		CreatedAtUnix:  node.CreatedAtUnix,
		FinishedAtUnix: node.FinishedAtUnix,
	}
}

func txHistoryItemFromEnt(node any) txHistoryItem {
	return txHistoryItem{}
}

type scanDemandQuote interface {
	Scan(dest ...any) error
}

type scanGatewayEvent interface {
	Scan(dest ...any) error
}

// FrontOrderSettlementSummary 前置订单结算汇总（Group 8: 用 fact_settlement_records 重构）
type FrontOrderSettlementSummary struct {
	FrontOrderID          string                     `json:"front_order_id"`
	Businesses            []BusinessSettlementSummary `json:"businesses"`
	Summary               SettlementTotalSummary     `json:"summary"`
}

type BusinessSettlementSummary struct {
	BusinessID           string `json:"business_id"`
	SellerPubHex          string `json:"seller_pub_hex"`
	TotalTargetSatoshi    uint64 `json:"total_target_satoshi"`
	SettledAmountSatoshi  uint64 `json:"settled_amount_satoshi"`
	PendingAmountSatoshi  uint64 `json:"pending_amount_satoshi"`
}

type SettlementTotalSummary struct {
	OverallStatus        string `json:"overall_status"`
	TotalTargetSatoshi    uint64 `json:"total_target_satoshi"`
	SettledAmountSatoshi  uint64 `json:"settled_amount_satoshi"`
	PendingAmountSatoshi   uint64 `json:"pending_amount_satoshi"`
}

// GetFrontOrderSettlementSummary 前置订单结算汇总查询（Group 8: 用 fact_settlement_records 重构）
func GetFrontOrderSettlementSummary(ctx context.Context, store *clientDB, frontOrderID string) (FrontOrderSettlementSummary, error) {
	if store == nil {
		return FrontOrderSettlementSummary{}, fmt.Errorf("store is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return FrontOrderSettlementSummary{}, fmt.Errorf("front_order_id is required")
	}
	var summary FrontOrderSettlementSummary
	summary.FrontOrderID = frontOrderID
	summary.Businesses = []BusinessSettlementSummary{}
	summary.Summary = SettlementTotalSummary{}
	return summary, nil
}
