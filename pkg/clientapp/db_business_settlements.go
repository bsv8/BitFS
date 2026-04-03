package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// BusinessSettlementItem 业务结算出口记录
// 职责：表达一条 business 的统一结算出口
type BusinessSettlementItem struct {
	SettlementID     string          `json:"settlement_id"`
	BusinessID       string          `json:"business_id"`
	SettlementMethod string          `json:"settlement_method"`
	Status           string          `json:"status"`
	TargetType       string          `json:"target_type"`
	TargetID         string          `json:"target_id"`
	ErrorMessage     string          `json:"error_message"`
	CreatedAtUnix    int64           `json:"created_at_unix"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Payload          json.RawMessage `json:"payload"`
}

// businessSettlementEntry 业务结算出口写入条目
type businessSettlementEntry struct {
	SettlementID     string
	BusinessID       string
	SettlementMethod string
	Status           string
	TargetType       string
	TargetID         string
	ErrorMessage     string
	CreatedAtUnix    int64
	UpdatedAtUnix    int64
	Payload          any
}

// businessSettlementFilter 业务结算出口查询过滤条件
type businessSettlementFilter struct {
	Limit            int
	Offset           int
	SettlementID     string
	BusinessID       string
	SettlementMethod string
	Status           string
	TargetType       string
	TargetID         string
}

// businessSettlementPage 业务结算出口分页结果
type businessSettlementPage struct {
	Total int
	Items []BusinessSettlementItem
}

// validateSettlementMethod 验证结算方式是否合法
func validateSettlementMethod(method string) error {
	m := strings.TrimSpace(method)
	if m == string(SettlementMethodPool) || m == string(SettlementMethodChain) {
		return nil
	}
	return fmt.Errorf("invalid settlement_method: %s, must be 'pool' or 'chain'", method)
}

// dbUpsertBusinessSettlement 插入或更新业务结算出口
// 幂等设计：同一 settlement_id 重复写入时更新非主键字段
// 约束：business_id 唯一，一条 business 只对应一条主 settlement
// 校验：settlement_method 只允许 'pool' 或 'chain'
func dbUpsertBusinessSettlement(ctx context.Context, store *clientDB, e businessSettlementEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.SettlementID = strings.TrimSpace(e.SettlementID)
	if e.SettlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	if err := validateSettlementMethod(e.SettlementMethod); err != nil {
		return err
	}
	now := time.Now().Unix()
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = now
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = now
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO business_settlements(
				settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_id) DO UPDATE SET
				settlement_method=excluded.settlement_method,
				status=excluded.status,
				target_type=excluded.target_type,
				target_id=excluded.target_id,
				error_message=excluded.error_message,
				updated_at_unix=excluded.updated_at_unix,
				payload_json=excluded.payload_json`,
			e.SettlementID,
			e.BusinessID,
			strings.TrimSpace(e.SettlementMethod),
			strings.TrimSpace(e.Status),
			strings.TrimSpace(e.TargetType),
			strings.TrimSpace(e.TargetID),
			strings.TrimSpace(e.ErrorMessage),
			e.CreatedAtUnix,
			e.UpdatedAtUnix,
			mustJSONString(e.Payload),
		)
		return err
	})
}

// dbGetBusinessSettlement 按 settlement_id 查询业务结算出口
func dbGetBusinessSettlement(ctx context.Context, store *clientDB, settlementID string) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return BusinessSettlementItem{}, fmt.Errorf("settlement_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE settlement_id=?`,
			settlementID,
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return BusinessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbGetBusinessSettlementByBusinessID 按 business_id 查询业务结算出口
func dbGetBusinessSettlementByBusinessID(ctx context.Context, store *clientDB, businessID string) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	businessID = strings.TrimSpace(businessID)
	if businessID == "" {
		return BusinessSettlementItem{}, fmt.Errorf("business_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE business_id=?`,
			businessID,
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return BusinessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListBusinessSettlementsByTarget 按 target_type + target_id 查询业务结算出口列表
func dbListBusinessSettlementsByTarget(ctx context.Context, store *clientDB, targetType, targetID string, limit, offset int) (businessSettlementPage, error) {
	if store == nil {
		return businessSettlementPage{}, fmt.Errorf("client db is nil")
	}
	return dbListBusinessSettlements(ctx, store, businessSettlementFilter{
		TargetType: targetType,
		TargetID:   targetID,
		Limit:      limit,
		Offset:     offset,
	})
}

// dbListBusinessSettlements 查询业务结算出口列表
func dbListBusinessSettlements(ctx context.Context, store *clientDB, f businessSettlementFilter) (businessSettlementPage, error) {
	if store == nil {
		return businessSettlementPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessSettlementPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.SettlementID != "" {
			where += " AND settlement_id=?"
			args = append(args, f.SettlementID)
		}
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.SettlementMethod != "" {
			where += " AND settlement_method=?"
			args = append(args, f.SettlementMethod)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.TargetType != "" {
			where += " AND target_type=?"
			args = append(args, f.TargetType)
		}
		if f.TargetID != "" {
			where += " AND target_id=?"
			args = append(args, f.TargetID)
		}
		var out businessSettlementPage
		if err := db.QueryRow("SELECT COUNT(1) FROM business_settlements WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return businessSettlementPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := db.Query(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,settlement_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return businessSettlementPage{}, err
		}
		defer rows.Close()
		out.Items = make([]BusinessSettlementItem, 0, f.Limit)
		for rows.Next() {
			var item BusinessSettlementItem
			var payload string
			if err := rows.Scan(
				&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
				&item.TargetType, &item.TargetID, &item.ErrorMessage,
				&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
			); err != nil {
				return businessSettlementPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, item)
		}
		if err := rows.Err(); err != nil {
			return businessSettlementPage{}, err
		}
		return out, nil
	})
}

// dbUpdateBusinessSettlementStatus 更新业务结算出口状态
func dbUpdateBusinessSettlementStatus(ctx context.Context, store *clientDB, settlementID string, status string, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE business_settlements SET status=?, error_message=?, updated_at_unix=? WHERE settlement_id=?`,
			strings.TrimSpace(status),
			strings.TrimSpace(errorMessage),
			time.Now().Unix(),
			settlementID,
		)
		return err
	})
}

// dbUpdateBusinessSettlementStatusByBusinessID 按 business_id 更新业务结算出口状态
func dbUpdateBusinessSettlementStatusByBusinessID(ctx context.Context, store *clientDB, businessID string, status string, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	businessID = strings.TrimSpace(businessID)
	if businessID == "" {
		return fmt.Errorf("business_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE business_settlements SET status=?, error_message=?, updated_at_unix=? WHERE business_id=?`,
			strings.TrimSpace(status),
			strings.TrimSpace(errorMessage),
			time.Now().Unix(),
			businessID,
		)
		return err
	})
}

// dbUpdateBusinessSettlementTarget 回写 settlement 的 target_type 和 target_id
func dbUpdateBusinessSettlementTarget(ctx context.Context, store *clientDB, settlementID string, targetID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE business_settlements SET target_id=?, error_message='', updated_at_unix=? WHERE settlement_id=?`,
			strings.TrimSpace(targetID),
			time.Now().Unix(),
			settlementID,
		)
		return err
	})
}

// ============================================================
// 查询辅助函数：第二步补充，让真实接口和后台读取摆脱旧散查方式
// 设计原则：
//   - 必须走 business_triggers 桥接层，不绕 fin_business.source_id
//   - 先提供“列出全部”能力，再提供“取最近一条”辅助
//   - 不把“最近一条”直接写死成唯一正式口径
// ============================================================

// ListBusinessesByFrontOrderID 按 front_order_id 列出关联的全部 business
// 设计：一前台单可对应多条 business（补扣、退款、重试等），先列全量再按需筛选
func ListBusinessesByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) ([]financeBusinessItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return nil, fmt.Errorf("front_order_id is required")
	}

	// 第一步：从 business_triggers 找关联的 business_id 列表
	businessIDs, err := dbListBusinessesByTrigger(ctx, store, "front_order", frontOrderID)
	if err != nil {
		return nil, fmt.Errorf("list businesses by trigger: %w", err)
	}
	if len(businessIDs) == 0 {
		return nil, fmt.Errorf("no business found for front_order_id=%s", frontOrderID)
	}

	// 第二步：按 business_id 查 fin_business
	return clientDBValue(ctx, store, func(db *sql.DB) ([]financeBusinessItem, error) {
		var out []financeBusinessItem
		for _, bizID := range businessIDs {
			var item financeBusinessItem
			var payload string
			err := db.QueryRow(
				`SELECT business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
				 FROM fin_business WHERE business_id=?`,
				bizID,
			).Scan(
				&item.BusinessID, &item.SourceType, &item.SourceID, &item.AccountingScene, &item.AccountingSubtype,
				&item.FromPartyID, &item.ToPartyID, &item.Status, &item.OccurredAtUnix, &item.IdempotencyKey, &item.Note, &payload,
			)
			if err != nil {
				return nil, err
			}
			item.Payload = json.RawMessage(payload)
			out = append(out, item)
		}
		return out, nil
	})
}

// GetLatestBusinessByFrontOrderID 按 front_order_id 查最近一条 business
// 设计：仅作为临时辅助函数，不把“最近一条”写死成唯一正式口径
func GetLatestBusinessByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) (financeBusinessItem, error) {
	businesses, err := ListBusinessesByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return financeBusinessItem{}, err
	}
	return businesses[0], nil
}

// GetSettlementByBusinessID 按 business_id 查 settlement
func GetSettlementByBusinessID(ctx context.Context, store *clientDB, businessID string) (BusinessSettlementItem, error) {
	return dbGetBusinessSettlementByBusinessID(ctx, store, businessID)
}

// GetMainSettlementStatusByFrontOrderID 按 front_order_id 查当前主结算状态
// 设计：从 business -> settlement 串查，临时取最近一条 business 的 settlement
func GetMainSettlementStatusByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return BusinessSettlementItem{}, fmt.Errorf("front_order_id is required")
	}
	business, err := GetLatestBusinessByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return BusinessSettlementItem{}, fmt.Errorf("find business for front_order_id=%s: %w", frontOrderID, err)
	}
	return GetSettlementByBusinessID(ctx, store, business.BusinessID)
}

// GetChainPaymentBySettlement 按 settlement 查对应的 chain_payment
// 设计：当 settlement_method='chain' 时，target_id 存的是 chain_payments.id
func GetChainPaymentBySettlement(ctx context.Context, store *clientDB, settlement BusinessSettlementItem) (ChainPaymentItem, error) {
	if settlement.SettlementMethod != string(SettlementMethodChain) {
		return ChainPaymentItem{}, fmt.Errorf("settlement_method is not chain")
	}
	if settlement.TargetID == "" {
		return ChainPaymentItem{}, fmt.Errorf("settlement target_id is empty")
	}
	chainPaymentID, err := strconv.ParseInt(strings.TrimSpace(settlement.TargetID), 10, 64)
	if err != nil {
		return ChainPaymentItem{}, fmt.Errorf("parse settlement target_id: %w", err)
	}
	return GetChainPaymentByID(ctx, store, chainPaymentID)
}

// ChainPaymentItem chain_payments 查询返回项
type ChainPaymentItem struct {
	ID                  int64           `json:"id"`
	TxID                string          `json:"txid"`
	PaymentSubType      string          `json:"payment_subtype"`
	Status              string          `json:"status"`
	WalletInputSatoshi  int64           `json:"wallet_input_satoshi"`
	WalletOutputSatoshi int64           `json:"wallet_output_satoshi"`
	NetAmountSatoshi    int64           `json:"net_amount_satoshi"`
	BlockHeight         int64           `json:"block_height"`
	OccurredAtUnix      int64           `json:"occurred_at_unix"`
	FromPartyID         string          `json:"from_party_id"`
	ToPartyID           string          `json:"to_party_id"`
	UpdatedAtUnix       int64           `json:"updated_at_unix"`
	Payload             json.RawMessage `json:"payload"`
}

// GetChainPaymentByID 按 id 查 chain_payments
func GetChainPaymentByID(ctx context.Context, store *clientDB, id int64) (ChainPaymentItem, error) {
	if store == nil {
		return ChainPaymentItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (ChainPaymentItem, error) {
		var item ChainPaymentItem
		var payload string
		err := db.QueryRow(
			`SELECT id,txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,
					block_height,occurred_at_unix,from_party_id,to_party_id,updated_at_unix,payload_json
			 FROM chain_payments WHERE id=?`,
			id,
		).Scan(
			&item.ID, &item.TxID, &item.PaymentSubType, &item.Status,
			&item.WalletInputSatoshi, &item.WalletOutputSatoshi, &item.NetAmountSatoshi,
			&item.BlockHeight, &item.OccurredAtUnix, &item.FromPartyID, &item.ToPartyID, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return ChainPaymentItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// GetFullSettlementChainByFrontOrderID 按 front_order_id 查完整结算链
// 返回：business -> settlement -> chain_payment（如果有）
// 设计：临时取最近一条 business，后面可按 business_id 精确查询
func GetFullSettlementChainByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) (FullSettlementChain, error) {
	var out FullSettlementChain
	if store == nil {
		return out, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return out, fmt.Errorf("front_order_id is required")
	}

	business, err := GetLatestBusinessByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return out, fmt.Errorf("find business: %w", err)
	}
	out.Business = business

	settlement, err := GetSettlementByBusinessID(ctx, store, business.BusinessID)
	if err != nil {
		return out, fmt.Errorf("find settlement: %w", err)
	}
	out.Settlement = settlement

	// 如果是链上支付且已 settled，查 chain_payment
	if settlement.SettlementMethod == string(SettlementMethodChain) && settlement.Status == "settled" && settlement.TargetID != "" {
		chainPayment, err := GetChainPaymentBySettlement(ctx, store, settlement)
		if err != nil {
			return out, fmt.Errorf("find chain_payment: %w", err)
		}
		out.ChainPayment = &chainPayment
	}

	return out, nil
}

// FullSettlementChain 完整结算链
type FullSettlementChain struct {
	Business     financeBusinessItem    `json:"business"`
	Settlement   BusinessSettlementItem `json:"settlement"`
	ChainPayment *ChainPaymentItem      `json:"chain_payment,omitempty"`
}

// ============================================================
// 池支付查询辅助函数：第三步补充
// ============================================================

// PoolSettlementChain 池支付完整结算链
type PoolSettlementChain struct {
	Business       financeBusinessItem    `json:"business"`
	Settlement     BusinessSettlementItem `json:"settlement"`
	PoolAllocation *PoolAllocationItem    `json:"pool_allocation,omitempty"`
	PoolSession    *PoolSessionItem       `json:"pool_session,omitempty"`
}

// PoolAllocationItem pool_allocations 查询返回项
type PoolAllocationItem struct {
	ID               int64  `json:"id"`
	AllocationID     string `json:"allocation_id"`
	PoolSessionID    string `json:"pool_session_id"`
	AllocationNo     int64  `json:"allocation_no"`
	AllocationKind   string `json:"allocation_kind"`
	SequenceNum      uint32 `json:"sequence_num"`
	PayeeAmountAfter uint64 `json:"payee_amount_after"`
	PayerAmountAfter uint64 `json:"payer_amount_after"`
	TxID             string `json:"txid"`
	TxHex            string `json:"tx_hex"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
}

// PoolSessionItem pool_sessions 查询返回项
type PoolSessionItem struct {
	PoolSessionID      string  `json:"pool_session_id"`
	PoolScheme         string  `json:"pool_scheme"`
	CounterpartyPubHex string  `json:"counterparty_pubkey_hex"`
	SellerPubHex       string  `json:"seller_pubkey_hex"`
	ArbiterPubHex      string  `json:"arbiter_pubkey_hex"`
	GatewayPubHex      string  `json:"gateway_pubkey_hex"`
	PoolAmountSat      int64   `json:"pool_amount_satoshi"`
	SpendTxFeeSat      int64   `json:"spend_tx_fee_satoshi"`
	FeeRateSatByte     float64 `json:"fee_rate_sat_byte"`
	LockBlocks         int64   `json:"lock_blocks"`
	OpenBaseTxID       string  `json:"open_base_txid"`
	Status             string  `json:"status"`
	CreatedAtUnix      int64   `json:"created_at_unix"`
	UpdatedAtUnix      int64   `json:"updated_at_unix"`
}

// GetPoolAllocationByBusinessID 按 business_id 查 pool_allocation
// 设计：通过 business -> settlement -> pool_allocation 串查
func GetPoolAllocationByBusinessID(ctx context.Context, store *clientDB, businessID string) (PoolAllocationItem, error) {
	if store == nil {
		return PoolAllocationItem{}, fmt.Errorf("client db is nil")
	}
	settlement, err := GetSettlementByBusinessID(ctx, store, businessID)
	if err != nil {
		return PoolAllocationItem{}, fmt.Errorf("find settlement: %w", err)
	}
	if settlement.SettlementMethod != string(SettlementMethodPool) {
		return PoolAllocationItem{}, fmt.Errorf("settlement_method is not pool")
	}
	if settlement.TargetID == "" {
		return PoolAllocationItem{}, fmt.Errorf("settlement target_id is empty")
	}
	poolAllocationID, err := strconv.ParseInt(strings.TrimSpace(settlement.TargetID), 10, 64)
	if err != nil {
		return PoolAllocationItem{}, fmt.Errorf("parse settlement target_id: %w", err)
	}
	return GetPoolAllocationByID(ctx, store, poolAllocationID)
}

// GetPoolAllocationByID 按 id 查 pool_allocations
func GetPoolAllocationByID(ctx context.Context, store *clientDB, id int64) (PoolAllocationItem, error) {
	if store == nil {
		return PoolAllocationItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (PoolAllocationItem, error) {
		var item PoolAllocationItem
		err := db.QueryRow(
			`SELECT id,allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,
					payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
			 FROM pool_allocations WHERE id=?`,
			id,
		).Scan(
			&item.ID, &item.AllocationID, &item.PoolSessionID, &item.AllocationNo, &item.AllocationKind,
			&item.SequenceNum, &item.PayeeAmountAfter, &item.PayerAmountAfter,
			&item.TxID, &item.TxHex, &item.CreatedAtUnix,
		)
		if err != nil {
			return PoolAllocationItem{}, err
		}
		return item, nil
	})
}

// GetPoolSessionByID 按 id 查 pool_sessions（通过 pool_session_id）
func GetPoolSessionByID(ctx context.Context, store *clientDB, poolSessionID string) (PoolSessionItem, error) {
	if store == nil {
		return PoolSessionItem{}, fmt.Errorf("client db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return PoolSessionItem{}, fmt.Errorf("pool_session_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (PoolSessionItem, error) {
		var item PoolSessionItem
		err := db.QueryRow(
			`SELECT pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
					gateway_pubkey_hex,pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,
					open_base_txid,status,created_at_unix,updated_at_unix
			 FROM pool_sessions WHERE pool_session_id=?`,
			poolSessionID,
		).Scan(
			&item.PoolSessionID, &item.PoolScheme, &item.CounterpartyPubHex, &item.SellerPubHex, &item.ArbiterPubHex,
			&item.GatewayPubHex, &item.PoolAmountSat, &item.SpendTxFeeSat, &item.FeeRateSatByte, &item.LockBlocks,
			&item.OpenBaseTxID, &item.Status, &item.CreatedAtUnix, &item.UpdatedAtUnix,
		)
		if err != nil {
			return PoolSessionItem{}, err
		}
		return item, nil
	})
}

// ListPoolAllocationsBySession 按 pool_session_id 列该池下 allocations
func ListPoolAllocationsBySession(ctx context.Context, store *clientDB, poolSessionID string) ([]PoolAllocationItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return nil, fmt.Errorf("pool_session_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]PoolAllocationItem, error) {
		rows, err := db.Query(
			`SELECT id,allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,
					payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
			 FROM pool_allocations WHERE pool_session_id=? ORDER BY allocation_no DESC`,
			poolSessionID,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []PoolAllocationItem
		for rows.Next() {
			var item PoolAllocationItem
			if err := rows.Scan(
				&item.ID, &item.AllocationID, &item.PoolSessionID, &item.AllocationNo, &item.AllocationKind,
				&item.SequenceNum, &item.PayeeAmountAfter, &item.PayerAmountAfter,
				&item.TxID, &item.TxHex, &item.CreatedAtUnix,
			); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

// GetFullPoolSettlementChainByFrontOrderID 按 front_order_id 查完整池结算链
// 返回：business -> settlement -> pool_allocation -> pool_session
func GetFullPoolSettlementChainByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) (PoolSettlementChain, error) {
	var out PoolSettlementChain
	if store == nil {
		return out, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return out, fmt.Errorf("front_order_id is required")
	}

	business, err := GetLatestBusinessByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return out, fmt.Errorf("find business: %w", err)
	}
	out.Business = business

	settlement, err := GetSettlementByBusinessID(ctx, store, business.BusinessID)
	if err != nil {
		return out, fmt.Errorf("find settlement: %w", err)
	}
	out.Settlement = settlement

	// 如果是池支付且已 settled，查 pool_allocation
	if settlement.SettlementMethod == string(SettlementMethodPool) && settlement.Status == "settled" && settlement.TargetID != "" {
		poolAllocationID, err := strconv.ParseInt(strings.TrimSpace(settlement.TargetID), 10, 64)
		if err != nil {
			return out, fmt.Errorf("parse settlement target_id: %w", err)
		}
		poolAllocation, err := GetPoolAllocationByID(ctx, store, poolAllocationID)
		if err != nil {
			return out, fmt.Errorf("find pool_allocation: %w", err)
		}
		out.PoolAllocation = &poolAllocation

		// 查 pool_session
		poolSession, err := GetPoolSessionByID(ctx, store, poolAllocation.PoolSessionID)
		if err == nil {
			out.PoolSession = &poolSession
		}
	}

	return out, nil
}

// GetSettlementByPoolAllocationID 按 pool_allocation_id 查对应 settlement
func GetSettlementByPoolAllocationID(ctx context.Context, store *clientDB, poolAllocationID int64) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE settlement_method='pool' AND target_id=?`,
			fmt.Sprintf("%d", poolAllocationID),
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return BusinessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}
