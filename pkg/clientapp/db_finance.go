package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// settle_businesses 语义说明（第七次迭代收口）：
// - 一条 settle_businesses = 一条独立收费事实
// - 失败重试不新建 business，只更新原记录
// - 退款、冲正、撤销如果产生新的资金动作，必须新建新的 business
// - 本阶段表名保持 settle_businesses，逻辑上已收口为 businesses
// - 默认一条 settle_businesses 对应一条主 business_settlement

// financeBusinessFilter 业务查询过滤条件
// 设计说明：
//   - 主口径（唯一模型）：SourceType/SourceID/AccountingScene/AccountingSubtype
//   - 第六次迭代起新代码只使用主口径字段（旧列在已升级数据库中仍存在但不再引用）
//   - 所有查询统一使用主口径字段
//
// 第四阶段新增：BusinessRole 用于区分正式收费和过程财务对象
//   - formal：正式收费对象（如 biz_download_pool_*）
//   - process：过程财务对象（如 biz_c2c_open_* / biz_c2c_close_*）
//   - 空值：默认返回全部（保持向后兼容）
type financeBusinessFilter struct {
	Limit  int
	Offset int

	// 第四阶段新增：业务角色筛选
	BusinessRole string // "formal" | "process"

	// 主口径 - 唯一模型字段
	BusinessID        string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string

	Status      string
	FromPartyID string
	ToPartyID   string
	Query       string

	// 结算状态过滤：默认只看 confirmed；all 表示不过滤
	SettlementState string
}

type financeBusinessPage struct {
	Total int
	Items []financeBusinessItem
}

// financeBusinessItem 业务记录项
// 第六次迭代起只使用主口径字段
// 旧列在已升级数据库中仍存在，但新代码不再引用
// 第四阶段新增：BusinessRole 明确表达该记录是正式收费还是过程财务
type financeBusinessItem struct {
	BusinessID string `json:"business_id"`

	// 第四阶段新增：业务角色（formal | process）
	BusinessRole string `json:"business_role"`

	// 主口径 - 唯一模型字段
	SourceType        string `json:"source_type"`
	SourceID          string `json:"source_id"`
	AccountingScene   string `json:"accounting_scene"`
	AccountingSubtype string `json:"accounting_subtype"`

	FromPartyID    string          `json:"from_party_id"`
	ToPartyID      string          `json:"to_party_id"`
	Status         string          `json:"status"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

// financeProcessEventFilter 流程事件查询过滤条件
// 设计说明：
//   - 主口径（唯一模型）：SourceType/SourceID/AccountingScene/AccountingSubtype
//   - 第六次迭代起新代码只使用主口径字段（旧列在已升级数据库中仍存在但不再引用）
//   - 所有查询统一使用主口径字段
type financeProcessEventFilter struct {
	Limit  int
	Offset int

	// 主口径 - 唯一模型字段
	ProcessID         string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string

	EventType string
	Status    string
	Query     string

	// 结算状态过滤：默认只看 confirmed；all 表示不过滤
	SettlementState string
}

type financeProcessEventPage struct {
	Total int
	Items []financeProcessEventItem
}

// financeProcessEventItem 流程事件记录项
// 第六次迭代起只使用主口径字段
// 旧列在已升级数据库中仍存在，但新代码不再引用
type financeProcessEventItem struct {
	ID        int64  `json:"id"`
	ProcessID string `json:"process_id"`

	// 主口径 - 唯一模型字段
	SourceType        string `json:"source_type"`
	SourceID          string `json:"source_id"`
	AccountingScene   string `json:"accounting_scene"`
	AccountingSubtype string `json:"accounting_subtype"`

	EventType      string          `json:"event_type"`
	Status         string          `json:"status"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type financeBreakdownFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	Query      string

	// 结算状态过滤：默认只看 confirmed；all 表示不过滤
	SettlementState string
}

type financeBreakdownPage struct {
	Total int
	Items []financeBreakdownItem
}

type financeBreakdownItem struct {
	ID                 int64           `json:"id"`
	BusinessID         string          `json:"business_id"`
	TxID               string          `json:"txid"`
	TxRole             string          `json:"tx_role"`
	GrossInputSatoshi  int64           `json:"gross_input_satoshi"`
	ChangeBackSatoshi  int64           `json:"change_back_satoshi"`
	ExternalInSatoshi  int64           `json:"external_in_satoshi"`
	CounterpartyOutSat int64           `json:"counterparty_out_satoshi"`
	MinerFeeSatoshi    int64           `json:"miner_fee_satoshi"`
	NetOutSatoshi      int64           `json:"net_out_satoshi"`
	NetInSatoshi       int64           `json:"net_in_satoshi"`
	CreatedAtUnix      int64           `json:"created_at_unix"`
	Note               string          `json:"note"`
	Payload            json.RawMessage `json:"payload"`
}

type financeUTXOLinkFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	UTXOID     string
	TxRole     string
	IOSide     string
	UTXORole   string
	Query      string

	// 结算状态过滤：默认只看 confirmed；all 表示不过滤
	SettlementState string
}

type settlementCycleSourceResolution struct {
	SourceType           string
	SourceID             string
	SettlementCycleID    int64
	SettlementCycleState string
}

func resolvePoolAllocationSourceToSettlementCycle(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return resolvePoolAllocationSourceToSettlementCycleDB(db, sourceID)
	})
}

func resolveChainPaymentSourceToSettlementCycle(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return resolveChainPaymentSourceToSettlementCycleDB(db, sourceID)
	})
}

func dbGetSettlementCycleStateByID(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		return dbGetSettlementCycleStateByIDDB(db, id)
	})
}

func normalizeSettlementStateFilter(state string) (string, error) {
	state = strings.ToLower(strings.TrimSpace(state))
	switch state {
	case "":
		return "confirmed", nil
	case "confirmed", "pending", "failed":
		return state, nil
	case "all":
		return "all", nil
	default:
		return "", fmt.Errorf("settlement_state must be confirmed, pending, failed or all")
	}
}

// 财务查询只认 settlement_cycle；旧口径不在这里兼容。
func resolveSettlementCycleSourceDB(db *sql.DB, sourceType, sourceID string) (settlementCycleSourceResolution, error) {
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" && sourceID == "" {
		return settlementCycleSourceResolution{}, nil
	}
	if sourceType == "" {
		sourceType = "settlement_cycle"
	}
	if sourceType != "settlement_cycle" {
		return settlementCycleSourceResolution{}, fmt.Errorf("source_type must be settlement_cycle")
	}
	if sourceID == "" {
		return settlementCycleSourceResolution{SourceType: "settlement_cycle"}, nil
	}
	cycleID, err := strconv.ParseInt(sourceID, 10, 64)
	if err != nil || cycleID <= 0 {
		return settlementCycleSourceResolution{}, fmt.Errorf("settlement_cycle source_id must be a positive integer")
	}
	state, err := dbGetSettlementCycleStateByIDDB(db, cycleID)
	if err != nil {
		return settlementCycleSourceResolution{}, err
	}
	return settlementCycleSourceResolution{
		SourceType:           "settlement_cycle",
		SourceID:             fmt.Sprintf("%d", cycleID),
		SettlementCycleID:    cycleID,
		SettlementCycleState: state,
	}, nil
}

func resolvePoolAllocationSourceToSettlementCycleDB(db *sql.DB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	if allocID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && allocID > 0 {
		return dbGetSettlementCycleByPoolEvent(db, allocID)
	}
	var poolAllocID int64
	if err := db.QueryRow(`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`, sourceID).Scan(&poolAllocID); err != nil {
		return 0, err
	}
	if poolAllocID <= 0 {
		return 0, fmt.Errorf("pool allocation id is required")
	}
	return dbGetSettlementCycleByPoolEvent(db, poolAllocID)
}

func resolveChainPaymentSourceToSettlementCycleDB(db *sql.DB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	if paymentID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && paymentID > 0 {
		return dbGetSettlementCycleByChainPayment(db, paymentID)
	}
	var paymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, strings.ToLower(sourceID)).Scan(&paymentID); err != nil {
		return 0, err
	}
	return dbGetSettlementCycleByChainPayment(db, paymentID)
}

func resolveWalletChainSourceToSettlementCycleDB(db *sql.DB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	var paymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, strings.ToLower(sourceID)).Scan(&paymentID); err != nil {
		return 0, err
	}
	return dbGetSettlementCycleByChainPayment(db, paymentID)
}

func dbGetSettlementCycleStateByIDDB(db *sql.DB, id int64) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_cycle_id must be positive")
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM fact_settlement_cycles WHERE id=?`, id).Scan(&state); err != nil {
		return "", err
	}
	return strings.TrimSpace(state), nil
}

type financeUTXOLinkPage struct {
	Total int
	Items []financeUTXOLinkItem
}

type financeUTXOLinkItem struct {
	ID            int64           `json:"id"`
	BusinessID    string          `json:"business_id"`
	TxID          string          `json:"txid"`
	UTXOID        string          `json:"utxo_id"`
	TxRole        string          `json:"tx_role"`
	IOSide        string          `json:"io_side"`
	UTXORole      string          `json:"utxo_role"`
	AmountSatoshi int64           `json:"amount_satoshi"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	Note          string          `json:"note"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListFinanceBusinesses(ctx context.Context, store *clientDB, f financeBusinessFilter) (financeBusinessPage, error) {
	if store == nil {
		return financeBusinessPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessPage, error) {
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeBusinessPage{}, err
		}
		if f.SourceType != "" || f.SourceID != "" {
			resolved, err := resolveSettlementCycleSourceDB(db, f.SourceType, f.SourceID)
			if err != nil {
				return financeBusinessPage{}, err
			}
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}
		where := ""
		args := make([]any, 0, 16)
		where += " AND sb.source_type='settlement_cycle'"
		where += " AND sc.id = CAST(sb.source_id AS INTEGER)"
		if settlementState != "all" {
			where += " AND sc.state=?"
			args = append(args, settlementState)
		}

		// 第六阶段：business_role 成为唯一可信来源，不再使用前缀兜底
		// formal：正式收费对象
		// process：过程财务对象
		if f.BusinessRole == "formal" {
			where += " AND business_role='formal'"
		} else if f.BusinessRole == "process" {
			where += " AND business_role='process'"
		}

		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.SourceType != "" {
			where += " AND source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND source_id=?"
			args = append(args, f.SourceID)
		}
		if f.AccountingScene != "" {
			where += " AND accounting_scene=?"
			args = append(args, f.AccountingScene)
		}
		if f.AccountingSubtype != "" {
			where += " AND accounting_subtype=?"
			args = append(args, f.AccountingSubtype)
		}
		if f.SourceType != "" {
			where += " AND sb.source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND sb.source_id=?"
			args = append(args, f.SourceID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.FromPartyID != "" {
			where += " AND from_party_id=?"
			args = append(args, f.FromPartyID)
		}
		if f.ToPartyID != "" {
			where += " AND to_party_id=?"
			args = append(args, f.ToPartyID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (business_id LIKE ? OR sb.note LIKE ? OR idempotency_key LIKE ? OR source_type LIKE ? OR source_id LIKE ? OR accounting_scene LIKE ? OR accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeBusinessPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM settle_businesses sb
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE 1=1`+where,
			args...).Scan(&out.Total); err != nil {
			return financeBusinessPage{}, err
		}
		rows, err := db.Query(
			`SELECT sb.business_id,sb.business_role,sb.source_type,sb.source_id,sb.accounting_scene,sb.accounting_subtype,sb.from_party_id,sb.to_party_id,sb.status,sb.occurred_at_unix,sb.idempotency_key,sb.note,sb.payload_json
			 FROM settle_businesses sb
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			 WHERE 1=1`+where+` ORDER BY sb.occurred_at_unix DESC,sb.business_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBusinessPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBusinessItem, 0, f.Limit)
		for rows.Next() {
			var it financeBusinessItem
			var payload string
			if err := rows.Scan(&it.BusinessID, &it.BusinessRole, &it.SourceType, &it.SourceID, &it.AccountingScene, &it.AccountingSubtype, &it.FromPartyID, &it.ToPartyID, &it.Status, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeBusinessPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			// 第六阶段：business_role 直接来自数据库字段，不再运行时推断
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBusinessPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBusiness(ctx context.Context, store *clientDB, businessID string) (financeBusinessItem, error) {
	if store == nil {
		return financeBusinessItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessItem, error) {
		var out financeBusinessItem
		var payload string
		err := db.QueryRow(
			`SELECT sb.business_id,sb.business_role,sb.source_type,sb.source_id,sb.accounting_scene,sb.accounting_subtype,sb.from_party_id,sb.to_party_id,sb.status,sb.occurred_at_unix,sb.idempotency_key,sb.note,sb.payload_json
			 FROM settle_businesses sb
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			 WHERE sb.business_id=?`,
			businessID,
		).Scan(&out.BusinessID, &out.BusinessRole, &out.SourceType, &out.SourceID, &out.AccountingScene, &out.AccountingSubtype, &out.FromPartyID, &out.ToPartyID, &out.Status, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeBusinessItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceProcessEvents(ctx context.Context, store *clientDB, f financeProcessEventFilter) (financeProcessEventPage, error) {
	if store == nil {
		return financeProcessEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventPage, error) {
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		if f.SourceType != "" || f.SourceID != "" {
			resolved, err := resolveSettlementCycleSourceDB(db, f.SourceType, f.SourceID)
			if err != nil {
				return financeProcessEventPage{}, err
			}
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}
		where := ""
		args := make([]any, 0, 16)
		where += " AND pe.source_type='settlement_cycle'"
		where += " AND sc.id = CAST(pe.source_id AS INTEGER)"
		if settlementState != "all" {
			where += " AND sc.state=?"
			args = append(args, settlementState)
		}
		if f.ProcessID != "" {
			where += " AND process_id=?"
			args = append(args, f.ProcessID)
		}
		if f.SourceType != "" {
			where += " AND source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND source_id=?"
			args = append(args, f.SourceID)
		}
		if f.AccountingScene != "" {
			where += " AND accounting_scene=?"
			args = append(args, f.AccountingScene)
		}
		if f.AccountingSubtype != "" {
			where += " AND accounting_subtype=?"
			args = append(args, f.AccountingSubtype)
		}
		if f.SourceType != "" {
			where += " AND pe.source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND pe.source_id=?"
			args = append(args, f.SourceID)
		}
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (process_id LIKE ? OR note LIKE ? OR idempotency_key LIKE ? OR source_type LIKE ? OR source_id LIKE ? OR accounting_scene LIKE ? OR accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeProcessEventPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM settle_process_events pe
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(pe.source_id AS INTEGER)
			  WHERE 1=1`+where,
			args...).Scan(&out.Total); err != nil {
			return financeProcessEventPage{}, err
		}
		rows, err := db.Query(
			`SELECT pe.id,pe.process_id,pe.source_type,pe.source_id,pe.accounting_scene,pe.accounting_subtype,pe.event_type,pe.status,pe.occurred_at_unix,pe.idempotency_key,pe.note,pe.payload_json
			 FROM settle_process_events pe
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(pe.source_id AS INTEGER)
			 WHERE 1=1`+where+` ORDER BY pe.occurred_at_unix DESC,pe.id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeProcessEventItem, 0, f.Limit)
		for rows.Next() {
			var it financeProcessEventItem
			var payload string
			if err := rows.Scan(&it.ID, &it.ProcessID, &it.SourceType, &it.SourceID, &it.AccountingScene, &it.AccountingSubtype, &it.EventType, &it.Status, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeProcessEventPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeProcessEventPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceProcessEvent(ctx context.Context, store *clientDB, id int64) (financeProcessEventItem, error) {
	if store == nil {
		return financeProcessEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventItem, error) {
		var out financeProcessEventItem
		var payload string
		err := db.QueryRow(
			`SELECT pe.id,pe.process_id,pe.source_type,pe.source_id,pe.accounting_scene,pe.accounting_subtype,pe.event_type,pe.status,pe.occurred_at_unix,pe.idempotency_key,pe.note,pe.payload_json
			 FROM settle_process_events pe
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(pe.source_id AS INTEGER)
			 WHERE pe.id=?`,
			id,
		).Scan(&out.ID, &out.ProcessID, &out.SourceType, &out.SourceID, &out.AccountingScene, &out.AccountingSubtype, &out.EventType, &out.Status, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeProcessEventItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

// dbListFinanceBusinessesByPoolAllocationID 按 allocation_id 查财务业务
// 第十一阶段收口：增加 businessRole 参数，不再默认全量
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 fact_pool_session_events.id 过滤新口径记录；
// - businessRole 必填：formal 或 process，不再允许空值。
func dbListFinanceBusinessesByPoolAllocationID(ctx context.Context, store *clientDB, allocationID string, businessRole string, limit, offset int) (financeBusinessPage, error) {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return financeBusinessPage{}, fmt.Errorf("allocation_id is required")
	}
	businessRole = strings.TrimSpace(businessRole)
	if businessRole != "formal" && businessRole != "process" {
		return financeBusinessPage{}, fmt.Errorf("businessRole must be 'formal' or 'process'")
	}
	settlementCycleID, err := resolvePoolAllocationSourceToSettlementCycle(ctx, store, allocationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return financeBusinessPage{Items: []financeBusinessItem{}}, nil
		}
		return financeBusinessPage{}, err
	}
	return dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:           limit,
		Offset:          offset,
		BusinessRole:    businessRole,
		SourceType:      "settlement_cycle",
		SourceID:        fmt.Sprintf("%d", settlementCycleID),
		SettlementState: "confirmed",
	})
}

// dbListFinanceProcessEventsByPoolAllocationID 按 allocation_id 查流程事件
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 fact_pool_session_events.id 过滤新口径记录。
func dbListFinanceProcessEventsByPoolAllocationID(ctx context.Context, store *clientDB, allocationID string, limit, offset int) (financeProcessEventPage, error) {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return financeProcessEventPage{}, fmt.Errorf("allocation_id is required")
	}
	settlementCycleID, err := resolvePoolAllocationSourceToSettlementCycle(ctx, store, allocationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
		}
		return financeProcessEventPage{}, err
	}
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:           limit,
		Offset:          offset,
		SourceType:      "settlement_cycle",
		SourceID:        fmt.Sprintf("%d", settlementCycleID),
		SettlementState: "confirmed",
	})
}

// dbListFinanceBusinessesByTxID 按 txid 查财务业务
// 第十一阶段收口：增加 businessRole 参数，不再默认全量
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_chain_payments.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索；
// - businessRole 必填：formal 或 process，不再允许空值。
func dbListFinanceBusinessesByTxID(ctx context.Context, store *clientDB, txid string, businessRole string, limit, offset int) (financeBusinessPage, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return financeBusinessPage{}, fmt.Errorf("txid is required")
	}
	businessRole = strings.TrimSpace(businessRole)
	if businessRole != "formal" && businessRole != "process" {
		return financeBusinessPage{}, fmt.Errorf("businessRole must be 'formal' or 'process'")
	}
	settlementCycleID, err := resolveChainPaymentSourceToSettlementCycle(ctx, store, txid)
	if err != nil {
		return financeBusinessPage{Items: []financeBusinessItem{}}, nil
	}
	return dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:           limit,
		Offset:          offset,
		BusinessRole:    businessRole,
		SourceType:      "settlement_cycle",
		SourceID:        fmt.Sprintf("%d", settlementCycleID),
		SettlementState: "confirmed",
	})
}

// dbListFinanceProcessEventsByTxID 按 txid 查流程事件
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_chain_payments.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索。
func dbListFinanceProcessEventsByTxID(ctx context.Context, store *clientDB, txid string, limit, offset int) (financeProcessEventPage, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return financeProcessEventPage{}, fmt.Errorf("txid is required")
	}
	settlementCycleID, err := resolveChainPaymentSourceToSettlementCycle(ctx, store, txid)
	if err != nil {
		return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
	}
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:           limit,
		Offset:          offset,
		SourceType:      "settlement_cycle",
		SourceID:        fmt.Sprintf("%d", settlementCycleID),
		SettlementState: "confirmed",
	})
}

func dbListFinanceBreakdowns(ctx context.Context, store *clientDB, f financeBreakdownFilter) (financeBreakdownPage, error) {
	if store == nil {
		return financeBreakdownPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownPage, error) {
		where := ""
		args := make([]any, 0, 12)
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeBreakdownPage{}, err
		}
		if f.BusinessID != "" {
			where += " AND b.business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND b.txid=?"
			args = append(args, f.TxID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (b.business_id LIKE ? OR b.txid LIKE ? OR b.note LIKE ?)"
			args = append(args, like, like, like)
		}
		if settlementState != "all" {
			where += " AND sc.state=?"
			args = append(args, settlementState)
		}
		var out financeBreakdownPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM settle_tx_breakdown b
			   JOIN settle_businesses sb ON sb.business_id = b.business_id
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE sb.source_type='settlement_cycle' AND 1=1`+where,
			args...).Scan(&out.Total); err != nil {
			return financeBreakdownPage{}, err
		}
		rows, err := db.Query(
			`SELECT b.id,b.business_id,b.txid,b.tx_role,b.gross_input_satoshi,b.change_back_satoshi,b.external_in_satoshi,b.counterparty_out_satoshi,b.miner_fee_satoshi,b.net_out_satoshi,b.net_in_satoshi,b.created_at_unix,b.note,b.payload_json
			 FROM settle_tx_breakdown b
			 JOIN settle_businesses sb ON sb.business_id = b.business_id
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			 WHERE sb.source_type='settlement_cycle' AND 1=1`+where+` ORDER BY b.id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBreakdownPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBreakdownItem, 0, f.Limit)
		for rows.Next() {
			var it financeBreakdownItem
			var payload string
			if err := rows.Scan(
				&it.ID, &it.BusinessID, &it.TxID, &it.TxRole, &it.GrossInputSatoshi, &it.ChangeBackSatoshi, &it.ExternalInSatoshi, &it.CounterpartyOutSat,
				&it.MinerFeeSatoshi, &it.NetOutSatoshi, &it.NetInSatoshi, &it.CreatedAtUnix, &it.Note, &payload,
			); err != nil {
				return financeBreakdownPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBreakdownPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBreakdown(ctx context.Context, store *clientDB, id int64) (financeBreakdownItem, error) {
	if store == nil {
		return financeBreakdownItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownItem, error) {
		var out financeBreakdownItem
		var payload string
		err := db.QueryRow(
			`SELECT b.id,b.business_id,b.txid,b.tx_role,b.gross_input_satoshi,b.change_back_satoshi,b.external_in_satoshi,b.counterparty_out_satoshi,b.miner_fee_satoshi,b.net_out_satoshi,b.net_in_satoshi,b.created_at_unix,b.note,b.payload_json
			 FROM settle_tx_breakdown b
			 JOIN settle_businesses sb ON sb.business_id = b.business_id
			 JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			 WHERE sb.source_type='settlement_cycle' AND b.id=?`,
			id,
		).Scan(
			&out.ID, &out.BusinessID, &out.TxID, &out.TxRole, &out.GrossInputSatoshi, &out.ChangeBackSatoshi, &out.ExternalInSatoshi, &out.CounterpartyOutSat,
			&out.MinerFeeSatoshi, &out.NetOutSatoshi, &out.NetInSatoshi, &out.CreatedAtUnix, &out.Note, &payload,
		)
		if err != nil {
			return financeBreakdownItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceUTXOLinks(ctx context.Context, store *clientDB, f financeUTXOLinkFilter) (financeUTXOLinkPage, error) {
	if store == nil {
		return financeUTXOLinkPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkPage, error) {
		where := ""
		args := make([]any, 0, 12)
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeUTXOLinkPage{}, err
		}
		if f.BusinessID != "" {
			where += " AND l.business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND l.txid=?"
			args = append(args, f.TxID)
		}
		if f.UTXOID != "" {
			where += " AND l.utxo_id=?"
			args = append(args, f.UTXOID)
		}
		if f.TxRole != "" {
			where += " AND b.tx_role=?"
			args = append(args, f.TxRole)
		}
		if f.IOSide != "" {
			where += " AND l.io_side=?"
			args = append(args, f.IOSide)
		}
		if f.UTXORole != "" {
			where += " AND l.utxo_role=?"
			args = append(args, f.UTXORole)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (l.business_id LIKE ? OR l.txid LIKE ? OR l.utxo_id LIKE ? OR l.note LIKE ? OR b.tx_role LIKE ? OR l.utxo_role LIKE ?)"
			args = append(args, like, like, like, like, like, like)
		}
		if settlementState != "all" {
			where += " AND sc.state=?"
			args = append(args, settlementState)
		}
		var out financeUTXOLinkPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM settle_tx_utxo_links l
			   JOIN settle_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			   JOIN settle_businesses sb ON sb.business_id=l.business_id
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE sb.source_type='settlement_cycle' AND 1=1`+where,
			args...,
		).Scan(&out.Total); err != nil {
			return financeUTXOLinkPage{}, err
		}
		rows, err := db.Query(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,b.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM settle_tx_utxo_links l
			   JOIN settle_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			   JOIN settle_businesses sb ON sb.business_id=l.business_id
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE sb.source_type='settlement_cycle' AND 1=1`+where+` ORDER BY l.id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeUTXOLinkPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeUTXOLinkItem, 0, f.Limit)
		for rows.Next() {
			var it financeUTXOLinkItem
			var payload string
			if err := rows.Scan(&it.ID, &it.BusinessID, &it.TxID, &it.UTXOID, &it.TxRole, &it.IOSide, &it.UTXORole, &it.AmountSatoshi, &it.CreatedAtUnix, &it.Note, &payload); err != nil {
				return financeUTXOLinkPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeUTXOLinkPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceUTXOLink(ctx context.Context, store *clientDB, id int64) (financeUTXOLinkItem, error) {
	if store == nil {
		return financeUTXOLinkItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkItem, error) {
		var out financeUTXOLinkItem
		var payload string
		err := db.QueryRow(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,b.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM settle_tx_utxo_links l
			   JOIN settle_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			   JOIN settle_businesses sb ON sb.business_id=l.business_id
			   JOIN fact_settlement_cycles sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE sb.source_type='settlement_cycle' AND l.id=?`,
			id,
		).Scan(&out.ID, &out.BusinessID, &out.TxID, &out.UTXOID, &out.TxRole, &out.IOSide, &out.UTXORole, &out.AmountSatoshi, &out.CreatedAtUnix, &out.Note, &payload)
		if err != nil {
			return financeUTXOLinkItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}
