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

// settle_records 语义说明（一次性收口）：
// - 一条 settle_records 同时承载业务事实和结算出口
// - 失败重试不新建 business，只更新原记录
// - 退款、冲正、撤销如果产生新的资金动作，必须新建新的 business
// - 业务/结算查询都从 settle_records 出发，不再分表

// financeBusinessFilter 业务查询过滤条件
// 设计说明：
//   - 主口径（唯一模型）：SourceType/SourceID/AccountingScene/AccountingSubtype
//   - 第六次迭代起新代码只使用主口径字段（旧列在已升级数据库中仍存在但不再引用）
//   - 所有查询统一使用主口径字段
//
// 第四阶段新增：BusinessRole 用于区分正式收费和过程财务对象
//   - formal：正式收费对象（如 biz_download_pool_*）
//   - process：过程财务对象（如 biz_c2c_open_* / biz_c2c_close_*）
//   - 空值：默认返回全部（主口径查询）
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

type settlementCycleSourceResolution struct {
	SourceType           string
	SourceID             string
	SettlementPaymentAttemptID    int64
	SettlementPaymentAttemptState string
}

func resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return resolvePoolAllocationSourceToSettlementPaymentAttemptDB(ctx, db, sourceID)
	})
}

func resolveChainPaymentSourceToSettlementPaymentAttempt(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return resolveChainPaymentSourceToSettlementPaymentAttemptDB(ctx, db, sourceID)
	})
}

func dbGetSettlementPaymentAttemptStateByID(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		return dbGetSettlementPaymentAttemptStateByIDDB(ctx, db, id)
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

// 财务查询只认 settlement_payment_attempt；这里把空输入收口到主口径。
func resolveSettlementPaymentAttemptSourceDB(ctx context.Context, db *sql.DB, sourceType, sourceID string) (settlementCycleSourceResolution, error) {
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" && sourceID == "" {
		return settlementCycleSourceResolution{}, nil
	}
	if sourceType == "" {
		sourceType = "settlement_payment_attempt"
	}
	if sourceID == "" {
		return settlementCycleSourceResolution{SourceType: sourceType}, nil
	}
	var (
		paymentAttemptID int64
		err     error
	)
	switch sourceType {
	case "settlement_payment_attempt":
		paymentAttemptID, err = strconv.ParseInt(sourceID, 10, 64)
		if err != nil || paymentAttemptID <= 0 {
			return settlementCycleSourceResolution{}, fmt.Errorf("settlement_payment_attempt source_id must be a positive integer")
		}
	case "chain_quote_pay", "chain_direct_pay", "chain_asset_create", "pool_session_quote_pay":
		paymentAttemptID, err = dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, sourceType, sourceID)
		if err != nil {
			return settlementCycleSourceResolution{}, err
		}
	default:
		return settlementCycleSourceResolution{}, fmt.Errorf("source_type must be settlement_payment_attempt, pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create")
	}
	state, err := dbGetSettlementPaymentAttemptStateByIDDB(ctx, db, paymentAttemptID)
	if err != nil {
		return settlementCycleSourceResolution{}, err
	}
	return settlementCycleSourceResolution{
		SourceType:           sourceType,
		SourceID:             sourceID,
		SettlementPaymentAttemptID:    paymentAttemptID,
		SettlementPaymentAttemptState: state,
	}, nil
}

func dbGetSettlementPaymentAttemptByPoolSessionIDDB(ctx context.Context, db sqlConn, poolSessionID string) (int64, error) {
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return 0, fmt.Errorf("pool_session_id is required")
	}
	var channelID int64
	if err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_channel_pool_session_quote_pay WHERE pool_session_id=?`, poolSessionID).Scan(&channelID); err != nil {
		return 0, err
	}
	return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "pool_session_quote_pay", fmt.Sprintf("%d", channelID))
}

func resolvePoolAllocationSourceToSettlementPaymentAttemptDB(ctx context.Context, db *sql.DB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	if allocID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && allocID > 0 {
		var poolSessionID string
		if err := QueryRowContext(ctx, db, `SELECT pool_session_id FROM fact_pool_session_events WHERE id=?`, allocID).Scan(&poolSessionID); err != nil {
			return 0, err
		}
		return dbGetSettlementPaymentAttemptByPoolSessionIDDB(ctx, db, poolSessionID)
	}
	var poolSessionID string
	if err := QueryRowContext(ctx, db, `SELECT pool_session_id FROM fact_pool_session_events WHERE allocation_id=?`, sourceID).Scan(&poolSessionID); err != nil {
		return 0, err
	}
	return dbGetSettlementPaymentAttemptByPoolSessionIDDB(ctx, db, poolSessionID)
}

func resolveChainPaymentSourceToSettlementPaymentAttemptDB(ctx context.Context, db *sql.DB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	for _, sourceType := range []string{"chain_quote_pay", "chain_direct_pay", "chain_asset_create"} {
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, sourceType, strings.TrimSpace(sourceID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	txid := strings.ToLower(strings.TrimSpace(sourceID))
	if txid != "" {
		var channelID int64
		if err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txid).Scan(&channelID); err == nil {
			return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_quote_pay", fmt.Sprintf("%d", channelID))
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_channel_chain_direct_pay WHERE txid=?`, txid).Scan(&channelID); err == nil {
			return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_direct_pay", fmt.Sprintf("%d", channelID))
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_channel_chain_asset_create WHERE txid=?`, txid).Scan(&channelID); err == nil {
			return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_asset_create", fmt.Sprintf("%d", channelID))
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	if paymentID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && paymentID > 0 {
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_quote_pay", fmt.Sprintf("%d", paymentID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_direct_pay", fmt.Sprintf("%d", paymentID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, "chain_asset_create", fmt.Sprintf("%d", paymentID))
	}
	return 0, sql.ErrNoRows
}

func dbGetSettlementPaymentAttemptStateByIDDB(ctx context.Context, db *sql.DB, id int64) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	var state string
	if err := QueryRowContext(ctx, db, `SELECT state FROM fact_settlement_payment_attempts WHERE id=?`, id).Scan(&state); err != nil {
		return "", err
	}
	return strings.TrimSpace(state), nil
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
			resolved, err := resolveSettlementPaymentAttemptSourceDB(ctx, db, f.SourceType, f.SourceID)
			if err != nil {
				return financeBusinessPage{}, err
			}
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}
		where := ""
		args := make([]any, 0, 16)
		where += " AND sb.source_type='settlement_payment_attempt'"
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
			where += " AND sb.business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.SourceType != "" {
			where += " AND sb.source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND sb.source_id=?"
			args = append(args, f.SourceID)
		}
		if f.AccountingScene != "" {
			where += " AND sb.accounting_scene=?"
			args = append(args, f.AccountingScene)
		}
		if f.AccountingSubtype != "" {
			where += " AND sb.accounting_subtype=?"
			args = append(args, f.AccountingSubtype)
		}
		if f.Status != "" {
			where += " AND sb.status=?"
			args = append(args, f.Status)
		}
		if f.FromPartyID != "" {
			where += " AND sb.from_party_id=?"
			args = append(args, f.FromPartyID)
		}
		if f.ToPartyID != "" {
			where += " AND sb.to_party_id=?"
			args = append(args, f.ToPartyID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (sb.business_id LIKE ? OR sb.note LIKE ? OR sb.idempotency_key LIKE ? OR sb.source_type LIKE ? OR sb.source_id LIKE ? OR sb.accounting_scene LIKE ? OR sb.accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeBusinessPage
		if err := QueryRowContext(ctx, db,
			`SELECT COUNT(1)
			   FROM settle_records sb
			   JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(sb.source_id AS INTEGER)
			  WHERE 1=1`+where,
			args...).Scan(&out.Total); err != nil {
			return financeBusinessPage{}, err
		}
		rows, err := QueryContext(ctx, db,
			`SELECT sb.business_id,sb.business_role,sb.source_type,sb.source_id,sb.accounting_scene,sb.accounting_subtype,sb.from_party_id,sb.to_party_id,sb.status,sb.occurred_at_unix,sb.idempotency_key,sb.note,sb.payload_json
			 FROM settle_records sb
			 JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(sb.source_id AS INTEGER)
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
		err := QueryRowContext(ctx, db,
			`SELECT sb.business_id,sb.business_role,sb.source_type,sb.source_id,sb.accounting_scene,sb.accounting_subtype,sb.from_party_id,sb.to_party_id,sb.status,sb.occurred_at_unix,sb.idempotency_key,sb.note,sb.payload_json
			 FROM settle_records sb
			 JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(sb.source_id AS INTEGER)
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
			resolved, err := resolveSettlementPaymentAttemptSourceDB(ctx, db, f.SourceType, f.SourceID)
			if err != nil {
				return financeProcessEventPage{}, err
			}
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}
		where := ""
		args := make([]any, 0, 16)
		where += " AND pe.source_type='settlement_payment_attempt'"
		where += " AND sc.id = CAST(pe.source_id AS INTEGER)"
		if settlementState != "all" {
			where += " AND sc.state=?"
			args = append(args, settlementState)
		}
		if f.ProcessID != "" {
			where += " AND pe.process_id=?"
			args = append(args, f.ProcessID)
		}
		if f.SourceType != "" {
			where += " AND pe.source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND pe.source_id=?"
			args = append(args, f.SourceID)
		}
		if f.AccountingScene != "" {
			where += " AND pe.accounting_scene=?"
			args = append(args, f.AccountingScene)
		}
		if f.AccountingSubtype != "" {
			where += " AND pe.accounting_subtype=?"
			args = append(args, f.AccountingSubtype)
		}
		if f.EventType != "" {
			where += " AND pe.event_type=?"
			args = append(args, f.EventType)
		}
		if f.Status != "" {
			where += " AND pe.status=?"
			args = append(args, f.Status)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (pe.process_id LIKE ? OR pe.note LIKE ? OR pe.idempotency_key LIKE ? OR pe.source_type LIKE ? OR pe.source_id LIKE ? OR pe.accounting_scene LIKE ? OR pe.accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeProcessEventPage
		if err := QueryRowContext(ctx, db,
			`SELECT COUNT(1)
			   FROM settle_process_events pe
			   JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(pe.source_id AS INTEGER)
			  WHERE 1=1`+where,
			args...).Scan(&out.Total); err != nil {
			return financeProcessEventPage{}, err
		}
		rows, err := QueryContext(ctx, db,
			`SELECT pe.id,pe.process_id,pe.source_type,pe.source_id,pe.accounting_scene,pe.accounting_subtype,pe.event_type,pe.status,pe.occurred_at_unix,pe.idempotency_key,pe.note,pe.payload_json
			 FROM settle_process_events pe
			 JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(pe.source_id AS INTEGER)
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
		err := QueryRowContext(ctx, db,
			`SELECT pe.id,pe.process_id,pe.source_type,pe.source_id,pe.accounting_scene,pe.accounting_subtype,pe.event_type,pe.status,pe.occurred_at_unix,pe.idempotency_key,pe.note,pe.payload_json
			 FROM settle_process_events pe
			 JOIN fact_settlement_payment_attempts sc ON sc.id = CAST(pe.source_id AS INTEGER)
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
	settlementPaymentAttemptID, err := resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx, store, allocationID)
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
		SourceType:      "settlement_payment_attempt",
		SourceID:        fmt.Sprintf("%d", settlementPaymentAttemptID),
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
	settlementPaymentAttemptID, err := resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx, store, allocationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
		}
		return financeProcessEventPage{}, err
	}
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:           limit,
		Offset:          offset,
		SourceType:      "settlement_payment_attempt",
		SourceID:        fmt.Sprintf("%d", settlementPaymentAttemptID),
		SettlementState: "confirmed",
	})
}

// dbListFinanceBusinessesByTxID 按 txid 查财务业务
// 第十一阶段收口：增加 businessRole 参数，不再默认全量
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_settlement_channel_chain_quote_pay.id 过滤新口径记录；
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
	settlementPaymentAttemptID, err := resolveChainPaymentSourceToSettlementPaymentAttempt(ctx, store, txid)
	if err != nil {
		return financeBusinessPage{Items: []financeBusinessItem{}}, nil
	}
	return dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:           limit,
		Offset:          offset,
		BusinessRole:    businessRole,
		SourceType:      "settlement_payment_attempt",
		SourceID:        fmt.Sprintf("%d", settlementPaymentAttemptID),
		SettlementState: "confirmed",
	})
}

// dbListFinanceProcessEventsByTxID 按 txid 查流程事件
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_settlement_channel_chain_quote_pay.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索。
func dbListFinanceProcessEventsByTxID(ctx context.Context, store *clientDB, txid string, limit, offset int) (financeProcessEventPage, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return financeProcessEventPage{}, fmt.Errorf("txid is required")
	}
	settlementPaymentAttemptID, err := resolveChainPaymentSourceToSettlementPaymentAttempt(ctx, store, txid)
	if err != nil {
		return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
	}
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:           limit,
		Offset:          offset,
		SourceType:      "settlement_payment_attempt",
		SourceID:        fmt.Sprintf("%d", settlementPaymentAttemptID),
		SettlementState: "confirmed",
	})
}
