package clientapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSchemaDeleted 表示底层结算层 schema 已删除
	ErrSchemaDeleted = errors.New("settlement layer schema deleted")
)

// order_settlements 语义说明（一次性收口）：
// - 一条 order_settlements 同时承载业务事实和结算出口
// - 失败重试不新建 order，只更新原记录
// - 退款、冲正、撤销如果产生新的资金动作，必须新建新的 order
// - 业务/结算查询都从 order_settlements 出发，不再分表

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
	OrderID           string
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
	OrderID string `json:"order_id"`

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
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type settlementCycleSourceResolution struct {
	SourceType                    string
	SourceID                      string
	SettlementPaymentAttemptID    int64
	SettlementPaymentAttemptState string
}

func resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return 0, ErrSchemaDeleted
}

func resolveChainPaymentSourceToSettlementPaymentAttempt(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return 0, ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptStateByID(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return "", ErrSchemaDeleted
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

func resolveSettlementPaymentAttemptSourceStore(ctx context.Context, store *clientDB, sourceType, sourceID string) (settlementCycleSourceResolution, error) {
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
	return settlementCycleSourceResolution{}, ErrSchemaDeleted
}

func resolvePoolAllocationSourceToSettlementPaymentAttemptEntRoot(ctx context.Context, root EntReadRoot, sourceID string) (int64, error) {
	if root == nil {
		return 0, fmt.Errorf("root is nil")
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return 0, ErrSchemaDeleted
}

func resolveChainPaymentSourceToSettlementPaymentAttemptEntRoot(ctx context.Context, root EntReadRoot, sourceID string) (int64, error) {
	if root == nil {
		return 0, fmt.Errorf("root is nil")
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return 0, ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptByPoolSessionIDEntRoot(ctx context.Context, root EntReadRoot, poolSessionID string) (int64, error) {
	if root == nil {
		return 0, fmt.Errorf("root is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return 0, fmt.Errorf("pool_session_id is required")
	}
	return 0, ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptStateByIDEntRoot(ctx context.Context, root EntReadRoot, id int64) (string, error) {
	if root == nil {
		return "", fmt.Errorf("root is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	return "", ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptStateByIDStore(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	return "", ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptBySourceStore(ctx context.Context, store *clientDB, sourceType, sourceID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	return 0, ErrSchemaDeleted
}

func dbGetSettlementPaymentAttemptByPoolSessionIDStore(ctx context.Context, store *clientDB, poolSessionID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return 0, fmt.Errorf("pool_session_id is required")
	}
	return 0, ErrSchemaDeleted
}

func dbListFinanceBusinesses(ctx context.Context, store *clientDB, f financeBusinessFilter) (financeBusinessPage, error) {
	if store == nil {
		return financeBusinessPage{}, fmt.Errorf("client db is nil")
	}
	return financeBusinessPage{}, ErrSchemaDeleted
}

func dbGetFinanceBusiness(ctx context.Context, store *clientDB, businessID string) (financeBusinessItem, error) {
	if store == nil {
		return financeBusinessItem{}, fmt.Errorf("client db is nil")
	}
	return financeBusinessItem{}, ErrSchemaDeleted
}

func dbListFinanceProcessEvents(ctx context.Context, store *clientDB, f financeProcessEventFilter) (financeProcessEventPage, error) {
	if store == nil {
		return financeProcessEventPage{}, fmt.Errorf("client db is nil")
	}
	return financeProcessEventPage{}, ErrSchemaDeleted
}

func dbGetFinanceProcessEvent(ctx context.Context, store *clientDB, id int64) (financeProcessEventItem, error) {
	if store == nil {
		return financeProcessEventItem{}, fmt.Errorf("client db is nil")
	}
	return financeProcessEventItem{}, ErrSchemaDeleted
}

// dbListFinanceBusinessesByPoolAllocationID 按 allocation_id 查财务业务
// 第十一阶段收口：增加 businessRole 参数，不再默认全量
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 fact_pool_session_events.id 过滤新口径记录；
// - businessRole 必填：formal 或 process，不再允许空值。

// dbListFinanceProcessEventsByPoolAllocationID 按 allocation_id 查流程事件
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 fact_pool_session_events.id 过滤新口径记录。

// dbListFinanceBusinessesByTxID 按 txid 查财务业务
// 第十一阶段收口：增加 businessRole 参数，不再默认全量
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_settlement_channel_chain_quote_pay.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索；
// - businessRole 必填：formal 或 process，不再允许空值。

// dbListFinanceProcessEventsByTxID 按 txid 查流程事件
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 fact_settlement_channel_chain_quote_pay.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索。
