package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factpoolsessionevents"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelchainassetcreate"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelchaindirectpay"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelchainquotepay"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelpoolsessionquotepay"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementpaymentattempts"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/ordersettlementevents"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/ordersettlements"
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		return resolvePoolAllocationSourceToSettlementPaymentAttemptEntTx(ctx, tx, sourceID)
	})
}

func resolveChainPaymentSourceToSettlementPaymentAttempt(ctx context.Context, store *clientDB, sourceID string) (int64, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		return resolveChainPaymentSourceToSettlementPaymentAttemptEntTx(ctx, tx, sourceID)
	})
}

func dbGetSettlementPaymentAttemptStateByID(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		return dbGetSettlementPaymentAttemptStateByIDEntTx(ctx, tx, id)
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
	var (
		paymentAttemptID int64
		err              error
	)
	switch sourceType {
	case "settlement_payment_attempt":
		paymentAttemptID, err = strconv.ParseInt(sourceID, 10, 64)
		if err != nil || paymentAttemptID <= 0 {
			return settlementCycleSourceResolution{}, fmt.Errorf("settlement_payment_attempt source_id must be a positive integer")
		}
	case "pool_session_quote_pay", "chain_quote_pay", "chain_direct_pay", "chain_asset_create":
		paymentAttemptID, err = dbGetSettlementPaymentAttemptBySourceStore(ctx, store, sourceType, sourceID)
		if err != nil {
			return settlementCycleSourceResolution{}, err
		}
	default:
		return settlementCycleSourceResolution{}, fmt.Errorf("source_type must be settlement_payment_attempt, pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create")
	}
	state, err := dbGetSettlementPaymentAttemptStateByIDStore(ctx, store, paymentAttemptID)
	if err != nil {
		return settlementCycleSourceResolution{}, err
	}
	return settlementCycleSourceResolution{
		SourceType:                    "settlement_payment_attempt",
		SourceID:                      fmt.Sprintf("%d", paymentAttemptID),
		SettlementPaymentAttemptID:    paymentAttemptID,
		SettlementPaymentAttemptState: state,
	}, nil
}

func resolvePoolAllocationSourceToSettlementPaymentAttemptEntTx(ctx context.Context, tx *gen.Tx, sourceID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	if allocID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && allocID > 0 {
		row, err := tx.FactPoolSessionEvents.Query().
			Where(factpoolsessionevents.IDEQ(int(allocID))).
			Only(ctx)
		if err != nil {
			return 0, err
		}
		return dbGetSettlementPaymentAttemptByPoolSessionIDEntTx(ctx, tx, row.PoolSessionID)
	}
	row, err := tx.FactPoolSessionEvents.Query().
		Where(factpoolsessionevents.AllocationIDEQ(sourceID)).
		Only(ctx)
	if err != nil {
		return 0, err
	}
	return dbGetSettlementPaymentAttemptByPoolSessionIDEntTx(ctx, tx, row.PoolSessionID)
}

func resolveChainPaymentSourceToSettlementPaymentAttemptEntTx(ctx context.Context, tx *gen.Tx, sourceID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return 0, fmt.Errorf("source_id is required")
	}
	for _, sourceType := range []string{"chain_quote_pay", "chain_direct_pay", "chain_asset_create"} {
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, sourceType, strings.TrimSpace(sourceID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	txid := strings.ToLower(strings.TrimSpace(sourceID))
	if txid != "" {
		if channel, err := tx.FactSettlementChannelChainQuotePay.Query().
			Where(factsettlementchannelchainquotepay.TxidEQ(txid)).
			Only(ctx); err == nil {
			return dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_quote_pay", fmt.Sprintf("%d", channel.ID))
		} else if !gen.IsNotFound(err) {
			return 0, err
		}
		if channel, err := tx.FactSettlementChannelChainDirectPay.Query().
			Where(factsettlementchannelchaindirectpay.TxidEQ(txid)).
			Only(ctx); err == nil {
			return dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_direct_pay", fmt.Sprintf("%d", channel.ID))
		} else if !gen.IsNotFound(err) {
			return 0, err
		}
		if channel, err := tx.FactSettlementChannelChainAssetCreate.Query().
			Where(factsettlementchannelchainassetcreate.TxidEQ(txid)).
			Only(ctx); err == nil {
			return dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_asset_create", fmt.Sprintf("%d", channel.ID))
		} else if !gen.IsNotFound(err) {
			return 0, err
		}
	}
	if paymentID, err := strconv.ParseInt(sourceID, 10, 64); err == nil && paymentID > 0 {
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_quote_pay", fmt.Sprintf("%d", paymentID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if paymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_direct_pay", fmt.Sprintf("%d", paymentID)); err == nil {
			return paymentAttemptID, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		return dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_asset_create", fmt.Sprintf("%d", paymentID))
	}
	return 0, sql.ErrNoRows
}

func dbGetSettlementPaymentAttemptByPoolSessionIDEntTx(ctx context.Context, tx *gen.Tx, poolSessionID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return 0, fmt.Errorf("pool_session_id is required")
	}
	node, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
		Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(poolSessionID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return 0, sql.ErrNoRows
		}
		return 0, err
	}
	return node.SettlementPaymentAttemptID, nil
}

func dbGetSettlementPaymentAttemptStateByIDEntTx(ctx context.Context, tx *gen.Tx, id int64) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("tx is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	node, err := tx.FactSettlementPaymentAttempts.Query().
		Where(factsettlementpaymentattempts.IDEQ(id)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return "", sql.ErrNoRows
		}
		return "", err
	}
	return strings.TrimSpace(node.State), nil
}

func dbGetSettlementPaymentAttemptStateByIDStore(ctx context.Context, store *clientDB, id int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	if id <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		node, err := tx.FactSettlementPaymentAttempts.Query().
			Where(factsettlementpaymentattempts.IDEQ(id)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", sql.ErrNoRows
			}
			return "", err
		}
		return strings.TrimSpace(node.State), nil
	})
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		node, err := tx.FactSettlementPaymentAttempts.Query().
			Where(
				factsettlementpaymentattempts.SourceTypeEQ(sourceType),
				factsettlementpaymentattempts.SourceIDEQ(sourceID),
			).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, fmt.Errorf("%w: settlement payment attempt not found for %s:%s", sql.ErrNoRows, sourceType, sourceID)
			}
			return 0, err
		}
		return node.ID, nil
	})
}

func dbGetSettlementPaymentAttemptByPoolSessionIDStore(ctx context.Context, store *clientDB, poolSessionID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return 0, fmt.Errorf("pool_session_id is required")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		node, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
			Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(poolSessionID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, sql.ErrNoRows
			}
			return 0, err
		}
		return node.SettlementPaymentAttemptID, nil
	})
}

// dbUpsertSettlementPaymentAttemptEntTx 在 ent 事务内幂等写入结算周期。
// 设计说明：
// - 和 store 版本保持同一校验规则；
// - 让调用方在同一事务里完成 channel / attempt 双写。
func dbUpsertSettlementPaymentAttemptEntTx(ctx context.Context, tx *gen.Tx, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	if paymentAttemptID == "" {
		return 0, fmt.Errorf("payment_attempt_id is required")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	switch sourceType {
	case "pool_session_quote_pay", "chain_quote_pay", "chain_direct_pay", "chain_asset_create":
	default:
		return 0, fmt.Errorf("source_type must be pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create, got %s", sourceType)
	}
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "failed" {
		return 0, fmt.Errorf("state must be pending/confirmed/failed, got %s", state)
	}
	now := time.Now().Unix()
	occurredAt := occurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	confirmedAt := int64(0)
	if state == "confirmed" {
		confirmedAt = occurredAt
	}

	existing, err := tx.FactSettlementPaymentAttempts.Query().
		Where(
			factsettlementpaymentattempts.SourceTypeEQ(sourceType),
			factsettlementpaymentattempts.SourceIDEQ(sourceID),
		).
		Only(ctx)
	if err == nil {
		nextState := state
		if existing.State == "confirmed" && state == "pending" {
			nextState = existing.State
		}
		nextConfirmedAt := existing.ConfirmedAtUnix
		if state == "confirmed" {
			nextConfirmedAt = occurredAt
		}
		_, err = tx.FactSettlementPaymentAttempts.UpdateOneID(existing.ID).
			SetSourceType(sourceType).
			SetSourceID(sourceID).
			SetState(nextState).
			SetGrossAmountSatoshi(grossSatoshi).
			SetGateFeeSatoshi(gateFeeSatoshi).
			SetNetAmountSatoshi(netSatoshi).
			SetCycleIndex(int64(paymentAttemptIndex)).
			SetOccurredAtUnix(occurredAt).
			SetConfirmedAtUnix(nextConfirmedAt).
			SetNote(strings.TrimSpace(note)).
			SetPayloadJSON(mustJSONString(payload)).
			Save(ctx)
		return existing.ID, err
	}
	if err != nil && !gen.IsNotFound(err) {
		return 0, err
	}
	node, err := tx.FactSettlementPaymentAttempts.Create().
		SetPaymentAttemptID(paymentAttemptID).
		SetSourceType(sourceType).
		SetSourceID(sourceID).
		SetState(state).
		SetGrossAmountSatoshi(grossSatoshi).
		SetGateFeeSatoshi(gateFeeSatoshi).
		SetNetAmountSatoshi(netSatoshi).
		SetCycleIndex(int64(paymentAttemptIndex)).
		SetOccurredAtUnix(occurredAt).
		SetConfirmedAtUnix(confirmedAt).
		SetNote(strings.TrimSpace(note)).
		SetPayloadJSON(mustJSONString(payload)).
		Save(ctx)
	if err != nil {
		return 0, err
	}
	return node.ID, nil
}

// dbGetSettlementPaymentAttemptBySourceEntTx 在 ent 事务内按 source_type/source_id 查询主键。
func dbGetSettlementPaymentAttemptBySourceEntTx(ctx context.Context, tx *gen.Tx, sourceType string, sourceID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	node, err := tx.FactSettlementPaymentAttempts.Query().
		Where(
			factsettlementpaymentattempts.SourceTypeEQ(sourceType),
			factsettlementpaymentattempts.SourceIDEQ(sourceID),
		).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return 0, fmt.Errorf("%w: settlement payment attempt not found for %s:%s", sql.ErrNoRows, sourceType, sourceID)
		}
		return 0, err
	}
	return node.ID, nil
}

// dbGetSettlementPaymentAttemptSourceTxIDEntTx 在 ent 事务内反查来源 txid。
func dbGetSettlementPaymentAttemptSourceTxIDEntTx(ctx context.Context, tx *gen.Tx, settlementPaymentAttemptID int64) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("tx is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id is required")
	}
	node, err := tx.FactSettlementPaymentAttempts.Query().
		Where(factsettlementpaymentattempts.IDEQ(settlementPaymentAttemptID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return "", fmt.Errorf("settlement payment attempt not found: %d", settlementPaymentAttemptID)
		}
		return "", err
	}
	switch strings.ToLower(strings.TrimSpace(node.SourceType)) {
	case "chain_quote_pay":
		channel, err := tx.FactSettlementChannelChainQuotePay.Query().
			Where(factsettlementchannelchainquotepay.SettlementPaymentAttemptIDEQ(settlementPaymentAttemptID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(channel.Txid)), nil
	case "chain_direct_pay":
		channel, err := tx.FactSettlementChannelChainDirectPay.Query().
			Where(factsettlementchannelchaindirectpay.SettlementPaymentAttemptIDEQ(settlementPaymentAttemptID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(channel.Txid)), nil
	case "chain_asset_create":
		channel, err := tx.FactSettlementChannelChainAssetCreate.Query().
			Where(factsettlementchannelchainassetcreate.SettlementPaymentAttemptIDEQ(settlementPaymentAttemptID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(channel.Txid)), nil
	case "pool_session_quote_pay":
		channel, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
			Where(factsettlementchannelpoolsessionquotepay.SettlementPaymentAttemptIDEQ(settlementPaymentAttemptID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(channel.Txid)), nil
	default:
		return "", fmt.Errorf("settlement payment attempt %d source_type %s cannot derive txid", settlementPaymentAttemptID, strings.TrimSpace(node.SourceType))
	}
}

func dbListFinanceBusinesses(ctx context.Context, store *clientDB, f financeBusinessFilter) (financeBusinessPage, error) {
	if store == nil {
		return financeBusinessPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (financeBusinessPage, error) {
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeBusinessPage{}, err
		}
		resolved, err := resolveSettlementPaymentAttemptSourceStore(ctx, store, f.SourceType, f.SourceID)
		if err != nil {
			return financeBusinessPage{}, err
		}
		if resolved.SourceType != "" {
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}

		nodes, err := tx.OrderSettlements.Query().
			Where(
				ordersettlements.SourceTypeEQ("settlement_payment_attempt"),
			).
			Order(ordersettlements.ByCreatedAtUnix(entsql.OrderDesc()), ordersettlements.ByOrderID(entsql.OrderDesc())).
			All(ctx)
		if err != nil {
			return financeBusinessPage{}, err
		}
		attemptIDs := make([]int64, 0, len(nodes))
		for _, n := range nodes {
			if id, parseErr := strconv.ParseInt(strings.TrimSpace(n.SourceID), 10, 64); parseErr == nil && id > 0 {
				attemptIDs = append(attemptIDs, id)
			}
		}
		attemptState := make(map[int64]string, len(attemptIDs))
		if len(attemptIDs) > 0 {
			attempts, err := tx.FactSettlementPaymentAttempts.Query().
				Where(factsettlementpaymentattempts.IDIn(attemptIDs...)).
				All(ctx)
			if err != nil {
				return financeBusinessPage{}, err
			}
			for _, a := range attempts {
				attemptState[a.ID] = strings.TrimSpace(a.State)
			}
		}

		filtered := make([]financeBusinessItem, 0, len(nodes))
		for _, n := range nodes {
			item := orderSettlementToFinanceBusinessItem(n)
			if f.BusinessRole == "formal" && item.BusinessRole != "formal" {
				continue
			}
			if f.BusinessRole == "process" && item.BusinessRole != "process" {
				continue
			}
			if f.OrderID != "" && item.OrderID != f.OrderID {
				continue
			}
			if f.SourceType != "" && item.SourceType != f.SourceType {
				continue
			}
			if f.SourceID != "" && item.SourceID != f.SourceID {
				continue
			}
			if f.AccountingScene != "" && item.AccountingScene != f.AccountingScene {
				continue
			}
			if f.AccountingSubtype != "" && item.AccountingSubtype != f.AccountingSubtype {
				continue
			}
			if f.Status != "" && item.Status != f.Status {
				continue
			}
			if f.FromPartyID != "" && item.FromPartyID != f.FromPartyID {
				continue
			}
			if f.ToPartyID != "" && item.ToPartyID != f.ToPartyID {
				continue
			}
			if f.Query != "" {
				like := strings.ToLower(strings.TrimSpace(f.Query))
				hit := strings.Contains(strings.ToLower(item.OrderID), like) ||
					strings.Contains(strings.ToLower(item.Note), like) ||
					strings.Contains(strings.ToLower(item.IdempotencyKey), like) ||
					strings.Contains(strings.ToLower(item.SourceType), like) ||
					strings.Contains(strings.ToLower(item.SourceID), like) ||
					strings.Contains(strings.ToLower(item.AccountingScene), like) ||
					strings.Contains(strings.ToLower(item.AccountingSubtype), like)
				if !hit {
					continue
				}
			}
			if settlementState != "all" {
				id, parseErr := strconv.ParseInt(strings.TrimSpace(item.SourceID), 10, 64)
				if parseErr != nil || attemptState[id] != settlementState {
					continue
				}
			}
			filtered = append(filtered, item)
		}

		var out financeBusinessPage
		out.Total = len(filtered)
		if f.Limit <= 0 {
			f.Limit = 20
		}
		if f.Offset < 0 {
			f.Offset = 0
		}
		if f.Offset < len(filtered) {
			end := f.Offset + f.Limit
			if end > len(filtered) {
				end = len(filtered)
			}
			out.Items = append(out.Items, filtered[f.Offset:end]...)
		} else {
			out.Items = []financeBusinessItem{}
		}
		return out, nil
	})
}

func dbGetFinanceBusiness(ctx context.Context, store *clientDB, businessID string) (financeBusinessItem, error) {
	if store == nil {
		return financeBusinessItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (financeBusinessItem, error) {
		node, err := tx.OrderSettlements.Query().
			Where(ordersettlements.OrderIDEQ(strings.TrimSpace(businessID))).
			Order(ordersettlements.ByCreatedAtUnix(entsql.OrderDesc()), ordersettlements.BySettlementID(entsql.OrderDesc())).
			First(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return financeBusinessItem{}, sql.ErrNoRows
			}
			return financeBusinessItem{}, err
		}
		return orderSettlementToFinanceBusinessItem(node), nil
	})
}

func dbListFinanceProcessEvents(ctx context.Context, store *clientDB, f financeProcessEventFilter) (financeProcessEventPage, error) {
	if store == nil {
		return financeProcessEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (financeProcessEventPage, error) {
		settlementState, err := normalizeSettlementStateFilter(f.SettlementState)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		resolved, err := resolveSettlementPaymentAttemptSourceStore(ctx, store, f.SourceType, f.SourceID)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		if resolved.SourceType != "" {
			f.SourceType = resolved.SourceType
			f.SourceID = resolved.SourceID
		}

		nodes, err := tx.OrderSettlementEvents.Query().
			Where(ordersettlementevents.SourceTypeEQ("settlement_payment_attempt")).
			Order(ordersettlementevents.ByOccurredAtUnix(entsql.OrderDesc()), ordersettlementevents.ByID(entsql.OrderDesc())).
			All(ctx)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		attemptIDs := make([]int64, 0, len(nodes))
		for _, n := range nodes {
			if id, parseErr := strconv.ParseInt(strings.TrimSpace(n.SourceID), 10, 64); parseErr == nil && id > 0 {
				attemptIDs = append(attemptIDs, id)
			}
		}
		attemptState := make(map[int64]string, len(attemptIDs))
		if len(attemptIDs) > 0 {
			attempts, err := tx.FactSettlementPaymentAttempts.Query().
				Where(factsettlementpaymentattempts.IDIn(attemptIDs...)).
				All(ctx)
			if err != nil {
				return financeProcessEventPage{}, err
			}
			for _, a := range attempts {
				attemptState[a.ID] = strings.TrimSpace(a.State)
			}
		}

		filtered := make([]financeProcessEventItem, 0, len(nodes))
		for _, n := range nodes {
			item := financeProcessEventItem{
				ID:                n.ID,
				ProcessID:         strings.TrimSpace(n.ProcessID),
				SourceType:        strings.TrimSpace(n.SourceType),
				SourceID:          strings.TrimSpace(n.SourceID),
				AccountingScene:   strings.TrimSpace(n.AccountingScene),
				AccountingSubtype: strings.TrimSpace(n.AccountingSubtype),
				EventType:         strings.TrimSpace(n.EventType),
				Status:            strings.TrimSpace(n.Status),
				OccurredAtUnix:    n.OccurredAtUnix,
				IdempotencyKey:    strings.TrimSpace(n.OrderID),
				Note:              strings.TrimSpace(n.Note),
				Payload:           json.RawMessage(n.PayloadJSON),
			}
			if f.ProcessID != "" && item.ProcessID != f.ProcessID {
				continue
			}
			if f.SourceType != "" && item.SourceType != f.SourceType {
				continue
			}
			if f.SourceID != "" && item.SourceID != f.SourceID {
				continue
			}
			if f.AccountingScene != "" && item.AccountingScene != f.AccountingScene {
				continue
			}
			if f.AccountingSubtype != "" && item.AccountingSubtype != f.AccountingSubtype {
				continue
			}
			if f.EventType != "" && item.EventType != f.EventType {
				continue
			}
			if f.Status != "" && item.Status != f.Status {
				continue
			}
			if f.Query != "" {
				like := strings.ToLower(strings.TrimSpace(f.Query))
				hit := strings.Contains(strings.ToLower(item.ProcessID), like) ||
					strings.Contains(strings.ToLower(item.Note), like) ||
					strings.Contains(strings.ToLower(item.IdempotencyKey), like) ||
					strings.Contains(strings.ToLower(item.SourceType), like) ||
					strings.Contains(strings.ToLower(item.SourceID), like) ||
					strings.Contains(strings.ToLower(item.AccountingScene), like) ||
					strings.Contains(strings.ToLower(item.AccountingSubtype), like)
				if !hit {
					continue
				}
			}
			if settlementState != "all" {
				id, parseErr := strconv.ParseInt(strings.TrimSpace(item.SourceID), 10, 64)
				if parseErr != nil || attemptState[id] != settlementState {
					continue
				}
			}
			filtered = append(filtered, item)
		}

		var out financeProcessEventPage
		out.Total = len(filtered)
		if f.Limit <= 0 {
			f.Limit = 20
		}
		if f.Offset < 0 {
			f.Offset = 0
		}
		if f.Offset < len(filtered) {
			end := f.Offset + f.Limit
			if end > len(filtered) {
				end = len(filtered)
			}
			out.Items = append(out.Items, filtered[f.Offset:end]...)
		} else {
			out.Items = []financeProcessEventItem{}
		}
		return out, nil
	})
}

func dbGetFinanceProcessEvent(ctx context.Context, store *clientDB, id int64) (financeProcessEventItem, error) {
	if store == nil {
		return financeProcessEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (financeProcessEventItem, error) {
		node, err := tx.OrderSettlementEvents.Query().
			Where(ordersettlementevents.IDEQ(id)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return financeProcessEventItem{}, sql.ErrNoRows
			}
			return financeProcessEventItem{}, err
		}
		return financeProcessEventItem{
			ID:                node.ID,
			ProcessID:         strings.TrimSpace(node.ProcessID),
			SourceType:        strings.TrimSpace(node.SourceType),
			SourceID:          strings.TrimSpace(node.SourceID),
			AccountingScene:   strings.TrimSpace(node.AccountingScene),
			AccountingSubtype: strings.TrimSpace(node.AccountingSubtype),
			EventType:         strings.TrimSpace(node.EventType),
			Status:            strings.TrimSpace(node.Status),
			OccurredAtUnix:    node.OccurredAtUnix,
			IdempotencyKey:    strings.TrimSpace(node.OrderID),
			Note:              strings.TrimSpace(node.Note),
			Payload:           json.RawMessage(node.PayloadJSON),
		}, nil
	})
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
