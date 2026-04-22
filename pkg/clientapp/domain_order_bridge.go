package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/orders"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/ordersettlementevents"
)

// SettlementMethod 结算方式枚举
type SettlementMethod string

const (
	// SettlementMethodPool 通过费用池结算
	SettlementMethodPool SettlementMethod = "pool"
	// SettlementMethodChain 通过直接链上支付结算
	SettlementMethodChain SettlementMethod = "chain"
)

// Valid 验证结算方式是否合法
func (m SettlementMethod) Valid() error {
	switch m {
	case SettlementMethodPool, SettlementMethodChain:
		return nil
	default:
		return fmt.Errorf("invalid settlement_method: %s, must be 'pool' or 'chain'", m)
	}
}

// CreateBusinessWithFrontTriggerAndPendingSettlementInput 创建业务主链桥接输入参数
// 职责：第一阶段桥接器，负责落 front_order、business、business_trigger、business_settlement(pending)
// 说明：不负责真正支付、不负责池创建、不负责 chain_payment
// 第七阶段整改：BusinessRole 由调用方显式传入，桥接器不猜业务分类
type CreateBusinessWithFrontTriggerAndPendingSettlementInput struct {
	// 前台订单字段
	FrontOrderID      string
	FrontType         string
	FrontSubtype      string
	OwnerPubkeyHex    string
	TargetObjectType  string
	TargetObjectID    string
	FrontOrderNote    string
	FrontOrderPayload any

	// 业务字段
	BusinessID        string
	BusinessRole      string // 第七阶段新增：调用方显式传入 "formal" 或 "process"
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubType string
	FromPartyID       string
	ToPartyID         string
	BusinessNote      string
	BusinessPayload   any

	// 触发器字段
	TriggerID      string // 可选，未传时默认为 "trg_{order_id}"
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
	TriggerNote    string
	TriggerPayload any

	// 结算字段
	SettlementID         string
	SettlementMethod     SettlementMethod // 只允许 "pool" 或 "chain"
	SettlementTargetType string
	SettlementTargetID   string
	SettlementPayload    any
}

// CreateBusinessWithFrontTriggerAndPendingSettlement 创建业务主链桥接（事务实现）
// 职责：
//  1. 确保 front_order 存在
//  2. 创建或更新 business
//  3. 创建 business_trigger（支持一前台单多条 business）
//  4. 创建 business_settlement(status='pending')
//
// 设计约束：
//   - 单事务：四步全部成功或全部回滚
//   - 幂等设计：同一 order_id/order_id/trigger_id/settlement_id 重复调用不产生脏数据
//   - 不替代底层支付逻辑，只是主链骨架落地
func CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, store *clientDB, in CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}

	// 参数校验
	if strings.TrimSpace(in.FrontOrderID) == "" {
		return fmt.Errorf("order_id is required")
	}
	if strings.TrimSpace(in.BusinessID) == "" {
		return fmt.Errorf("order_id is required")
	}
	if strings.TrimSpace(in.SettlementID) == "" {
		return fmt.Errorf("settlement_id is required")
	}

	// 第八阶段：BusinessRole 是必填入参，桥接器不再猜角色
	in.BusinessRole = strings.TrimSpace(in.BusinessRole)
	if in.BusinessRole == "" {
		return fmt.Errorf("business_role is required: must be 'formal' or 'process'")
	}
	if in.BusinessRole != "formal" && in.BusinessRole != "process" {
		return fmt.Errorf("invalid business_role '%s': must be 'formal' or 'process'", in.BusinessRole)
	}

	// 验证 settlement_method 枚举
	if err := in.SettlementMethod.Valid(); err != nil {
		return err
	}

	// 生成 trigger_id（未传时默认为 "trg_{order_id}" ）
	triggerID := strings.TrimSpace(in.TriggerID)
	if triggerID == "" {
		triggerID = "trg_" + in.BusinessID
	}

	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		now := time.Now().Unix()

		// 1. 确保 front_order 存在
		if err := dbUpsertFrontOrderEntTx(ctx, tx, frontOrderEntry{
			FrontOrderID:     in.FrontOrderID,
			FrontType:        in.FrontType,
			FrontSubtype:     in.FrontSubtype,
			OwnerPubkeyHex:   in.OwnerPubkeyHex,
			TargetObjectType: in.TargetObjectType,
			TargetObjectID:   in.TargetObjectID,
			Status:           "pending",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
			Note:             in.FrontOrderNote,
			Payload:          in.FrontOrderPayload,
		}); err != nil {
			obs.Error(ServiceName, "bridge_front_order_failed", map[string]any{
				"error":    err.Error(),
				"order_id": in.FrontOrderID,
			})
			return fmt.Errorf("upsert front_order: %w", err)
		}

		// 2. 创建或更新 business
		// 第七阶段整改：BusinessRole 由调用方显式传入，桥接器不猜业务分类
		// 桥接器只负责落链，不负责做业务类型裁判
		idempotencyKey := "bridge:" + in.BusinessID
		if err := dbUpsertSettleRecordTx(ctx, tx, finBusinessEntry{
			OrderID:                in.BusinessID,
			BusinessRole:           in.BusinessRole, // 调用方显式传入
			SourceType:             in.SourceType,
			SourceID:               in.SourceID,
			AccountingScene:        in.AccountingScene,
			AccountingSubType:      in.AccountingSubType,
			FromPartyID:            in.FromPartyID,
			ToPartyID:              in.ToPartyID,
			Status:                 "pending",
			OccurredAtUnix:         now,
			IdempotencyKey:         idempotencyKey,
			Note:                   in.BusinessNote,
			Payload:                in.BusinessPayload,
			SettlementID:           in.SettlementID,
			SettlementMethod:       string(in.SettlementMethod),
			SettlementStatus:       "pending",
			SettlementTargetType:   in.SettlementTargetType,
			SettlementTargetID:     in.SettlementTargetID,
			SettlementErrorMessage: "",
			SettlementPayload:      in.SettlementPayload,
		}); err != nil {
			obs.Error(ServiceName, "bridge_business_failed", map[string]any{
				"error":    err.Error(),
				"order_id": in.BusinessID,
			})
			return fmt.Errorf("append business: %w", err)
		}

		// 3. 创建 business_settlement(status='pending')
		if err := dbUpsertSettleRecordSettlementTx(ctx, tx, businessSettlementEntry{
			SettlementID:     in.SettlementID,
			OrderID:          in.BusinessID,
			SettlementMethod: string(in.SettlementMethod),
			Status:           "pending",
			TargetType:       in.SettlementTargetType,
			TargetID:         in.SettlementTargetID,
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
			Payload:          in.SettlementPayload,
		}); err != nil {
			obs.Error(ServiceName, "bridge_settlement_failed", map[string]any{
				"error":         err.Error(),
				"settlement_id": in.SettlementID,
				"order_id":      in.BusinessID,
			})
			return fmt.Errorf("upsert business_settlement: %w", err)
		}

		// 4. 创建 business_trigger（支持一前台单多条 business）
		// 幂等约束在 (order_id, trigger_type, trigger_id_value, trigger_role) 上
		if err := dbAppendBusinessTriggerTx(ctx, tx, businessTriggerEntry{
			TriggerID:      triggerID,
			OrderID:        in.BusinessID,
			SettlementID:   in.SettlementID,
			TriggerType:    in.TriggerType,
			TriggerIDValue: in.TriggerIDValue,
			TriggerRole:    in.TriggerRole,
			CreatedAtUnix:  now,
			Note:           in.TriggerNote,
			Payload:        in.TriggerPayload,
		}); err != nil {
			obs.Error(ServiceName, "bridge_trigger_failed", map[string]any{
				"error":      err.Error(),
				"trigger_id": triggerID,
				"order_id":   in.BusinessID,
			})
			return fmt.Errorf("append business_trigger: %w", err)
		}

		obs.Info(ServiceName, "bridge_success", map[string]any{
			"front_order_id":    in.FrontOrderID,
			"order_id":          in.BusinessID,
			"trigger_id":        triggerID,
			"settlement_id":     in.SettlementID,
			"settlement_method": in.SettlementMethod,
		})
		return nil
	})
}

// dbUpsertFrontOrderEntTx 事务内插入或更新前台业务单。
// 设计说明：
// - 这里直接收口到 ent，避免桥接层再碰旧 SQL；
// - order_id 仍然是唯一定位，不再靠旧主键语义猜行。
func dbUpsertFrontOrderEntTx(ctx context.Context, tx EntWriteRoot, e frontOrderEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	e.FrontOrderID = strings.TrimSpace(e.FrontOrderID)
	if e.FrontOrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = e.CreatedAtUnix
	}
	existing, err := tx.Orders.Query().
		Where(orders.OrderIDEQ(e.FrontOrderID)).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetOrderType(strings.TrimSpace(e.FrontType)).
			SetOrderSubtype(strings.TrimSpace(e.FrontSubtype)).
			SetOwnerPubkeyHex(strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))).
			SetTargetObjectType(strings.TrimSpace(e.TargetObjectType)).
			SetTargetObjectID(strings.TrimSpace(e.TargetObjectID)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetUpdatedAtUnix(e.UpdatedAtUnix).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("upsert front_order: %w", err)
		}
		return nil
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.Orders.Create().
		SetOrderID(e.FrontOrderID).
		SetOrderType(strings.TrimSpace(e.FrontType)).
		SetOrderSubtype(strings.TrimSpace(e.FrontSubtype)).
		SetOwnerPubkeyHex(strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))).
		SetTargetObjectType(strings.TrimSpace(e.TargetObjectType)).
		SetTargetObjectID(strings.TrimSpace(e.TargetObjectID)).
		SetStatus(strings.TrimSpace(e.Status)).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		SetCreatedAtUnix(e.CreatedAtUnix).
		SetUpdatedAtUnix(e.UpdatedAtUnix).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("upsert front_order: %w", err)
	}
	return nil
}

// dbUpsertSettleRecordTx 事务内写业务与结算主表。
// 设计说明：
// - 业务和结算一起落，不再把同一 order_id 压成一行；
// - settlement_id 负责精确定位，后续新执行单会顺延新增；
// - 这个入口只做桥接，不自己拼旧式 UPSERT。
func dbUpsertSettleRecordTx(ctx context.Context, tx EntWriteRoot, e finBusinessEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	return dbAppendFinBusinessTx(ctx, tx, e)
}

// dbAppendBusinessTriggerTx 事务内插入业务触发桥接记录
// 幂等约束在 (order_id, trigger_type, trigger_id_value, trigger_role) 上
// 支持"一前台单多条 business"：同一 trigger_type+trigger_id_value 可触发多个不同 business
func dbAppendBusinessTriggerTx(ctx context.Context, tx EntWriteRoot, e businessTriggerEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	e.TriggerID = strings.TrimSpace(e.TriggerID)
	if e.TriggerID == "" {
		return fmt.Errorf("trigger_id is required")
	}
	e.OrderID = strings.TrimSpace(e.OrderID)
	if e.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	e.SettlementID = strings.TrimSpace(e.SettlementID)
	if e.SettlementID == "" {
		e.SettlementID = e.OrderID
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	existing, err := tx.OrderSettlementEvents.Query().
		Where(
			ordersettlementevents.SettlementIDEQ(e.SettlementID),
			ordersettlementevents.EventTypeEQ("bridge_trigger"),
			ordersettlementevents.OrderIDEQ(e.OrderID),
		).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetProcessID(e.TriggerID).
			SetSettlementID(e.SettlementID).
			SetOrderID(e.OrderID).
			SetSourceType(strings.TrimSpace(e.TriggerType)).
			SetSourceID(strings.TrimSpace(e.TriggerIDValue)).
			SetAccountingScene("").
			SetAccountingSubtype(strings.TrimSpace(e.TriggerRole)).
			SetStatus("linked").
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetOccurredAtUnix(e.CreatedAtUnix).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.OrderSettlementEvents.Create().
		SetProcessID(e.TriggerID).
		SetSettlementID(e.SettlementID).
		SetOrderID(e.OrderID).
		SetSourceType(strings.TrimSpace(e.TriggerType)).
		SetSourceID(strings.TrimSpace(e.TriggerIDValue)).
		SetAccountingScene("").
		SetAccountingSubtype(strings.TrimSpace(e.TriggerRole)).
		SetEventType("bridge_trigger").
		SetStatus("linked").
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		SetOccurredAtUnix(e.CreatedAtUnix).
		Save(ctx)
	return err
}

// dbUpsertBusinessSettlementTx 事务内插入或更新业务结算出口
// 校验：settlement_method 只允许 'pool' 或 'chain'
func dbUpsertSettleRecordSettlementTx(ctx context.Context, tx EntWriteRoot, e businessSettlementEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	return dbUpsertBusinessSettlementEntTx(ctx, tx, e)
}
