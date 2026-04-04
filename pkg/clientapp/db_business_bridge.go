package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
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
	TriggerID      string // 可选，未传时默认为 "trg_{business_id}"
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
//   - 幂等设计：同一 front_order_id/business_id/trigger_id/settlement_id 重复调用不产生脏数据
//   - 不替代底层支付逻辑，只是主链骨架落地
func CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, store *clientDB, in CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}

	// 参数校验
	if strings.TrimSpace(in.FrontOrderID) == "" {
		return fmt.Errorf("front_order_id is required")
	}
	if strings.TrimSpace(in.BusinessID) == "" {
		return fmt.Errorf("business_id is required")
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

	// 生成 trigger_id（未传时默认为 "trg_{business_id}" ）
	triggerID := strings.TrimSpace(in.TriggerID)
	if triggerID == "" {
		triggerID = "trg_" + in.BusinessID
	}

	return store.Tx(ctx, func(tx *sql.Tx) error {
		now := time.Now().Unix()

		// 1. 确保 front_order 存在
		if err := dbUpsertFrontOrderTx(tx, frontOrderEntry{
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
			obs.Error("bitcast-client", "bridge_front_order_failed", map[string]any{
				"error":          err.Error(),
				"front_order_id": in.FrontOrderID,
			})
			return fmt.Errorf("upsert front_order: %w", err)
		}

		// 2. 创建或更新 business
		// 第七阶段整改：BusinessRole 由调用方显式传入，桥接器不猜业务分类
		// 桥接器只负责落链，不负责做业务类型裁判
		idempotencyKey := "bridge:" + in.BusinessID
		if err := dbAppendFinBusinessTx(tx, finBusinessEntry{
			BusinessID:        in.BusinessID,
			BusinessRole:      in.BusinessRole, // 调用方显式传入
			SourceType:        in.SourceType,
			SourceID:          in.SourceID,
			AccountingScene:   in.AccountingScene,
			AccountingSubType: in.AccountingSubType,
			FromPartyID:       in.FromPartyID,
			ToPartyID:         in.ToPartyID,
			Status:            "pending",
			OccurredAtUnix:    now,
			IdempotencyKey:    idempotencyKey,
			Note:              in.BusinessNote,
			Payload:           in.BusinessPayload,
		}); err != nil {
			obs.Error("bitcast-client", "bridge_business_failed", map[string]any{
				"error":       err.Error(),
				"business_id": in.BusinessID,
			})
			return fmt.Errorf("append business: %w", err)
		}

		// 3. 创建 business_trigger（支持一前台单多条 business）
		// 幂等约束在 (business_id, trigger_type, trigger_id_value, trigger_role) 上
		if err := dbAppendBusinessTriggerTx(tx, businessTriggerEntry{
			TriggerID:      triggerID,
			BusinessID:     in.BusinessID,
			TriggerType:    in.TriggerType,
			TriggerIDValue: in.TriggerIDValue,
			TriggerRole:    in.TriggerRole,
			CreatedAtUnix:  now,
			Note:           in.TriggerNote,
			Payload:        in.TriggerPayload,
		}); err != nil {
			obs.Error("bitcast-client", "bridge_trigger_failed", map[string]any{
				"error":       err.Error(),
				"trigger_id":  triggerID,
				"business_id": in.BusinessID,
			})
			return fmt.Errorf("append business_trigger: %w", err)
		}

		// 4. 创建 business_settlement(status='pending')
		if err := dbUpsertBusinessSettlementTx(tx, businessSettlementEntry{
			SettlementID:     in.SettlementID,
			BusinessID:       in.BusinessID,
			SettlementMethod: string(in.SettlementMethod),
			Status:           "pending",
			TargetType:       in.SettlementTargetType,
			TargetID:         in.SettlementTargetID,
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
			Payload:          in.SettlementPayload,
		}); err != nil {
			obs.Error("bitcast-client", "bridge_settlement_failed", map[string]any{
				"error":         err.Error(),
				"settlement_id": in.SettlementID,
				"business_id":   in.BusinessID,
			})
			return fmt.Errorf("upsert business_settlement: %w", err)
		}

		obs.Info("bitcast-client", "bridge_success", map[string]any{
			"front_order_id":    in.FrontOrderID,
			"business_id":       in.BusinessID,
			"trigger_id":        triggerID,
			"settlement_id":     in.SettlementID,
			"settlement_method": in.SettlementMethod,
		})
		return nil
	})
}

// dbUpsertFrontOrderTx 事务内插入或更新前台业务单
func dbUpsertFrontOrderTx(tx *sql.Tx, e frontOrderEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	e.FrontOrderID = strings.TrimSpace(e.FrontOrderID)
	if e.FrontOrderID == "" {
		return fmt.Errorf("front_order_id is required")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = e.CreatedAtUnix
	}
	_, err := tx.Exec(
		`INSERT INTO front_orders(
			front_order_id,front_type,front_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(front_order_id) DO UPDATE SET
			front_type=excluded.front_type,
			front_subtype=excluded.front_subtype,
			owner_pubkey_hex=excluded.owner_pubkey_hex,
			target_object_type=excluded.target_object_type,
			target_object_id=excluded.target_object_id,
			status=excluded.status,
			updated_at_unix=excluded.updated_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		e.FrontOrderID,
		strings.TrimSpace(e.FrontType),
		strings.TrimSpace(e.FrontSubtype),
		strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex)),
		strings.TrimSpace(e.TargetObjectType),
		strings.TrimSpace(e.TargetObjectID),
		strings.TrimSpace(e.Status),
		e.CreatedAtUnix,
		e.UpdatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbAppendFinBusinessTx 事务内插入或更新业务
func dbAppendFinBusinessTx(tx *sql.Tx, e finBusinessEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	// 第七阶段：business_role 必须是正式约束，不允许空值
	e.BusinessRole = strings.TrimSpace(e.BusinessRole)
	if e.BusinessRole == "" {
		return fmt.Errorf("business_role is required: must be 'formal' or 'process'")
	}
	if e.BusinessRole != "formal" && e.BusinessRole != "process" {
		return fmt.Errorf("business_role must be 'formal' or 'process', got '%s'", e.BusinessRole)
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.BusinessID
	}
	_, err := tx.Exec(
		`INSERT INTO fin_business(business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(idempotency_key) DO UPDATE SET
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json,
			source_type=excluded.source_type,
			source_id=excluded.source_id,
			accounting_scene=excluded.accounting_scene,
			accounting_subtype=excluded.accounting_subtype,
			business_role=excluded.business_role`,
		e.BusinessID,
		strings.TrimSpace(e.BusinessRole),
		strings.TrimSpace(e.SourceType),
		strings.TrimSpace(e.SourceID),
		strings.TrimSpace(e.AccountingScene),
		strings.TrimSpace(e.AccountingSubType),
		strings.TrimSpace(e.FromPartyID),
		strings.TrimSpace(e.ToPartyID),
		strings.TrimSpace(e.Status),
		e.OccurredAtUnix,
		e.IdempotencyKey,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbAppendBusinessTriggerTx 事务内插入业务触发桥接记录
// 幂等约束在 (business_id, trigger_type, trigger_id_value, trigger_role) 上
// 支持"一前台单多条 business"：同一 trigger_type+trigger_id_value 可触发多个不同 business
func dbAppendBusinessTriggerTx(tx *sql.Tx, e businessTriggerEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	e.TriggerID = strings.TrimSpace(e.TriggerID)
	if e.TriggerID == "" {
		return fmt.Errorf("trigger_id is required")
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	_, err := tx.Exec(
		`INSERT INTO business_triggers(
			trigger_id,business_id,trigger_type,trigger_id_value,trigger_role,created_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(business_id, trigger_type, trigger_id_value, trigger_role) DO UPDATE SET
			trigger_id=excluded.trigger_id,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		e.TriggerID,
		e.BusinessID,
		strings.TrimSpace(e.TriggerType),
		strings.TrimSpace(e.TriggerIDValue),
		strings.TrimSpace(e.TriggerRole),
		e.CreatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbUpsertBusinessSettlementTx 事务内插入或更新业务结算出口
// 校验：settlement_method 只允许 'pool' 或 'chain'
func dbUpsertBusinessSettlementTx(tx *sql.Tx, e businessSettlementEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
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
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = e.CreatedAtUnix
	}
	_, err := tx.Exec(
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
}
