package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// createDomainRegisterBusinessChain 创建域名注册业务主链
// 在准备阶段落地：front_order + business + trigger + settlement(pending)
// 设计说明：
//   - 不阻断原有业务流程，仅作为新主链骨架落地
//   - 失败时返回 error，但调用方选择继续旧流程
func createDomainRegisterBusinessChain(
	ctx context.Context,
	store *clientDB,
	frontOrderID, businessID, settlementID string,
	p TriggerDomainRegisterNameParams,
) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}

	name := strings.ToLower(strings.TrimSpace(p.Name))
	resolverPubkeyHex := strings.ToLower(strings.TrimSpace(p.ResolverPubkeyHex))
	targetPubkeyHex := strings.ToLower(strings.TrimSpace(p.TargetPubkeyHex))

	return CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		// 前台订单
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   targetPubkeyHex, // 域名所有者是 target
		TargetObjectType: "domain_name",
		TargetObjectID:   name,
		FrontOrderNote:   "域名注册: " + name,
		FrontOrderPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"target_pubkey_hex":   targetPubkeyHex,
		},

		// 业务（第七阶段整改：调用方显式传 business_role）
		BusinessID:        businessID,
		BusinessRole:      "formal", // 域名注册是正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:" + resolverPubkeyHex,
		BusinessNote:      "域名注册费用: " + name,
		BusinessPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"target_pubkey_hex":   targetPubkeyHex,
		},

		// 触发器
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		TriggerNote:    "前台订单触发注册",
		TriggerPayload: map[string]any{
			"trigger_reason": "domain_register_initiated",
		},

		// 结算
		SettlementID:         settlementID,
		SettlementMethod:     SettlementMethodChain, // 域名注册是直接链上支付
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "", // 待填充
		SettlementPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"status":              "pending",
		},
	})
}

// finalizeDomainRegisterSettlement 回写域名注册结算状态
// 在提交阶段调用：根据提交结果设置 settlement 状态
// 硬要求：settlement_method='chain' 时，target_id 必须写 fact_settlement_channel_chain_quote_pay.id
//   - 成功时：通过 txID 查 fact_settlement_channel_chain_quote_pay.id，写入 target_id
//   - 查不到 fact_settlement_channel_chain_quote_pay.id 时报错，不静默降级为 txid
//   - 失败时：写 status='failed' + error_message，target_id 留空
func finalizeDomainRegisterSettlement(
	ctx context.Context,
	store *clientDB,
	settlementID string,
	success bool,
	txID string,
	errMsg string,
) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}

	status := "settled"
	targetID := ""

	if success {
		txID = strings.ToLower(strings.TrimSpace(txID))
		if txID == "" {
			return fmt.Errorf("txid is required for successful settlement")
		}
		// 硬要求：必须拿到 fact_settlement_channel_chain_quote_pay.id，不降级
		chainPaymentID, err := dbGetChainPaymentByTxID(ctx, store, txID)
		if err != nil {
			return fmt.Errorf("find fact_settlement_channel_chain_quote_pay.id for txid=%s: %w", txID, err)
		}
		targetID = fmt.Sprintf("%d", chainPaymentID)
	} else {
		status = "failed"
	}

	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db,
			`UPDATE settle_records SET settlement_status=?, target_type='chain_quote_pay', target_id=?, error_message=?, updated_at_unix=strftime('%s','now') WHERE settlement_id=?`,
			status,
			targetID,
			errMsg,
			settlementID,
		)
		return err
	})
}
