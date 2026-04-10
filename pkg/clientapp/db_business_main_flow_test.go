package clientapp

import (
	"context"
	"fmt"
	"testing"
)

// TestBusinessMainFlow_FullChain 测试完整主链流程
// 覆盖：创建前台单 → 创建业务 → 创建 trigger → 创建 pending settlement → 回写 settled settlement
func TestBusinessMainFlow_FullChain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 步骤1：创建前台订单
	frontOrderID := "fo_domain_reg_001"
	frontOrder := frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "example.bsv",
		Status:           "pending",
		Note:             "域名注册直链支付",
		Payload: map[string]any{
			"domain_name": "example.bsv",
			"years":       1,
		},
	}
	if err := dbUpsertFrontOrder(ctx, store, frontOrder); err != nil {
		t.Fatalf("upsert front_order failed: %v", err)
	}

	// 验证 front_order 已创建
	fo, err := dbGetFrontOrder(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get front_order failed: %v", err)
	}
	if fo.Status != "pending" {
		t.Fatalf("expected front_order status pending, got %s", fo.Status)
	}

	// 步骤2：创建业务
	businessID := "biz_domain_reg_001"
	business := finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "formal", // 正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "registry:peer",
		Status:            "pending",
		IdempotencyKey:    "idem_domain_reg_001",
		Note:              "域名注册费用",
		Payload: map[string]any{
			"domain_name": "example.bsv",
			"amount_sat":  10000,
		},
	}
	if err := dbAppendFinBusiness(db, business); err != nil {
		t.Fatalf("append business failed: %v", err)
	}

	// 步骤3：创建 business_trigger
	triggerID := "trg_fo_domain_reg_001"
	trigger := businessTriggerEntry{
		TriggerID:      triggerID,
		BusinessID:     businessID,
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		Note:           "前台订单触发费用",
		Payload: map[string]any{
			"trigger_reason": "user_initiated",
		},
	}
	if err := dbAppendBusinessTrigger(ctx, store, trigger); err != nil {
		t.Fatalf("append business_trigger failed: %v", err)
	}

	// 验证 trigger 已创建
	triggers, err := dbListBusinessTriggersByBusinessID(ctx, store, businessID, 10, 0)
	if err != nil {
		t.Fatalf("list biz_business_triggers failed: %v", err)
	}
	if len(triggers.Items) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(triggers.Items))
	}
	if triggers.Items[0].TriggerType != "front_order" {
		t.Fatalf("expected trigger_type front_order, got %s", triggers.Items[0].TriggerType)
	}

	// 验证可通过 trigger 反查 business
	businessIDs, err := dbListBusinessesByTrigger(ctx, store, "front_order", frontOrderID)
	if err != nil {
		t.Fatalf("list businesses by trigger failed: %v", err)
	}
	if len(businessIDs) != 1 || businessIDs[0] != businessID {
		t.Fatalf("expected business_id %s, got %v", businessID, businessIDs)
	}

	// 步骤4：创建 pending settlement
	settlementID := "set_domain_reg_001"
	settlement := businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: string(SettlementMethodChain),
		Status:           "pending",
		TargetType:       "chain_payment",
		TargetID:         "",
		Payload: map[string]any{
			"estimated_fee_sat": 500,
		},
	}
	if err := dbUpsertBusinessSettlement(ctx, store, settlement); err != nil {
		t.Fatalf("upsert business_settlement failed: %v", err)
	}

	// 验证 settlement 已创建且状态为 pending
	settlementResult, err := dbGetBusinessSettlementByBusinessID(ctx, store, businessID)
	if err != nil {
		t.Fatalf("get business_settlement failed: %v", err)
	}
	if settlementResult.Status != "pending" {
		t.Fatalf("expected settlement status pending, got %s", settlementResult.Status)
	}

	// 步骤5：模拟支付完成，回写 settlement 状态为 settled
	if err := dbUpdateBusinessSettlementStatus(ctx, store, settlementID, "settled", ""); err != nil {
		t.Fatalf("update settlement status failed: %v", err)
	}

	// 验证 settlement 状态已更新
	settlementResult, err = dbGetBusinessSettlement(ctx, store, settlementID)
	if err != nil {
		t.Fatalf("get updated settlement failed: %v", err)
	}
	if settlementResult.Status != "settled" {
		t.Fatalf("expected settlement status settled, got %s", settlementResult.Status)
	}

	// 更新 front_order 状态为 completed
	if err := dbUpdateFrontOrderStatus(ctx, store, frontOrderID, "completed"); err != nil {
		t.Fatalf("update front_order status failed: %v", err)
	}

	// 验证完整流程
	fo, err = dbGetFrontOrder(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get final front_order failed: %v", err)
	}
	if fo.Status != "completed" {
		t.Fatalf("expected final front_order status completed, got %s", fo.Status)
	}

	t.Logf("✓ 完整主链流程测试通过: front_order=%s, business=%s, trigger=%s, settlement=%s",
		frontOrderID, businessID, triggerID, settlementID)
}

// TestBusinessBridgeFlow_CreateBusinessWithFrontTriggerAndPendingSettlement 测试桥接函数
func TestBusinessBridgeFlow_CreateBusinessWithFrontTriggerAndPendingSettlement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 使用桥接函数创建完整主链
	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		// 前台订单
		FrontOrderID:     "fo_bridge_test_001",
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03ddeeffaa",
		TargetObjectType: "domain",
		TargetObjectID:   "test.bsv",
		FrontOrderNote:   "桥接函数测试订单",
		FrontOrderPayload: map[string]any{
			"test": true,
		},

		// 业务
		BusinessID:        "biz_bridge_test_001",
		BusinessRole:      "formal", // 域名注册是正式收费对象
		SourceType:        "front_order",
		SourceID:          "fo_bridge_test_001",
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "registry:peer",
		BusinessNote:      "桥接函数测试业务",
		BusinessPayload: map[string]any{
			"amount_sat": 20000,
		},

		// 触发器
		TriggerType:    "front_order",
		TriggerIDValue: "fo_bridge_test_001",
		TriggerRole:    "primary",
		TriggerNote:    "桥接触发",
		TriggerPayload: map[string]any{
			"auto_created": true,
		},

		// 结算
		SettlementID:         "set_bridge_test_001",
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_payment",
		SettlementTargetID:   "",
		SettlementPayload: map[string]any{
			"estimated_confirmations": 1,
		},
	}

	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
		t.Fatalf("bridge function failed: %v", err)
	}

	// 验证 front_order 已创建
	fo, err := dbGetFrontOrder(ctx, store, input.FrontOrderID)
	if err != nil {
		t.Fatalf("get front_order failed: %v", err)
	}
	if fo.Status != "pending" {
		t.Fatalf("expected front_order status pending, got %s", fo.Status)
	}
	if fo.FrontType != "domain" {
		t.Fatalf("expected front_type domain, got %s", fo.FrontType)
	}

	// 验证 business_trigger 已创建
	triggerPage, err := dbListBusinessTriggersByBusinessID(ctx, store, input.BusinessID, 10, 0)
	if err != nil {
		t.Fatalf("list triggers failed: %v", err)
	}
	if len(triggerPage.Items) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(triggerPage.Items))
	}

	// 验证 settlement 已创建且状态为 pending
	settlement, err := dbGetBusinessSettlement(ctx, store, input.SettlementID)
	if err != nil {
		t.Fatalf("get settlement failed: %v", err)
	}
	if settlement.Status != "pending" {
		t.Fatalf("expected settlement status pending, got %s", settlement.Status)
	}
	if settlement.SettlementMethod != string(SettlementMethodChain) {
		t.Fatalf("expected settlement_method %s, got %s", SettlementMethodChain, settlement.SettlementMethod)
	}

	// 验证幂等性：重复调用不应报错
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
		t.Fatalf("bridge function should be idempotent, but got error: %v", err)
	}

	t.Logf("✓ 桥接函数测试通过: front_order=%s, business=%s, settlement=%s",
		input.FrontOrderID, input.BusinessID, input.SettlementID)
}

// TestBusinessBridgeFlow_IdempotentOnRetry 测试桥接函数幂等性
func TestBusinessBridgeFlow_IdempotentOnRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_idempotent_001",
		FrontType:        "test",
		FrontSubtype:     "idempotent",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "test",
		TargetObjectID:   "test1",

		BusinessID:        "biz_idempotent_001",
		BusinessRole:      "formal", // 测试场景用正式收费对象
		SourceType:        "front_order",
		SourceID:          "fo_idempotent_001",
		AccountingScene:   "test",
		AccountingSubType: "idempotent",
		FromPartyID:       "client:self",
		ToPartyID:         "test:peer",

		TriggerType:    "front_order",
		TriggerIDValue: "fo_idempotent_001",
		TriggerRole:    "primary",

		SettlementID:         "set_idempotent_001",
		SettlementMethod:     SettlementMethodPool,
		SettlementTargetType: "test_target",
		SettlementTargetID:   "target_1",
	}

	// 多次调用应该成功且数据一致
	for i := 0; i < 3; i++ {
		if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
			t.Fatalf("bridge call %d failed: %v", i+1, err)
		}
	}

	// 验证只有一条 front_order
	foPage, err := dbListFrontOrders(ctx, store, frontOrderFilter{
		FrontOrderID: input.FrontOrderID,
	})
	if err != nil {
		t.Fatalf("list biz_front_orders failed: %v", err)
	}
	if foPage.Total != 1 {
		t.Fatalf("expected 1 front_order, got %d", foPage.Total)
	}

	// 验证只有一条 settlement（通过 business_id 唯一约束）
	settlementPage, err := dbListBusinessSettlements(ctx, store, businessSettlementFilter{
		BusinessID: input.BusinessID,
	})
	if err != nil {
		t.Fatalf("list settlements failed: %v", err)
	}
	if settlementPage.Total != 1 {
		t.Fatalf("expected 1 settlement, got %d", settlementPage.Total)
	}

	t.Logf("✓ 幂等性测试通过: 3 次调用只产生 1 条记录")
}

// TestBusinessMainFlow_QueryByTarget 测试按 target 查询 settlement
func TestBusinessMainFlow_QueryByTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 插入测试数据
	for i := 0; i < 3; i++ {
		businessID := fmt.Sprintf("biz_target_test_%d", i)
		// 先插入 settle_records（第九阶段：必须带 business_role）
		if _, err := db.Exec(`INSERT INTO settle_records(
			settlement_id,business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("set_target_test_%d", i), businessID, "formal", "test", "test", "test", "test", "client:self", "test:peer", "pending", 1700000000+int64(i), fmt.Sprintf("idem_%d", i), "test", `{}`,
		); err != nil {
			t.Fatalf("insert settle_records failed: %v", err)
		}

		settlement := businessSettlementEntry{
			SettlementID:     fmt.Sprintf("set_target_test_%d", i),
			BusinessID:       businessID,
			SettlementMethod: string(SettlementMethodChain),
			Status:           "pending",
			TargetType:       "chain_payment",
			TargetID:         fmt.Sprintf("tx_target_%d", i),
		}
		if err := dbUpsertBusinessSettlement(ctx, store, settlement); err != nil {
			t.Fatalf("upsert settlement failed: %v", err)
		}
	}

	// 按 target_type + target_id 查询
	result, err := dbListBusinessSettlementsByTarget(ctx, store, "chain_payment", "tx_target_1", 10, 0)
	if err != nil {
		t.Fatalf("query by target failed: %v", err)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected 1 settlement for tx_target_1, got %d", len(result.Items))
	}
	if result.Items[0].TargetID != "tx_target_1" {
		t.Fatalf("expected target_id tx_target_1, got %s", result.Items[0].TargetID)
	}

	t.Logf("✓ 按 target 查询测试通过")
}
