package clientapp

import (
	"context"
	"testing"
)

// TestBusinessBridge_ValidationFailure_NoWrite 参数校验失败时不写数据
// 设计意图：验证事务开始前的参数校验失败不会导致脏数据写入
func TestBusinessBridge_ValidationFailure_NoWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 使用无效的 settlement_method，会在事务开始前校验失败
	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_validation_fail",
		FrontType:        "test",
		FrontSubtype:     "validation",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "test",
		TargetObjectID:   "test1",

		BusinessID:        "biz_validation_fail",
		SourceType:        "front_order",
		SourceID:          "fo_validation_fail",
		AccountingScene:   "test",
		AccountingSubType: "validation",
		FromPartyID:       "client:self",
		ToPartyID:         "test:peer",

		TriggerType:    "front_order",
		TriggerIDValue: "fo_validation_fail",
		TriggerRole:    "primary",

		SettlementID:         "set_validation_fail",
		SettlementMethod:     "invalid_method", // 无效方法，会在事务外校验失败
		SettlementTargetType: "test_target",
		SettlementTargetID:   "target_1",
	}

	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input)
	if err == nil {
		t.Fatal("expected error for invalid settlement_method")
	}

	// 验证没有写入任何数据
	foPage, _ := dbListFrontOrders(ctx, store, frontOrderFilter{FrontOrderID: "fo_validation_fail"})
	if foPage.Total != 0 {
		t.Fatalf("expected 0 front_order after validation failure, got %d", foPage.Total)
	}

	settlementPage, _ := dbListBusinessSettlements(ctx, store, businessSettlementFilter{SettlementID: "set_validation_fail"})
	if settlementPage.Total != 0 {
		t.Fatalf("expected 0 settlement after validation failure, got %d", settlementPage.Total)
	}

	t.Logf("✓ 参数校验失败测试通过：校验失败时未写入任何数据")
}

// TestBusinessBridge_TransactionRollback_MidTxFailure 真正的事务中途失败回滚测试
// 设计意图：前三步成功后，第四步 business_settlement 写入失败，整体回滚
// 实现：先完整创建第一条记录，然后第二次尝试复用 business_id 但使用新的 settlement_id，触发 settlement 层约束失败
func TestBusinessBridge_TransactionRollback_MidTxFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 步骤1：先正常创建一条完整的业务主链（第一次尝试）
	firstInput := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_first_attempt",
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "testdomain.bsv",

		BusinessID:        "biz_shared",
		SourceType:        "front_order",
		SourceID:          "fo_first_attempt",
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:peer",

		// 第一次使用特定的 trigger 组合
		TriggerID:      "trg_first",
		TriggerType:    "front_order",
		TriggerIDValue: "fo_first_attempt",
		TriggerRole:    "primary",

		SettlementID:         "set_first",
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_payment",
		SettlementTargetID:   "tx_first",
	}

	// 第一次调用成功
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, firstInput); err != nil {
		t.Fatalf("first attempt should succeed: %v", err)
	}

	// 验证第一次尝试的数据已存在
	firstFO, _ := dbGetFrontOrder(ctx, store, "fo_first_attempt")
	if firstFO.FrontOrderID == "" {
		t.Fatal("first front_order should exist")
	}
	firstSettlement, _ := dbGetBusinessSettlement(ctx, store, "set_first")
	if firstSettlement.SettlementID == "" {
		t.Fatal("first settlement should exist")
	}

	// 步骤2：第二次尝试，复用 business_id，但新的 front_order、trigger 和 settlement_id
	// 关键：trigger 的组合 (business_id, trigger_type, trigger_id_value, trigger_role) 必须与第一次不同
	// 这样 trigger 可以插入，然后 settlement 会因为 business_id 唯一约束而失败
	secondInput := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_second_attempt", // 新的 front_order
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03ddeeffaa",
		TargetObjectType: "domain",
		TargetObjectID:   "testdomain2.bsv",

		BusinessID:        "biz_shared", // 复用已有的 business_id
		SourceType:        "front_order",
		SourceID:          "fo_second_attempt",
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:peer2",

		// 第二次使用不同的 trigger 组合，触发器可以插入
		TriggerID:      "trg_second",
		TriggerType:    "front_order",
		TriggerIDValue: "fo_second_attempt",
		TriggerRole:    "primary",

		SettlementID:         "set_second", // 新的 settlement_id，但 business_id 冲突
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_payment",
		SettlementTargetID:   "tx_second",
	}

	// 第二次调用应该失败（在 settlement 写入阶段触发 business_id 唯一约束）
	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, secondInput)
	if err == nil {
		t.Fatal("expected transaction failure due to business_id unique constraint on settlement, got nil")
	}

	// 步骤3：验证旧数据仍然存在
	oldFO, _ := dbGetFrontOrder(ctx, store, "fo_first_attempt")
	if oldFO.FrontOrderID == "" {
		t.Fatal("old front_order should still exist")
	}
	oldSettlement, _ := dbGetBusinessSettlement(ctx, store, "set_first")
	if oldSettlement.SettlementID == "" {
		t.Fatal("old settlement should still exist")
	}

	// 步骤4：验证新尝试的数据没有残留（事务已回滚）

	// 4.1 验证新的 front_order 没有残留
	newFO, _ := dbGetFrontOrder(ctx, store, "fo_second_attempt")
	if newFO.FrontOrderID != "" {
		t.Fatal("new front_order should not exist after rollback")
	}

	// 4.2 验证新的 settlement 没有残留
	newSettlement, _ := dbGetBusinessSettlement(ctx, store, "set_second")
	if newSettlement.SettlementID != "" {
		t.Fatal("new settlement should not exist after rollback")
	}

	// 4.3 验证新的 trigger 没有残留
	// 查找 trg_second 应该不存在
	triggerPage, _ := dbListBusinessTriggers(ctx, store, businessTriggerFilter{TriggerID: "trg_second"})
	if triggerPage.Total != 0 {
		t.Fatalf("expected 0 trigger for trg_second after rollback, got %d", triggerPage.Total)
	}

	// 4.4 验证旧的 trigger 仍然存在
	oldTriggerPage, _ := dbListBusinessTriggers(ctx, store, businessTriggerFilter{TriggerID: "trg_first"})
	if oldTriggerPage.Total != 1 {
		t.Fatalf("expected 1 trigger for trg_first, got %d", oldTriggerPage.Total)
	}

	t.Logf("✓ 事务中途失败回滚测试通过：第一次成功，第二次前三步成功后第四步失败导致整体回滚，新数据无残留、旧数据仍在")
}
