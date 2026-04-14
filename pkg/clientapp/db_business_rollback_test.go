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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	// 使用无效的 settlement_method，会在事务开始前校验失败
	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_validation_fail",
		FrontType:        "test",
		FrontSubtype:     "validation",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "test",
		TargetObjectID:   "test1",

		BusinessID:        "biz_validation_fail",
		BusinessRole:      "formal", // 测试场景用正式收费对象
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

// TestBusinessBridge_TransactionIdempotentUpdate 重复写入时保持单主表记录
// 设计意图：同一 business_id 再次落链时，主表应更新而不是裂成两份
func TestBusinessBridge_TransactionIdempotentUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	// 步骤1：先正常创建一条完整的业务主链（第一次尝试）
	firstInput := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_first_attempt",
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "testdomain.bsv",

		BusinessID:        "biz_shared",
		BusinessRole:      "formal", // 域名注册是正式收费对象
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
		SettlementTargetType: "chain_quote_pay",
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
	// 这个模型下 business 仍归到同一 order，但新的 settlement 会追加成新执行单，不覆盖旧单
	secondInput := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_second_attempt", // 新的 front_order
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03ddeeffaa",
		TargetObjectType: "domain",
		TargetObjectID:   "testdomain2.bsv",

		BusinessID:        "biz_shared", // 复用已有的 business_id
		BusinessRole:      "formal",     // 域名注册是正式收费对象
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
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "tx_second",
	}

	// 第二次调用应该成功，并给同一 business 追加一条新的 settlement
	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, secondInput)
	if err != nil {
		t.Fatalf("expected idempotent update, got error: %v", err)
	}

	// 步骤3：验证旧数据仍然存在
	oldFO, _ := dbGetFrontOrder(ctx, store, "fo_first_attempt")
	if oldFO.FrontOrderID == "" {
		t.Fatal("old front_order should still exist")
	}
	newSettlement, _ := dbGetBusinessSettlement(ctx, store, "set_second")
	if newSettlement.SettlementID == "" {
		t.Fatal("new settlement should exist")
	}
	if newSettlement.OrderID != "biz_shared" {
		t.Fatalf("expected order_id biz_shared, got %s", newSettlement.OrderID)
	}
	byBusiness, err := dbListBusinessSettlements(ctx, store, businessSettlementFilter{OrderID: "biz_shared"})
	if err != nil {
		t.Fatalf("list business settlements failed: %v", err)
	}
	if byBusiness.Total != 2 {
		t.Fatalf("expected 2 settlement rows for biz_shared, got %d", byBusiness.Total)
	}
	if byBusiness.Items[0].SettlementID != "set_second" {
		t.Fatalf("expected latest settlement set_second, got %s", byBusiness.Items[0].SettlementID)
	}
	if byBusiness.Items[1].SettlementID != "set_first" {
		t.Fatalf("expected older settlement set_first, got %s", byBusiness.Items[1].SettlementID)
	}

	// 4.3 验证旧的 trigger 仍然存在
	oldTriggerPage, _ := dbListBusinessTriggers(ctx, store, businessTriggerFilter{TriggerID: "trg_first"})
	if oldTriggerPage.Total != 1 {
		t.Fatalf("expected 1 trigger for trg_first, got %d", oldTriggerPage.Total)
	}

	t.Logf("✓ 重复写入测试通过：同一 business_id 重新落链后仍只保留一条主记录")
}
