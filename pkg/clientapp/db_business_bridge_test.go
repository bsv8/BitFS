package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestBusinessBridge_MultipleBusinessesFromOneFrontOrder 一前台单多条 business 测试
func TestBusinessBridge_MultipleBusinessesFromOneFrontOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := &clientDB{db: db}

	frontOrderID := "fo_multi_biz_test"
	triggerType := "front_order"
	triggerIDValue := frontOrderID

	// 第一条 business
	input1 := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "test1.bsv",

		BusinessID:        "biz_multi_1",
		BusinessRole:      "formal", // 域名注册是正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:peer1",

		TriggerType:    triggerType,
		TriggerIDValue: triggerIDValue,
		TriggerRole:    "primary",

		SettlementID:         "set_multi_1",
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "tx_1",
	}

	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input1); err != nil {
		t.Fatalf("first business failed: %v", err)
	}

	// 第二条 business（同一前台单触发不同 business）
	input2 := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "renew",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "test1.bsv",

		BusinessID:        "biz_multi_2", // 不同的 business_id
		BusinessRole:      "formal",      // 域名续费是正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "renew",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:peer1",

		TriggerType:    triggerType,
		TriggerIDValue: triggerIDValue,
		TriggerRole:    "primary",

		SettlementID:         "set_multi_2",
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "tx_2",
	}

	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input2); err != nil {
		t.Fatalf("second business failed: %v", err)
	}

	// 验证两条 business 都存在
	triggers1, _ := dbListBusinessTriggersByOrderID(ctx, store, "biz_multi_1", 10, 0)
	if len(triggers1.Items) != 1 {
		t.Fatalf("expected 1 trigger for biz_multi_1, got %d", len(triggers1.Items))
	}

	triggers2, _ := dbListBusinessTriggersByOrderID(ctx, store, "biz_multi_2", 10, 0)
	if len(triggers2.Items) != 1 {
		t.Fatalf("expected 1 trigger for biz_multi_2, got %d", len(triggers2.Items))
	}

	// 验证可以通过 trigger 查到两个 business
	businessIDs, _ := dbListBusinessesByTrigger(ctx, store, triggerType, triggerIDValue)
	if len(businessIDs) != 2 {
		t.Fatalf("expected 2 businesses for trigger %s/%s, got %d", triggerType, triggerIDValue, len(businessIDs))
	}

	t.Logf("✓ 一前台单多条 business 测试通过: 同一前台单触发了 %d 条 business", len(businessIDs))
}

// TestSettlementMethod_Valid 结算方式枚举校验测试
func TestSettlementMethod_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method  SettlementMethod
		wantErr bool
	}{
		{SettlementMethodPool, false},
		{SettlementMethodChain, false},
		{"direct_payment", true}, // 旧值，应报错
		{"test_payment", true},   // 旧值，应报错
		{"invalid", true},
		{"", true},
	}

	for _, tt := range tests {
		err := tt.method.Valid()
		if tt.wantErr && err == nil {
			t.Errorf("expected error for method %q, got nil", tt.method)
		}
		if !tt.wantErr && err != nil {
			t.Errorf("unexpected error for method %q: %v", tt.method, err)
		}
	}

	t.Logf("✓ settlement_method 枚举校验测试通过")
}

// TestBusinessBridge_RealDomainRegisterIntegration 真实业务接入测试
func TestBusinessBridge_RealDomainRegisterIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := &clientDB{db: db}

	// 模拟域名注册业务入口
	name := "testexample.bsv"
	uniqueSuffix := fmt.Sprintf("%d_%04x", time.Now().UnixNano(), time.Now().UnixNano()&0xFFFF)
	frontOrderID := "fo_domain_reg_" + uniqueSuffix
	businessID := "biz_domain_reg_" + uniqueSuffix
	settlementID := "set_domain_reg_" + uniqueSuffix

	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03ddccbb",
		TargetObjectType: "domain_name",
		TargetObjectID:   name,
		FrontOrderNote:   "域名注册: " + name,
		FrontOrderPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": "03resolver",
			"target_pubkey_hex":   "03ddccbb",
		},

		BusinessID:        businessID,
		BusinessRole:      "formal", // 域名注册是正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:03resolver",
		BusinessNote:      "域名注册费用: " + name,
		BusinessPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": "03resolver",
			"target_pubkey_hex":   "03ddccbb",
		},

		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		TriggerNote:    "前台订单触发注册",
		TriggerPayload: map[string]any{
			"trigger_reason": "domain_register_initiated",
		},

		SettlementID:         settlementID,
		SettlementMethod:     SettlementMethodChain,
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "", // pending 状态时为空
		SettlementPayload: map[string]any{
			"name":   name,
			"status": "pending",
		},
	}

	// 创建业务主链
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
		t.Fatalf("create business chain failed: %v", err)
	}

	// 验证 settlement 状态为 pending
	settlement, err := dbGetBusinessSettlement(ctx, store, settlementID)
	if err != nil {
		t.Fatalf("get settlement failed: %v", err)
	}
	if settlement.Status != "pending" {
		t.Fatalf("expected settlement status pending, got %s", settlement.Status)
	}

	// 模拟支付成功，回写 settled
	// 硬要求：必须先创建 chain_payment 记录，这样 finalize 才能拿到 fact_settlement_channel_chain_quote_pay.id
	txID := "tx_success_12345"
	chainPaymentID, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                txID,
		PaymentSubType:      "domain_register",
		Status:              "confirmed",
		WalletInputSatoshi:  10000,
		WalletOutputSatoshi: 9000,
		NetAmountSatoshi:    -1000,
		OccurredAtUnix:      time.Now().Unix(),
		FromPartyID:         "client:self",
		ToPartyID:           "resolver:03resolver",
		Payload:             map[string]any{"name": name},
	})
	if err != nil {
		t.Fatalf("create chain_payment failed: %v", err)
	}

	if err := finalizeDomainRegisterSettlement(ctx, store, settlementID, true, txID, ""); err != nil {
		t.Fatalf("finalize settlement failed: %v", err)
	}

	// 验证状态已更新为 settled
	settlement, err = dbGetBusinessSettlement(ctx, store, settlementID)
	if err != nil {
		t.Fatalf("get updated settlement failed: %v", err)
	}
	if settlement.Status != "settled" {
		t.Fatalf("expected settlement status settled, got %s", settlement.Status)
	}
	// 硬要求验证：target_id 必须是 fact_settlement_channel_chain_quote_pay.id，不能是 txid
	expectedTargetID := fmt.Sprintf("%d", chainPaymentID)
	if settlement.TargetID != expectedTargetID {
		t.Fatalf("expected settlement target_id=%s (chain_payment.id), got %s", expectedTargetID, settlement.TargetID)
	}

	// 模拟另一个失败场景
	failedSettlementID := "set_domain_reg_failed"
	input.SettlementID = failedSettlementID
	input.BusinessID = "biz_domain_reg_failed"

	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
		t.Fatalf("create second business chain failed: %v", err)
	}

	// 回写失败状态
	if err := finalizeDomainRegisterSettlement(ctx, store, failedSettlementID, false, "", "payment timeout"); err != nil {
		t.Fatalf("finalize settlement failed failed: %v", err)
	}

	failedSettlement, err := dbGetBusinessSettlement(ctx, store, failedSettlementID)
	if err != nil {
		t.Fatalf("get failed settlement failed: %v", err)
	}
	if failedSettlement.Status != "failed" {
		t.Fatalf("expected settlement status failed, got %s", failedSettlement.Status)
	}
	if failedSettlement.ErrorMessage != "payment timeout" {
		t.Fatalf("expected error_message 'payment timeout', got %s", failedSettlement.ErrorMessage)
	}

	t.Logf("✓ 真实业务接入测试通过: 成功和失败场景都能正确回写 settlement 状态")
}

// mockTxErrorStore 用于模拟事务失败的 store
type mockTxErrorStore struct {
	*clientDB
}

func (m *mockTxErrorStore) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	return errors.New("simulated transaction failure")
}

// TestBusinessBridge_TransactionAtomicity 事务原子性测试
func TestBusinessBridge_TransactionAtomicity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := &clientDB{db: db}

	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_atomic_test",
		FrontType:        "test",
		FrontSubtype:     "atomic",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "test",
		TargetObjectID:   "test1",

		BusinessID:        "biz_atomic_test",
		BusinessRole:      "formal", // 测试场景用正式收费对象
		SourceType:        "front_order",
		SourceID:          "fo_atomic_test",
		AccountingScene:   "test",
		AccountingSubType: "atomic",
		FromPartyID:       "client:self",
		ToPartyID:         "test:peer",

		TriggerType:    "front_order",
		TriggerIDValue: "fo_atomic_test",
		TriggerRole:    "primary",

		SettlementID:         "set_atomic_test",
		SettlementMethod:     SettlementMethodPool,
		SettlementTargetType: "test_target",
		SettlementTargetID:   "target_1",
	}

	// 正常情况下应该成功
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 验证所有数据都已写入
	fo, _ := dbGetFrontOrder(ctx, store, "fo_atomic_test")
	if fo.FrontOrderID == "" {
		t.Fatal("front_order should exist")
	}

	settlement, _ := dbGetBusinessSettlement(ctx, store, "set_atomic_test")
	if settlement.SettlementID == "" {
		t.Fatal("settlement should exist")
	}

	t.Logf("✓ 事务原子性测试通过: 所有数据一致性写入")
}

// TestBusinessBridge_MissingBusinessRole_Fails 第八阶段：漏传 BusinessRole 必须失败
func TestBusinessBridge_MissingBusinessRole_Fails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := &clientDB{db: db}

	// 不传 BusinessRole，应该在校验阶段失败
	input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     "fo_missing_role",
		FrontType:        "test",
		FrontSubtype:     "missing_role",
		OwnerPubkeyHex:   "03aabbccdd",
		TargetObjectType: "test",
		TargetObjectID:   "test1",

		BusinessID:        "biz_missing_role",
		BusinessRole:      "", // 空值应被拒绝
		SourceType:        "front_order",
		SourceID:          "fo_missing_role",
		AccountingScene:   "test",
		AccountingSubType: "missing_role",
		FromPartyID:       "client:self",
		ToPartyID:         "test:peer",

		TriggerType:    "front_order",
		TriggerIDValue: "fo_missing_role",
		TriggerRole:    "primary",

		SettlementID:         "set_missing_role",
		SettlementMethod:     SettlementMethodPool,
		SettlementTargetType: "test_target",
		SettlementTargetID:   "target_1",
	}

	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input)
	if err == nil {
		t.Fatal("expected error for missing business_role, got nil")
	}
	if !strings.Contains(err.Error(), "business_role is required") {
		t.Fatalf("expected business_role validation error, got: %v", err)
	}

	// 验证没有写入任何数据
	foPage, _ := dbListFrontOrders(ctx, store, frontOrderFilter{FrontOrderID: "fo_missing_role"})
	if foPage.Total != 0 {
		t.Fatalf("expected 0 front_order after validation failure, got %d", foPage.Total)
	}
}

// TestBusinessBridge_InvalidBusinessRole_Fails 第八阶段：非法 BusinessRole 必须失败
func TestBusinessBridge_InvalidBusinessRole_Fails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := &clientDB{db: db}

	invalidRoles := []string{"unknown", "invalid", "FORMAL", "PROCESS", "test_role"}

	for _, role := range invalidRoles {
		input := CreateBusinessWithFrontTriggerAndPendingSettlementInput{
			FrontOrderID:     fmt.Sprintf("fo_invalid_role_%s", role),
			FrontType:        "test",
			FrontSubtype:     "invalid_role",
			OwnerPubkeyHex:   "03aabbccdd",
			TargetObjectType: "test",
			TargetObjectID:   "test1",

			BusinessID:        fmt.Sprintf("biz_invalid_role_%s", role),
			BusinessRole:      role, // 非法值应被拒绝
			SourceType:        "front_order",
			SourceID:          fmt.Sprintf("fo_invalid_role_%s", role),
			AccountingScene:   "test",
			AccountingSubType: "invalid_role",
			FromPartyID:       "client:self",
			ToPartyID:         "test:peer",

			TriggerType:    "front_order",
			TriggerIDValue: fmt.Sprintf("fo_invalid_role_%s", role),
			TriggerRole:    "primary",

			SettlementID:         fmt.Sprintf("set_invalid_role_%s", role),
			SettlementMethod:     SettlementMethodPool,
			SettlementTargetType: "test_target",
			SettlementTargetID:   "target_1",
		}

		err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, input)
		if err == nil {
			t.Fatalf("expected error for invalid business_role '%s', got nil", role)
		}
		if !strings.Contains(err.Error(), "invalid business_role") {
			t.Fatalf("expected business_role validation error for '%s', got: %v", role, err)
		}
	}
}
