package clientapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ============================================================
// 第五步测试：回填幂等性、一致性校验、旧表降级验证
// ============================================================

// TestBackfillDomainRegisterIdempotent 验证域名回填幂等性
func TestBackfillDomainRegisterIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 先插入一条 chain_payment（模拟历史数据）
	cpID, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                "abcd1234deadbeef567890abcdef1234abcd1234deadbeef567890abcdef1234",
		PaymentSubType:      "domain_register",
		Status:              "confirmed",
		WalletInputSatoshi:  10000,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    9000,
		BlockHeight:         1000,
		OccurredAtUnix:      time.Now().Unix(),
		FromPartyID:         "03testpubkey",
		ToPartyID:           "03resolver",
		Payload: map[string]any{
			"name": "test.bsv",
		},
	})
	if err != nil {
		t.Fatalf("insert chain_payment: %v", err)
	}

	// 2. 第一次回填
	result1, err := BackfillDomainRegisterHistory(ctx, store)
	if err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if result1.Success != 1 {
		t.Fatalf("expected 1 success, got %d", result1.Success)
	}
	if result1.Skipped != 0 {
		t.Fatalf("expected 0 skipped, got %d", result1.Skipped)
	}

	// 3. 验证第一次回填后 front_order 是否存在
	frontOrderID := fmt.Sprintf("fo_backfill_domain_%d", cpID)
	foCheck, err := dbGetFrontOrder(ctx, store, frontOrderID)
	if err != nil {
		t.Logf("front_order after first backfill: error=%v", err)
	} else {
		t.Logf("front_order after first backfill: id=%s, status=%s", foCheck.FrontOrderID, foCheck.Status)
	}

	// 4. 第二次回填（幂等）
	result2, err := BackfillDomainRegisterHistory(ctx, store)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	t.Logf("result2: Success=%d, Skipped=%d", result2.Success, result2.Skipped)
	if result2.Success != 0 {
		t.Fatalf("expected 0 success (already exists), got %d", result2.Success)
	}
	// 注意：第二次回填时，LEFT JOIN 发现 settlement 已存在，不会返回记录
	// 这是正确的过滤行为，幂等性通过 "查询不到需要处理的记录" 来实现
	// 所以 Skipped=0 也是正确的（没有进入处理循环，自然没有跳过计数）

	// 5. 验证数据完整性
	fo, err := dbGetFrontOrder(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get front_order: %v", err)
	}
	if fo.Status != "settled" {
		t.Fatalf("expected front_order status settled, got %s", fo.Status)
	}

	t.Logf("✓ backfill idempotent test passed: first=%+v, second=%+v", result1, result2)
}

// TestBackfillPoolPayGranularity 验证池支付回填粒度对齐
// 整改要点：
// 1. 粒度：一个 seller 一次下载 = 一条 business（只用第一次 pay allocation）
// 2. 完整主链：必须能通过 GetFrontOrderSettlementSummary 查到
func TestBackfillPoolPayGranularity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 pool_session
	sessionID := "sess_backfill_granularity_001"
	sellerPubHex := "03seller_granularity"
	buyerPubHex := "03buyer_granularity"
	_, err := store.db.Exec(`INSERT INTO pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, 
		arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi,
		fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?, '2of3', ?, ?, '03arbiter', '03gateway', 
		10000, 500, 1.0, 6, 'base_txid_001', 'active', ?, ?)`,
		sessionID, buyerPubHex, sellerPubHex, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert pool_session: %v", err)
	}

	// 2. 插入多条 pay allocation（模拟一次下载多次付款）
	// 第一次 pay
	_, err = store.db.Exec(`INSERT INTO pool_allocations(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?, ?, 1, 'pay', 2, 1000, 9000, 'txid_pay_001', 'tx_hex_001', ?)`,
		"poolalloc_"+sessionID+"_pay_2", sessionID, time.Now().Unix())
	if err != nil {
		t.Fatalf("insert first pay allocation: %v", err)
	}

	// 第二次 pay（同一个 session）
	_, err = store.db.Exec(`INSERT INTO pool_allocations(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?, ?, 2, 'pay', 3, 2000, 8000, 'txid_pay_002', 'tx_hex_002', ?)`,
		"poolalloc_"+sessionID+"_pay_3", sessionID, time.Now().Unix())
	if err != nil {
		t.Fatalf("insert second pay allocation: %v", err)
	}

	// 3. 回填
	result, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}

	// 4. 验证粒度：一个 session 只生成 1 条 business（不是 2 条）
	if result.Success != 1 {
		t.Fatalf("expected 1 success (one business per session), got %d", result.Success)
	}

	// 5. 验证完整主链：能通过 GetFrontOrderSettlementSummary 查到
	frontOrderID := "fo_backfill_pool_" + sessionID
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get settlement summary: %v", err)
	}
	if summary.FrontOrder == nil {
		t.Fatalf("front_order not found in summary")
	}
	if summary.FrontOrder.FrontOrderID != frontOrderID {
		t.Fatalf("front_order id mismatch")
	}
	if summary.Summary.OverallStatus != "settled" {
		t.Fatalf("expected settled, got %s", summary.Summary.OverallStatus)
	}

	// 6. 验证 settlement 指向第一次 pay allocation
	if len(summary.Businesses) != 1 {
		t.Fatalf("expected 1 business, got %d", len(summary.Businesses))
	}
	if summary.Businesses[0].Settlement.TargetType != "pool_allocation" {
		t.Fatalf("expected settlement target_type pool_allocation, got %s", summary.Businesses[0].Settlement.TargetType)
	}

	t.Logf("✓ pool pay granularity test passed: session=%s, businesses=%d, status=%s",
		sessionID, len(summary.Businesses), summary.Summary.OverallStatus)
}

// TestBackfillPoolPayIdempotent 验证池支付回填幂等性
func TestBackfillPoolPayIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 pool_session 和 pay allocation
	sessionID := "sess_backfill_idempotent_001"
	_, err := store.db.Exec(`INSERT INTO pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, 
		arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi,
		fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?, '2of3', '03buyer', '03seller', '03arbiter', '03gateway', 
		10000, 500, 1.0, 6, 'base_txid_001', 'active', ?, ?)`,
		sessionID, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert pool_session: %v", err)
	}

	_, err = store.db.Exec(`INSERT INTO pool_allocations(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?, ?, 1, 'pay', 2, 1000, 9000, 'txid_pay_001', 'tx_hex_001', ?)`,
		"poolalloc_"+sessionID+"_pay_2", sessionID, time.Now().Unix())
	if err != nil {
		t.Fatalf("insert pay allocation: %v", err)
	}

	// 2. 第一次回填
	result1, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if result1.Success != 1 {
		t.Fatalf("expected 1 success, got %d", result1.Success)
	}

	// 3. 第二次回填（幂等）
	result2, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if result2.Success != 0 {
		t.Fatalf("expected 0 success (already exists), got %d", result2.Success)
	}
	if result2.Skipped != 1 {
		t.Fatalf("expected 1 skipped (settlement exists), got %d", result2.Skipped)
	}

	t.Logf("✓ pool pay backfill idempotent test passed")
}

// TestBackfillPoolPayPartialRepair 验证半残状态可修复
// 场景：
//   1. 手工只插入 front_order（模拟半残）
//   2. 不插 business/trigger/settlement
//   3. 跑 BackfillPoolAllocationHistory
//   4. 验证：不会被错误跳过，后面三层会被补齐，最终能查到完整聚合
func TestBackfillPoolPayPartialRepair(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 pool_session 和 pay allocation
	sessionID := "sess_backfill_partial_001"
	sellerPubHex := "03seller_partial"
	buyerPubHex := "03buyer_partial"
	_, err := store.db.Exec(`INSERT INTO pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, 
		arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi,
		fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?, '2of3', ?, ?, '03arbiter', '03gateway', 
		10000, 500, 1.0, 6, 'base_txid_001', 'active', ?, ?)`,
		sessionID, buyerPubHex, sellerPubHex, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert pool_session: %v", err)
	}

	_, err = store.db.Exec(`INSERT INTO pool_allocations(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?, ?, 1, 'pay', 2, 1000, 9000, 'txid_pay_001', 'tx_hex_001', ?)`,
		"poolalloc_"+sessionID+"_pay_2", sessionID, time.Now().Unix())
	if err != nil {
		t.Fatalf("insert pay allocation: %v", err)
	}

	// 2. 手工只插入 front_order（制造半残状态）
	frontOrderID := "fo_backfill_pool_" + sessionID
	_, err = store.db.Exec(`INSERT INTO front_orders(
		front_order_id, front_type, front_subtype, owner_pubkey_hex, 
		target_object_type, target_object_id, status, created_at_unix, updated_at_unix, note, payload_json
	) VALUES(?, 'download', 'direct_transfer', ?, 'pool_session', ?, 'settled', ?, ?, '半残测试', '{}')`,
		frontOrderID, buyerPubHex, sessionID, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert partial front_order: %v", err)
	}

	// 3. 回填（不应该因为 front_order 存在就跳过，而应该补齐后面三层）
	result, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}

	// 验证：不是 Skipped，而是 Success（因为 settlement 不存在）
	if result.Skipped != 0 {
		t.Fatalf("expected 0 skipped (settlement not exists), got %d", result.Skipped)
	}
	if result.Success != 1 {
		t.Fatalf("expected 1 success (repaired), got %d", result.Success)
	}

	// 4. 验证最终能查到完整聚合
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get settlement summary: %v", err)
	}
	if summary.FrontOrder == nil {
		t.Fatalf("front_order not found")
	}
	if len(summary.Businesses) != 1 {
		t.Fatalf("expected 1 business, got %d", len(summary.Businesses))
	}
	if summary.Businesses[0].Settlement.SettlementID == "" {
		t.Fatalf("settlement not repaired")
	}
	if summary.Summary.OverallStatus != "settled" {
		t.Fatalf("expected settled, got %s", summary.Summary.OverallStatus)
	}

	t.Logf("✓ pool pay partial repair test passed: session=%s, repaired=%d", sessionID, result.Success)
}

// TestOldModelStatusDoesNotDominate 验证旧表状态不再主导业务判断
// 核心测试：即使 direct_transfer_pools.status='active'，只要 settlement='settled'，整体就是 settled
func TestOldModelStatusDoesNotDominate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建完整的第五步场景：新模型 settled，但旧模型 active
	frontOrderID := "fo_step5_dominance_test"
	businessID := "biz_step5_dominance"
	settlementID := "set_step5_dominance"

	// 创建 front_order
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   "03buyer",
		TargetObjectType: "seed_hash",
		TargetObjectID:   "seed_abc123",
		Status:           "settled",
		Note:             "第五步主导性测试",
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	// 创建 business
	_, err := store.db.Exec(`INSERT INTO fin_business(
		business_id, source_type, source_id, accounting_scene, accounting_subtype,
		from_party_id, to_party_id, status, occurred_at_unix, idempotency_key, note, payload_json
	) VALUES(?, 'front_order', ?, 'direct_transfer', 'pay', '03buyer', '03seller', 'posted', ?, ?, ?, '{}')`,
		businessID, frontOrderID, time.Now().Unix(), "test:"+businessID, "测试业务")
	if err != nil {
		t.Fatalf("insert business: %v", err)
	}

	// 创建 trigger
	_, err = store.db.Exec(`INSERT INTO business_triggers(
		trigger_id, business_id, trigger_type, trigger_id_value, trigger_role, created_at_unix, note, payload_json
	) VALUES(?, ?, 'front_order', ?, 'primary', ?, '', '{}')`,
		"trg_"+businessID, businessID, frontOrderID, time.Now().Unix())
	if err != nil {
		t.Fatalf("insert trigger: %v", err)
	}

	// 创建 settlement = settled
	_, err = store.db.Exec(`INSERT INTO business_settlements(
		settlement_id, business_id, settlement_method, status, target_type, target_id, created_at_unix, updated_at_unix, payload_json
	) VALUES(?, ?, 'pool', 'settled', 'pool_allocation', '12345', ?, ?, '{}')`,
		settlementID, businessID, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert settlement: %v", err)
	}

	// 2. 故意插入一条冲突的旧表记录（active 状态）
	_, err = store.db.Exec(`INSERT INTO direct_transfer_pools(
		session_id, deal_id, buyer_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex,
		pool_amount, spend_tx_fee, sequence_num, seller_amount, buyer_amount, current_tx_hex, base_tx_hex, base_txid, status, fee_rate_sat_byte, lock_blocks, created_at_unix, updated_at_unix
	) VALUES(?, 'deal_conflict', '03buyer', '03seller', '03arbiter',
		10000, 500, 1, 5000, 5000, 'tx_conflict', 'base_conflict', 'base_txid_conflict', 'active', 1.0, 6, ?, ?)`,
		"session_conflict_"+frontOrderID, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert direct_transfer_pools: %v", err)
	}

	// 3. 验证读模型以 settlement 为准
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get settlement summary: %v", err)
	}

	// 关键断言：即使旧表是 active，新模型认定 settled
	if summary.Summary.OverallStatus != "settled" {
		t.Fatalf("expected overall_status settled (new model dominates), got %s", summary.Summary.OverallStatus)
	}

	t.Logf("✓ old model status does not dominate: settlement=%s, old_pool=%s, result=%s",
		"settled", "active", summary.Summary.OverallStatus)
}

// TestReconciliationDetectsInconsistency 验证对账能发现不一致
func TestReconciliationDetectsInconsistency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建一个指向不存在 chain_payment 的 settlement（制造不一致）
	businessID := "biz_reconcile_test"
	settlementID := "set_reconcile_test"

	_, err := store.db.Exec(`INSERT INTO fin_business(
		business_id, source_type, source_id, accounting_scene, accounting_subtype,
		from_party_id, to_party_id, status, occurred_at_unix, idempotency_key, note, payload_json
	) VALUES(?, 'front_order', 'fo_test', 'domain', 'register', '03buyer', '03resolver', 'posted', ?, ?, ?, '{}')`,
		businessID, time.Now().Unix(), "test:"+businessID, "测试")
	if err != nil {
		t.Fatalf("insert business: %v", err)
	}

	// settlement 指向一个不存在的 chain_payment id
	_, err = store.db.Exec(`INSERT INTO business_settlements(
		settlement_id, business_id, settlement_method, status, target_type, target_id, created_at_unix, updated_at_unix, payload_json
	) VALUES(?, ?, 'chain', 'settled', 'chain_payment', '99999', ?, ?, '{}')`,
		settlementID, businessID, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert settlement: %v", err)
	}

	// 2. 运行对账
	report, err := RunDomainRegisterReconciliation(ctx, store)
	if err != nil {
		t.Fatalf("reconciliation: %v", err)
	}

	// 3. 验证发现不一致
	if report.Summary.TotalChecked == 0 {
		t.Fatalf("expected at least 1 checked")
	}
	if report.Summary.InconsistentCount == 0 {
		t.Fatalf("expected at least 1 inconsistency detected")
	}
	if len(report.Inconsistencies) == 0 {
		t.Fatalf("expected inconsistency items")
	}

	// 验证不一致描述包含关键信息
	found := false
	for _, inc := range report.Inconsistencies {
		if strings.Contains(inc.Description, "chain_payment") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected inconsistency about missing chain_payment")
	}

	t.Logf("✓ reconciliation detected inconsistency: %+v", report.Summary)
}

// TestDeprecatedFunctionsStillWork 验证 Deprecated 函数仍可用（兼容）
func TestDeprecatedFunctionsStillWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 插入测试数据
	_, err := store.db.Exec(`INSERT INTO direct_transfer_pools(
		session_id, deal_id, buyer_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex,
		pool_amount, spend_tx_fee, sequence_num, seller_amount, buyer_amount, current_tx_hex, base_tx_hex, base_txid, status, fee_rate_sat_byte, lock_blocks, created_at_unix, updated_at_unix
	) VALUES(?, 'deal_compat', '03buyer', '03seller', '03arbiter',
		10000, 500, 1, 5000, 5000, 'tx_compat', 'base_compat', 'base_txid_compat', 'active', 1.0, 6, ?, ?)`,
		"session_compat_test", time.Now().Unix(), time.Now().Unix())
	if err != nil {
		t.Fatalf("insert direct_transfer_pools: %v", err)
	}

	// 验证 Deprecated 函数仍可用
	item, err := dbGetDirectTransferPoolItem(ctx, store, "session_compat_test")
	if err != nil {
		t.Fatalf("dbGetDirectTransferPoolItem should still work: %v", err)
	}
	if item.SessionID != "session_compat_test" {
		t.Fatalf("expected session_compat_test, got %s", item.SessionID)
	}

	// 验证列表函数仍可用
	page, err := dbListDirectTransferPools(ctx, store, directTransferPoolFilter{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("dbListDirectTransferPools should still work: %v", err)
	}
	if page.Total != 1 {
		t.Fatalf("expected total 1, got %d", page.Total)
	}

	t.Logf("✓ deprecated functions still work for compatibility")
}
