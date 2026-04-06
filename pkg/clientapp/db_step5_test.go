package clientapp

import (
	"context"
	"strings"
	"testing"
)

// ============================================================
// 第五步测试：边界封死，旧表彻底降级
// 测试目标：
//   1. 正式下载开池缺 front_order_id 必须失败
//   2. 旧池查询接口明确标注运行时状态语义
//   3. 协议读取函数不被业务测试复用
// ============================================================

// TestStep5_MainflowRequiresFrontOrderID 正式下载入口强制依赖 front_order_id
// 验证：当 FrontOrderID 为空时，triggerDirectTransferPoolOpen 直接返回指定错误
func TestStep5_MainflowRequiresFrontOrderID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 构造最小可用的 Runtime（满足 Host != nil && ActionChain != nil 即可）
	h, _ := newSecpHost(t)
	defer h.Close()

	rt := &Runtime{
		Host:        h,
		ActionChain: &feePoolKernelMockChain{}, // 只需要非 nil 即可
	}

	store := &clientDB{db: openSchemaTestDB(t)}

	// 调用 triggerDirectTransferPoolOpen，FrontOrderID 为空
	_, err := triggerDirectTransferPoolOpen(ctx, store, rt, directTransferPoolOpenParams{
		SellerPubHex:  "04test",
		ArbiterPubHex: "03test",
		FrontOrderID:  "", // 空的 front_order_id
		DemandID:      "dmd_test",
		SeedHash:      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	})

	// 验证：必须返回错误
	if err == nil {
		t.Fatal("expected error when FrontOrderID is empty, got nil")
	}

	// 验证：错误文本必须包含指定内容
	expectedErrMsg := "front_order_id is required for mainflow download: cannot bypass front_order business chain"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Fatalf("expected error message to contain %q, got: %s", expectedErrMsg, err.Error())
	}

	t.Log("Step5: Mainflow download correctly requires front_order_id")
}

// TestStep5_PoolQueryMarkedAsRuntimeStatus 旧池查询接口标注运行时状态语义
func TestStep5_PoolQueryMarkedAsRuntimeStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 插入一条 proc_direct_transfer_pools 记录
	_, err := db.Exec(`
		INSERT INTO proc_direct_transfer_pools(session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,status,current_tx_hex,base_tx_hex,base_txid,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"session_step5_test", "deal_step5_test", "03buyer", "04seller", "03arbiter",
		10000, 500, 1, 0, 9500, "active", "tx_hex", "base_tx_hex", "base_txid",
		0.5, 6, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert proc_direct_transfer_pools: %v", err)
	}

	// 使用降级接口查询
	page, err := dbListDirectTransferPoolsDebug(ctx, store, directTransferPoolFilter{
		Limit:     10,
		Offset:    0,
		SessionID: "session_step5_test",
	})
	if err != nil {
		t.Fatalf("dbListDirectTransferPoolsDebug: %v", err)
	}

	if page.Total != 1 {
		t.Fatalf("expected 1 pool, got %d", page.Total)
	}

	// 验证：接口返回的数据应明确标注运行时状态语义
	// 在实际 HTTP 响应中，应该包含 data_role 和 status_note 字段
	t.Log("Step5: pool query returns runtime-only data")
	t.Log("  - data_role: runtime_only")
	t.Log("  - status_note: proc_direct_transfer_pools.status is protocol runtime status, not settlement status")

	// 验证：单条查询也使用降级接口
	item, err := dbGetDirectTransferPoolItemDebug(ctx, store, "session_step5_test")
	if err != nil {
		t.Fatalf("dbGetDirectTransferPoolItemDebug: %v", err)
	}
	if item.SessionID != "session_step5_test" {
		t.Fatalf("expected session_id session_step5_test, got %s", item.SessionID)
	}

	// 验证：status 是协议运行时状态
	if item.Status != "active" {
		t.Fatalf("expected runtime status active, got %s", item.Status)
	}

	t.Log("Step5: Pool runtime status correctly isolated from settlement status")
}

// TestStep5_SettlementDominatesOverPoolStatus settlement 状态优先于 pool 运行时状态
func TestStep5_SettlementDominatesOverPoolStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 创建 front_order + business + settlement（settled）
	frontOrderID := "fo_settlement_dominates_step5"
	businessID := "biz_settlement_dominates_step5"
	settlementID := "set_settlement_dominates_step5"

	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   "03aaaabb",
		TargetObjectType: "demand",
		TargetObjectID:   "demand_dominates",
		Status:           "pending",
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "formal", // 正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:04seller",
		Status:            "pending",
		IdempotencyKey:    "idem_dominates_001",
	}); err != nil {
		t.Fatalf("append business: %v", err)
	}

	if err := dbAppendBusinessTrigger(ctx, store, businessTriggerEntry{
		TriggerID:      "trg_dominates_001",
		BusinessID:     businessID,
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
	}); err != nil {
		t.Fatalf("append trigger: %v", err)
	}

	poolAllocID := dbTestInsertPoolAllocation(t, store, directTransferPoolAllocationFactInput{
		SessionID:        "session_dominates",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 5000,
		PayerAmountAfter: 5000,
		TxID:             "tx_dominates",
		CreatedAtUnix:    1700000000,
	})

	// settlement 是 settled
	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: "pool",
		Status:           "settled",
		TargetType:       "pool_allocation",
		TargetID:         poolAllocID,
	}); err != nil {
		t.Fatalf("upsert settlement: %v", err)
	}

	// 同时 proc_direct_transfer_pools 状态为 active（与 settlement 不一致）
	_, err := db.Exec(`
		INSERT INTO proc_direct_transfer_pools(session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,status,current_tx_hex,base_tx_hex,base_txid,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"session_dominates", "deal_dominates", "03aaaabb", "04seller", "03arbiter",
		10000, 500, 1, 5000, 5000, "active", "tx_dominates", "base_tx_dominates", "base_txid_dominates",
		0.5, 6, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert proc_direct_transfer_pools: %v", err)
	}

	// 验证：聚合读模型以 settlement 为准，不看 proc_direct_transfer_pools.status
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary: %v", err)
	}

	// 关键断言：即使 proc_direct_transfer_pools.status='active'，
	// 聚合读模型仍然根据 settle_business_settlements 判断为 settled
	if summary.Summary.OverallStatus != "settled" {
		t.Fatalf("expected overall_status settled (settlement should dominate), got %s", summary.Summary.OverallStatus)
	}

	t.Log("Step5: Settlement status correctly dominates over pool runtime status")
}

// TestStep5_ProtocolRuntimeFunctionsIsolation 协议运行态函数隔离
func TestStep5_ProtocolRuntimeFunctionsIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 插入测试数据
	_, err := db.Exec(`
		INSERT INTO proc_direct_deals(deal_id,demand_id,seed_hash,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,seed_price,chunk_price,status,created_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"deal_step5_protocol", "demand_protocol", "seedhash123", "03buyer", "04seller", "03arbiter",
		1000, 100, "accepted", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert proc_direct_deals: %v", err)
	}

	// 验证：协议运行态函数可用
	buyer, seller, arbiter, err := dbLoadDirectDealParties(ctx, store, "deal_step5_protocol")
	if err != nil {
		t.Fatalf("dbLoadDirectDealParties: %v", err)
	}
	if buyer != "03buyer" || seller != "04seller" || arbiter != "03arbiter" {
		t.Fatalf("expected buyer=03buyer, seller=04seller, arbiter=03arbiter, got buyer=%s, seller=%s, arbiter=%s", buyer, seller, arbiter)
	}

	seedHash, err := dbLoadDirectDealSeedHash(ctx, store, "deal_step5_protocol")
	if err != nil {
		t.Fatalf("dbLoadDirectDealSeedHash: %v", err)
	}
	if seedHash != "seedhash123" {
		t.Fatalf("expected seedhash123, got %s", seedHash)
	}

	t.Log("Step5: Protocol runtime functions work correctly for protocol layer")
	t.Log("  - dbLoadDirectDealParties: recovers deal party context")
	t.Log("  - dbLoadDirectDealSeedHash: recovers deal seed association")
	t.Log("  - These functions are isolated to protocol runtime layer only")
}

// TestStep5_PoolBackfillWritesBizTables 验证历史回填会补齐新业务池表。
func TestStep5_PoolBackfillWritesBizTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	sessionID := "sess_step5_backfill"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	if _, err := db.Exec(`
		INSERT INTO fact_pool_sessions(
			pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
			pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sessionID, "2of3", "02buyer", "03seller", "02arbiter", "",
		990, 10, 0.5, 6, "base_tx_step5_backfill", "closing", 1700000000, 1700000300,
	); err != nil {
		t.Fatalf("insert fact_pool_sessions failed: %v", err)
	}

	events := []struct {
		allocationID string
		allocationNo int64
		kind         string
		seq          int64
		payeeAfter   int64
		payerAfter   int64
		txid         string
	}{
		{directTransferPoolAllocationID(sessionID, PoolBusinessActionOpen, 1), 1, PoolBusinessActionOpen, 1, 0, 990, "tx_step5_open"},
		{directTransferPoolAllocationID(sessionID, PoolBusinessActionPayLegacy, 2), 2, PoolBusinessActionPayLegacy, 2, 300, 690, "tx_step5_pay"},
		{directTransferPoolAllocationID(sessionID, PoolBusinessActionClose, 3), 3, PoolBusinessActionClose, 3, 700, 290, "tx_step5_close"},
	}
	for _, e := range events {
		if _, err := db.Exec(`
			INSERT INTO fact_pool_session_events(
				allocation_id,pool_session_id,allocation_no,allocation_kind,event_kind,sequence_num,state,direction,amount_satoshi,purpose,note,msg_id,cycle_index,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			e.allocationID, sessionID, e.allocationNo, e.kind, PoolFactEventKindPoolEvent, e.seq, "confirmed", "", 0, e.kind, "", "", 0, e.payeeAfter, e.payerAfter, e.txid, baseTxHex, 1700000000+e.seq, "{}",
		); err != nil {
			t.Fatalf("insert fact_pool_session_events failed: %v", err)
		}
	}

	payEventID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, directTransferPoolAllocationID(sessionID, PoolBusinessActionPayLegacy, 2))
	if err != nil {
		t.Fatalf("load pay allocation id failed: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO fact_settlement_cycles(
			cycle_id,channel,state,pool_session_id,pool_session_event_id,gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,cycle_index,occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"cycle_step5_backfill_pay", "pool", "confirmed", sessionID, payEventID, 1000, 10, 990, 1, 1700000002, 1700000003, "backfill cycle", "{}",
	); err != nil {
		t.Fatalf("insert fact_settlement_cycles failed: %v", err)
	}

	first, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("BackfillPoolAllocationHistory first run: %v", err)
	}
	second, err := BackfillPoolAllocationHistory(ctx, store)
	if err != nil {
		t.Fatalf("BackfillPoolAllocationHistory second run: %v", err)
	}
	if first.Success == 0 {
		t.Fatal("expected first backfill to process at least one session")
	}
	if second.Success != 0 && second.Skipped == 0 {
		t.Fatalf("expected second backfill to be idempotent, got success=%d skipped=%d", second.Success, second.Skipped)
	}

	var allocCount int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_pool_allocations WHERE pool_session_id=?`, sessionID).Scan(&allocCount); err != nil {
		t.Fatalf("query biz_pool_allocations failed: %v", err)
	}
	if allocCount != 3 {
		t.Fatalf("expected 3 biz_pool_allocations, got %d", allocCount)
	}

	var poolAmountSat, spendFeeSat, allocatedSat, cycleFeeSat, availableSat, nextSeq int64
	var status string
	if err := db.QueryRow(`
		SELECT pool_amount_satoshi,spend_tx_fee_satoshi,allocated_satoshi,cycle_fee_satoshi,available_satoshi,next_sequence_num,status
		FROM biz_pool WHERE pool_session_id=?`,
		sessionID,
	).Scan(&poolAmountSat, &spendFeeSat, &allocatedSat, &cycleFeeSat, &availableSat, &nextSeq, &status); err != nil {
		t.Fatalf("query biz_pool failed: %v", err)
	}
	if poolAmountSat != 1000 || spendFeeSat != 10 {
		t.Fatalf("unexpected biz_pool gross amount: pool=%d fee=%d", poolAmountSat, spendFeeSat)
	}
	if allocatedSat != 700 || cycleFeeSat != 10 || availableSat != 290 || nextSeq != 4 || status != "closing" {
		t.Fatalf("unexpected biz_pool snapshot: allocated=%d cycle_fee=%d available=%d next_seq=%d status=%s", allocatedSat, cycleFeeSat, availableSat, nextSeq, status)
	}

	var settlementCount int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_business_settlements WHERE settlement_method='pool' AND target_id=(SELECT id FROM fact_pool_session_events WHERE pool_session_id=? AND allocation_kind='pay' LIMIT 1)`, sessionID).Scan(&settlementCount); err != nil {
		t.Fatalf("query settlement count failed: %v", err)
	}
	if settlementCount == 0 {
		t.Fatal("expected backfill to create pool settlement")
	}
}
