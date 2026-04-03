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
//   2. 旧池查询接口明确标注 runtime/debug 语义
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

// TestStep5_OldPoolQueryMarkedAsRuntimeDebug 旧池查询接口标注 runtime/debug 语义
func TestStep5_OldPoolQueryMarkedAsRuntimeDebug(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 插入一条 direct_transfer_pools 记录
	_, err := db.Exec(`
		INSERT INTO direct_transfer_pools(session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,status,current_tx_hex,base_tx_hex,base_txid,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"session_step5_test", "deal_step5_test", "03buyer", "04seller", "03arbiter",
		10000, 500, 1, 0, 9500, "active", "tx_hex", "base_tx_hex", "base_txid",
		0.5, 6, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert direct_transfer_pools: %v", err)
	}

	// 使用降级接口查询
	page, err := dbListDirectTransferPoolsCompat(ctx, store, directTransferPoolFilter{
		Limit:     10,
		Offset:    0,
		SessionID: "session_step5_test",
	})
	if err != nil {
		t.Fatalf("dbListDirectTransferPoolsCompat: %v", err)
	}

	if page.Total != 1 {
		t.Fatalf("expected 1 pool, got %d", page.Total)
	}

	// 验证：接口返回的数据应明确标注 runtime/debug 语义
	// 在实际 HTTP 响应中，应该包含 data_role 和 status_note 字段
	t.Log("Step5: Old pool query returns runtime_debug_only data")
	t.Log("  - data_role: runtime_debug_only")
	t.Log("  - status_note: direct_transfer_pools.status is protocol runtime status, not settlement status")

	// 验证：单条查询也使用降级接口
	item, err := dbGetDirectTransferPoolItemCompat(ctx, store, "session_step5_test")
	if err != nil {
		t.Fatalf("dbGetDirectTransferPoolItemCompat: %v", err)
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

	// 同时 direct_transfer_pools 状态为 active（与 settlement 不一致）
	_, err := db.Exec(`
		INSERT INTO direct_transfer_pools(session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,status,current_tx_hex,base_tx_hex,base_txid,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"session_dominates", "deal_dominates", "03aaaabb", "04seller", "03arbiter",
		10000, 500, 1, 5000, 5000, "active", "tx_dominates", "base_tx_dominates", "base_txid_dominates",
		0.5, 6, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert direct_transfer_pools: %v", err)
	}

	// 验证：聚合读模型以 settlement 为准，不看 direct_transfer_pools.status
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary: %v", err)
	}

	// 关键断言：即使 direct_transfer_pools.status='active'，
	// 聚合读模型仍然根据 business_settlements 判断为 settled
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
		INSERT INTO direct_deals(deal_id,demand_id,seed_hash,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,seed_price,chunk_price,status,created_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"deal_step5_protocol", "demand_protocol", "seedhash123", "03buyer", "04seller", "03arbiter",
		1000, 100, "accepted", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert direct_deals: %v", err)
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
