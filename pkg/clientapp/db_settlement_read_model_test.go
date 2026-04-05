package clientapp

import (
	"context"
	"fmt"
	"testing"
)

// ============================================================
// 第四步测试：统一读取口径，旧表降级
// 测试目标：
//   1. 域名聚合查询（单 business）
//   2. 下载聚合查询（多 business 多 seller）
//   3. 底层事实反查（chain_payment.id → settlement → business）
//   4. 旧表不主导（conflict 时仍以 settlement 为准）
// ============================================================

// TestStep4_DomainSettlementSummary 域名聚合查询测试（单 business）
func TestStep4_DomainSettlementSummary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 front_order
	frontOrderID := "fo_domain_step4_001"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03aaaabb",
		TargetObjectType: "domain",
		TargetObjectID:   "step4.bsv",
		Status:           "pending",
		Note:             "域名注册测试",
		Payload:          map[string]any{"domain_name": "step4.bsv"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	// 2. 创建 business
	businessID := "biz_domain_step4_001"
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "formal", // 正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "registry:peer",
		Status:            "pending",
		IdempotencyKey:    "idem_domain_step4_001",
		Note:              "域名注册费用",
		Payload:           map[string]any{"amount_sat": 10000},
	}); err != nil {
		t.Fatalf("append business: %v", err)
	}

	// 3. 创建 trigger 关联
	if err := dbAppendBusinessTrigger(ctx, store, businessTriggerEntry{
		TriggerID:      "trg_domain_step4_001",
		BusinessID:     businessID,
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		Note:           "前台订单触发",
	}); err != nil {
		t.Fatalf("append trigger: %v", err)
	}

	// 4. 创建 pending settlement
	settlementID := "set_domain_step4_001"
	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: "chain",
		Status:           "pending",
		TargetType:       "",
		TargetID:         "",
		Payload:          map[string]any{"status": "pending"},
	}); err != nil {
		t.Fatalf("upsert settlement (pending): %v", err)
	}

	// 5. 验证聚合查询：pending 状态
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary: %v", err)
	}

	if summary.FrontOrder == nil {
		t.Fatal("expected front_order not nil")
	}
	if summary.FrontOrder.FrontOrderID != frontOrderID {
		t.Fatalf("expected front_order_id %s, got %s", frontOrderID, summary.FrontOrder.FrontOrderID)
	}
	if len(summary.Businesses) != 1 {
		t.Fatalf("expected 1 business, got %d", len(summary.Businesses))
	}
	if summary.Businesses[0].Business.BusinessID != businessID {
		t.Fatalf("expected business_id %s, got %s", businessID, summary.Businesses[0].Business.BusinessID)
	}
	if summary.Summary.TotalBusinessCount != 1 {
		t.Fatalf("expected total_business_count 1, got %d", summary.Summary.TotalBusinessCount)
	}
	if summary.Summary.OverallStatus != "pending" {
		t.Fatalf("expected overall_status pending, got %s", summary.Summary.OverallStatus)
	}
	if summary.Summary.PendingCount != 1 {
		t.Fatalf("expected pending_count 1, got %d", summary.Summary.PendingCount)
	}

	// 6. 更新 settlement 为 settled，并写入 chain_payment
	chainPaymentID := dbTestInsertChainPayment(t, store, chainPaymentEntry{
		TxID:                "tx_domain_step4_001",
		PaymentSubType:      "domain_register",
		Status:              "confirmed",
		WalletInputSatoshi:  10000,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    10000,
		BlockHeight:         1000000,
		OccurredAtUnix:      1700000000,
		FromPartyID:         "client:self",
		ToPartyID:           "registry:peer",
		Payload:             map[string]any{"domain": "step4.bsv"},
	})

	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: "chain",
		Status:           "settled",
		TargetType:       "chain_payment",
		TargetID:         chainPaymentID,
		Payload:          map[string]any{"status": "settled"},
	}); err != nil {
		t.Fatalf("upsert settlement (settled): %v", err)
	}

	// 7. 验证聚合查询：settled 状态 + chain_payment 完整链路
	summary2, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary (settled): %v", err)
	}

	if summary2.Summary.OverallStatus != "settled" {
		t.Fatalf("expected overall_status settled, got %s", summary2.Summary.OverallStatus)
	}
	if summary2.Summary.SettledCount != 1 {
		t.Fatalf("expected settled_count 1, got %d", summary2.Summary.SettledCount)
	}
	if summary2.Businesses[0].ChainPayment == nil {
		t.Fatal("expected chain_payment not nil")
	}
	if summary2.Businesses[0].ChainPayment.TxID != "tx_domain_step4_001" {
		t.Fatalf("expected chain_payment txid tx_domain_step4_001, got %s", summary2.Businesses[0].ChainPayment.TxID)
	}
}

// TestStep4_DownloadSettlementSummary 下载聚合查询测试（多 business 多 seller）
func TestStep4_DownloadSettlementSummary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 front_order
	frontOrderID := "fo_download_step4_001"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   "03bbbacc",
		TargetObjectType: "demand",
		TargetObjectID:   "demand_001",
		Status:           "pending",
		Note:             "下载测试",
		Payload:          map[string]any{"demand_id": "demand_001", "seed_hash": "abc123"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	// 2. 创建多个 seller 的 business
	sellers := []struct {
		businessID   string
		settlementID string
		sellerPubHex string
	}{
		{"biz_download_seller1", "set_download_seller1", "04seller1111"},
		{"biz_download_seller2", "set_download_seller2", "04seller2222"},
		{"biz_download_seller3", "set_download_seller3", "04seller3333"},
	}

	for _, s := range sellers {
		// business
		if err := dbAppendFinBusiness(db, finBusinessEntry{
			BusinessID:        s.businessID,
			BusinessRole:      "formal", // 正式收费对象
			SourceType:        "front_order",
			SourceID:          frontOrderID,
			AccountingScene:   "direct_transfer",
			AccountingSubType: "download_pool",
			FromPartyID:       "client:self",
			ToPartyID:         "seller:" + s.sellerPubHex,
			Status:            "pending",
			IdempotencyKey:    "idem_download_" + s.businessID,
			Note:              "下载池支付: demand_001",
			Payload:           map[string]any{"seller": s.sellerPubHex},
		}); err != nil {
			t.Fatalf("append business (%s): %v", s.businessID, err)
		}

		// trigger
		if err := dbAppendBusinessTrigger(ctx, store, businessTriggerEntry{
			TriggerID:      "trg_download_" + s.businessID,
			BusinessID:     s.businessID,
			TriggerType:    "front_order",
			TriggerIDValue: frontOrderID,
			TriggerRole:    "primary",
			Note:           "前台订单触发池支付",
		}); err != nil {
			t.Fatalf("append trigger (%s): %v", s.businessID, err)
		}

		// pending settlement
		if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
			SettlementID:     s.settlementID,
			BusinessID:       s.businessID,
			SettlementMethod: "pool",
			Status:           "pending",
			TargetType:       "",
			TargetID:         "",
			Payload:          map[string]any{"seller": s.sellerPubHex},
		}); err != nil {
			t.Fatalf("upsert settlement pending (%s): %v", s.businessID, err)
		}
	}

	// 3. 验证聚合查询：全部 pending
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary: %v", err)
	}

	if len(summary.Businesses) != 3 {
		t.Fatalf("expected 3 businesses, got %d", len(summary.Businesses))
	}
	if summary.Summary.TotalBusinessCount != 3 {
		t.Fatalf("expected total_business_count 3, got %d", summary.Summary.TotalBusinessCount)
	}
	if summary.Summary.OverallStatus != "pending" {
		t.Fatalf("expected overall_status pending, got %s", summary.Summary.OverallStatus)
	}
	if summary.Summary.PendingCount != 3 {
		t.Fatalf("expected pending_count 3, got %d", summary.Summary.PendingCount)
	}

	// 4. 把 seller1 和 seller2 设为 settled，seller3 仍 pending
	poolAllocID1 := dbTestInsertPoolAllocation(t, store, directTransferPoolAllocationFactInput{
		SessionID:        "session_seller1",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 5000,
		PayerAmountAfter: 5000,
		TxID:             "tx_download_seller1",
		CreatedAtUnix:    1700000000,
	})
	poolAllocID2 := dbTestInsertPoolAllocation(t, store, directTransferPoolAllocationFactInput{
		SessionID:        "session_seller2",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 6000,
		PayerAmountAfter: 4000,
		TxID:             "tx_download_seller2",
		CreatedAtUnix:    1700000001,
	})

	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     sellers[0].settlementID,
		BusinessID:       sellers[0].businessID,
		SettlementMethod: "pool",
		Status:           "settled",
		TargetType:       "pool_allocation",
		TargetID:         poolAllocID1,
		Payload:          map[string]any{"status": "settled"},
	}); err != nil {
		t.Fatalf("upsert settlement settled (seller1): %v", err)
	}
	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     sellers[1].settlementID,
		BusinessID:       sellers[1].businessID,
		SettlementMethod: "pool",
		Status:           "settled",
		TargetType:       "pool_allocation",
		TargetID:         poolAllocID2,
		Payload:          map[string]any{"status": "settled"},
	}); err != nil {
		t.Fatalf("upsert settlement settled (seller2): %v", err)
	}

	// 5. 验证聚合查询：partial_settled
	summary2, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary (partial): %v", err)
	}

	if summary2.Summary.OverallStatus != "partial_settled" {
		t.Fatalf("expected overall_status partial_settled, got %s", summary2.Summary.OverallStatus)
	}
	if summary2.Summary.SettledCount != 2 {
		t.Fatalf("expected settled_count 2, got %d", summary2.Summary.SettledCount)
	}
	if summary2.Summary.PendingCount != 1 {
		t.Fatalf("expected pending_count 1, got %d", summary2.Summary.PendingCount)
	}

	// 验证 pool_allocation 完整链路
	if summary2.Businesses[0].PoolAllocation == nil {
		t.Fatal("expected pool_allocation not nil for seller1")
	}
	if summary2.Businesses[0].PoolAllocation.TxID != "tx_download_seller1" {
		t.Fatalf("expected pool_allocation txid tx_download_seller1, got %s", summary2.Businesses[0].PoolAllocation.TxID)
	}

	// 6. 把 seller3 也设为 settled，验证整体变为 settled
	poolAllocID3 := dbTestInsertPoolAllocation(t, store, directTransferPoolAllocationFactInput{
		SessionID:        "session_seller3",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 7000,
		PayerAmountAfter: 3000,
		TxID:             "tx_download_seller3",
		CreatedAtUnix:    1700000002,
	})
	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     sellers[2].settlementID,
		BusinessID:       sellers[2].businessID,
		SettlementMethod: "pool",
		Status:           "settled",
		TargetType:       "pool_allocation",
		TargetID:         poolAllocID3,
		Payload:          map[string]any{"status": "settled"},
	}); err != nil {
		t.Fatalf("upsert settlement settled (seller3): %v", err)
	}

	summary3, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary (all settled): %v", err)
	}

	if summary3.Summary.OverallStatus != "settled" {
		t.Fatalf("expected overall_status settled, got %s", summary3.Summary.OverallStatus)
	}
	if summary3.Summary.SettledCount != 3 {
		t.Fatalf("expected settled_count 3, got %d", summary3.Summary.SettledCount)
	}
}

// TestStep4_SettlementReverseLookup 底层事实反查测试
func TestStep4_SettlementReverseLookup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建完整链路
	frontOrderID := "fo_reverse_step4_001"
	businessID := "biz_reverse_step4_001"
	settlementID := "set_reverse_step4_001"

	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03ccccdd",
		TargetObjectType: "domain",
		TargetObjectID:   "reverse.bsv",
		Status:           "pending",
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}
	settlementSourceID := ""

	chainPaymentID := dbTestInsertChainPayment(t, store, chainPaymentEntry{
		TxID:               "tx_reverse_001",
		PaymentSubType:     "domain_register",
		Status:             "confirmed",
		WalletInputSatoshi: 10000,
		NetAmountSatoshi:   10000,
		BlockHeight:        1000000,
		OccurredAtUnix:     1700000000,
		FromPartyID:        "client:self",
		ToPartyID:          "registry:peer",
	})
	chainPaymentIntID, err := parseInt64(chainPaymentID)
	if err != nil {
		t.Fatalf("parse chain_payment_id: %v", err)
	}
	settlementCycleID, err := dbGetSettlementCycleByChainPayment(db, chainPaymentIntID)
	if err != nil {
		t.Fatalf("lookup settlement cycle id failed: %v", err)
	}
	settlementSourceID = fmt.Sprintf("%d", settlementCycleID)
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "formal", // 正式收费对象
		SourceType:        "settlement_cycle",
		SourceID:          settlementSourceID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "registry:peer",
		Status:            "confirmed",
		IdempotencyKey:    "idem_reverse_001",
	}); err != nil {
		t.Fatalf("append business: %v", err)
	}
	if err := dbAppendBusinessTrigger(ctx, store, businessTriggerEntry{
		TriggerID:      "trg_reverse_001",
		BusinessID:     businessID,
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
	}); err != nil {
		t.Fatalf("append trigger: %v", err)
	}

	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: "chain",
		Status:           "settled",
		TargetType:       "chain_payment",
		TargetID:         chainPaymentID,
	}); err != nil {
		t.Fatalf("upsert settlement: %v", err)
	}

	// 2. 通过 chain_payment.id 反查 settlement
	cpID, err := parseInt64(chainPaymentID)
	if err != nil {
		t.Fatalf("parse chain_payment_id: %v", err)
	}
	settlement, err := GetSettlementByChainPaymentID(ctx, store, cpID)
	if err != nil {
		t.Fatalf("GetSettlementByChainPaymentID: %v", err)
	}
	if settlement.SettlementID != settlementID {
		t.Fatalf("expected SettlementID %s, got %s", settlementID, settlement.SettlementID)
	}
	if settlement.BusinessID != businessID {
		t.Fatalf("expected business_id %s, got %s", businessID, settlement.BusinessID)
	}

	// 3. 再通过 settlement 查 business（验证完整反查链）
	biz, err := dbGetFinanceBusiness(ctx, store, businessID)
	if err != nil {
		t.Fatalf("get business: %v", err)
	}
	if biz.BusinessID != businessID {
		t.Fatalf("expected business_id %s, got %s", businessID, biz.BusinessID)
	}
	if biz.SourceID != settlementSourceID {
		t.Fatalf("expected source_id (settlement_cycle_id) %s, got %s", settlementSourceID, biz.SourceID)
	}
}

// TestStep4_OldTablesNotDominant 旧表不主导测试
func TestStep4_OldTablesNotDominant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 创建 front_order + business + settlement（settled）
	frontOrderID := "fo_not_dominant_step4_001"
	businessID := "biz_not_dominant_step4_001"
	settlementID := "set_not_dominant_step4_001"

	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   "03ddddee",
		TargetObjectType: "demand",
		TargetObjectID:   "demand_not_dominant",
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
		ToPartyID:         "seller:04sellerold",
		Status:            "pending",
		IdempotencyKey:    "idem_not_dominant_001",
	}); err != nil {
		t.Fatalf("append business: %v", err)
	}
	if err := dbAppendBusinessTrigger(ctx, store, businessTriggerEntry{
		TriggerID:      "trg_not_dominant_001",
		BusinessID:     businessID,
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
	}); err != nil {
		t.Fatalf("append trigger: %v", err)
	}

	poolAllocID := dbTestInsertPoolAllocation(t, store, directTransferPoolAllocationFactInput{
		SessionID:        "session_not_dominant",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 5000,
		PayerAmountAfter: 5000,
		TxID:             "tx_not_dominant",
		CreatedAtUnix:    1700000000,
	})

	if err := dbUpsertBusinessSettlement(ctx, store, businessSettlementEntry{
		SettlementID:     settlementID,
		BusinessID:       businessID,
		SettlementMethod: "pool",
		Status:           "settled", // settlement 是 settled
		TargetType:       "pool_allocation",
		TargetID:         poolAllocID,
	}); err != nil {
		t.Fatalf("upsert settlement: %v", err)
	}

	// 2. 直接写一个 proc_direct_transfer_pools 状态为 active（和 settlement 不一致）
	_, err := db.Exec(`
		INSERT INTO proc_direct_transfer_pools(session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,status,current_tx_hex,base_tx_hex,base_txid,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(session_id) DO UPDATE SET status=excluded.status`,
		"session_not_dominant", "deal_not_dominant", "03ddddee", "04sellerold", "03arbiterold",
		10000, 500, 1, 5000, 5000, "active", "tx_not_dominant", "base_tx_not_dominant", "base_txid_not_dominant",
		2.0, 6, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert proc_direct_transfer_pools: %v", err)
	}

	// 3. 验证聚合读模型仍然以 settlement 为准，不看 proc_direct_transfer_pools
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary: %v", err)
	}

	// 关键断言：即使 proc_direct_transfer_pools.status='active'，
	// 聚合读模型仍然根据 settle_business_settlements 判断为 settled
	if summary.Summary.OverallStatus != "settled" {
		t.Fatalf("expected overall_status settled (settlement should dominate), got %s", summary.Summary.OverallStatus)
	}
	if summary.Summary.SettledCount != 1 {
		t.Fatalf("expected settled_count 1, got %d", summary.Summary.SettledCount)
	}

	// 4. 再验证主读函数（GetFullPoolSettlementChainByFrontOrderID）也返回 settled
	// 注：这是调试函数，只取最近一条 business，不代表正式聚合口径
	poolChain, err := GetFullPoolSettlementChainByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFullPoolSettlementChainByFrontOrderID: %v", err)
	}
	if poolChain.Settlement.Status != "settled" {
		t.Fatalf("expected pool chain settlement status settled, got %s", poolChain.Settlement.Status)
	}
}

// ============================================================
// 测试辅助函数
// ============================================================

func dbTestInsertChainPayment(t *testing.T, store *clientDB, entry chainPaymentEntry) string {
	t.Helper()
	id, err := dbUpsertChainPaymentWithSettlementCycle(context.Background(), store, entry)
	if err != nil {
		t.Fatalf("dbUpsertChainPaymentWithSettlementCycle: %v", err)
	}
	return fmt.Sprintf("%d", id)
}

func dbTestInsertPoolAllocation(t *testing.T, store *clientDB, input directTransferPoolAllocationFactInput) string {
	t.Helper()
	ctx := context.Background()
	allocationID := directTransferPoolAllocationID(input.SessionID, input.AllocationKind, input.SequenceNum)

	_, err := store.db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		allocationID, input.SessionID, 1, input.AllocationKind, input.SequenceNum,
		input.PayeeAmountAfter, input.PayerAmountAfter, input.TxID, input.TxHex, input.CreatedAtUnix,
	)
	if err != nil {
		t.Fatalf("insert pool_allocation: %v", err)
	}

	// 返回自增 id（string 形式，和 settlement target_id 一致）
	id, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		t.Fatalf("get pool_allocation id: %v", err)
	}
	return fmt.Sprintf("%d", id)
}

func parseInt64(s string) (int64, error) {
	var id int64
	_, err := fmt.Sscanf(s, "%d", &id)
	return id, err
}

// TestStep4_FrontOrderNoBusinessYet front_order 存在但还没有 business 的场景
// 这是第四步读口径切换后最容易碰到的真实时刻
func TestStep4_FrontOrderNoBusinessYet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	store := &clientDB{db: db}

	// 1. 只创建 front_order，不创建任何 business
	frontOrderID := "fo_nobiz_step4_001"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   "03eeefff",
		TargetObjectType: "domain",
		TargetObjectID:   "nobiz.bsv",
		Status:           "pending",
		Note:             "刚发起的前台单",
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	// 2. 调聚合查询，不应报错
	summary, err := GetFrontOrderSettlementSummary(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary should not error for front_order with no business: %v", err)
	}

	// 3. 验证返回空摘要
	if summary.FrontOrder == nil {
		t.Fatal("expected front_order not nil")
	}
	if summary.FrontOrder.FrontOrderID != frontOrderID {
		t.Fatalf("expected front_order_id %s, got %s", frontOrderID, summary.FrontOrder.FrontOrderID)
	}
	if len(summary.Businesses) != 0 {
		t.Fatalf("expected 0 businesses, got %d", len(summary.Businesses))
	}
	if summary.Summary.TotalBusinessCount != 0 {
		t.Fatalf("expected total_business_count 0, got %d", summary.Summary.TotalBusinessCount)
	}
	if summary.Summary.PendingCount != 0 {
		t.Fatalf("expected pending_count 0, got %d", summary.Summary.PendingCount)
	}
	if summary.Summary.OverallStatus != "pending" {
		t.Fatalf("expected overall_status pending, got %s", summary.Summary.OverallStatus)
	}
}

// ============================================================
// 主读函数测试
// ============================================================

// TestMainSettlementStatusByFrontOrderID_StillWorks
// 验证：主读函数仍返回单条视图，但不代表正式聚合汇总口径
func TestMainSettlementStatusByFrontOrderID_StillWorks(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	frontOrderID := "front_order_main_test_1"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:   frontOrderID,
		FrontType:      "download",
		FrontSubtype:   "direct_transfer",
		OwnerPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectID: "demand_main_1",
		Status:         "pending",
		Note:           "主口径测试",
		Payload:        map[string]any{"test": "main"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	businessID := "biz_download_pool_main_test_1"
	settlementID := "set_download_pool_main_test_1"
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:      frontOrderID,
		FrontType:         "download",
		FrontSubtype:      "direct_transfer",
		OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType:  "demand",
		TargetObjectID:    "demand_main_1",
		FrontOrderNote:    "主口径测试",
		BusinessID:        businessID,
		BusinessRole:      "formal", // 主口径测试用正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		TriggerType:       "front_order",
		TriggerIDValue:    frontOrderID,
		TriggerRole:       "primary",
		SettlementID:      settlementID,
		SettlementMethod:  SettlementMethodPool,
	}); err != nil {
		t.Fatalf("create business chain: %v", err)
	}

	// 调试函数应返回最近一条 business 的 settlement
	settlement, err := GetMainSettlementStatusByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetMainSettlementStatusByFrontOrderID: %v", err)
	}
	if settlement.SettlementID != settlementID {
		t.Fatalf("expected settlementID %s, got %s", settlementID, settlement.SettlementID)
	}
	if settlement.Status != "pending" {
		t.Fatalf("expected status pending, got %s", settlement.Status)
	}
}

// TestMainFullPoolSettlementChain_StillWorks
// 验证：主读函数仍可读，但不代表正式聚合汇总口径
func TestMainFullPoolSettlementChain_StillWorks(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	frontOrderID := "front_order_main_chain_test"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:   frontOrderID,
		FrontType:      "download",
		FrontSubtype:   "direct_transfer",
		OwnerPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectID: "demand_main_chain",
		Status:         "pending",
		Note:           "主口径链测试",
		Payload:        map[string]any{"test": "main_chain"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}

	businessID := "biz_download_pool_main_chain"
	settlementID := "set_download_pool_main_chain"
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:      frontOrderID,
		FrontType:         "download",
		FrontSubtype:      "direct_transfer",
		OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType:  "demand",
		TargetObjectID:    "demand_main_chain",
		FrontOrderNote:    "主口径链测试",
		BusinessID:        businessID,
		BusinessRole:      "formal", // 主口径测试用正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		TriggerType:       "front_order",
		TriggerIDValue:    frontOrderID,
		TriggerRole:       "primary",
		SettlementID:      settlementID,
		SettlementMethod:  SettlementMethodPool,
	}); err != nil {
		t.Fatalf("create business chain: %v", err)
	}

	// 调试函数应返回单条视图
	poolChain, err := GetFullPoolSettlementChainByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("GetFullPoolSettlementChainByFrontOrderID: %v", err)
	}
	if poolChain.Business.BusinessID != businessID {
		t.Fatalf("expected business_id %s, got %s", businessID, poolChain.Business.BusinessID)
	}
	if poolChain.Settlement.SettlementID != settlementID {
		t.Fatalf("expected settlement_id %s, got %s", settlementID, poolChain.Settlement.SettlementID)
	}
}
