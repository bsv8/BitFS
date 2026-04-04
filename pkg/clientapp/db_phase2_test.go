package clientapp

import (
	"context"
	"database/sql"
	"testing"
)

// ==================== 第二阶段专项测试 ====================
// 目标：验证第二阶段清理目标
// 1. open/close 不再是正式收费 business
// 2. 正式收费主线只认 biz_download_pool_*
// 3. 第一次成功 pay 回写 settlement，target_id = pool_allocations.id
// 4. 正式查询只从 front_order -> business_triggers -> business_settlements 走

// seedPhase2DirectTransferPoolFacts 为第二阶段测试准备 pool 事实
// 创建完整的 pool_sessions 和 pool_allocations 记录
func seedPhase2DirectTransferPoolFacts(t *testing.T, db *sql.DB) (sessionID, dealID string) {
	t.Helper()

	ctx := context.Background()
	store := newClientDB(db, nil)
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID = "sess_phase2_" + randHex(4)
	dealID = "deal_phase2_" + randHex(4)
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	// 创建 pool session 和 allocations
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_phase2_" + randHex(4),
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, baseTxHex, 300); err != nil {
		t.Fatalf("seed pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}
	return sessionID, dealID
}

// TestPhase2_OpenIsProcessFact open 生成过程型 fin_business，不是正式收费对象
func TestPhase2_OpenIsProcessFact(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	sessionID, _ := seedPhase2DirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 触发 open accounting
	err := dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_phase2_test",
		BaseTxID:          "base_tx_phase2_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	if err != nil {
		t.Fatalf("open accounting failed: %v", err)
	}

	// 验证：fin_business 中存在 biz_c2c_open_*，但被标记为过程型财务对象
	var businessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&businessCount); err != nil {
		t.Fatalf("query fin_business failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：open 应生成 1 条过程型 fin_business 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 标记为过程型
	var accountingScene, accountingSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM fin_business WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query fin_business fields failed: %v", err)
	}
	if accountingScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：open 的 accounting_scene 应为 direct_transfer_process，got %s", accountingScene)
	}
	if accountingSubtype != "pool_open_lock" {
		t.Fatalf("第二阶段：open 的 accounting_subtype 应为 pool_open_lock，got %s", accountingSubtype)
	}

	// 验证：fin_process_events 中存在过程事件
	var processCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_process_events WHERE process_id=?`, "proc_c2c_transfer_"+sessionID).Scan(&processCount); err != nil {
		t.Fatalf("query fin_process_events failed: %v", err)
	}
	if processCount == 0 {
		t.Fatal("第二阶段：open 应生成 fin_process_event 过程记录")
	}

	// 验证：fin_tx_breakdown 挂的是正式 business_id，不是临时 process_id
	var txBreakdownCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&txBreakdownCount); err != nil {
		t.Fatalf("query fin_tx_breakdown failed: %v", err)
	}
	if txBreakdownCount == 0 {
		t.Fatal("第二阶段：open 的 tx_breakdown 应挂正式 business_id")
	}

	// 验证：没有使用临时的 proc_c2c_open_* 作为 business_id
	var wrongProcessIDCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=?`, "proc_c2c_open_"+sessionID).Scan(&wrongProcessIDCount); err != nil {
		t.Fatalf("query fin_tx_breakdown with wrong id failed: %v", err)
	}
	if wrongProcessIDCount != 0 {
		t.Fatal("第二阶段：禁止用 proc_c2c_open_* 充当 business_id")
	}
}

// TestPhase2_CloseIsProcessFact close 生成过程型 fin_business，不是正式收费对象
func TestPhase2_CloseIsProcessFact(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	sessionID, _ := seedPhase2DirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 触发 close accounting
	err := dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_phase2_test", baseTxHex, 700, 290, sellerPubHex)
	if err != nil {
		t.Fatalf("close accounting failed: %v", err)
	}

	// 验证：fin_business 中存在 biz_c2c_close_*，但被标记为过程型财务对象
	var businessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&businessCount); err != nil {
		t.Fatalf("query fin_business failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 fin_business 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 标记为过程型
	var accountingScene, accountingSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM fin_business WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query fin_business fields failed: %v", err)
	}
	if accountingScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：close 的 accounting_scene 应为 direct_transfer_process，got %s", accountingScene)
	}
	if accountingSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 的 accounting_subtype 应为 pool_close_settle，got %s", accountingSubtype)
	}

	// 验证：fin_process_events 中存在过程事件
	var processCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_process_events WHERE process_id=? AND accounting_subtype='close'`, "proc_c2c_transfer_"+sessionID).Scan(&processCount); err != nil {
		t.Fatalf("query fin_process_events failed: %v", err)
	}
	if processCount == 0 {
		t.Fatal("第二阶段：close 应生成 fin_process_event 过程记录")
	}

	// 验证：fin_tx_breakdown 挂的是正式 business_id，不是临时 process_id
	var txBreakdownCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&txBreakdownCount); err != nil {
		t.Fatalf("query fin_tx_breakdown failed: %v", err)
	}
	if txBreakdownCount == 0 {
		t.Fatal("第二阶段：close 的 tx_breakdown 应挂正式 business_id")
	}

	// 验证：没有使用临时的 proc_c2c_close_* 作为 business_id
	var wrongProcessIDCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=?`, "proc_c2c_close_"+sessionID).Scan(&wrongProcessIDCount); err != nil {
		t.Fatalf("query fin_tx_breakdown with wrong id failed: %v", err)
	}
	if wrongProcessIDCount != 0 {
		t.Fatal("第二阶段：禁止用 proc_c2c_close_* 充当 business_id")
	}
}

// TestPhase2_PayProcessEventUsesProcessID pay 的过程事件使用 process_id
func TestPhase2_PayProcessEventUsesProcessID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	sessionID, _ := seedPhase2DirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)

	// 触发 pay accounting
	err := dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_phase2_test")
	if err != nil {
		t.Fatalf("pay accounting failed: %v", err)
	}

	// 验证：fin_process_events 中存在过程事件
	var processCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_process_events WHERE process_id=? AND accounting_subtype='chunk_pay'`, "proc_c2c_transfer_"+sessionID).Scan(&processCount); err != nil {
		t.Fatalf("query fin_process_events failed: %v", err)
	}
	if processCount == 0 {
		t.Fatal("第二阶段：pay 应生成 fin_process_event 过程记录")
	}

	// 验证：pay 不生成 biz_c2c_pay_* 正式收费对象
	var payBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2").Scan(&payBusinessCount); err != nil {
		t.Fatalf("query fin_business pay failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBusinessCount)
	}

	// 验证：fin_tx_breakdown 存在，且 business_id 指向 download business
	var breakdownCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_tx_breakdown WHERE business_id=? AND txid=?`, "biz_download_pool_test_"+sessionID, "pay_tx_phase2_test").Scan(&breakdownCount); err != nil {
		t.Fatalf("query fin_tx_breakdown failed: %v", err)
	}
	if breakdownCount == 0 {
		t.Fatal("第二阶段：pay 应生成 fin_tx_breakdown 记录，挂到 download business")
	}
}

// TestPhase2_FormalBusinessOnlyViaFrontOrder 正式业务只能通过 front_order 主线读取
func TestPhase2_FormalBusinessOnlyViaFrontOrder(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	frontOrderID := "front_order_phase2_test"
	businessID := "biz_download_pool_phase2_test"
	settlementID := "set_download_pool_phase2_test"

	// 创建 front_order -> business -> settlement 主线
	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType: "demand",
		TargetObjectID:   "demand_phase2_test",
		FrontOrderNote:   "第二阶段测试",
		FrontOrderPayload: map[string]any{
			"test": "phase2",
		},
		BusinessID:        businessID,
		BusinessRole:      "formal", // 测试用正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		BusinessNote:      "第二阶段测试业务",
		BusinessPayload: map[string]any{
			"test": "phase2_business",
		},
		TriggerType:      "front_order",
		TriggerIDValue:   frontOrderID,
		TriggerRole:      "primary",
		TriggerNote:      "测试触发器",
		TriggerPayload:   map[string]any{},
		SettlementID:     settlementID,
		SettlementMethod: SettlementMethodPool,
		SettlementPayload: map[string]any{
			"test": "phase2_settlement",
		},
	})
	if err != nil {
		t.Fatalf("create business chain failed: %v", err)
	}

	// 验证：通过 front_order 能查到 business
	businesses, err := ListBusinessesByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("list businesses by front_order failed: %v", err)
	}
	if len(businesses) != 1 {
		t.Fatalf("expected 1 business, got %d", len(businesses))
	}
	if businesses[0].BusinessID != businessID {
		t.Fatalf("unexpected business_id: got=%s want=%s", businesses[0].BusinessID, businessID)
	}

	// 验证：通过 business_id 能查到 settlement
	settlement, err := GetSettlementByBusinessID(ctx, store, businessID)
	if err != nil {
		t.Fatalf("get settlement by business_id failed: %v", err)
	}
	if settlement.SettlementID != settlementID {
		t.Fatalf("unexpected settlement_id: got=%s want=%s", settlement.SettlementID, settlementID)
	}

	// 验证：settlement 初始状态为 pending
	if settlement.Status != "pending" {
		t.Fatalf("expected settlement status pending, got %s", settlement.Status)
	}
}

// TestPhase2_NoDealIdDirectQuery 禁止从 deal_id/session_id 直接起跳查结算状态
func TestPhase2_NoDealIdDirectQuery(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	sessionID, dealID := seedPhase2DirectTransferPoolFacts(t, db)

	// 验证：不应存在从 deal_id 直接查 settlement 的快捷方式
	// 正式查询必须走 front_order -> business_triggers -> business_settlements

	// 模拟错误用法：尝试直接用 deal_id 查 fin_business（不应有结果）
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE source_type='deal' AND source_id=?`, dealID).Scan(&count); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("第二阶段：禁止直接用 deal_id 作为 source_id 查 fin_business")
	}

	// 模拟错误用法：尝试直接用 session_id 查 business_settlements（不应有结果）
	if err := db.QueryRow(`SELECT COUNT(1) FROM business_settlements WHERE target_type='session' AND target_id=?`, sessionID).Scan(&count); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("第二阶段：禁止直接用 session_id 作为 target_id 查 settlement")
	}
}

// Phase2TestRuntime 第二阶段测试用的简化 Runtime
type Phase2TestRuntime struct {
	Runtime
}

// TestPhase2_MainSettlementStatusReadsFromBusinessSettlements
// 正式结算状态只从 business_settlements 读取
func TestPhase2_MainSettlementStatusReadsFromBusinessSettlements(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	frontOrderID := "front_order_phase2_settlement_test"
	businessID := "biz_download_pool_settlement_test"
	settlementID := "set_download_pool_settlement_test"

	// 创建主线
	err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:      frontOrderID,
		FrontType:         "download",
		FrontSubtype:      "direct_transfer",
		OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType:  "demand",
		TargetObjectID:    "demand_settlement_test",
		BusinessID:        businessID,
		BusinessRole:      "formal", // 测试用正式收费对象
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
	})
	if err != nil {
		t.Fatalf("create business chain failed: %v", err)
	}

	// 更新 settlement 为 settled，target_id 指向 pool_allocation.id
	if err := dbUpdateBusinessSettlementStatus(ctx, store, settlementID, "settled", ""); err != nil {
		t.Fatalf("update settlement status failed: %v", err)
	}
	if err := dbUpdateBusinessSettlementTarget(ctx, store, settlementID, "pool_allocation", "123"); err != nil {
		t.Fatalf("update settlement target failed: %v", err)
	}

	// 验证：通过 GetMainSettlementStatusByFrontOrderIDCompat 能查到 settled 状态
	// 注：这是兼容函数，只取最近一条 business，不代表正式聚合口径
	// 正式应使用 GetFrontOrderSettlementSummary（返回全部 business + 汇总状态）
	settlement, err := GetMainSettlementStatusByFrontOrderIDCompat(ctx, store, frontOrderID)
	if err != nil {
		t.Fatalf("get settlement status failed: %v", err)
	}
	if settlement.Status != "settled" {
		t.Fatalf("expected settlement status settled, got %s", settlement.Status)
	}
	if settlement.TargetType != "pool_allocation" {
		t.Fatalf("expected target_type pool_allocation, got %s", settlement.TargetType)
	}
	if settlement.TargetID != "123" {
		t.Fatalf("expected target_id 123, got %s", settlement.TargetID)
	}
}
