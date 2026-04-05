package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func seedDirectTransferPoolFacts(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx := context.Background()
	store := newClientDB(db, nil)
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_third_iter_1"
	dealID := "deal_third_iter_1"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

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
		BaseTxID:       "base_tx_third_iter_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, strings.ToLower(baseTxHex), baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, strings.ToLower(baseTxHex), 300); err != nil {
		t.Fatalf("seed pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, strings.ToLower(baseTxHex)); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}
}

func newWalletAccountingTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

func seedWalletUTXO(t *testing.T, db *sql.DB, utxoID string, txid string, vout int, value int64) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, "wallet1", "addr1", txid, vout, value, "confirmed", "plain_bsv", "",
		txid, "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("insert wallet_utxo %s failed: %v", utxoID, err)
	}
}

func TestRecordDirectPoolCloseAccounting_AppendsUTXOLinks(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 一个最小可解析交易：1 输入，2 输出（seller=700, buyer=290）。
	finalTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      "sess_1",
		DealID:         "deal_sess_1",
		BuyerPeerID:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArbiterPeerID:  "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ArbiterPubKey:  "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_sess_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, "sess_1", "deal_sess_1", "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", finalTxHex, finalTxHex); err != nil {
		t.Fatalf("seed close facts open failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, "sess_1", 3, 700, 290, finalTxHex); err != nil {
		t.Fatalf("seed close facts close failed: %v", err)
	}
	dbRecordDirectPoolCloseAccounting(ctx, store, "sess_1", 3, "", finalTxHex, 700, 290, "seller_peer_1")

	// 第二阶段整改验证：close 生成过程型 settle_businesses，但不是正式收费对象
	var businessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&businessCount); err != nil {
		t.Fatalf("query settle_businesses failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 标记为过程型
	var accountingScene, accountingSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query settle_businesses fields failed: %v", err)
	}
	if accountingScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：close 的 accounting_scene 应为 direct_transfer_process，got %s", accountingScene)
	}
	if accountingSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 的 accounting_subtype 应为 pool_close_settle，got %s", accountingSubtype)
	}

	// 第二阶段：tx_breakdown 和 utxo_links 挂正式的 business_id
	var txid string
	if err := db.QueryRow(`SELECT txid FROM settle_tx_breakdown WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&txid); err != nil {
		t.Fatalf("query settle_tx_breakdown failed: %v", err)
	}
	if txid == "" {
		t.Fatalf("settle_tx_breakdown txid should not be empty")
	}

	type linkCheck struct {
		txRole   string
		ioSide   string
		utxoRole string
		amount   int64
		count    int
	}
	checks := []linkCheck{
		{txRole: "close_final", ioSide: "input", utxoRole: "pool_input", count: 1},
		{txRole: "close_final", ioSide: "output", utxoRole: "settle_to_seller", amount: 700, count: 1},
		{txRole: "close_final", ioSide: "output", utxoRole: "settle_to_buyer", amount: 290, count: 1},
	}
	for _, c := range checks {
		var gotCount int
		var gotAmount int64
		if err := db.QueryRow(
			`SELECT COUNT(1),COALESCE(SUM(l.amount_satoshi),0)
			   FROM settle_tx_utxo_links l
			   JOIN settle_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE l.business_id=? AND l.txid=? AND b.tx_role=? AND l.io_side=? AND l.utxo_role=?`,
			"biz_c2c_close_sess_1", txid, c.txRole, c.ioSide, c.utxoRole,
		).Scan(&gotCount, &gotAmount); err != nil {
			t.Fatalf("query settle_tx_utxo_links tx_role=%s io_side=%s utxo_role=%s failed: %v", c.txRole, c.ioSide, c.utxoRole, err)
		}
		if gotCount != c.count {
			t.Fatalf("tx_role=%s io_side=%s utxo_role=%s count mismatch: got=%d want=%d", c.txRole, c.ioSide, c.utxoRole, gotCount, c.count)
		}
		if c.amount > 0 && gotAmount != c.amount {
			t.Fatalf("tx_role=%s io_side=%s utxo_role=%s amount mismatch: got=%d want=%d", c.txRole, c.ioSide, c.utxoRole, gotAmount, c.amount)
		}
	}
}

func TestDirectTransferPoolRuntimeWritesPoolFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_pool_fact_1"
	dealID := "deal_pool_fact_1"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	currentTxHex := strings.ToLower(baseTxHex)

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
		BaseTxID:       "base_tx_pool_fact_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, currentTxHex, baseTxHex); err != nil {
		t.Fatalf("open runtime write failed: %v", err)
	}

	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, currentTxHex, 300); err != nil {
		t.Fatalf("pay runtime write failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, currentTxHex); err != nil {
		t.Fatalf("close runtime write failed: %v", err)
	}

	var status, scheme, counterparty, openBaseTxID string
	var amountSat, feeSat int64
	if err := db.QueryRow(
		`SELECT status,pool_scheme,counterparty_pubkey_hex,open_base_txid,pool_amount_satoshi,spend_tx_fee_satoshi
		   FROM fact_pool_sessions WHERE pool_session_id=?`,
		sessionID,
	).Scan(&status, &scheme, &counterparty, &openBaseTxID, &amountSat, &feeSat); err != nil {
		t.Fatalf("query pool session failed: %v", err)
	}
	if status != "closing" || scheme != "2of3" || strings.ToLower(counterparty) != sellerPubHex || openBaseTxID != strings.ToLower("base_tx_pool_fact_1") {
		t.Fatalf("unexpected pool session row: status=%s scheme=%s counterparty=%s open_base_txid=%s", status, scheme, counterparty, openBaseTxID)
	}
	if amountSat != 990 || feeSat != 10 {
		t.Fatalf("pool session amount mismatch: amount=%d fee=%d", amountSat, feeSat)
	}

	type allocRow struct {
		Kind  string
		Seq   int64
		Payee int64
		Payer int64
		TxID  string
		TxHex string
	}
	rows, err := db.Query(
		`SELECT allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex
		   FROM fact_pool_session_events WHERE pool_session_id=? ORDER BY allocation_no ASC`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("query pool allocations failed: %v", err)
	}
	defer rows.Close()
	got := make([]allocRow, 0, 3)
	for rows.Next() {
		var row allocRow
		if err := rows.Scan(&row.Kind, &row.Seq, &row.Payee, &row.Payer, &row.TxID, &row.TxHex); err != nil {
			t.Fatalf("scan pool allocation failed: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pool allocations failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("pool allocation count mismatch: got=%d want=3", len(got))
	}
	wantKinds := []string{"open", "pay", "close"}
	for i, want := range wantKinds {
		if got[i].Kind != want {
			t.Fatalf("allocation kind mismatch at %d: got=%s want=%s", i, got[i].Kind, want)
		}
	}
	if got[0].Seq != 1 || got[0].Payee != 0 || got[0].Payer != 990 {
		t.Fatalf("open allocation mismatch: %+v", got[0])
	}
	if got[1].Seq != 2 || got[1].Payee != 300 || got[1].Payer != 690 {
		t.Fatalf("pay allocation mismatch: %+v", got[1])
	}
	if got[2].Seq != 3 || got[2].Payee != 700 || got[2].Payer != 290 {
		t.Fatalf("close allocation mismatch: %+v", got[2])
	}
}

// TestDirectTransferAccounting_SourceIDUsesAutoIncrementID 验证第二步整改 + 第二阶段
// source_id 应该是 fact_pool_session_events.id（整数），而不是 allocation_id（业务键）
// 第二阶段：open/close 为过程型财务对象，只验证 pay 的 source_id 格式
func TestDirectTransferAccounting_SourceIDUsesAutoIncrementID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 第二阶段整改验证：open/close 生成过程型 settle_businesses，不是正式收费对象
	// 设计意图：open/close 保留为过程型财务对象，正式收费主线只认 biz_download_pool_*
	var openBusinessCount, closeBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&openBusinessCount); err != nil {
		t.Fatalf("query settle_businesses open failed: %v", err)
	}
	if openBusinessCount != 1 {
		t.Fatalf("第二阶段：open 应生成 1 条过程型 settle_businesses 记录，got %d", openBusinessCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&closeBusinessCount); err != nil {
		t.Fatalf("query settle_businesses close failed: %v", err)
	}
	if closeBusinessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", closeBusinessCount)
	}

	// 验证：open/close 的 accounting_scene 标记为过程型
	var openScene, openSubtype, closeScene, closeSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&openScene, &openSubtype); err != nil {
		t.Fatalf("query open settle_businesses fields failed: %v", err)
	}
	if openScene != "direct_transfer_process" || openSubtype != "pool_open_lock" {
		t.Fatalf("第二阶段：open 应标记为过程型: scene=%s subtype=%s", openScene, openSubtype)
	}
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&closeScene, &closeSubtype); err != nil {
		t.Fatalf("query close settle_businesses fields failed: %v", err)
	}
	if closeScene != "direct_transfer_process" || closeSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 应标记为过程型: scene=%s subtype=%s", closeScene, closeSubtype)
	}

	// 获取 pay allocation 的自增 id
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)

	// 验证：pay 不再生成 biz_c2c_pay_* 正式收费对象，只保留 fin_process_event + settle_tx_breakdown
	var payBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2").Scan(&payBusinessCount); err != nil {
		t.Fatalf("query settle_businesses pay failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBusinessCount)
	}

	// 验证：fin_process_event 存在，process_id = proc_c2c_transfer_<sessionID>
	var procCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE process_id=? AND accounting_subtype='chunk_pay'`, "proc_c2c_transfer_"+sessionID).Scan(&procCount); err != nil {
		t.Fatalf("query settle_process_events pay failed: %v", err)
	}
	if procCount == 0 {
		t.Fatal("pay 应生成 fin_process_event 过程记录")
	}

	// 验证：settle_process_events 的 source_id 是 fact_pool_session_events.id
	var procSourceType, procSourceIDStr string
	if err := db.QueryRow(
		`SELECT source_type, source_id FROM settle_process_events WHERE process_id=? AND accounting_subtype='chunk_pay' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&procSourceType, &procSourceIDStr); err != nil {
		t.Fatalf("query settle_process_events pay source failed: %v", err)
	}
	if procSourceType != "pool_allocation" {
		t.Fatalf("unexpected source_type for pay process: got=%s want=pool_allocation", procSourceType)
	}
	wantSourceIDStr := fmt.Sprintf("%d", payID)
	if procSourceIDStr != wantSourceIDStr {
		if procSourceIDStr == payAllocID {
			t.Fatalf("source_id is still allocation_id for pay: got=%s (expected integer id)", procSourceIDStr)
		}
		t.Fatalf("unexpected source_id for pay: got=%s want=%s", procSourceIDStr, wantSourceIDStr)
	}

	// 验证：settle_tx_breakdown 存在，且 business_id 指向 download business
	var breakdownCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_tx_breakdown WHERE business_id=? AND txid=?`, "biz_download_pool_test_"+sessionID, "pay_tx_third_iter_1").Scan(&breakdownCount); err != nil {
		t.Fatalf("query settle_tx_breakdown pay failed: %v", err)
	}
	if breakdownCount == 0 {
		t.Fatal("pay 应生成 settle_tx_breakdown 记录，挂到 download business")
	}
}

// TestDirectTransferAccounting_ProcessEventsSourceIDUsesAutoIncrementID 验证第二步整改
// process events 的 source_id 应该是 fact_pool_session_events.id（整数）
func TestDirectTransferAccounting_ProcessEventsSourceIDUsesAutoIncrementID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 获取各 allocation 的自增 id
	openAllocID := directTransferPoolAllocationID(sessionID, "open", 1)
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	closeAllocID := directTransferPoolAllocationID(sessionID, "close", 3)

	openID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, openAllocID)
	payID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	closeID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, closeAllocID)

	type wantRow struct {
		subtype        string
		expectedSource int64
	}
	checks := []wantRow{
		{subtype: "open", expectedSource: openID},
		{subtype: "chunk_pay", expectedSource: payID},
		{subtype: "close", expectedSource: closeID},
	}
	for _, want := range checks {
		var gotSourceType, gotSourceIDStr, gotAccountingScene, gotAccountingSubtype string
		if err := db.QueryRow(
			`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM settle_process_events WHERE process_id=? AND accounting_subtype=? ORDER BY id DESC LIMIT 1`,
			"proc_c2c_transfer_"+sessionID, want.subtype,
		).Scan(&gotSourceType, &gotSourceIDStr, &gotAccountingScene, &gotAccountingSubtype); err != nil {
			t.Fatalf("query settle_process_events %s failed: %v", want.subtype, err)
		}
		if gotSourceType != "pool_allocation" {
			t.Fatalf("unexpected source_type for %s: got=%s want=pool_allocation", want.subtype, gotSourceType)
		}
		// 第二步整改验证：source_id 应该是整数主键
		wantSourceIDStr := fmt.Sprintf("%d", want.expectedSource)
		if gotSourceIDStr != wantSourceIDStr {
			if gotSourceIDStr == openAllocID || gotSourceIDStr == payAllocID || gotSourceIDStr == closeAllocID {
				t.Fatalf("source_id is still allocation_id for %s: got=%s (expected integer id)", want.subtype, gotSourceIDStr)
			}
			t.Fatalf("unexpected source_id for %s: got=%s want=%s", want.subtype, gotSourceIDStr, wantSourceIDStr)
		}
		if gotAccountingScene == "" || gotAccountingSubtype == "" {
			t.Fatalf("missing accounting fields for %s", want.subtype)
		}
	}
}

func TestDirectTransferAccounting_ReadableByPrimaryQueries(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 验证：pay 不再生成 biz_c2c_pay_* 正式收费对象
	var payBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_pay_sess_third_iter_1_2").Scan(&payBusinessCount); err != nil {
		t.Fatalf("query settle_businesses pay failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBusinessCount)
	}

	// 验证：process events 可读取
	page, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		ProcessID:         "proc_c2c_transfer_" + sessionID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "close",
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("process read failed: %v", err)
	}
	if page.Total == 0 || len(page.Items) == 0 {
		t.Fatalf("process read returned no rows")
	}
	// 第六次迭代：验证主口径字段
	if page.Items[0].SourceType != "pool_allocation" || page.Items[0].SourceID == "" {
		t.Fatalf("process new fields incorrect: %+v", page.Items[0])
	}
}

func TestDirectTransferAccounting_CloseUsesExplicitSequence(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_close_seq_7"
	dealID := "deal_close_seq_7"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

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
		BaseTxID:       "base_tx_close_seq_7",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 7, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}

	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 7, "close_tx_seq_7", baseTxHex, 700, 290, sellerPubHex)

	// 第二阶段整改：验证 close 生成过程型 settle_businesses，不是正式收费对象
	wantAllocID := directTransferPoolAllocationID(sessionID, "close", 7)
	wantID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, wantAllocID)
	wantIDStr := fmt.Sprintf("%d", wantID)

	// 验证：settle_businesses 中存在 biz_c2c_close_*，但被标记为过程型
	var businessCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_close_"+sessionID,
	).Scan(&businessCount); err != nil {
		t.Fatalf("query settle_businesses failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 和 accounting_subtype 标记为过程型
	var closeScene, closeSubtype string
	if err := db.QueryRow(
		`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_close_"+sessionID,
	).Scan(&closeScene, &closeSubtype); err != nil {
		t.Fatalf("query close settle_businesses fields failed: %v", err)
	}
	if closeScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：close 的 accounting_scene 应为 direct_transfer_process，got %s", closeScene)
	}
	if closeSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 的 accounting_subtype 应为 pool_close_settle，got %s", closeSubtype)
	}

	// 验证：settle_process_events 的 source_id 是 fact_pool_session_events.id
	var processSourceID string
	if err := db.QueryRow(
		`SELECT source_id FROM settle_process_events WHERE process_id=? AND accounting_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processSourceID); err != nil {
		t.Fatalf("query process source id failed: %v", err)
	}
	if processSourceID != wantIDStr {
		if processSourceID == wantAllocID {
			t.Fatalf("process source_id is still allocation_id (old logic): got=%s want=%s", processSourceID, wantIDStr)
		}
		t.Fatalf("unexpected process source_id: got=%s want=%s", processSourceID, wantIDStr)
	}
}

// TestFinBusinessIdempotency 验证 settle_businesses 幂等写入
// 第六次迭代整改：验证 ON CONFLICT(idempotency_key) DO UPDATE 幂等性
func TestFinBusinessIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	businessID := "biz_idem_test_1"
	idempotencyKey := "idem_key_1"

	// 第一次写入
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "process", // 过程财务对象
		SourceType:        "pool_allocation",
		SourceID:          "alloc_1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "open",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:abc",
		Status:            "pending",
		OccurredAtUnix:    1700000001,
		IdempotencyKey:    idempotencyKey,
		Note:              "first write",
		Payload:           map[string]any{"seq": 1},
	}); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// 第二次写入（相同 idempotency_key）- 应该更新而不是报错
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "process", // 过程财务对象
		SourceType:        "pool_allocation",
		SourceID:          "alloc_1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "open",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:abc",
		Status:            "posted",   // 更新状态
		OccurredAtUnix:    1700000002, // 更新时间
		IdempotencyKey:    idempotencyKey,
		Note:              "second write",           // 更新 note
		Payload:           map[string]any{"seq": 2}, // 更新 payload
	}); err != nil {
		t.Fatalf("second write (idempotent update) failed: %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM settle_businesses WHERE idempotency_key=?`,
		idempotencyKey,
	).Scan(&status, &note, &payload); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "posted" {
		t.Fatalf("status not updated: got=%s want=posted", status)
	}
	if note != "second write" {
		t.Fatalf("note not updated: got=%s want='second write'", note)
	}
	if !strings.Contains(payload, `"seq":2`) {
		t.Fatalf("payload not updated: got=%s", payload)
	}

	_ = ctx
	_ = store
}

// TestFinProcessEventIdempotency 验证 settle_process_events 幂等写入
// 第六次迭代整改：验证 ON CONFLICT(idempotency_key) DO UPDATE 幂等性
func TestFinProcessEventIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	processID := "proc_idem_test_1"
	idempotencyKey := "idem_proc_1"

	// 第一次写入
	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         processID,
		SourceType:        "pool_allocation",
		SourceID:          "alloc_1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "pay",
		EventType:         "accounting",
		Status:            "pending",
		OccurredAtUnix:    1700000001,
		IdempotencyKey:    idempotencyKey,
		Note:              "first event",
		Payload:           map[string]any{"amount": 100},
	}); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// 第二次写入（相同 idempotency_key）- 应该更新而不是报错
	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         processID,
		SourceType:        "pool_allocation",
		SourceID:          "alloc_1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "pay",
		EventType:         "accounting",
		Status:            "applied",  // 更新状态
		OccurredAtUnix:    1700000002, // 更新时间
		IdempotencyKey:    idempotencyKey,
		Note:              "second event",                // 更新 note
		Payload:           map[string]any{"amount": 200}, // 更新 payload
	}); err != nil {
		t.Fatalf("second write (idempotent update) failed: %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM settle_process_events WHERE idempotency_key=?`,
		idempotencyKey,
	).Scan(&status, &note, &payload); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "applied" {
		t.Fatalf("status not updated: got=%s want=applied", status)
	}
	if note != "second event" {
		t.Fatalf("note not updated: got=%s want='second event'", note)
	}
	if !strings.Contains(payload, `"amount":200`) {
		t.Fatalf("payload not updated: got=%s", payload)
	}

	_ = ctx
	_ = store
}

// TestDirectTransferAccountingIdempotency 验证真实业务链路的幂等性
// 第六次迭代整改：用真实业务函数重复调用验证幂等性
func TestDirectTransferAccountingIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	// 复用 seedDirectTransferPoolFacts 创建的 sessionID，避免重复创建 pool_allocation
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 先创建 open 记录
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_idem_real",
		BaseTxID:          "base_tx_idem_real",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})

	// 第一次 pay 记录
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_idem_real_1")

	// 获取第一次写入后的状态 - 验证 pay 不生成 settle_businesses
	var firstCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&firstCount); err != nil {
		t.Fatalf("first count failed: %v", err)
	}
	if firstCount != 0 {
		t.Fatalf("expected 0 settle_businesses for pay after first write, got %d (pay no longer creates business)", firstCount)
	}

	// 验证 fin_process_event 存在
	var firstProcCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`,
		"c2c_pay_event:"+sessionID+":2",
	).Scan(&firstProcCount); err != nil {
		t.Fatalf("first process count failed: %v", err)
	}
	if firstProcCount != 1 {
		t.Fatalf("expected 1 process event after first write, got %d", firstProcCount)
	}

	// 第二次 pay 记录（相同 idempotency_key）- 应该更新而不是报错
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_idem_real_2")

	// 验证 settle_businesses 仍然没有
	var secondCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&secondCount); err != nil {
		t.Fatalf("second count failed: %v", err)
	}
	if secondCount != 0 {
		t.Fatalf("expected 0 settle_businesses for pay after idempotent update, got %d", secondCount)
	}

	// 验证 process event 也只有一条（幂等更新）
	var procCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`,
		"c2c_pay_event:"+sessionID+":2",
	).Scan(&procCount); err != nil {
		t.Fatalf("process event count failed: %v", err)
	}
	if procCount != 1 {
		t.Fatalf("expected 1 process event record, got %d", procCount)
	}
}

// TestRecordWalletChainAccounting_WritesBreakdownWithTxRole 验证 wallet chain 链路写出的 breakdown 带 tx_role
func TestRecordWalletChainAccounting_WritesBreakdownWithTxRole(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_role_1"
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 800,
		NetSat:          -200,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var txRole string
	if err := db.QueryRow(
		`SELECT tx_role FROM settle_tx_breakdown WHERE business_id=? AND txid=?`,
		"biz_wallet_chain_"+txid, txid,
	).Scan(&txRole); err != nil {
		t.Fatalf("query breakdown failed: %v", err)
	}
	if txRole != "internal_change" {
		t.Fatalf("expected tx_role=internal_change, got=%s", txRole)
	}
}

// TestDirectTransferAccounting_CompatibilityQueryByAllocationID 验证兼容层
// 使用 allocation_id 查询能正确映射到新的 source_id
func TestDirectTransferAccounting_CompatibilityQueryByAllocationID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"

	// 第二阶段：改用 pay 测试（pay 不再生成 settle_businesses，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_compat_test")

	// 使用 pay allocation_id 查询（兼容层）
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)

	// 验证：pay 不再生成 settle_businesses（正式查询应返回空）
	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, payAllocID, "formal", 10, 0)
	if err != nil {
		t.Fatalf("compatibility query failed: %v", err)
	}
	if bizPage.Total != 0 {
		t.Fatalf("pay 不应生成 settle_businesses，got %d", bizPage.Total)
	}

	// 验证：process events 存在（pay 生成 process events）
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, payAllocID, 10, 0)
	if err != nil {
		t.Fatalf("compatibility query for process events failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatal("compatibility query for process events returned no results")
	}
}

// TestRecordWalletChainAccounting_UsesChainPaymentID 验证第二步整改
// wallet_chain 财务记录的 source_id 应该是 fact_chain_payments.id
func TestRecordWalletChainAccounting_UsesChainPaymentID(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_payment_id_test"
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            txid,
		Category:        "REPAYMENT",
		WalletInputSat:  0,
		WalletOutputSat: 5000,
		NetSat:          5000,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	// 验证 fact_chain_payments 记录已创建
	var chainPaymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&chainPaymentID); err != nil {
		t.Fatalf("chain_payment not found: %v", err)
	}
	if chainPaymentID <= 0 {
		t.Fatalf("expected positive chain_payment id, got %d", chainPaymentID)
	}

	// 验证 settle_businesses 的 source_id 是 fact_chain_payments.id
	var sourceType, sourceID string
	if err := db.QueryRow(
		`SELECT source_type, source_id FROM settle_businesses WHERE business_id=?`,
		"biz_wallet_chain_"+txid,
	).Scan(&sourceType, &sourceID); err != nil {
		t.Fatalf("settle_businesses not found: %v", err)
	}

	if sourceType != "chain_payment" {
		t.Fatalf("unexpected source_type: got=%s want=chain_payment", sourceType)
	}
	expectedSourceID := fmt.Sprintf("%d", chainPaymentID)
	if sourceID != expectedSourceID {
		// 如果 source_id 是 txid，说明还是旧逻辑
		if sourceID == txid {
			t.Fatalf("source_id is still txid (old logic), expected chain_payment id: %s", expectedSourceID)
		}
		t.Fatalf("unexpected source_id: got=%s want=%s", sourceID, expectedSourceID)
	}
}

// TestRecordWalletChainAccounting_QueryByTxIDUsesChainPaymentID
// 第二阶段兼容层：查询可以继续用 txid，但底层必须换算成 fact_chain_payments.id
func TestRecordWalletChainAccounting_QueryByTxIDUsesChainPaymentID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	txid := "tx_chain_query_test"

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            txid,
		Category:        "REPAYMENT",
		WalletInputSat:  0,
		WalletOutputSat: 5000,
		NetSat:          5000,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var chainPaymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&chainPaymentID); err != nil {
		t.Fatalf("chain_payment lookup failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", chainPaymentID)

	// 第十一阶段：必须显式传 businessRole
	// wallet_chain 记录是过程型财务对象，所以传 process
	bizPage, err := dbListFinanceBusinessesByTxID(ctx, store, txid, "process", 10, 0)
	if err != nil {
		t.Fatalf("business lookup by txid failed: %v", err)
	}
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("business lookup by txid mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "chain_payment" || bizPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("business lookup by txid returned wrong source: %+v", bizPage.Items[0])
	}

	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         "proc_wallet_chain_query_" + txid,
		SourceType:        "chain_payment",
		SourceID:          wantSourceID,
		AccountingScene:   "wallet_transfer",
		AccountingSubType: "query_probe",
		EventType:         "accounting",
		Status:            "applied",
		IdempotencyKey:    "wallet_chain_query:" + txid,
		Note:              "query probe",
		Payload:           map[string]any{"txid": txid},
	}); err != nil {
		t.Fatalf("append process event failed: %v", err)
	}

	procPage, err := dbListFinanceProcessEventsByTxID(ctx, store, txid, 10, 0)
	if err != nil {
		t.Fatalf("process lookup by txid failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("process lookup by txid mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}
	if procPage.Items[0].SourceType != "chain_payment" || procPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("process lookup by txid returned wrong source: %+v", procPage.Items[0])
	}
}

// TestRecordWalletChainAccounting_ChainPaymentUTXOLinks 验证 wallet_chain 链路补 utxo links
func TestRecordWalletChainAccounting_ChainPaymentUTXOLinks(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)

	txid := "tx_chain_utxo_link_test"
	inputUTXO := txid + ":0"
	changeUTXO := txid + ":1"
	counterpartyUTXO := txid + ":2"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 3000)
	seedWalletUTXO(t, db, changeUTXO, txid, 1, 800)
	seedWalletUTXO(t, db, counterpartyUTXO, txid, 2, 600)

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  3000,
		WalletOutputSat: 800,
		NetSat:          -2200,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000, Note: "wallet input"},
			{UTXOID: changeUTXO, IOSide: "output", UTXORole: "wallet_change", AmountSatoshi: 800, Note: "wallet change"},
			{UTXOID: counterpartyUTXO, IOSide: "output", UTXORole: "counterparty_out", AmountSatoshi: 600, Note: "must skip"},
			{UTXOID: "missing:0", IOSide: "output", UTXORole: "external_in", AmountSatoshi: 1, Note: "missing must skip"},
		},
		ProcessEvents: []finProcessEventEntry{
			{
				ProcessID:         "proc_wallet_chain_" + txid,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "change_probe",
				EventType:         "accounting",
				Status:            "applied",
				IdempotencyKey:    "wallet_chain_event:" + txid,
				Note:              "wallet chain event",
				Payload:           map[string]any{"mark": "yes"},
			},
		},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var paymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&paymentID); err != nil {
		t.Fatalf("query chain_payment failed: %v", err)
	}
	type wantLink struct {
		utxoID   string
		ioSide   string
		utxoRole string
		amount   int64
	}
	wants := []wantLink{
		{utxoID: inputUTXO, ioSide: "input", utxoRole: "wallet_input", amount: 3000},
		{utxoID: changeUTXO, ioSide: "output", utxoRole: "wallet_change", amount: 800},
	}
	for _, want := range wants {
		var count int
		var amount int64
		if err := db.QueryRow(
			`SELECT COUNT(1),COALESCE(SUM(amount_satoshi),0) FROM fact_chain_payment_utxo_links
			  WHERE chain_payment_id=? AND utxo_id=? AND io_side=? AND utxo_role=?`,
			paymentID, want.utxoID, want.ioSide, want.utxoRole,
		).Scan(&count, &amount); err != nil {
			t.Fatalf("query link %s failed: %v", want.utxoID, err)
		}
		if count != 1 {
			t.Fatalf("expected 1 link for %s, got %d", want.utxoID, count)
		}
		if amount != want.amount {
			t.Fatalf("unexpected amount for %s: got=%d want=%d", want.utxoID, amount, want.amount)
		}
	}

	var skippedCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE chain_payment_id=? AND utxo_role IN ('counterparty_out','external_in')`,
		paymentID,
	).Scan(&skippedCount); err != nil {
		t.Fatalf("query skipped links failed: %v", err)
	}
	if skippedCount != 0 {
		t.Fatalf("expected skipped roles not to be written, got %d", skippedCount)
	}

	var eventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE process_id=?`, "proc_wallet_chain_"+txid).Scan(&eventCount); err != nil {
		t.Fatalf("query process event failed: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 process event, got %d", eventCount)
	}
}

func TestRecordWalletChainAccounting_Idempotent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_idem_1"
	inputUTXO := txid + ":0"
	changeUTXO := txid + ":1"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 3000)
	seedWalletUTXO(t, db, changeUTXO, txid, 1, 800)

	in := walletChainAccountingInput{
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  3000,
		WalletOutputSat: 800,
		NetSat:          -2200,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000},
			{UTXOID: changeUTXO, IOSide: "output", UTXORole: "wallet_change", AmountSatoshi: 800},
		},
	}
	if err := recordWalletChainAccounting(db, in); err != nil {
		t.Fatalf("first record failed: %v", err)
	}
	if err := recordWalletChainAccounting(db, in); err != nil {
		t.Fatalf("second record failed: %v", err)
	}

	var paymentCount, linkCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payments WHERE txid=?`, txid).Scan(&paymentCount); err != nil {
		t.Fatalf("count fact_chain_payments failed: %v", err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 chain_payment, got %d", paymentCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE chain_payment_id=(SELECT id FROM fact_chain_payments WHERE txid=?)`, txid).Scan(&linkCount); err != nil {
		t.Fatalf("count fact_chain_payment_utxo_links failed: %v", err)
	}
	if linkCount != 2 {
		t.Fatalf("expected 2 fact_chain_payment_utxo_links, got %d", linkCount)
	}
}

func TestRecordWalletChainAccounting_FeePoolSettleLink(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_fee_pool_settle_1"
	settleUTXO := txid + ":0"
	seedWalletUTXO(t, db, settleUTXO, txid, 0, 1234)

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            txid,
		Category:        "FEE_POOL",
		WalletInputSat:  1234,
		WalletOutputSat: 1234,
		NetSat:          0,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: settleUTXO, IOSide: "output", UTXORole: "fee_pool_settle", AmountSatoshi: 1234, Note: "fee settle"},
		},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE utxo_role='fee_pool_settle' AND utxo_id=?`, settleUTXO).Scan(&count); err != nil {
		t.Fatalf("query fee_pool_settle link failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 fee_pool_settle link, got %d", count)
	}
}

func TestReconcileWalletUTXOSet_RecordsChainAccountingFromSyncEntry(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("1", 64)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(address)
	walletScriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}

	prevTxID := "tx_sync_prev_1"
	txid := "tx_sync_chain_accounting_1"
	seedWalletUTXO(t, db, prevTxID+":0", prevTxID, 0, 5000)

	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			txid + ":0": {TxID: txid, Vout: 0, Value: 1500},
		},
		ConfirmedLiveTxIDs: map[string]struct{}{txid: struct{}{}},
		Balance:            1500,
		Count:              1,
	}
	history := []walletHistoryTxRecord{
		{
			TxID:   txid,
			Height: 123,
			Tx: whatsonchain.TxDetail{
				TxID: txid,
				Vin: []whatsonchain.TxInput{
					{TxID: prevTxID, Vout: 0},
				},
				Vout: []whatsonchain.TxOutput{
					{N: 0, ValueSatoshi: 1500, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: walletScriptHex}},
					{N: 1, ValueSatoshi: 3300, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: "76a914111111111111111111111111111111111111111188ac"}},
				},
			},
		},
	}
	cursor := walletUTXOHistoryCursor{
		WalletID:            walletID,
		Address:             address,
		NextConfirmedHeight: 1,
	}

	if err := reconcileWalletUTXOSet(context.Background(), store, address, snapshot, history, cursor, "round-sync-1", "", "sync-test", time.Now().Unix(), 1); err != nil {
		t.Fatalf("reconcileWalletUTXOSet failed: %v", err)
	}

	var paymentCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payments WHERE txid=?`, txid).Scan(&paymentCount); err != nil {
		t.Fatalf("count fact_chain_payments failed: %v", err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 chain_payment, got %d", paymentCount)
	}

	var linkCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE chain_payment_id=(SELECT id FROM fact_chain_payments WHERE txid=?)`, txid).Scan(&linkCount); err != nil {
		t.Fatalf("count fact_chain_payment_utxo_links failed: %v", err)
	}
	if linkCount != 2 {
		t.Fatalf("expected 2 fact_chain_payment_utxo_links, got %d", linkCount)
	}

	var inputRole, outputRole string
	if err := db.QueryRow(
		`SELECT utxo_role FROM fact_chain_payment_utxo_links WHERE chain_payment_id=(SELECT id FROM fact_chain_payments WHERE txid=?) AND io_side='input' LIMIT 1`,
		txid,
	).Scan(&inputRole); err != nil {
		t.Fatalf("query input role failed: %v", err)
	}
	if inputRole != "wallet_input" {
		t.Fatalf("unexpected input role: %s", inputRole)
	}
	if err := db.QueryRow(
		`SELECT utxo_role FROM fact_chain_payment_utxo_links WHERE chain_payment_id=(SELECT id FROM fact_chain_payments WHERE txid=?) AND io_side='output' LIMIT 1`,
		txid,
	).Scan(&outputRole); err != nil {
		t.Fatalf("query output role failed: %v", err)
	}
	if outputRole != "wallet_change" {
		t.Fatalf("unexpected output role: %s", outputRole)
	}
}

// TestDirectTransferAccounting_RejectsMissingPoolAllocationFacts
// 第二阶段硬约束：查不到 fact_pool_session_events.id 时，直连池财务写入必须直接失败
func TestDirectTransferAccounting_RejectsMissingPoolAllocationFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	openErr := dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         "sess_missing_fact_1",
		DealID:            "deal_missing_fact_1",
		BaseTxID:          "base_tx_missing_fact_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
	if openErr == nil {
		t.Fatal("expected open accounting to fail without pool allocation fact")
	}

	payErr := dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_sess_missing_fact_1", "sess_missing_fact_1", 2, 300, "pay_tx_missing_fact_1")
	if payErr == nil {
		t.Fatal("expected pay accounting to fail without pool allocation fact")
	}

	closeErr := dbRecordDirectPoolCloseAccounting(ctx, store, "sess_missing_fact_1", 3, "close_tx_missing_fact_1", baseTxHex, 700, 290, "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if closeErr == nil {
		t.Fatal("expected close accounting to fail without pool allocation fact")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id IN (?,?,?)`,
		"biz_c2c_open_sess_missing_fact_1",
		"biz_c2c_pay_sess_missing_fact_1_2",
		"biz_c2c_close_sess_missing_fact_1",
	).Scan(&count); err != nil {
		t.Fatalf("count settle_businesses failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no finance rows to be written, got %d", count)
	}
}
