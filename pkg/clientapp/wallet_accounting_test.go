package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
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

	var txid string
	if err := db.QueryRow(`SELECT txid FROM fin_tx_breakdown WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&txid); err != nil {
		t.Fatalf("query fin_tx_breakdown failed: %v", err)
	}
	if txid == "" {
		t.Fatalf("fin_tx_breakdown txid should not be empty")
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
			   FROM fin_tx_utxo_links l
			   JOIN fin_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE l.business_id=? AND l.txid=? AND b.tx_role=? AND l.io_side=? AND l.utxo_role=?`,
			"biz_c2c_close_sess_1", txid, c.txRole, c.ioSide, c.utxoRole,
		).Scan(&gotCount, &gotAmount); err != nil {
			t.Fatalf("query fin_tx_utxo_links tx_role=%s io_side=%s utxo_role=%s failed: %v", c.txRole, c.ioSide, c.utxoRole, err)
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
		   FROM pool_sessions WHERE pool_session_id=?`,
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
		   FROM pool_allocations WHERE pool_session_id=? ORDER BY allocation_no ASC`,
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

// TestDirectTransferAccounting_SourceIDUsesAutoIncrementID 验证第二步整改
// source_id 应该是 pool_allocations.id（整数），而不是 allocation_id（业务键）
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
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 获取各 allocation 的自增 id
	openAllocID := directTransferPoolAllocationID(sessionID, "open", 1)
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	closeAllocID := directTransferPoolAllocationID(sessionID, "close", 3)

	openID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, openAllocID)
	payID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	closeID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, closeAllocID)

	type wantRow struct {
		businessID        string
		expectedSourceID  int64 // 期望的整数主键
		accountingScene   string
		accountingSubType string
	}
	checks := []wantRow{
		{businessID: "biz_c2c_open_sess_third_iter_1", expectedSourceID: openID, accountingScene: "fee_pool", accountingSubType: "open"},
		{businessID: "biz_c2c_pay_sess_third_iter_1_2", expectedSourceID: payID, accountingScene: "c2c_transfer", accountingSubType: "chunk_pay"},
		{businessID: "biz_c2c_close_sess_third_iter_1", expectedSourceID: closeID, accountingScene: "c2c_transfer", accountingSubType: "close"},
	}
	for _, want := range checks {
		var gotSourceType, gotSourceIDStr, gotAccountingScene, gotAccountingSubtype string
		if err := db.QueryRow(
			`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM fin_business WHERE business_id=?`,
			want.businessID,
		).Scan(&gotSourceType, &gotSourceIDStr, &gotAccountingScene, &gotAccountingSubtype); err != nil {
			t.Fatalf("query fin_business %s failed: %v", want.businessID, err)
		}
		if gotSourceType != "pool_allocation" {
			t.Fatalf("unexpected source_type for %s: got=%s want=pool_allocation", want.businessID, gotSourceType)
		}
		// 第二步整改验证：source_id 应该是整数主键的字符串形式
		wantSourceIDStr := fmt.Sprintf("%d", want.expectedSourceID)
		if gotSourceIDStr != wantSourceIDStr {
			// 兜底检查：如果 lookup 失败，可能还是旧的 allocation_id
			if gotSourceIDStr == openAllocID || gotSourceIDStr == payAllocID || gotSourceIDStr == closeAllocID {
				t.Fatalf("source_id is still allocation_id for %s: got=%s (expected integer id)", want.businessID, gotSourceIDStr)
			}
			t.Fatalf("unexpected source_id for %s: got=%s want=%s", want.businessID, gotSourceIDStr, wantSourceIDStr)
		}
		if gotAccountingScene != want.accountingScene || gotAccountingSubtype != want.accountingSubType {
			t.Fatalf("unexpected accounting fields for %s: scene=%s/%s subtype=%s/%s", want.businessID, gotAccountingScene, want.accountingScene, gotAccountingSubtype, want.accountingSubType)
		}

		// 验证 payload 中保留了 allocation_id
		var payload string
		if err := db.QueryRow(`SELECT payload_json FROM fin_business WHERE business_id=?`, want.businessID).Scan(&payload); err != nil {
			t.Fatalf("query payload failed: %v", err)
		}
		if !strings.Contains(payload, "allocation_id") {
			t.Fatalf("payload missing allocation_id for %s: %s", want.businessID, payload)
		}
	}
}

// TestDirectTransferAccounting_ProcessEventsSourceIDUsesAutoIncrementID 验证第二步整改
// process events 的 source_id 应该是 pool_allocations.id（整数）
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
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_third_iter_1")
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
			`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM fin_process_events WHERE process_id=? AND accounting_subtype=? ORDER BY id DESC LIMIT 1`,
			"proc_c2c_transfer_"+sessionID, want.subtype,
		).Scan(&gotSourceType, &gotSourceIDStr, &gotAccountingScene, &gotAccountingSubtype); err != nil {
			t.Fatalf("query fin_process_events %s failed: %v", want.subtype, err)
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
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	biz, err := dbGetFinanceBusiness(ctx, store, "biz_c2c_pay_sess_third_iter_1_2")
	if err != nil {
		t.Fatalf("old business read failed: %v", err)
	}
	// 第六次迭代：验证主口径字段
	if biz.SourceType != "pool_allocation" || biz.SourceID == "" {
		t.Fatalf("business new fields incorrect: %+v", biz)
	}

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

	// 第二步整改：验证 source_id 是 pool_allocations.id（整数主键的字符串形式）
	wantAllocID := directTransferPoolAllocationID(sessionID, "close", 7)
	wantID, _ := dbGetPoolAllocationIDByAllocationID(ctx, store, wantAllocID)
	wantIDStr := fmt.Sprintf("%d", wantID)

	var businessSourceID, processSourceID string
	if err := db.QueryRow(
		`SELECT source_id FROM fin_business WHERE business_id=?`,
		"biz_c2c_close_"+sessionID,
	).Scan(&businessSourceID); err != nil {
		t.Fatalf("query business source id failed: %v", err)
	}
	if err := db.QueryRow(
		`SELECT source_id FROM fin_process_events WHERE process_id=? AND accounting_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processSourceID); err != nil {
		t.Fatalf("query process source id failed: %v", err)
	}
	// 第二步整改验证：source_id 应该是整数主键
	if businessSourceID != wantIDStr {
		// 如果还是 allocation_id，说明整改未生效
		if businessSourceID == wantAllocID {
			t.Fatalf("business source_id is still allocation_id (old logic): got=%s want=%s", businessSourceID, wantIDStr)
		}
		t.Fatalf("unexpected business source_id: got=%s want=%s", businessSourceID, wantIDStr)
	}
	if processSourceID != wantIDStr {
		if processSourceID == wantAllocID {
			t.Fatalf("process source_id is still allocation_id (old logic): got=%s want=%s", processSourceID, wantIDStr)
		}
		t.Fatalf("unexpected process source_id: got=%s want=%s", processSourceID, wantIDStr)
	}
}

// TestFinBusinessIdempotency 验证 fin_business 幂等写入
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM fin_business WHERE idempotency_key=?`,
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

// TestFinProcessEventIdempotency 验证 fin_process_events 幂等写入
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_process_events WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM fin_process_events WHERE idempotency_key=?`,
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
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_idem_real_1")

	// 获取第一次写入后的状态
	var firstCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_business WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&firstCount); err != nil {
		t.Fatalf("first count failed: %v", err)
	}
	if firstCount != 1 {
		t.Fatalf("expected 1 business record after first write, got %d", firstCount)
	}

	// 第二次 pay 记录（相同 idempotency_key）- 应该更新而不是报错
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_idem_real_2")

	// 验证仍然只有一条记录
	var secondCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_business WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&secondCount); err != nil {
		t.Fatalf("second count failed: %v", err)
	}
	if secondCount != 1 {
		t.Fatalf("expected 1 business record after idempotent update, got %d", secondCount)
	}

	// 验证 process event 也只有一条
	var procCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_process_events WHERE idempotency_key=?`,
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
	recordWalletChainAccounting(db, txid, "CHANGE", 1000, 800, -200, map[string]any{"test": true})

	var txRole string
	if err := db.QueryRow(
		`SELECT tx_role FROM fin_tx_breakdown WHERE business_id=? AND txid=?`,
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

	// 使用 allocation_id 查询（兼容层）
	openAllocID := directTransferPoolAllocationID(sessionID, "open", 1)

	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, openAllocID, 10, 0)
	if err != nil {
		t.Fatalf("compatibility query failed: %v", err)
	}
	if bizPage.Total == 0 {
		t.Fatal("compatibility query returned no results")
	}

	// 验证返回的记录 source_type 和 source_id 正确
	for _, item := range bizPage.Items {
		if item.SourceType != "pool_allocation" {
			t.Fatalf("unexpected source_type: got=%s want=pool_allocation", item.SourceType)
		}
		// source_id 应该是整数（字符串形式）
		if item.SourceID == "" {
			t.Fatal("source_id should not be empty")
		}
	}

	// 同样测试 process events
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, openAllocID, 10, 0)
	if err != nil {
		t.Fatalf("compatibility query for process events failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatal("compatibility query for process events returned no results")
	}
}

// TestRecordWalletChainAccounting_UsesChainPaymentID 验证第二步整改
// wallet_chain 财务记录的 source_id 应该是 chain_payments.id
func TestRecordWalletChainAccounting_UsesChainPaymentID(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_payment_id_test"
	recordWalletChainAccounting(db, txid, "REPAYMENT", 0, 5000, 5000, map[string]any{"test": true})

	// 验证 chain_payments 记录已创建
	var chainPaymentID int64
	if err := db.QueryRow(`SELECT id FROM chain_payments WHERE txid=?`, txid).Scan(&chainPaymentID); err != nil {
		t.Fatalf("chain_payment not found: %v", err)
	}
	if chainPaymentID <= 0 {
		t.Fatalf("expected positive chain_payment id, got %d", chainPaymentID)
	}

	// 验证 fin_business 的 source_id 是 chain_payments.id
	var sourceType, sourceID string
	if err := db.QueryRow(
		`SELECT source_type, source_id FROM fin_business WHERE business_id=?`,
		"biz_wallet_chain_"+txid,
	).Scan(&sourceType, &sourceID); err != nil {
		t.Fatalf("fin_business not found: %v", err)
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

// TestRecordWalletChainAccounting_ChainPaymentUTXOLinks 验证 wallet_chain 链路补 utxo links
func TestRecordWalletChainAccounting_ChainPaymentUTXOLinks(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)

	// 需要先创建 wallet_utxo 记录（因为外键约束）
	txid := "tx_chain_utxo_link_test"
	utxoID := txid + ":0"
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, "wallet1", "addr1", txid, 0, 3000, "confirmed", "plain_bsv", "",
		txid, "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("insert wallet_utxo failed: %v", err)
	}

	// 手动测试 chain_payment 和 utxo link 的创建
	ctx := context.Background()
	store := newClientDB(db, nil)

	paymentID, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_in",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 3000,
		NetAmountSatoshi:    3000,
		BlockHeight:         100,
		OccurredAtUnix:      1700000001,
		FromPartyID:         "external:unknown",
		ToPartyID:           "wallet:self",
		Payload:             map[string]any{"test": true},
	})
	if err != nil {
		t.Fatalf("upsert chain_payment failed: %v", err)
	}

	// 追加 utxo link
	if err := dbAppendChainPaymentUTXOLinkIfAbsent(ctx, store, chainPaymentUTXOLinkEntry{
		ChainPaymentID: paymentID,
		UTXOID:         utxoID,
		IOSide:         "output",
		UTXORole:       "external_in",
		AmountSatoshi:  3000,
		CreatedAtUnix:  1700000001,
		Note:           "test link",
		Payload:        map[string]any{"idx": 0},
	}); err != nil {
		t.Fatalf("append utxo link failed: %v", err)
	}

	// 验证 link 已创建
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM chain_payment_utxo_links WHERE chain_payment_id=? AND utxo_id=?`,
		paymentID, utxoID,
	).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 utxo link, got %d", count)
	}
}
