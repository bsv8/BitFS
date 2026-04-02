package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

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

	// 一个最小可解析交易：1 输入，2 输出（seller=700, buyer=290）。
	finalTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	dbRecordDirectPoolCloseAccounting(nil, newClientDB(db, nil), "sess_1", "", finalTxHex, 700, 290, "seller_peer_1")

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
			   JOIN fin_business_txs bt ON bt.business_id=l.business_id AND bt.txid=l.txid
			  WHERE l.business_id=? AND l.txid=? AND bt.tx_role=? AND l.io_side=? AND l.utxo_role=?`,
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
