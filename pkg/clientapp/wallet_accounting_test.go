package clientapp

import (
	"database/sql"
	"path/filepath"
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
	recordDirectPoolCloseAccounting(db, "sess_1", "", finalTxHex, 700, 290, "seller_peer_1")

	var txid string
	if err := db.QueryRow(`SELECT txid FROM fin_tx_breakdown WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&txid); err != nil {
		t.Fatalf("query fin_tx_breakdown failed: %v", err)
	}
	if txid == "" {
		t.Fatalf("fin_tx_breakdown txid should not be empty")
	}

	type roleCheck struct {
		role   string
		amount int64
		count  int
	}
	checks := []roleCheck{
		{role: "settle_input", count: 1},
		{role: "settle_to_seller", amount: 700, count: 1},
		{role: "settle_to_buyer", amount: 290, count: 1},
	}
	for _, c := range checks {
		var gotCount int
		var gotAmount int64
		if err := db.QueryRow(
			`SELECT COUNT(1),COALESCE(SUM(amount_satoshi),0) FROM biz_utxo_links WHERE business_id=? AND txid=? AND role=?`,
			"biz_c2c_close_sess_1", txid, c.role,
		).Scan(&gotCount, &gotAmount); err != nil {
			t.Fatalf("query biz_utxo_links role=%s failed: %v", c.role, err)
		}
		if gotCount != c.count {
			t.Fatalf("role=%s count mismatch: got=%d want=%d", c.role, gotCount, c.count)
		}
		if c.amount > 0 && gotAmount != c.amount {
			t.Fatalf("role=%s amount mismatch: got=%d want=%d", c.role, gotAmount, c.amount)
		}
	}
}
