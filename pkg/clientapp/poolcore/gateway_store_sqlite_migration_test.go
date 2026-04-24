package poolcore

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestInitGatewayStore_UpgradeLegacyStatusToLifecycleState(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// 模拟历史库：会话表只有 status，没有 lifecycle_state 及相关新列。
	_, err = db.Exec(`
		CREATE TABLE fee_pool_sessions (
			spend_txid TEXT PRIMARY KEY,
			client_pubkey_hex TEXT NOT NULL,
			client_bsv_pubkey_hex TEXT NOT NULL,
			server_bsv_pubkey_hex TEXT NOT NULL,
			input_amount_satoshi INTEGER NOT NULL,
			pool_amount_satoshi INTEGER NOT NULL,
			spend_tx_fee_satoshi INTEGER NOT NULL,
			sequence_num INTEGER NOT NULL,
			server_amount_satoshi INTEGER NOT NULL,
			client_amount_satoshi INTEGER NOT NULL,
			base_txid TEXT NOT NULL,
			final_txid TEXT NOT NULL,
			base_tx_hex TEXT NOT NULL,
			current_tx_hex TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create legacy fee_pool_sessions failed: %v", err)
	}

	insertLegacy := func(spendTxID, status string) {
		t.Helper()
		_, err := db.Exec(
			`INSERT INTO fee_pool_sessions(
				spend_txid,client_pubkey_hex,client_bsv_pubkey_hex,server_bsv_pubkey_hex,
				input_amount_satoshi,pool_amount_satoshi,spend_tx_fee_satoshi,sequence_num,server_amount_satoshi,client_amount_satoshi,
				base_txid,final_txid,base_tx_hex,current_tx_hex,status,created_at_unix,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			spendTxID, "client_1", "02aa", "03bb",
			1000, 900, 10, 1, 10, 890,
			"base_1", "", "00", "00", status, 100, 100,
		)
		if err != nil {
			t.Fatalf("insert legacy session failed: %v", err)
		}
	}
	insertLegacy("tx_active", "ACTIVE")
	insertLegacy("tx_frozen", "paused")
	insertLegacy("tx_submit", "near_expiry")
	insertLegacy("tx_settled_ext", "settled_external")
	insertLegacy("tx_closed", "done")
	insertLegacy("tx_unknown", "legacy_x")

	if err := InitGatewayStore(db); err != nil {
		t.Fatalf("init gateway store failed: %v", err)
	}

	hasLifecycle, err := tableHasColumn(db, "fee_pool_sessions", "lifecycle_state")
	if err != nil {
		t.Fatalf("check lifecycle_state failed: %v", err)
	}
	if !hasLifecycle {
		t.Fatalf("lifecycle_state should exist after upgrade")
	}
	hasStatus, err := tableHasColumn(db, "fee_pool_sessions", "status")
	if err != nil {
		t.Fatalf("check status failed: %v", err)
	}
	if hasStatus {
		t.Fatalf("status should be removed after upgrade")
	}

	assertLifecycle := func(spendTxID, want string) {
		t.Helper()
		var got string
		if err := db.QueryRow(`SELECT lifecycle_state FROM fee_pool_sessions WHERE spend_txid=?`, spendTxID).Scan(&got); err != nil {
			t.Fatalf("query lifecycle_state failed: spend_txid=%s err=%v", spendTxID, err)
		}
		if got != want {
			t.Fatalf("lifecycle_state mismatch: spend_txid=%s got=%s want=%s", spendTxID, got, want)
		}
	}
	assertLifecycle("tx_active", "active")
	assertLifecycle("tx_frozen", "frozen")
	assertLifecycle("tx_submit", "should_submit")
	assertLifecycle("tx_settled_ext", "settled_external")
	assertLifecycle("tx_closed", "closed")
	assertLifecycle("tx_unknown", "pending_base_tx")

	var lifecycleIndexCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_fee_pool_lifecycle'`).Scan(&lifecycleIndexCount); err != nil {
		t.Fatalf("query lifecycle index failed: %v", err)
	}
	if lifecycleIndexCount != 1 {
		t.Fatalf("lifecycle index should exist after upgrade")
	}
}
