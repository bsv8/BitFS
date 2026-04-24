package poolcore

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestServiceTimeline_OnlinePauseInterrupted(t *testing.T) {
	db := openTimelineTestDB(t)
	insertActiveSession(t, db, "client_a", "tx_a")

	svc := &GatewayService{DB: db}
	if err := svc.MarkClientOnline("client_a", "12D3KooWAAAA", "unit_online"); err != nil {
		t.Fatalf("mark online failed: %v", err)
	}
	assertServiceState(t, db, "client_a", serviceStateServing, true, true, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_spans WHERE client_pubkey_hex='client_a' AND status='open'`, 1)

	if err := svc.MarkClientOffline("client_a", "12D3KooWAAAA", "unit_offline"); err != nil {
		t.Fatalf("mark offline failed: %v", err)
	}
	assertServiceState(t, db, "client_a", serviceStatePaused, false, true, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_pauses WHERE client_pubkey_hex='client_a' AND status='open'`, 1)

	closeSessionStatus(t, db, "tx_a")
	if err := svc.SyncServiceStateByClient("client_a", "unit_close", "tx_a", ""); err != nil {
		t.Fatalf("sync by client failed: %v", err)
	}
	assertServiceState(t, db, "client_a", serviceStateInterrupted, false, false, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_spans WHERE client_pubkey_hex='client_a' AND status='closed'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_pauses WHERE client_pubkey_hex='client_a' AND status='closed'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_interruptions WHERE client_pubkey_hex='client_a' AND status='open'`, 1)
}

func TestServiceTimeline_OverlapPoolsKeepServingSpan(t *testing.T) {
	db := openTimelineTestDB(t)
	insertActiveSession(t, db, "client_b", "tx_1")
	insertFrozenSession(t, db, "client_b", "tx_2")

	svc := &GatewayService{DB: db}
	if err := svc.MarkClientOnline("client_b", "12D3KooWBBBB", "unit_online"); err != nil {
		t.Fatalf("mark online failed: %v", err)
	}
	assertServiceState(t, db, "client_b", serviceStateServing, true, true, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_spans WHERE client_pubkey_hex='client_b' AND status='open'`, 1)

	closeSessionStatus(t, db, "tx_1")
	if err := svc.SyncServiceStateByClient("client_b", "unit_close_first", "tx_1", ""); err != nil {
		t.Fatalf("sync first close failed: %v", err)
	}
	assertServiceState(t, db, "client_b", serviceStateInterrupted, true, false, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_spans WHERE client_pubkey_hex='client_b' AND status='open'`, 0)
	assertCount(t, db, `SELECT COUNT(*) FROM fee_pool_service_interruptions WHERE client_pubkey_hex='client_b' AND status='open'`, 1)
}

func openTimelineTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := InitGatewayStore(db); err != nil {
		t.Fatalf("init gateway store failed: %v", err)
	}
	return db
}

func insertActiveSession(t *testing.T, db *sql.DB, clientID string, spendTxID string) {
	t.Helper()
	err := InsertSession(db, GatewaySessionRow{
		SpendTxID:                 spendTxID,
		ClientID:                  clientID,
		ClientBSVCompressedPubHex: "02aa",
		ServerBSVCompressedPubHex: "03bb",
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  1,
		ServerAmountSat:           1,
		ClientAmountSat:           998,
		BaseTxID:                  "base_" + spendTxID,
		FinalTxID:                 "",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            "active",
	})
	if err != nil {
		t.Fatalf("insert active session failed: %v", err)
	}
}

func closeSessionStatus(t *testing.T, db *sql.DB, spendTxID string) {
	t.Helper()
	_, err := db.Exec(`UPDATE fee_pool_sessions SET lifecycle_state='closed' WHERE spend_txid=?`, spendTxID)
	if err != nil {
		t.Fatalf("close session status failed: %v", err)
	}
}

func insertFrozenSession(t *testing.T, db *sql.DB, clientID string, spendTxID string) {
	t.Helper()
	err := InsertSession(db, GatewaySessionRow{
		SpendTxID:                 spendTxID,
		ClientID:                  clientID,
		ClientBSVCompressedPubHex: "02aa",
		ServerBSVCompressedPubHex: "03bb",
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  1,
		ServerAmountSat:           1,
		ClientAmountSat:           998,
		BaseTxID:                  "base_" + spendTxID,
		FinalTxID:                 "",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            "frozen",
	})
	if err != nil {
		t.Fatalf("insert frozen session failed: %v", err)
	}
}

func assertServiceState(t *testing.T, db *sql.DB, clientID string, wantState string, wantOnline bool, wantCoverage bool, wantSessionCount int) {
	t.Helper()
	var state string
	var onlineInt int
	var coverageInt int
	var sessionCount int
	err := db.QueryRow(
		`SELECT state,online,coverage_active,active_session_count FROM fee_pool_service_state WHERE client_pubkey_hex=?`,
		clientID,
	).Scan(&state, &onlineInt, &coverageInt, &sessionCount)
	if err != nil {
		t.Fatalf("query service state failed: %v", err)
	}
	if state != wantState {
		t.Fatalf("state mismatch: got=%s want=%s", state, wantState)
	}
	if (onlineInt == 1) != wantOnline {
		t.Fatalf("online mismatch: got=%v want=%v", onlineInt == 1, wantOnline)
	}
	if (coverageInt == 1) != wantCoverage {
		t.Fatalf("coverage mismatch: got=%v want=%v", coverageInt == 1, wantCoverage)
	}
	if sessionCount != wantSessionCount {
		t.Fatalf("active_session_count mismatch: got=%d want=%d", sessionCount, wantSessionCount)
	}
}

func assertCount(t *testing.T, db *sql.DB, q string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(q).Scan(&got); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d query=%s", got, want, q)
	}
}
