package clientapp

import (
	"database/sql"
	"strings"
	"testing"
)

func mustSettlementCycleIDByChainPaymentID(t *testing.T, db *sql.DB, chainPaymentID int64) int64 {
	t.Helper()
	var txid string
	if err := db.QueryRow(`SELECT txid FROM fact_chain_payments WHERE id=?`, chainPaymentID).Scan(&txid); err != nil {
		t.Fatalf("resolve chain payment txid failed: %v", err)
	}
	var cycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type=? AND source_id=?`, "chain_payment", txid).Scan(&cycleID); err != nil {
		t.Fatalf("resolve settlement cycle by chain payment failed: %v", err)
	}
	return cycleID
}

func mustSettlementCycleIDByPoolAllocationID(t *testing.T, db *sql.DB, allocationID string) int64 {
	t.Helper()
	allocationID = strings.TrimSpace(allocationID)
	var poolSessionID string
	err := db.QueryRow(`SELECT pool_session_id FROM fact_pool_session_events WHERE allocation_id=?`, allocationID).Scan(&poolSessionID)
	if err != nil {
		if err := db.QueryRow(`SELECT pool_session_id FROM biz_pool_allocations WHERE allocation_id=?`, allocationID).Scan(&poolSessionID); err != nil {
			t.Fatalf("resolve pool session id failed: %v", err)
		}
	}
	var cycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type=? AND source_id=?`, "pool_session", poolSessionID).Scan(&cycleID); err != nil {
		t.Fatalf("resolve settlement cycle by pool allocation failed: %v", err)
	}
	return cycleID
}
