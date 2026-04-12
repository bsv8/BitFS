package clientapp

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func mustSettlementCycleIDByChainPaymentID(t *testing.T, db *sql.DB, chainPaymentID int64) int64 {
	t.Helper()
	var cycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type=? AND source_id=?`, "chain_quote_pay", fmt.Sprintf("%d", chainPaymentID)).Scan(&cycleID); err != nil {
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
	var channelID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_channel_pool_session_quote_pay WHERE pool_session_id=?`, poolSessionID).Scan(&channelID); err != nil {
		t.Fatalf("resolve pool channel id failed: %v", err)
	}
	var cycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type=? AND source_id=?`, "pool_session_quote_pay", fmt.Sprintf("%d", channelID)).Scan(&cycleID); err != nil {
		t.Fatalf("resolve settlement cycle by pool allocation failed: %v", err)
	}
	return cycleID
}
