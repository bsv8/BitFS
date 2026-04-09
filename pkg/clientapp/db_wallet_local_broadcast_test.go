package clientapp

import (
	"context"
	"testing"
)

func TestDbUpsertWalletLocalBroadcastStoreTx_PreservesMaxObservedTime(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	walletID := "wallet:addr1"
	address := "addr1"
	txid := "tx_local_broadcast_store_001"
	if err := dbUpsertWalletLocalBroadcastStoreTx(ctx, tx, walletID, address, txid, "deadbeef", 1700001000, 1700001000); err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}
	if err := dbUpsertWalletLocalBroadcastStoreTx(ctx, tx, walletID, address, txid, "deadbeef", 1700002000, 1700002000); err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}
	if err := dbUpsertWalletLocalBroadcastStoreTx(ctx, tx, walletID, address, txid, "deadbeef", 0, 1700003000); err != nil {
		t.Fatalf("third upsert failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx failed: %v", err)
	}

	var createdAt, updatedAt, observedAt int64
	if err := db.QueryRow(`SELECT created_at_unix,updated_at_unix,observed_at_unix FROM wallet_local_broadcast_txs WHERE txid=?`, txid).Scan(&createdAt, &updatedAt, &observedAt); err != nil {
		t.Fatalf("query wallet_local_broadcast_txs failed: %v", err)
	}
	if createdAt != 1700001000 {
		t.Fatalf("created_at_unix mismatch: got=%d want=1700001000", createdAt)
	}
	if updatedAt != 1700003000 {
		t.Fatalf("updated_at_unix mismatch: got=%d want=1700003000", updatedAt)
	}
	if observedAt != 1700002000 {
		t.Fatalf("observed_at_unix should keep the larger value, got=%d want=1700002000", observedAt)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM wallet_local_broadcast_txs WHERE txid=?`, txid).Scan(&count); err != nil {
		t.Fatalf("count wallet_local_broadcast_txs failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 wallet_local_broadcast_txs row, got %d", count)
	}
	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&cycleCount); err != nil {
		t.Fatalf("count fact_settlement_cycles failed: %v", err)
	}
	if cycleCount != 0 {
		t.Fatalf("expected no chain_bsv settlement cycle, got %d", cycleCount)
	}
	var factCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payments WHERE txid=?`, txid).Scan(&factCount); err != nil {
		t.Fatalf("count fact_chain_payments failed: %v", err)
	}
	if factCount != 0 {
		t.Fatalf("expected no fact_chain_payments row, got %d", factCount)
	}
}
