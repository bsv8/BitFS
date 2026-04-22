package clientapp

import (
	"context"
	"errors"
	"testing"
)

func TestWalletSyncErrorPathsPersistWithSQLiteFriendlyTypes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	meta := walletSyncRoundMeta{
		RoundID:       "round-regression",
		Address:       "wallet-addr-1",
		TriggerSource: "periodic_tick",
		APIBaseURL:    "https://example.invalid",
		StartedAtUnix: 1700000000,
	}
	if err := dbUpdateWalletUTXOSyncStateError(context.Background(), store, "wallet-addr-1", meta, errors.New("http 418: teapot"), "periodic_tick"); err != nil {
		t.Fatalf("dbUpdateWalletUTXOSyncStateError failed: %v", err)
	}
	if err := dbUpdateWalletUTXOSyncCursorError(context.Background(), store, "wallet-addr-1", "cursor boom"); err != nil {
		t.Fatalf("dbUpdateWalletUTXOSyncCursorError failed: %v", err)
	}

	var utxoCount, plainCount, protectedCount, unknownCount, httpStatus int64
	if err := db.QueryRow(
		`SELECT utxo_count,plain_bsv_utxo_count,protected_utxo_count,unknown_utxo_count,last_http_status
		 FROM wallet_utxo_sync_state WHERE address=?`,
		"wallet-addr-1",
	).Scan(&utxoCount, &plainCount, &protectedCount, &unknownCount, &httpStatus); err != nil {
		t.Fatalf("load wallet_utxo_sync_state failed: %v", err)
	}
	if utxoCount != 0 || plainCount != 0 || protectedCount != 0 || unknownCount != 0 {
		t.Fatalf("unexpected sync counts: utxo=%d plain=%d protected=%d unknown=%d", utxoCount, plainCount, protectedCount, unknownCount)
	}
	if httpStatus != 418 {
		t.Fatalf("last_http_status mismatch: got=%d want=418", httpStatus)
	}

	var nextConfirmed, anchorHeight, roundTip int64
	var lastError string
	if err := db.QueryRow(
		`SELECT next_confirmed_height,anchor_height,round_tip_height,last_error
		 FROM wallet_utxo_sync_cursor WHERE address=?`,
		"wallet-addr-1",
	).Scan(&nextConfirmed, &anchorHeight, &roundTip, &lastError); err != nil {
		t.Fatalf("load wallet_utxo_sync_cursor failed: %v", err)
	}
	if nextConfirmed != 0 || anchorHeight != 0 || roundTip != 0 {
		t.Fatalf("unexpected sync cursor values: next=%d anchor=%d round_tip=%d", nextConfirmed, anchorHeight, roundTip)
	}
	if lastError == "" {
		t.Fatalf("expected cursor last_error to be persisted")
	}
}
