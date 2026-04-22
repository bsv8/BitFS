package clientapp

import (
	"context"
	"strings"
	"testing"
)

func TestDBLoadWalletUTXOsByID_StrictUnspent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	addr := "bitfs_test_addr"
	walletID := walletIDByAddress(addr)
	rows := []struct {
		utxoID string
		txid   string
		vout   uint32
		value  uint64
		state  string
	}{
		{utxoID: "utxo_unspent", txid: "tx_unspent", vout: 0, value: 1000, state: "unspent"},
		{utxoID: "utxo_spent", txid: "tx_spent", vout: 1, value: 2000, state: "spent"},
	}
	for _, row := range rows {
		if _, err := db.Exec(`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			row.utxoID, walletID, addr, row.txid, row.vout, row.value, row.state, walletUTXOAllocationPlainBSV, "", row.txid, "", 1700000000, 1700000000, 0,
		); err != nil {
			t.Fatalf("insert wallet_utxo failed: %v", err)
		}
	}

	store := newClientDB(db, nil)
	if _, err := dbLoadWalletUTXOsByID(context.Background(), store, addr, []string{"utxo_unspent", "utxo_spent"}); err == nil {
		t.Fatal("dbLoadWalletUTXOsByID should reject spent or missing utxos")
	}
	items, err := dbLoadWalletUTXOsByID(context.Background(), store, addr, []string{"utxo_unspent"})
	if err != nil {
		t.Fatalf("dbLoadWalletUTXOsByID unspent failed: %v", err)
	}
	if len(items) != 1 || items[0].TxID != "tx_unspent" {
		t.Fatalf("unexpected utxo items: %+v", items)
	}
}

func TestFactChainPaymentPayloadTxHexSupportsLegacyKey(t *testing.T) {
	t.Parallel()

	legacy, err := chainPaymentPayloadTxHex(`{"txHex":"deadbeef"}`)
	if err != nil {
		t.Fatalf("legacy payload parse failed: %v", err)
	}
	if legacy != "deadbeef" {
		t.Fatalf("legacy payload mismatch: %s", legacy)
	}

	current, err := chainPaymentPayloadTxHex(`{"tx_hex":"cafebabe"}`)
	if err != nil {
		t.Fatalf("current payload parse failed: %v", err)
	}
	if current != "cafebabe" {
		t.Fatalf("current payload mismatch: %s", current)
	}

	if _, err := chainPaymentPayloadTxHex(`{"note":"missing"}`); err == nil || !strings.Contains(err.Error(), "missing tx_hex") {
		t.Fatalf("missing tx_hex should fail with clear error, got: %v", err)
	}
}
