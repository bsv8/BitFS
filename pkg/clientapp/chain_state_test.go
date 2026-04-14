package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandleAdminChainUTXOStatus_UsesWalletUTXORows(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 99, 9999, now, "", "chain_utxo_worker", "test", 1,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES
		 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
		 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
		 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"a:0", walletID, addr, "a", 0, 100, "unspent", "P2PKH", "", now, "plain_bsv", "", "a", "", now, now, int64(0),
		"b:1", walletID, addr, "b", 1, 200, "unspent", "bsv20", "detected by indexer", now, "protected_asset", "detected by indexer", "b", "", now, now, int64(0),
		"c:2", walletID, addr, "c", 2, 300, "spent", "P2PKH", "", now, "plain_bsv", "", "c", "", now, now, now,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	srv := &httpAPIServer{db: db, rt: rt, store: newClientDB(db, nil)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/chain/utxo/status", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminChainUTXOStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body walletUTXOSyncState
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Address != addr {
		t.Fatalf("address mismatch: got=%s want=%s", body.Address, addr)
	}
	if body.WalletID != walletID {
		t.Fatalf("wallet_id mismatch: got=%s want=%s", body.WalletID, walletID)
	}
	if body.UTXOCount != 2 {
		t.Fatalf("utxo_count mismatch: got=%d want=2", body.UTXOCount)
	}
	if body.BalanceSatoshi != 300 {
		t.Fatalf("balance_satoshi mismatch: got=%d want=300", body.BalanceSatoshi)
	}
	if body.PlainBSVUTXOCount != 1 || body.PlainBSVBalanceSatoshi != 100 {
		t.Fatalf("plain_bsv aggregate mismatch: %+v", body)
	}
	if body.ProtectedUTXOCount != 1 || body.ProtectedBalanceSatoshi != 200 {
		t.Fatalf("protected aggregate mismatch: %+v", body)
	}
}
