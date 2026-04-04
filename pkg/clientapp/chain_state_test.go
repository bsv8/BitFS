package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestInitIndexDB_MigratesLegacyChainTables(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE chain_tip_snapshot(id INTEGER PRIMARY KEY CHECK(id=1),tip_height INTEGER NOT NULL,updated_at_unix INTEGER NOT NULL,last_error TEXT NOT NULL,last_updated_by TEXT NOT NULL,last_trigger TEXT NOT NULL,last_duration_ms INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create legacy chain_tip_snapshot: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wallet_utxo_snapshot(address TEXT PRIMARY KEY,utxo_count INTEGER NOT NULL,balance_satoshi INTEGER NOT NULL,updated_at_unix INTEGER NOT NULL,last_error TEXT NOT NULL,last_updated_by TEXT NOT NULL,last_trigger TEXT NOT NULL,last_duration_ms INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create legacy wallet_utxo_snapshot: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wallet_utxo_items(address TEXT NOT NULL,txid TEXT NOT NULL,vout INTEGER NOT NULL,value_satoshi INTEGER NOT NULL,updated_at_unix INTEGER NOT NULL,PRIMARY KEY(address,txid,vout))`); err != nil {
		t.Fatalf("create legacy wallet_utxo_items: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wallet_chain_tx_raw(txid TEXT PRIMARY KEY,block_height INTEGER NOT NULL,status TEXT NOT NULL,occurred_at_unix INTEGER NOT NULL,wallet_input_satoshi INTEGER NOT NULL,wallet_output_satoshi INTEGER NOT NULL,net_amount_satoshi INTEGER NOT NULL,category TEXT NOT NULL,payload_json TEXT NOT NULL,updated_at_unix INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create legacy wallet_chain_tx_raw: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO chain_tip_snapshot(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms) VALUES(1,321,111,'','chain_tip_worker','startup',9)`); err != nil {
		t.Fatalf("seed legacy chain_tip_snapshot: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	exists, err := hasTable(db, "proc_chain_tip_state")
	if err != nil {
		t.Fatalf("hasTable proc_chain_tip_state: %v", err)
	}
	if !exists {
		t.Fatalf("expected proc_chain_tip_state exists")
	}
	for _, table := range []string{"chain_tip_snapshot", "wallet_utxo_snapshot", "wallet_utxo_items", "wallet_chain_tx_raw"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s: %v", table, err)
		}
		if exists {
			t.Fatalf("expected legacy table dropped: %s", table)
		}
	}

	state, err := dbLoadChainTipState(context.Background(), newClientDB(db, nil))
	if err != nil {
		t.Fatalf("loadChainTipState: %v", err)
	}
	if state.TipHeight != 321 || state.UpdatedAtUnix != 111 || state.LastDurationMS != 9 {
		t.Fatalf("unexpected migrated state: %+v", state)
	}
}

func TestInitIndexDB_MigratesLegacyWalletUTXOAllocationColumns(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wallet_utxo(
		utxo_id TEXT PRIMARY KEY,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		txid TEXT NOT NULL,
		vout INTEGER NOT NULL,
		value_satoshi INTEGER NOT NULL,
		state TEXT NOT NULL,
		created_txid TEXT NOT NULL,
		spent_txid TEXT NOT NULL,
		created_at_unix INTEGER NOT NULL,
		updated_at_unix INTEGER NOT NULL,
		spent_at_unix INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create legacy wallet_utxo: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES('legacy:0','wallet','addr','legacy',0,2,'unspent','legacy','',1,1,0)`); err != nil {
		t.Fatalf("seed legacy wallet_utxo: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	var allocationClass string
	var allocationReason string
	if err := db.QueryRow(`SELECT allocation_class,allocation_reason FROM wallet_utxo WHERE utxo_id='legacy:0'`).Scan(&allocationClass, &allocationReason); err != nil {
		t.Fatalf("query migrated allocation fields: %v", err)
	}
	if allocationClass != walletUTXOAllocationPlainBSV {
		t.Fatalf("allocation_class mismatch: got=%s want=%s", allocationClass, walletUTXOAllocationPlainBSV)
	}
	if allocationReason != "" {
		t.Fatalf("allocation_reason mismatch: got=%q want empty", allocationReason)
	}
}

func TestHandleAdminChainUTXOStatus_UsesWalletUTXORows(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
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
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES
		 ('a:0',?,?,?,?,?,'unspent','plain_bsv','','a','',?,?,0),
		 ('b:1',?,?,?,?,?,'unspent','protected_asset','detected by indexer','b','',?,?,0),
		 ('c:2',?,?,?,?,?,'spent','plain_bsv','','c','',?,?,?)`,
		walletID, addr, "a", 0, 100, now, now,
		walletID, addr, "b", 1, 200, now, now,
		walletID, addr, "c", 2, 300, now, now, now,
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
