package clientapp

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

func TestApplyLocalBroadcastWalletProjection_UpdatesWalletUTXOView(t *testing.T) {
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{DB: db, runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 100, now, "", "chain_utxo_worker", "test_seed", 1, "round-seed", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID,
		addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0,
		100,
		"unspent",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	prevHash, err := chainhash.NewHashFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewHashFromHex: %v", err)
	}
	prevTxID := strings.ToLower(prevHash.String())
	tx := txsdk.NewTransaction()
	tx.AddInput(&txsdk.TransactionInput{
		SourceTXID:       prevHash,
		SourceTxOutIndex: 0,
	})
	if err := tx.PayToAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf", 40); err != nil {
		t.Fatalf("PayToAddress external: %v", err)
	}
	if err := tx.PayToAddress(addr, 59); err != nil {
		t.Fatalf("PayToAddress change: %v", err)
	}
	txID := strings.ToLower(tx.TxID().String())

	if err := applyLocalBroadcastWalletProjection(rt, tx, "test_local_projection"); err != nil {
		t.Fatalf("applyLocalBroadcastWalletProjection: %v", err)
	}

	utxos, err := getWalletUTXOsFromDB(rt)
	if err != nil {
		t.Fatalf("getWalletUTXOsFromDB: %v", err)
	}
	if len(utxos) != 1 {
		t.Fatalf("utxo count mismatch: got=%d want=1", len(utxos))
	}
	if utxos[0].Value != 59 {
		t.Fatalf("utxo value mismatch: got=%d want=59", utxos[0].Value)
	}
	if got := utxos[0].TxID; got != txID {
		t.Fatalf("utxo txid mismatch: got=%s want=%s", got, txID)
	}

	var state string
	var spentTxID string
	if err := db.QueryRow(`SELECT state,spent_txid FROM wallet_utxo WHERE utxo_id=?`, prevTxID+":0").Scan(&state, &spentTxID); err != nil {
		t.Fatalf("query spent input row: %v", err)
	}
	if state != "spent" {
		t.Fatalf("spent input state mismatch: got=%s want=spent", state)
	}
	if spentTxID != txID {
		t.Fatalf("spent input txid mismatch: got=%s want=%s", spentTxID, txID)
	}

	syncState, err := loadWalletUTXOSyncState(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState: %v", err)
	}
	if syncState.UTXOCount != 1 || syncState.BalanceSatoshi != 59 {
		t.Fatalf("wallet_utxo_sync_state mismatch: %+v", syncState)
	}
	if syncState.LastUpdatedBy != "local_wallet_projection" {
		t.Fatalf("last_updated_by mismatch: got=%s", syncState.LastUpdatedBy)
	}
	if syncState.LastTrigger != "test_local_projection" {
		t.Fatalf("last_trigger mismatch: got=%s", syncState.LastTrigger)
	}
}

func TestReconcileWalletUTXOSet_PreservesPendingLocalBroadcastWhenUpstreamLags(t *testing.T) {
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{DB: db, runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 100, now, "", "chain_utxo_worker", "test_seed", 1, "round-seed", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID,
		addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0,
		100,
		"unspent",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	prevHash, err := chainhash.NewHashFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewHashFromHex: %v", err)
	}
	prevTxID := strings.ToLower(prevHash.String())
	localTx := txsdk.NewTransaction()
	localTx.AddInput(&txsdk.TransactionInput{
		SourceTXID:       prevHash,
		SourceTxOutIndex: 0,
	})
	if err := localTx.PayToAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf", 40); err != nil {
		t.Fatalf("PayToAddress external: %v", err)
	}
	if err := localTx.PayToAddress(addr, 59); err != nil {
		t.Fatalf("PayToAddress change: %v", err)
	}
	localTxID := strings.ToLower(localTx.TxID().String())

	if err := applyLocalBroadcastWalletProjection(rt, localTx, "test_local_projection"); err != nil {
		t.Fatalf("applyLocalBroadcastWalletProjection: %v", err)
	}

	staleSnapshot := liveWalletSnapshot{
		Live: map[string]dual2of2.UTXO{
			prevTxID + ":0": {TxID: prevTxID, Vout: 0, Value: 100},
		},
		ObservedMempoolTxs: nil,
		ConfirmedLiveTxIDs: map[string]struct{}{prevTxID: {}},
		Balance:            100,
		Count:              1,
	}
	cursor := walletUTXOHistoryCursor{WalletID: walletID, NextConfirmedHeight: 1, RoundTipHeight: 1}
	if err := reconcileWalletUTXOSet(db, addr, staleSnapshot, nil, cursor, "round-stale", "", "periodic_tick", now+1, 5); err != nil {
		t.Fatalf("reconcileWalletUTXOSet: %v", err)
	}

	utxos, err := getWalletUTXOsFromDB(rt)
	if err != nil {
		t.Fatalf("getWalletUTXOsFromDB: %v", err)
	}
	if len(utxos) != 1 {
		t.Fatalf("utxo count mismatch after stale reconcile: got=%d want=1", len(utxos))
	}
	if utxos[0].TxID != localTxID || utxos[0].Value != 59 {
		t.Fatalf("wallet utxo mismatch after stale reconcile: %+v", utxos[0])
	}

	var observedAt int64
	if err := db.QueryRow(`SELECT observed_at_unix FROM wallet_local_broadcast_txs WHERE txid=?`, localTxID).Scan(&observedAt); err != nil {
		t.Fatalf("query wallet_local_broadcast_txs: %v", err)
	}
	if observedAt != 0 {
		t.Fatalf("observed_at_unix mismatch: got=%d want=0", observedAt)
	}
}
