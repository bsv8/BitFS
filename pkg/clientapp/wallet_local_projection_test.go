package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
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
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, int64(1), int64(100), now, "", "chain_utxo_worker", "test_seed", int64(1), "round-seed", "", "", int64(0),
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID,
		addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		int64(0),
		int64(100),
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}
	// Step 6：getWalletUTXOsFromDB 现在走 fact 口径，需要种 fact 记录
	if _, err := db.Exec(
		`INSERT INTO fact_bsv_utxos(utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID, addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		int64(0), int64(100), "unspent", "plain_bsv", "", now, now, int64(0), "seed utxo for test", "{}",
	); err != nil {
		t.Fatalf("seed fact_bsv_utxos: %v", err)
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

	if err := applyLocalBroadcastWalletProjection(context.Background(), newClientDB(db, nil), rt, tx, "test_local_projection"); err != nil {
		t.Fatalf("applyLocalBroadcastWalletProjection: %v", err)
	}
	var inputUpdatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, prevTxID+":0").Scan(&inputUpdatedAt1); err != nil {
		t.Fatalf("query spent input updated_at after first projection: %v", err)
	}
	var changeUpdatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, txID+":1").Scan(&changeUpdatedAt1); err != nil {
		t.Fatalf("query change output updated_at after first projection: %v", err)
	}
	var factUpdatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, txID+":1").Scan(&factUpdatedAt1); err != nil {
		t.Fatalf("query fact output updated_at after first projection: %v", err)
	}
	if err := applyLocalBroadcastWalletProjection(context.Background(), newClientDB(db, nil), rt, tx, "test_local_projection"); err != nil {
		t.Fatalf("applyLocalBroadcastWalletProjection replay: %v", err)
	}
	var inputUpdatedAt2 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, prevTxID+":0").Scan(&inputUpdatedAt2); err != nil {
		t.Fatalf("query spent input updated_at after replay: %v", err)
	}
	if inputUpdatedAt2 != inputUpdatedAt1 {
		t.Fatalf("expected spent input updated_at unchanged, got %d vs %d", inputUpdatedAt2, inputUpdatedAt1)
	}
	var changeUpdatedAt2 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, txID+":1").Scan(&changeUpdatedAt2); err != nil {
		t.Fatalf("query change output updated_at after replay: %v", err)
	}
	if changeUpdatedAt2 != changeUpdatedAt1 {
		t.Fatalf("expected change output updated_at unchanged, got %d vs %d", changeUpdatedAt2, changeUpdatedAt1)
	}
	var factUpdatedAt2 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, txID+":1").Scan(&factUpdatedAt2); err != nil {
		t.Fatalf("query fact output updated_at after replay: %v", err)
	}
	if factUpdatedAt2 != factUpdatedAt1 {
		t.Fatalf("expected fact_bsv_utxos updated_at unchanged, got %d vs %d", factUpdatedAt2, factUpdatedAt1)
	}

	utxos, err := getWalletUTXOsFromDB(context.Background(), newClientDB(db, nil), rt)
	if err != nil {
		t.Fatalf("getWalletUTXOsFromDB: %v", err)
	}
	if len(utxos) != 1 {
		t.Fatalf("eligible utxo count mismatch: got=%d want=1", len(utxos))
	}
	if utxos[0].TxID != txID || utxos[0].Value != 59 {
		t.Fatalf("eligible utxo mismatch: %+v", utxos[0])
	}

	var changeState, changeClass, changeReason string
	var changeValue uint64
	if err := db.QueryRow(`SELECT state,allocation_class,allocation_reason,value_satoshi FROM wallet_utxo WHERE utxo_id=?`, txID+":1").Scan(&changeState, &changeClass, &changeReason, &changeValue); err != nil {
		t.Fatalf("query change output row: %v", err)
	}
	if changeState != "unspent" {
		t.Fatalf("change output state mismatch: got=%s want=unspent", changeState)
	}
	if changeClass != walletUTXOAllocationPlainBSV {
		t.Fatalf("change output allocation_class mismatch: got=%s want=%s", changeClass, walletUTXOAllocationPlainBSV)
	}
	if changeReason != "" {
		t.Fatalf("change output allocation_reason mismatch: got=%q want empty", changeReason)
	}
	if changeValue != 59 {
		t.Fatalf("change output value mismatch: got=%d want=59", changeValue)
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

	syncState, err := dbLoadWalletUTXOSyncState(context.Background(), newClientDB(db, nil), addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState: %v", err)
	}
	if syncState.UTXOCount != 1 || syncState.BalanceSatoshi != 59 {
		t.Fatalf("wallet_utxo_sync_state mismatch: %+v", syncState)
	}
	if syncState.PlainBSVUTXOCount != 1 || syncState.PlainBSVBalanceSatoshi != 59 {
		t.Fatalf("wallet_utxo_sync_state plain_bsv mismatch: %+v", syncState)
	}
	if syncState.ProtectedUTXOCount != 0 || syncState.UnknownUTXOCount != 0 || syncState.UnknownBalanceSatoshi != 0 {
		t.Fatalf("wallet_utxo_sync_state protection buckets mismatch: %+v", syncState)
	}
	if syncState.LastUpdatedBy != "local_wallet_projection" {
		t.Fatalf("last_updated_by mismatch: got=%s", syncState.LastUpdatedBy)
	}
	if syncState.LastTrigger != "test_local_projection" {
		t.Fatalf("last_trigger mismatch: got=%s", syncState.LastTrigger)
	}
}

func TestApplyLocalBroadcastWalletProjection_SQLTraceRepeatUpdateTop(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "client.log")
	if _, err := initSQLTrace(logFile, true); err != nil {
		t.Fatalf("initSQLTrace: %v", err)
	}
	t.Cleanup(func() { _ = closeSQLTrace() })

	dbPath := filepath.Join(dir, "client-index.sqlite")
	opened, err := sqliteactor.Open(dbPath, true)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if opened.Actor != nil {
			_ = opened.Actor.Close()
		}
		if opened.DB != nil {
			_ = opened.DB.Close()
		}
	})
	if err := applySQLitePragmas(opened.DB); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(opened.DB); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}
	store := newClientDB(opened.DB, opened.Actor)

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

	oldTxID := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	newTxID := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	utxoID := oldTxID + ":0"
	if _, err := opened.DB.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID,
		walletID,
		addr,
		oldTxID,
		int64(0),
		int64(100),
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		oldTxID,
		"",
		now,
		now,
		int64(0),
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	current := map[string]utxoStateRow{
		strings.ToLower(utxoID): {
			UTXOID:           utxoID,
			TxID:             oldTxID,
			Vout:             0,
			Value:            100,
			State:            "unspent",
			AllocationClass:  walletUTXOAllocationPlainBSV,
			AllocationReason: "",
			CreatedTxID:      oldTxID,
			SpentTxID:        "",
			CreatedAtUnix:    now,
			SpentAtUnix:      0,
		},
	}
	desired := map[string]utxoStateRow{
		strings.ToLower(utxoID): {
			UTXOID:           utxoID,
			TxID:             newTxID,
			Vout:             0,
			Value:            150,
			State:            "unspent",
			AllocationClass:  walletUTXOAllocationPlainBSV,
			AllocationReason: "projection-trace",
			CreatedTxID:      newTxID,
			SpentTxID:        "",
			CreatedAtUnix:    now,
			SpentAtUnix:      0,
		},
	}

	roundID := "round-local-projection-trace"
	trigger := "test_local_projection_trace"
	ctx := sqlTraceContextWithMeta(context.Background(), roundID, trigger, "wallet_local_projection", "wallet_local_projection_test.runLocalProjectionTraceReplay")
	if err := store.Tx(ctx, func(tx *sql.Tx) error {
		if err := applyWalletUTXODiffTx(ctx, tx, current, desired, walletID, addr, now); err != nil {
			return err
		}
		return applyWalletUTXODiffTx(ctx, tx, current, desired, walletID, addr, now+1)
	}); err != nil {
		t.Fatalf("applyWalletUTXODiffTx replay: %v", err)
	}

	flushSQLTraceRoundSummary(roundID)
	summaryPath := currentSQLTraceSummaryPath()
	if summaryPath == "" {
		t.Fatalf("expected summary path when trace enabled")
	}
	b, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var summary struct {
		RepeatUpdateTop []struct {
			Count       int      `json:"count"`
			CallerChain []string `json:"caller_chain"`
		} `json:"repeat_update_top"`
		MostCommonCallerChain string `json:"most_common_caller_chain"`
	}
	if err := json.Unmarshal(b, &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if len(summary.RepeatUpdateTop) == 0 {
		t.Fatalf("expected repeat_update_top to be populated")
	}
	if summary.RepeatUpdateTop[0].Count < 2 {
		t.Fatalf("expected repeated update count >=2, got %d", summary.RepeatUpdateTop[0].Count)
	}
	if len(summary.RepeatUpdateTop[0].CallerChain) == 0 {
		t.Fatalf("expected repeat update caller chain")
	}
	if summary.MostCommonCallerChain == "" {
		t.Fatalf("expected most_common_caller_chain")
	}
	if !strings.Contains(summary.MostCommonCallerChain, "TestApplyLocalBroadcastWalletProjection_SQLTraceRepeatUpdateTop") {
		t.Fatalf("expected caller chain to include test function, got %s", summary.MostCommonCallerChain)
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
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
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
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID,
		addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0,
		100,
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}
	// Step 6：getWalletUTXOsFromDB 现在走 fact 口径，需要种 fact 记录
	if _, err := db.Exec(
		`INSERT INTO fact_bsv_utxos(utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID, addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0, 100, "unspent", "plain_bsv", "", now, now, 0, "seed utxo for test", "{}",
	); err != nil {
		t.Fatalf("seed fact_bsv_utxos: %v", err)
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

	if err := applyLocalBroadcastWalletProjection(context.Background(), newClientDB(db, nil), rt, localTx, "test_local_projection"); err != nil {
		t.Fatalf("applyLocalBroadcastWalletProjection: %v", err)
	}

	staleSnapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			prevTxID + ":0": {TxID: prevTxID, Vout: 0, Value: 100},
		},
		ObservedMempoolTxs: nil,
		ConfirmedLiveTxIDs: map[string]struct{}{prevTxID: {}},
		Balance:            100,
		Count:              1,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, NextConfirmedHeight: 1, RoundTipHeight: 1}
	if err := SyncWalletAndApplyFacts(context.Background(), newClientDB(db, nil), addr, staleSnapshot, nil, cursor, "round-stale", "", "periodic_tick", now+1, 5); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts: %v", err)
	}

	utxos, err := getWalletUTXOsFromDB(context.Background(), newClientDB(db, nil), rt)
	if err != nil {
		t.Fatalf("getWalletUTXOsFromDB: %v", err)
	}
	if len(utxos) != 1 {
		t.Fatalf("eligible utxo count mismatch after stale reconcile: got=%d want=1", len(utxos))
	}
	if utxos[0].TxID != localTxID || utxos[0].Value != 59 {
		t.Fatalf("wallet utxo mismatch after stale reconcile: %+v", utxos[0])
	}

	var changeState, changeClass string
	var changeValue uint64
	if err := db.QueryRow(`SELECT state,allocation_class,value_satoshi FROM wallet_utxo WHERE utxo_id=?`, localTxID+":1").Scan(&changeState, &changeClass, &changeValue); err != nil {
		t.Fatalf("query stale reconcile change row: %v", err)
	}
	if changeState != "unspent" || changeClass != walletUTXOAllocationPlainBSV || changeValue != 59 {
		t.Fatalf("stale reconcile change row mismatch: state=%s class=%s value=%d", changeState, changeClass, changeValue)
	}

	var observedAt int64
	if err := db.QueryRow(`SELECT wallet_observed_at_unix FROM fact_chain_payments WHERE txid=?`, localTxID).Scan(&observedAt); err != nil {
		t.Fatalf("query fact_chain_payments: %v", err)
	}
	if observedAt != 0 {
		t.Fatalf("wallet_observed_at_unix mismatch: got=%d want=0", observedAt)
	}
}

func TestGetWalletUTXOsFromDB_ExcludesProtectedAssetOutputs(t *testing.T) {
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
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 2, 107, now, "", "chain_utxo_worker", "test_seed", 1, "round-seed", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID,
		addr,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		0,
		100,
		"unspent",
		walletUTXOAllocationProtectedAsset,
		"detected by indexer",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		now-10,
		now-10,
		0,
	); err != nil {
		t.Fatalf("seed protected wallet_utxo: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1",
		walletID,
		addr,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		1,
		7,
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"",
		now-5,
		now-5,
		0,
	); err != nil {
		t.Fatalf("seed plain wallet_utxo: %v", err)
	}
	// Step 6：getWalletUTXOsFromDB 现在走 fact 口径，需要种 fact 记录
	if _, err := db.Exec(
		`INSERT INTO fact_bsv_utxos(utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1",
		walletID, addr,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		1, 7, "unspent", "plain_bsv", "", now, now, 0, "plain_bsv utxo for test", "{}",
	); err != nil {
		t.Fatalf("seed fact_bsv_utxos: %v", err)
	}

	utxos, err := getWalletUTXOsFromDB(context.Background(), newClientDB(db, nil), rt)
	if err != nil {
		t.Fatalf("getWalletUTXOsFromDB: %v", err)
	}
	if got, want := len(utxos), 1; got != want {
		t.Fatalf("eligible utxo count mismatch: got=%d want=%d", got, want)
	}
	if got, want := utxos[0].TxID, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; got != want {
		t.Fatalf("eligible txid mismatch: got=%s want=%s", got, want)
	}
	if got, want := utxos[0].Value, uint64(7); got != want {
		t.Fatalf("eligible value mismatch: got=%d want=%d", got, want)
	}
}

func TestAlignWalletUTXOSyncCursor_RewindsToOlderCurrentUnspent(t *testing.T) {
	t.Parallel()

	cursor := walletUTXOSyncCursor{
		WalletID:            "wallet:test",
		Address:             "addr",
		AnchorHeight:        120,
		NextConfirmedHeight: 160,
		NextPageToken:       "page-old",
		RoundTipHeight:      199,
	}
	next := alignWalletUTXOSyncCursor(cursor, 100, 200)
	if next.AnchorHeight != 100 {
		t.Fatalf("anchor height mismatch: got=%d want=100", next.AnchorHeight)
	}
	if next.NextConfirmedHeight != 100 {
		t.Fatalf("next confirmed height mismatch: got=%d want=100", next.NextConfirmedHeight)
	}
	if next.NextPageToken != "" {
		t.Fatalf("next page token mismatch: got=%q want empty", next.NextPageToken)
	}
}
