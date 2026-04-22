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

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func newOrchestratorTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dbPath := t.TempDir() + "/orchestrator-test.sqlite"
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db, dbPath
}

// TestSyncWalletAndApplyFacts_NormalCase 正常情况：同步后 fact 正确写入
func TestSyncWalletAndApplyFacts_NormalCase(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	confirmedTxID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			confirmedTxID + ":0": {TxID: confirmedTxID, Vout: 0, Value: 1000},
		},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{confirmedTxID: {}},
		Balance:               1000,
		Count:                 1,
		OldestConfirmedHeight: 100,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 100}

	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts: %v", err)
	}

	var utxoCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos`).Scan(&utxoCount); err != nil {
		t.Fatalf("count utxos: %v", err)
	}
	if utxoCount != 1 {
		t.Fatalf("expected 1 utxo, got %d", utxoCount)
	}

	var utxoID, utxoState, carrierType string
	if err := db.QueryRow(`SELECT utxo_id, utxo_state, carrier_type FROM fact_bsv_utxos`).Scan(&utxoID, &utxoState, &carrierType); err != nil {
		t.Fatalf("query utxo: %v", err)
	}
	if utxoID != confirmedTxID+":0" {
		t.Fatalf("expected utxo_id %s, got %s", confirmedTxID+":0", utxoID)
	}
	if utxoState != "unspent" {
		t.Fatalf("expected utxo_state 'unspent', got %s", utxoState)
	}
	if carrierType != "plain_bsv" {
		t.Fatalf("expected carrier_type 'plain_bsv', got %s", carrierType)
	}
}

func TestSyncWalletAndApplyFacts_WritesSyncStateWithoutSQLiteTypeError(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)

	txid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			txid + ":0": {TxID: txid, Vout: 0, Value: 123},
		},
		ConfirmedLiveTxIDs:    map[string]struct{}{txid: {}},
		Balance:               123,
		Count:                 1,
		OldestConfirmedHeight: 1,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 1}

	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-sqlite", "", "periodic_tick", time.Now().Unix(), 1); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts: %v", err)
	}

	var utxoCount int64
	var balanceSatoshi int64
	if err := db.QueryRow(`SELECT utxo_count,balance_satoshi FROM wallet_utxo_sync_state WHERE address=?`, address).Scan(&utxoCount, &balanceSatoshi); err != nil {
		t.Fatalf("load wallet_utxo_sync_state failed: %v", err)
	}
	if utxoCount != 1 || balanceSatoshi != 123 {
		t.Fatalf("wallet_utxo_sync_state mismatch: utxo=%d balance=%d", utxoCount, balanceSatoshi)
	}
}

// TestSyncWalletAndApplyFacts_Idempotent 重复触发：fact 不重复写入
func TestSyncWalletAndApplyFacts_Idempotent(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	confirmedTxID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			confirmedTxID + ":0": {TxID: confirmedTxID, Vout: 0, Value: 2000},
		},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{confirmedTxID: {}},
		Balance:               2000,
		Count:                 1,
		OldestConfirmedHeight: 200,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 200}

	// 第一次同步
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-1: %v", err)
	}

	var createdAt1 int64
	if err := db.QueryRow(`SELECT created_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&createdAt1); err != nil {
		t.Fatalf("query utxo after round-1: %v", err)
	}
	var updatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&updatedAt1); err != nil {
		t.Fatalf("query utxo updated_at after round-1: %v", err)
	}
	var walletUpdatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, confirmedTxID+":0").Scan(&walletUpdatedAt1); err != nil {
		t.Fatalf("query wallet_utxo updated_at after round-1: %v", err)
	}

	// 第二次同步（重复触发）
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-2", "", "test", now+1, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-2: %v", err)
	}

	var utxoCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos`).Scan(&utxoCount); err != nil {
		t.Fatalf("count utxos after round-2: %v", err)
	}
	if utxoCount != 1 {
		t.Fatalf("expected 1 utxo (idempotent), got %d", utxoCount)
	}

	var createdAt2 int64
	if err := db.QueryRow(`SELECT created_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&createdAt2); err != nil {
		t.Fatalf("query utxo after round-2: %v", err)
	}
	if createdAt2 != createdAt1 {
		t.Fatalf("expected same created_at (idempotent), got %d vs %d", createdAt2, createdAt1)
	}
	var updatedAt2 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&updatedAt2); err != nil {
		t.Fatalf("query utxo updated_at after round-2: %v", err)
	}
	if updatedAt2 < updatedAt1 {
		t.Fatalf("expected updated_at not to move backward on noop replay, got %d vs %d", updatedAt2, updatedAt1)
	}
	var walletUpdatedAt2 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, confirmedTxID+":0").Scan(&walletUpdatedAt2); err != nil {
		t.Fatalf("query wallet_utxo updated_at after round-2: %v", err)
	}
	if walletUpdatedAt2 < walletUpdatedAt1 {
		t.Fatalf("expected wallet_utxo updated_at not to move backward on noop replay, got %d vs %d", walletUpdatedAt2, walletUpdatedAt1)
	}
}

// TestSyncWalletAndApplyFacts_WalletUTXOUpdatedAtMovesOnStateChange 真实变化时更新时间要推进。
func TestSyncWalletAndApplyFacts_WalletUTXOUpdatedAtMovesOnStateChange(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	confirmedTxID := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	snapshot1 := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			confirmedTxID + ":0": {TxID: confirmedTxID, Vout: 0, Value: 4000},
		},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{confirmedTxID: {}},
		Balance:               4000,
		Count:                 1,
		OldestConfirmedHeight: 400,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 400}

	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot1, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-1: %v", err)
	}

	var walletUpdatedAt1 int64
	if err := db.QueryRow(`SELECT updated_at_unix FROM wallet_utxo WHERE utxo_id=?`, confirmedTxID+":0").Scan(&walletUpdatedAt1); err != nil {
		t.Fatalf("query wallet_utxo updated_at after round-1: %v", err)
	}
	var spentState1 string
	if err := db.QueryRow(`SELECT state FROM wallet_utxo WHERE utxo_id=?`, confirmedTxID+":0").Scan(&spentState1); err != nil {
		t.Fatalf("query wallet_utxo state after round-1: %v", err)
	}
	if spentState1 != "unspent" {
		t.Fatalf("expected unspent after round-1, got %s", spentState1)
	}

	spendTxID := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	snapshot2 := liveWalletSnapshot{
		Live:                  map[string]poolcore.UTXO{},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{},
		Balance:               0,
		Count:                 0,
		OldestConfirmedHeight: 401,
	}
	history := []walletHistoryTxRecord{
		{
			TxID:   spendTxID,
			Height: 401,
			Tx: whatsonchain.TxDetail{
				TxID: spendTxID,
				Vin: []whatsonchain.TxInput{
					{TxID: confirmedTxID, Vout: 0},
				},
			},
		},
	}
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot2, history, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-2", "", "test", now+1, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-2: %v", err)
	}

	var walletUpdatedAt2 int64
	var spentState2 string
	var spentTxID string
	if err := db.QueryRow(`SELECT updated_at_unix,state,spent_txid FROM wallet_utxo WHERE utxo_id=?`, confirmedTxID+":0").Scan(&walletUpdatedAt2, &spentState2, &spentTxID); err != nil {
		t.Fatalf("query wallet_utxo after round-2: %v", err)
	}
	if spentState2 != "spent" {
		t.Fatalf("expected spent after round-2, got %s", spentState2)
	}
	if spentTxID != spendTxID {
		t.Fatalf("expected spent_txid %s, got %s", spendTxID, spentTxID)
	}
	if walletUpdatedAt2 <= walletUpdatedAt1 {
		t.Fatalf("expected wallet_utxo updated_at to move forward, got %d vs %d", walletUpdatedAt2, walletUpdatedAt1)
	}
}

func TestSyncWalletAndApplyFacts_SnapshotMissingMarksSpentWithoutSpentTxID(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	txid := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address}

	snapshot1 := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			txid + ":0": {TxID: txid, Vout: 0, Value: 1234},
		},
		ConfirmedLiveTxIDs: map[string]struct{}{txid: {}},
		Balance:            1234,
		Count:              1,
	}
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot1, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-a", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-a: %v", err)
	}

	snapshot2 := liveWalletSnapshot{
		Live:               map[string]poolcore.UTXO{},
		ConfirmedLiveTxIDs: map[string]struct{}{},
		Balance:            0,
		Count:              0,
	}
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot2, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-b", "", "test", now+1, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-b: %v", err)
	}

	var state string
	var spentTxID string
	if err := db.QueryRow(`SELECT state, COALESCE(spent_txid,'') FROM wallet_utxo WHERE utxo_id=?`, txid+":0").Scan(&state, &spentTxID); err != nil {
		t.Fatalf("query wallet_utxo after snapshot missing failed: %v", err)
	}
	if state != "spent" {
		t.Fatalf("wallet_utxo state mismatch: got=%s want=spent", state)
	}
	if spentTxID != "" {
		t.Fatalf("wallet_utxo spent_txid mismatch: got=%s want empty", spentTxID)
	}

	var factState string
	if err := db.QueryRow(`SELECT utxo_state FROM fact_bsv_utxos WHERE utxo_id=?`, txid+":0").Scan(&factState); err != nil {
		t.Fatalf("query fact_bsv_utxos after snapshot missing failed: %v", err)
	}
	if factState != "spent" {
		t.Fatalf("fact_bsv_utxos state mismatch: got=%s want=spent", factState)
	}
}

// TestSyncWalletAndApplyFacts_FactFailureAndRecovery 真实失败注入测试：
// 分步验证 fact 写入失败-重试恢复场景
// 设计说明：
// - ApplyConfirmedUTXOChanges 是幂等的：UNIQUE 冲突时返回已存在记录而非报错
// - 因此"失败"场景只能通过外部故障（DB 不可用）触发
// - 测试流程：写入成功 -> 关闭 DB -> 写入失败 -> 重开 DB -> 写入恢复成功
func TestSyncWalletAndApplyFacts_FactFailureAndRecovery(t *testing.T) {
	// 使用显式路径以便后续重开数据库
	dbPath := t.TempDir() + "/failure-recovery-test.sqlite"
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	confirmedTxID := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			confirmedTxID + ":0": {TxID: confirmedTxID, Vout: 0, Value: 3000},
		},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{confirmedTxID: {}},
		Balance:               3000,
		Count:                 1,
		OldestConfirmedHeight: 300,
	}
	cursor := walletUTXOSyncCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 300}

	// 步骤1：分步执行第一阶段，获取 changes
	changes, err := reconcileWalletUTXOSetAndReturnChanges(
		context.Background(), store, address, snapshot, nil, cursor, &testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex}, "round-1", "", "test", now, 10,
	)
	if err != nil {
		t.Fatalf("reconcileWalletUTXOSetAndReturnChanges: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}

	// 步骤2：第一次 fact 写入（成功）
	if err := ApplyConfirmedUTXOChanges(context.Background(), store, changes, now); err != nil {
		t.Fatalf("ApplyConfirmedUTXOChanges first call: %v", err)
	}

	// 验证写入成功
	var count1 int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&count1); err != nil {
		t.Fatalf("count after first write: %v", err)
	}
	if count1 != 1 {
		t.Fatalf("expected 1 utxo after first write, got %d", count1)
	}

	// 步骤3：模拟 fact 层故障 - 关闭数据库连接
	// 关闭后下次写入会触发 "sql: database is closed" 错误
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// 步骤4：第二次 fact 写入（因 DB 关闭而失败）
	err = ApplyConfirmedUTXOChanges(context.Background(), store, changes, now+1)
	if err == nil {
		t.Fatalf("expected error when DB is closed, got nil")
	}

	// 步骤5：重开数据库（模拟恢复）
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()
	if err := applySQLitePragmas(db2); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db2); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	// 更新 store 指向新连接
	store = newClientDB(db2, nil)

	// 步骤6：第三次 fact 写入（恢复后成功）
	if err := ApplyConfirmedUTXOChanges(context.Background(), store, changes, now+2); err != nil {
		t.Fatalf("ApplyConfirmedUTXOChanges recovery: %v", err)
	}

	// 验证恢复成功
	var countFinal int
	if err := db2.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&countFinal); err != nil {
		t.Fatalf("count after recovery: %v", err)
	}
	if countFinal != 1 {
		t.Fatalf("expected 1 utxo after recovery, got %d", countFinal)
	}

	// 步骤7：验证幂等（再次写入不重复）
	if err := ApplyConfirmedUTXOChanges(context.Background(), store, changes, now+3); err != nil {
		t.Fatalf("ApplyConfirmedUTXOChanges idempotent: %v", err)
	}

	var countIdempotent int
	if err := db2.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE utxo_id=?`, confirmedTxID+":0").Scan(&countIdempotent); err != nil {
		t.Fatalf("count after idempotent call: %v", err)
	}
	if countIdempotent != 1 {
		t.Fatalf("expected 1 utxo after idempotent call, got %d", countIdempotent)
	}
}

func TestSQLTraceDebugGateAndCallerChain(t *testing.T) {
	dir := t.TempDir()

	logFile := filepath.Join(dir, "client.log")
	if mgr, err := initSQLTrace(logFile, false); err != nil {
		t.Fatalf("initSQLTrace disabled: %v", err)
	} else if mgr != nil {
		t.Fatalf("expected nil manager when debug=false")
	}
	if got := currentSQLTracePath(); got != "" {
		t.Fatalf("expected empty trace path when debug=false, got %s", got)
	}
	openedFalse, err := sqliteactor.Open(filepath.Join(dir, "disabled.sqlite"), false)
	if err != nil {
		t.Fatalf("open disabled db: %v", err)
	}
	t.Cleanup(func() {
		if openedFalse.Actor != nil {
			_ = openedFalse.Actor.Close()
		}
		if openedFalse.DB != nil {
			_ = openedFalse.DB.Close()
		}
	})
	storeFalse := newClientDB(openedFalse.DB, openedFalse.Actor)
	if err := storeFalse.WriteTx(context.Background(), func(db moduleapi.WriteTx) error {
		_, err := db.ExecContext(context.Background(), `CREATE TABLE trace_gate(id INTEGER PRIMARY KEY, value TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("disabled trace exec: %v", err)
	}
	_ = closeSQLTrace()
	if _, err := os.Stat(filepath.Join(dir, "sql_trace")); !os.IsNotExist(err) {
		t.Fatalf("expected no sql_trace directory when debug=false, got err=%v", err)
	}

	logFile = filepath.Join(dir, "client-debug.log")
	mgr, err := initSQLTrace(logFile, true)
	if err != nil {
		t.Fatalf("initSQLTrace enabled: %v", err)
	}
	if mgr == nil {
		t.Fatalf("expected manager when debug=true")
	}
	t.Cleanup(func() { _ = closeSQLTrace() })

	openedTrue, err := sqliteactor.Open(filepath.Join(dir, "enabled.sqlite"), true)
	if err != nil {
		t.Fatalf("open enabled db: %v", err)
	}
	t.Cleanup(func() {
		if openedTrue.Actor != nil {
			_ = openedTrue.Actor.Close()
		}
		if openedTrue.DB != nil {
			_ = openedTrue.DB.Close()
		}
	})

	storeTrue := newClientDB(openedTrue.DB, openedTrue.Actor)
	roundID := "round-trace"
	trigger := "trigger-trace"
	if err := runSQLTraceReplay(storeTrue, roundID, trigger); err != nil {
		t.Fatalf("runSQLTraceReplay: %v", err)
	}
	flushSQLTraceRoundSummary(roundID)

	summaryPath := currentSQLTraceSummaryPath()
	if summaryPath == "" {
		t.Fatalf("expected summary path when debug=true")
	}
	summaryBytes, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var summary struct {
		RoundID               string `json:"round_id"`
		TotalSQL              int    `json:"total_sql"`
		TotalWrite            int    `json:"total_write"`
		MostCommonCallerChain string `json:"most_common_caller_chain"`
		RepeatUpdateTop       []struct {
			SQLFingerprint string   `json:"sql_fingerprint"`
			Count          int      `json:"count"`
			Intent         string   `json:"intent"`
			Stage          string   `json:"stage"`
			CallerChain    []string `json:"caller_chain"`
		} `json:"repeat_update_top"`
	}
	if err := json.Unmarshal(summaryBytes, &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if summary.RoundID != roundID {
		t.Fatalf("summary round_id mismatch: got=%s want=%s", summary.RoundID, roundID)
	}
	if summary.TotalSQL < 4 {
		t.Fatalf("expected at least 4 sql events, got %d", summary.TotalSQL)
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
	if !strings.Contains(summary.MostCommonCallerChain, "runSQLTraceReplay") {
		t.Fatalf("expected most common caller chain to include helper, got %s", summary.MostCommonCallerChain)
	}

	eventsPath := filepath.Join(currentSQLTracePath(), "sql_trace.jsonl")
	eventsBytes, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(eventsBytes)), "\n")
	if len(lines) == 0 {
		t.Fatalf("expected trace events")
	}
	var firstEvent sqliteactor.TraceEvent
	if err := json.Unmarshal([]byte(lines[0]), &firstEvent); err != nil {
		t.Fatalf("unmarshal trace event: %v", err)
	}
	if firstEvent.RoundID != roundID {
		t.Fatalf("trace round_id mismatch: got=%s want=%s", firstEvent.RoundID, roundID)
	}
	if firstEvent.Trigger != trigger {
		t.Fatalf("trace trigger mismatch: got=%s want=%s", firstEvent.Trigger, trigger)
	}
	if len(firstEvent.CallerChain) == 0 {
		t.Fatalf("expected caller_chain on trace event")
	}
}

func runSQLTraceReplay(store *clientDB, roundID string, trigger string) error {
	ctx := sqlTraceContextWithMeta(context.Background(), roundID, trigger, "sql_trace_replay", "wallet_fact_orchestrator_test.runSQLTraceReplay")
	return store.WriteTx(ctx, func(db moduleapi.WriteTx) error {
		if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS trace_gate(id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO trace_gate(id, value) VALUES(?, ?)`, int64(1), "a"); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE trace_gate SET value=? WHERE id=?`, "b", int64(1)); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE trace_gate SET value=? WHERE id=?`, "c", int64(1)); err != nil {
			return err
		}
		return nil
	})
}
