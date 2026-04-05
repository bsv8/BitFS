package clientapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
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
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
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
	cursor := walletUTXOHistoryCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 100}

	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts: %v", err)
	}

	var flowCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&flowCount); err != nil {
		t.Fatalf("count flows: %v", err)
	}
	if flowCount != 1 {
		t.Fatalf("expected 1 flow, got %d", flowCount)
	}

	var utxoID, direction string
	if err := db.QueryRow(`SELECT utxo_id, direction FROM fact_chain_asset_flows`).Scan(&utxoID, &direction); err != nil {
		t.Fatalf("query flow: %v", err)
	}
	if utxoID != confirmedTxID+":0" {
		t.Fatalf("expected utxo_id %s, got %s", confirmedTxID+":0", utxoID)
	}
	if direction != "IN" {
		t.Fatalf("expected direction IN, got %s", direction)
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
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
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
	cursor := walletUTXOHistoryCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 200}

	// 第一次同步
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-1: %v", err)
	}

	var flowID1 int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&flowID1); err != nil {
		t.Fatalf("query flow after round-1: %v", err)
	}

	// 第二次同步（重复触发）
	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, nil, cursor, "round-2", "", "test", now+1, 10); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts round-2: %v", err)
	}

	var flowCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&flowCount); err != nil {
		t.Fatalf("count flows after round-2: %v", err)
	}
	if flowCount != 1 {
		t.Fatalf("expected 1 flow (idempotent), got %d", flowCount)
	}

	var flowID2 int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&flowID2); err != nil {
		t.Fatalf("query flow after round-2: %v", err)
	}
	if flowID2 != flowID1 {
		t.Fatalf("expected same flow id (idempotent), got %d vs %d", flowID2, flowID1)
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
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
	cursor := walletUTXOHistoryCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 300}

	// 步骤1：分步执行第一阶段，获取 changes
	changes, err := reconcileWalletUTXOSetAndReturnChanges(
		context.Background(), store, address, snapshot, nil, cursor, "round-1", "", "test", now, 10,
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&count1); err != nil {
		t.Fatalf("count after first write: %v", err)
	}
	if count1 != 1 {
		t.Fatalf("expected 1 flow after first write, got %d", count1)
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
	if err := initIndexDB(db2); err != nil {
		t.Fatalf("init db: %v", err)
	}

	// 更新 store 指向新连接
	store = newClientDB(db2, nil)

	// 步骤6：第三次 fact 写入（恢复后成功）
	if err := ApplyConfirmedUTXOChanges(context.Background(), store, changes, now+2); err != nil {
		t.Fatalf("ApplyConfirmedUTXOChanges recovery: %v", err)
	}

	// 验证恢复成功
	var countFinal int
	if err := db2.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&countFinal); err != nil {
		t.Fatalf("count after recovery: %v", err)
	}
	if countFinal != 1 {
		t.Fatalf("expected 1 flow after recovery, got %d", countFinal)
	}

	// 步骤7：验证幂等（再次写入不重复）
	if err := ApplyConfirmedUTXOChanges(context.Background(), store, changes, now+3); err != nil {
		t.Fatalf("ApplyConfirmedUTXOChanges idempotent: %v", err)
	}

	var countIdempotent int
	if err := db2.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&countIdempotent); err != nil {
		t.Fatalf("count after idempotent call: %v", err)
	}
	if countIdempotent != 1 {
		t.Fatalf("expected 1 flow after idempotent call, got %d", countIdempotent)
	}
}
