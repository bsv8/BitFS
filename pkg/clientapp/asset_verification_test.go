package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ==================== Unknown 资产确认测试 ====================

func TestDbListUnknownOneSatUTXOs(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入测试数据：unknown 1-sat UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert unknown utxo: %v", err)
	}
	
	// 插入非 1-sat 的 unknown（不应被列出）
	_, err = db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx2:0", walletID, address, "tx2", 0, 1000, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx2", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert non-1sat unknown utxo: %v", err)
	}
	
	// 插入 plain_bsv 的 1-sat（不应被列出）
	_, err = db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx3:0", walletID, address, "tx3", 0, 1, "unspent", walletUTXOAllocationPlainBSV, "", "tx3", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert plain bsv utxo: %v", err)
	}
	
	// 插入 spent 的 unknown 1-sat（不应被列出）
	_, err = db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx4:0", walletID, address, "tx4", 0, 1, "spent", walletUTXOAllocationUnknown, "awaiting evidence", "tx4", "tx5", now, now, now,
	)
	if err != nil {
		t.Fatalf("insert spent unknown utxo: %v", err)
	}
	
	rows, err := dbListUnknownOneSatUTXOs(context.Background(), store, address, 100)
	if err != nil {
		t.Fatalf("list unknown utxos: %v", err)
	}
	
	if len(rows) != 1 {
		t.Fatalf("expected 1 unknown utxo, got %d", len(rows))
	}
	
	if rows[0].UTXOID != "tx1:0" {
		t.Fatalf("expected utxo_id tx1:0, got %s", rows[0].UTXOID)
	}
}

func TestDbUpdateUTXOAllocationClass(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	// 更新为 plain_bsv
	if err := dbUpdateUTXOAllocationClass(context.Background(), store, "tx1:0", walletUTXOAllocationPlainBSV, "verified as plain BSV"); err != nil {
		t.Fatalf("update allocation class: %v", err)
	}
	
	// 验证更新
	var class, reason string
	err = db.QueryRow(`SELECT allocation_class, allocation_reason FROM wallet_utxo WHERE utxo_id=?`, "tx1:0").Scan(&class, &reason)
	if err != nil {
		t.Fatalf("query updated utxo: %v", err)
	}
	
	if class != walletUTXOAllocationPlainBSV {
		t.Fatalf("expected class %s, got %s", walletUTXOAllocationPlainBSV, class)
	}
	
	if reason != "verified as plain BSV" {
		t.Fatalf("expected reason 'verified as plain BSV', got %s", reason)
	}
}

func TestProcessAssetVerificationResult_PlainBSV(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	// 处理非 token 证据
	row := unknownUTXORow{
		UTXOID:       "tx1:0",
		WalletID:     walletID,
		Address:      address,
		TxID:         "tx1",
		Vout:         0,
		ValueSatoshi: 1,
		CreatedAtUnix: now,
	}
	evidence := &wocTokenEvidence{IsToken: false}
	
	if err := processAssetVerificationResult(context.Background(), store, row, evidence, "test"); err != nil {
		t.Fatalf("process verification result: %v", err)
	}
	
	// 验证 allocation_class 更新
	var class string
	err = db.QueryRow(`SELECT allocation_class FROM wallet_utxo WHERE utxo_id=?`, "tx1:0").Scan(&class)
	if err != nil {
		t.Fatalf("query utxo: %v", err)
	}
	
	if class != walletUTXOAllocationPlainBSV {
		t.Fatalf("expected class %s, got %s", walletUTXOAllocationPlainBSV, class)
	}
	
	// 验证 fact_chain_asset_flows 写入
	var flowID, assetKind string
	err = db.QueryRow(`SELECT flow_id, asset_kind FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN'`, "tx1:0").Scan(&flowID, &assetKind)
	if err != nil {
		t.Fatalf("query asset flow: %v", err)
	}
	
	if assetKind != "BSV" {
		t.Fatalf("expected asset_kind BSV, got %s", assetKind)
	}
	
	if flowID != "flow_in_tx1:0" {
		t.Fatalf("expected flow_id flow_in_tx1:0, got %s", flowID)
	}
}

func TestProcessAssetVerificationResult_BSV21Token(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	// 处理 BSV21 token 证据
	row := unknownUTXORow{
		UTXOID:       "tx1:0",
		WalletID:     walletID,
		Address:      address,
		TxID:         "tx1",
		Vout:         0,
		ValueSatoshi: 1,
		CreatedAtUnix: now,
	}
	evidence := &wocTokenEvidence{
		IsToken:       true,
		TokenStandard: "bsv21",
		TokenID:       "token123",
		Symbol:        "TEST",
		QuantityText:  "1000",
	}
	
	if err := processAssetVerificationResult(context.Background(), store, row, evidence, "test"); err != nil {
		t.Fatalf("process verification result: %v", err)
	}
	
	// 验证 allocation_class 更新为 protected_asset
	var class string
	err = db.QueryRow(`SELECT allocation_class FROM wallet_utxo WHERE utxo_id=?`, "tx1:0").Scan(&class)
	if err != nil {
		t.Fatalf("query utxo: %v", err)
	}
	
	if class != walletUTXOAllocationProtectedAsset {
		t.Fatalf("expected class %s, got %s", walletUTXOAllocationProtectedAsset, class)
	}
	
	// 验证 fact_chain_asset_flows 写入
	var assetKind, tokenID, quantityText string
	err = db.QueryRow(`SELECT asset_kind, token_id, quantity_text FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN'`, "tx1:0").Scan(&assetKind, &tokenID, &quantityText)
	if err != nil {
		t.Fatalf("query asset flow: %v", err)
	}
	
	if assetKind != "BSV21" {
		t.Fatalf("expected asset_kind BSV21, got %s", assetKind)
	}
	
	if tokenID != "token123" {
		t.Fatalf("expected token_id token123, got %s", tokenID)
	}
	
	if quantityText != "1000" {
		t.Fatalf("expected quantity_text 1000, got %s", quantityText)
	}
}

func TestProcessAssetVerificationResult_BSV20Token(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	// 处理 BSV20 token 证据
	row := unknownUTXORow{
		UTXOID:       "tx1:0",
		WalletID:     walletID,
		Address:      address,
		TxID:         "tx1",
		Vout:         0,
		ValueSatoshi: 1,
		CreatedAtUnix: now,
	}
	evidence := &wocTokenEvidence{
		IsToken:       true,
		TokenStandard: "bsv20",
		TokenID:       "token456",
		Symbol:        "TEST20",
		QuantityText:  "5000",
	}
	
	if err := processAssetVerificationResult(context.Background(), store, row, evidence, "test"); err != nil {
		t.Fatalf("process verification result: %v", err)
	}
	
	// 验证 allocation_class 更新为 protected_asset
	var class string
	err = db.QueryRow(`SELECT allocation_class FROM wallet_utxo WHERE utxo_id=?`, "tx1:0").Scan(&class)
	if err != nil {
		t.Fatalf("query utxo: %v", err)
	}
	
	if class != walletUTXOAllocationProtectedAsset {
		t.Fatalf("expected class %s, got %s", walletUTXOAllocationProtectedAsset, class)
	}
	
	// 验证 fact_chain_asset_flows 写入
	var assetKind, tokenID, quantityText string
	err = db.QueryRow(`SELECT asset_kind, token_id, quantity_text FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN'`, "tx1:0").Scan(&assetKind, &tokenID, &quantityText)
	if err != nil {
		t.Fatalf("query asset flow: %v", err)
	}
	
	if assetKind != "BSV20" {
		t.Fatalf("expected asset_kind BSV20, got %s", assetKind)
	}
	
	if tokenID != "token456" {
		t.Fatalf("expected token_id token456, got %s", tokenID)
	}
	
	if quantityText != "5000" {
		t.Fatalf("expected quantity_text 5000, got %s", quantityText)
	}
}

func TestProcessAssetVerificationResult_Idempotent(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	
	now := time.Now().Unix()
	
	// 插入 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	row := unknownUTXORow{
		UTXOID:       "tx1:0",
		WalletID:     walletID,
		Address:      address,
		TxID:         "tx1",
		Vout:         0,
		ValueSatoshi: 1,
		CreatedAtUnix: now,
	}
	evidence := &wocTokenEvidence{IsToken: false}
	
	// 第一次处理
	if err := processAssetVerificationResult(context.Background(), store, row, evidence, "test"); err != nil {
		t.Fatalf("first process: %v", err)
	}
	
	// 第二次处理（幂等）
	if err := processAssetVerificationResult(context.Background(), store, row, evidence, "test"); err != nil {
		t.Fatalf("second process: %v", err)
	}
	
	// 验证只写入了一条 flow
	var count int
	err = db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN'`, "tx1:0").Scan(&count)
	if err != nil {
		t.Fatalf("count flows: %v", err)
	}
	
	if count != 1 {
		t.Fatalf("expected 1 flow, got %d", count)
	}
}

func TestDbListUnknownOneSatUTXOs_WrongAddress(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address1 := seedTestAddress(t, db)
	address2 := seedTestAddressWithKey(t, db, strings.Repeat("2", 64))
	walletID1 := walletIDByAddress(address1)
	
	now := time.Now().Unix()
	
	// 插入 address1 的 unknown UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID1, address1, "tx1", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx1", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("insert utxo: %v", err)
	}
	
	// 查询 address2，应该返回空
	rows, err := dbListUnknownOneSatUTXOs(context.Background(), store, address2, 100)
	if err != nil {
		t.Fatalf("list unknown utxos: %v", err)
	}
	
	if len(rows) != 0 {
		t.Fatalf("expected 0 utxos for address2, got %d", len(rows))
	}
}

// ==================== 测试辅助函数 ====================

func newAssetVerificationTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "asset-verification.sqlite")
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
	return db
}

func seedTestAddress(t *testing.T, db *sql.DB) string {
	t.Helper()
	return seedTestAddressWithKey(t, db, strings.Repeat("1", 64))
}

func seedTestAddressWithKey(t *testing.T, db *sql.DB, privKeyHex string) string {
	t.Helper()
	
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = privKeyHex
	
	rt := &Runtime{
		runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex),
	}
	
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("build actor: %v", err)
	}
	
	return strings.TrimSpace(actor.Addr)
}
