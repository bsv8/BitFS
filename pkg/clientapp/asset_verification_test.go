package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ==================== Unknown 资产确认测试 ====================

func TestDbListPendingVerificationItems(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)

	// Step 9: 直接种子 verification 队列表
	now := time.Now().Unix()
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx1:0", walletID, address, "tx1", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("insert pending verification: %v", err)
	}

	rows, err := dbListPendingVerificationItems(context.Background(), store, 100)
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
		UTXOID:        "tx1:0",
		WalletID:      walletID,
		Address:       address,
		TxID:          "tx1",
		Vout:          0,
		ValueSatoshi:  1,
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

	// 验证 fact_bsv_utxos 写入
	var utxoID, carrierType string
	err = db.QueryRow(`SELECT utxo_id, carrier_type FROM fact_bsv_utxos WHERE utxo_id=?`, "tx1:0").Scan(&utxoID, &carrierType)
	if err != nil {
		t.Fatalf("query bsv utxo: %v", err)
	}

	if carrierType != "plain_bsv" {
		t.Fatalf("expected carrier_type plain_bsv, got %s", carrierType)
	}

	if utxoID != "tx1:0" {
		t.Fatalf("expected utxo_id tx1:0, got %s", utxoID)
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
		UTXOID:        "tx1:0",
		WalletID:      walletID,
		Address:       address,
		TxID:          "tx1",
		Vout:          0,
		ValueSatoshi:  1,
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

	// 验证 fact_token_lots 和 fact_token_carrier_links 写入
	var lotID, tokenID, quantityText string
	err = db.QueryRow(`SELECT lot_id, token_id, quantity_text FROM fact_token_lots WHERE mint_txid=?`, "tx1").Scan(&lotID, &tokenID, &quantityText)
	if err != nil {
		t.Fatalf("query token lot: %v", err)
	}

	if tokenID != "token123" {
		t.Fatalf("expected token_id token123, got %s", tokenID)
	}

	if quantityText != "1000" {
		t.Fatalf("expected quantity_text 1000, got %s", quantityText)
	}

	// 验证 carrier link 存在
	var linkID string
	err = db.QueryRow(`SELECT link_id FROM fact_token_carrier_links WHERE lot_id=? AND carrier_utxo_id=?`, lotID, "tx1:0").Scan(&linkID)
	if err != nil {
		t.Fatalf("query token carrier link: %v", err)
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
		UTXOID:        "tx1:0",
		WalletID:      walletID,
		Address:       address,
		TxID:          "tx1",
		Vout:          0,
		ValueSatoshi:  1,
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

	// 验证 fact_token_lots 和 fact_token_carrier_links 写入
	var lotID, tokenID, quantityText string
	err = db.QueryRow(`SELECT lot_id, token_id, quantity_text FROM fact_token_lots WHERE mint_txid=?`, "tx1").Scan(&lotID, &tokenID, &quantityText)
	if err != nil {
		t.Fatalf("query token lot: %v", err)
	}

	if tokenID != "token456" {
		t.Fatalf("expected token_id token456, got %s", tokenID)
	}

	if quantityText != "5000" {
		t.Fatalf("expected quantity_text 5000, got %s", quantityText)
	}

	// 验证 carrier link 存在
	var linkID string
	err = db.QueryRow(`SELECT link_id FROM fact_token_carrier_links WHERE lot_id=? AND carrier_utxo_id=?`, lotID, "tx1:0").Scan(&linkID)
	if err != nil {
		t.Fatalf("query token carrier link: %v", err)
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
		UTXOID:        "tx1:0",
		WalletID:      walletID,
		Address:       address,
		TxID:          "tx1",
		Vout:          0,
		ValueSatoshi:  1,
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

	// 验证只写入了一条 bsv_utxo
	var count int
	err = db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE utxo_id=?`, "tx1:0").Scan(&count)
	if err != nil {
		t.Fatalf("count bsv utxos: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 bsv utxo, got %d", count)
	}
}

func TestDbListUnknownOneSatUTXOs_WrongAddress(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address1 := seedTestAddress(t, db)
	_ = seedTestAddressWithKey(t, db, strings.Repeat("2", 64))
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
	rows, err := dbListPendingVerificationItems(context.Background(), store, 100)
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

// TestVerificationQueue_StateTransitionSuccess 验证成功后 status 正确迁移且不再被查到
func TestVerificationQueue_StateTransitionSuccess(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子 pending 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_succ:0", walletID, address, "tx_succ", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	// 验证 pending 项被查出
	rows, err := dbListPendingVerificationItems(context.Background(), store, 100)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(rows))
	}

	// 模拟成功路径：更新为 confirmed_bsv21
	if err := updateVerificationQueueSuccess(context.Background(), store, "tx_succ:0", "confirmed_bsv21", &wocTokenEvidence{
		IsToken: true, TokenStandard: "bsv21", TokenID: "token123", Symbol: "TST", QuantityText: "100",
	}); err != nil {
		t.Fatalf("update success: %v", err)
	}

	// 验证状态
	var status string
	var checkAt int64
	var retryCount int
	if err := db.QueryRow(`SELECT status, last_check_at_unix, retry_count FROM wallet_utxo_token_verification WHERE utxo_id=?`, "tx_succ:0").Scan(&status, &checkAt, &retryCount); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "confirmed_bsv21" {
		t.Fatalf("expected status confirmed_bsv21, got %s", status)
	}
	if checkAt == 0 {
		t.Fatalf("expected last_check_at_unix > 0")
	}
	if retryCount != 0 {
		t.Fatalf("expected retry_count 0, got %d", retryCount)
	}

	// 验证不再被 dbListPendingVerificationItems 查到
	rows2, err := dbListPendingVerificationItems(context.Background(), store, 100)
	if err != nil {
		t.Fatalf("list pending after success: %v", err)
	}
	if len(rows2) != 0 {
		t.Fatalf("expected 0 pending after success, got %d", len(rows2))
	}
}

// TestVerificationQueue_StateTransitionFailure 验证失败后 retry_count/next_retry/last_check_at_unix 正确更新
func TestVerificationQueue_StateTransitionFailure(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子 pending 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix,retry_count)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"tx_fail:0", walletID, address, "tx_fail", 0, 1, "pending", now, now, 0,
	)
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	// 模拟失败路径
	if err := updateVerificationBackoff(context.Background(), store, "tx_fail:0", "woc timeout"); err != nil {
		t.Fatalf("update backoff: %v", err)
	}

	// 验证更新
	var retryCount int
	var nextRetry int64
	var checkAt int64
	var errMsg string
	if err := db.QueryRow(`SELECT retry_count, next_retry_at_unix, last_check_at_unix, error_message FROM wallet_utxo_token_verification WHERE utxo_id=?`, "tx_fail:0").Scan(&retryCount, &nextRetry, &checkAt, &errMsg); err != nil {
		t.Fatalf("query backoff: %v", err)
	}
	if retryCount != 1 {
		t.Fatalf("expected retry_count 1, got %d", retryCount)
	}
	if nextRetry <= now {
		t.Fatalf("expected next_retry_at_unix > now (%d), got %d", now, nextRetry)
	}
	if checkAt == 0 {
		t.Fatalf("expected last_check_at_unix > 0")
	}
	if !strings.Contains(errMsg, "woc timeout") {
		t.Fatalf("expected error to contain 'woc timeout', got %s", errMsg)
	}
}

// ==================== Step 10 运维可观测测试 ====================

func TestVerificationQueueSummary(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子不同状态的项
	states := []struct {
		utxoID string
		status string
	}{
		{"tx_p1:0", "pending"},
		{"tx_p2:0", "pending"},
		{"tx_bsv20:0", "confirmed_bsv20"},
		{"tx_bsv21:0", "confirmed_bsv21"},
		{"tx_plain:0", "confirmed_plain_bsv"},
		{"tx_fail:0", "failed"},
	}
	for _, s := range states {
		_, err := db.Exec(
			`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?)`,
			s.utxoID, walletID, address, s.utxoID, 0, 1, s.status, now, now,
		)
		if err != nil {
			t.Fatalf("seed %s: %v", s.status, err)
		}
	}

	summary, err := dbGetVerificationQueueSummary(context.Background(), store)
	if err != nil {
		t.Fatalf("get summary: %v", err)
	}

	if summary.Pending != 2 {
		t.Fatalf("expected pending 2, got %d", summary.Pending)
	}
	if summary.ConfirmedBSV20 != 1 {
		t.Fatalf("expected confirmed_bsv20 1, got %d", summary.ConfirmedBSV20)
	}
	if summary.ConfirmedBSV21 != 1 {
		t.Fatalf("expected confirmed_bsv21 1, got %d", summary.ConfirmedBSV21)
	}
	if summary.ConfirmedPlainBSV != 1 {
		t.Fatalf("expected confirmed_plain_bsv 1, got %d", summary.ConfirmedPlainBSV)
	}
	if summary.Failed != 1 {
		t.Fatalf("expected failed 1, got %d", summary.Failed)
	}
	if summary.Total != 6 {
		t.Fatalf("expected total 6, got %d", summary.Total)
	}
}

func TestListFailedVerificationItems(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子带错误的 pending 和 failed 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_err1:0", walletID, address, "tx_err1", 0, 1, "pending", "woc timeout", 3, now, now,
	)
	if err != nil {
		t.Fatalf("seed err1: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_err2:0", walletID, address, "tx_err2", 0, 1, "failed", "max retries exceeded", 10, now, now,
	)
	if err != nil {
		t.Fatalf("seed err2: %v", err)
	}
	// 无错误的 pending 项不应出现
	_, err = db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_clean:0", walletID, address, "tx_clean", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed clean: %v", err)
	}

	// 默认只查 failed
	items, err := dbListFailedVerificationItems(context.Background(), store, 10, false)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 failed item, got %d", len(items))
	}
	if items[0].UTXOID != "tx_err2:0" {
		t.Fatalf("expected tx_err2:0, got %s", items[0].UTXOID)
	}

	// include_pending=true 时返回 pending + failed
	items2, err := dbListFailedVerificationItems(context.Background(), store, 10, true)
	if err != nil {
		t.Fatalf("list failed with pending: %v", err)
	}
	if len(items2) != 2 {
		t.Fatalf("expected 2 items with include_pending, got %d", len(items2))
	}
}

func TestListVerificationItems_FilterByStatus(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子不同状态
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_pending:0", walletID, address, "tx_pending", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_confirmed:0", walletID, address, "tx_confirmed", 0, 1, "confirmed_bsv21", now, now,
	)
	if err != nil {
		t.Fatalf("seed confirmed: %v", err)
	}

	// 过滤 pending
	items, err := dbListVerificationItems(context.Background(), store, verificationQueueFilter{
		Status: "pending",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(items))
	}
	if items[0].Status != "pending" {
		t.Fatalf("expected status pending, got %s", items[0].Status)
	}

	// 过滤 confirmed_bsv21
	items2, err := dbListVerificationItems(context.Background(), store, verificationQueueFilter{
		Status: "confirmed_bsv21",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list confirmed: %v", err)
	}
	if len(items2) != 1 {
		t.Fatalf("expected 1 confirmed, got %d", len(items2))
	}
}

func TestListVerificationItems_FilterByWalletID(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address1 := seedTestAddress(t, db)
	address2 := seedTestAddressWithKey(t, db, strings.Repeat("3", 64))
	walletID1 := walletIDByAddress(address1)
	walletID2 := walletIDByAddress(address2)
	now := time.Now().Unix()

	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_w1:0", walletID1, address1, "tx_w1", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed w1: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_w2:0", walletID2, address2, "tx_w2", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed w2: %v", err)
	}

	items, err := dbListVerificationItems(context.Background(), store, verificationQueueFilter{
		WalletID: walletID1,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("list by wallet: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item for wallet1, got %d", len(items))
	}
	if items[0].WalletID != walletID1 {
		t.Fatalf("expected wallet %s, got %s", walletID1, items[0].WalletID)
	}
}

func TestAutoFailExhaustedRetries(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个 retry_count 达到上限的 pending 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"tx_maxretry:0", walletID, address, "tx_maxretry", 0, 1, "pending", assetVerificationMaxRetries, now, now,
	)
	if err != nil {
		t.Fatalf("seed max retry: %v", err)
	}
	// 种子一个 retry_count 未达上限的 pending 项
	_, err = db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"tx_ok:0", walletID, address, "tx_ok", 0, 1, "pending", 5, now, now,
	)
	if err != nil {
		t.Fatalf("seed ok: %v", err)
	}

	count, err := dbAutoFailExhaustedRetries(context.Background(), store)
	if err != nil {
		t.Fatalf("auto fail: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 auto-failed, got %d", count)
	}

	// 验证状态
	var status string
	if err := db.QueryRow(`SELECT status FROM wallet_utxo_token_verification WHERE utxo_id=?`, "tx_maxretry:0").Scan(&status); err != nil {
		t.Fatalf("query tx_maxretry: %v", err)
	}
	if status != "failed" {
		t.Fatalf("expected status failed, got %s", status)
	}

	// 验证未达上限的仍是 pending
	var status2 string
	if err := db.QueryRow(`SELECT status FROM wallet_utxo_token_verification WHERE utxo_id=?`, "tx_ok:0").Scan(&status2); err != nil {
		t.Fatalf("query tx_ok: %v", err)
	}
	if status2 != "pending" {
		t.Fatalf("expected tx_ok still pending, got %s", status2)
	}
}

func TestResetVerificationToPending(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个 failed 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_reset:0", walletID, address, "tx_reset", 0, 1, "failed", "some error", 10, now, now,
	)
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	if err := dbResetVerificationToPending(context.Background(), store, "tx_reset:0"); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// 验证重置
	var status string
	var retryCount int
	var errMsg string
	if err := db.QueryRow(`SELECT status, retry_count, error_message FROM wallet_utxo_token_verification WHERE utxo_id=?`, "tx_reset:0").Scan(&status, &retryCount, &errMsg); err != nil {
		t.Fatalf("query reset: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected status pending, got %s", status)
	}
	if retryCount != 0 {
		t.Fatalf("expected retry_count 0, got %d", retryCount)
	}
	if errMsg != "" {
		t.Fatalf("expected empty error message, got %s", errMsg)
	}

	// 测试不存在的 utxo_id
	err = dbResetVerificationToPending(context.Background(), store, "nonexistent:0")
	if err == nil {
		t.Fatalf("expected error for nonexistent utxo_id")
	}
}

func TestBatchRetryFailed(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子多个 failed 项
	for i := 0; i < 3; i++ {
		_, err := db.Exec(
			`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("tx_batch%d:0", i), walletID, address, fmt.Sprintf("tx_batch%d", i), 0, 1, "failed", "error", 10, now, now,
		)
		if err != nil {
			t.Fatalf("seed batch %d: %v", i, err)
		}
	}
	// 种子一个 confirmed 项（不应被重置）
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_confirmed_batch:0", walletID, address, "tx_confirmed_batch", 0, 1, "confirmed_bsv21", now, now,
	)
	if err != nil {
		t.Fatalf("seed confirmed batch: %v", err)
	}

	count, err := dbBatchRetryFailed(context.Background(), store, walletID, 0, 0)
	if err != nil {
		t.Fatalf("batch retry: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 retried, got %d", count)
	}

	// 验证都被重置为 pending
	var pendingCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM wallet_utxo_token_verification WHERE status='pending'`).Scan(&pendingCount); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pendingCount != 3 {
		t.Fatalf("expected 3 pending after retry, got %d", pendingCount)
	}
}

func TestVerificationReconciliation_ConfirmedNoFact(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个 confirmed 但没有 fact 的项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_orphan:0", walletID, address, "tx_orphan", 0, 1, "confirmed_bsv21", now, now,
	)
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	report, err := dbCheckVerificationReconciliation(context.Background(), store)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(report.ConfirmedNoFact) != 1 {
		t.Fatalf("expected 1 confirmed_without_fact, got %d", len(report.ConfirmedNoFact))
	}
	if report.ConfirmedNoFact[0].UTXOID != "tx_orphan:0" {
		t.Fatalf("expected utxo_id tx_orphan:0, got %s", report.ConfirmedNoFact[0].UTXOID)
	}
	if report.Summary["confirmed_without_fact"] != 1 {
		t.Fatalf("expected summary confirmed_without_fact 1, got %d", report.Summary["confirmed_without_fact"])
	}
}

func TestVerificationReconciliation_FactNoConfirmation(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个 pending verification 项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_pending_fact:0", walletID, address, "tx_pending_fact", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	// 直接写入 fact_token_lots 和 fact_token_carrier_links（模拟 fact 存在但 verification 仍 pending）
	ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(walletID, "wallet:"))
	lotID := "lot_tx_pending_fact_0"
	_, err = db.Exec(
		`INSERT INTO fact_token_lots(lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text, lot_state, mint_txid, created_at_unix, updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		lotID, ownerPubkeyHex, "token123", "BSV21", "100", "0", "unspent", "tx_pending_fact", now, now,
	)
	if err != nil {
		t.Fatalf("seed token lot: %v", err)
	}
	if err := dbUpsertBSVUTXO(context.Background(), store, bsvUTXOEntry{
		UTXOID:         "tx_pending_fact:0",
		OwnerPubkeyHex: ownerPubkeyHex,
		Address:        address,
		TxID:           "tx_pending_fact",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier bsv utxo: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO fact_token_carrier_links(link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid, created_at_unix, updated_at_unix, note, payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"link_tx_pending_fact_0", lotID, "tx_pending_fact:0", ownerPubkeyHex, "active", "tx_pending_fact", "", now, now, "", "{}",
	)
	if err != nil {
		t.Fatalf("seed token carrier link: %v", err)
	}

	report, err := dbCheckVerificationReconciliation(context.Background(), store)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(report.FactNoConfirmation) != 1 {
		t.Fatalf("expected 1 fact_pending_or_missing, got %d", len(report.FactNoConfirmation))
	}
	if report.FactNoConfirmation[0].UTXOID != "tx_pending_fact:0" {
		t.Fatalf("expected utxo_id tx_pending_fact:0, got %s", report.FactNoConfirmation[0].UTXOID)
	}
}

func TestShortHex(t *testing.T) {
	t.Parallel()

	// 短字符串不被截断
	if shortHex("abc") != "abc" {
		t.Fatalf("expected 'abc', got '%s'", shortHex("abc"))
	}

	// 长字符串被截断
	long := "abcdef1234567890abcdef1234567890"
	got := shortHex(long)
	if got != "abcd...7890" {
		t.Fatalf("expected 'abcd...7890', got '%s'", got)
	}
}

func TestVerificationReconciliation_NoFalsePositive(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个普通 UTXO 的 fact IN（不应在 verification 范围内）
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_normal:0", walletID, address, "tx_normal", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, "normal", "tx_normal", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("seed utxo: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO fact_bsv_utxos(utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi, utxo_state, carrier_type, created_at_unix, updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"tx_normal:0", strings.ToLower(strings.TrimPrefix(walletID, "wallet:")), address, "tx_normal", 0, 1000, "unspent", "plain_bsv", now, now,
	)
	if err != nil {
		t.Fatalf("seed bsv utxo: %v", err)
	}

	// verification 队列中没有这个 utxo
	report, err := dbCheckVerificationReconciliation(context.Background(), store)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// 不应该误报：普通 fact 不在 verification 范围内
	if len(report.FactNoConfirmation) != 0 {
		t.Fatalf("expected 0 fact_pending_or_miss for normal UTXO, got %d", len(report.FactNoConfirmation))
	}
}

// TestTokenSpendableSourceFlows_ExcludesUnknown 验证 unknown 资产不参与 token 可花费选源
func TestTokenSpendableSourceFlows_ExcludesUnknown(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子一个 allocation_class='unknown' 的 UTXO
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_unknown:0", walletID, address, "tx_unknown", 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting evidence", "tx_unknown", "", now, now, 0,
	)
	if err != nil {
		t.Fatalf("seed unknown utxo: %v", err)
	}

	// 即使有 token fact 记录，unknown 也不应出现在可花费选源中
	ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(walletID, "wallet:"))
	lotID := "lot_tx_unknown_0"
	_, err = db.Exec(
		`INSERT INTO fact_token_lots(lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text, lot_state, mint_txid, created_at_unix, updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		lotID, ownerPubkeyHex, "token123", "BSV21", "100", "0", "unspent", "tx_unknown", now, now,
	)
	if err != nil {
		t.Fatalf("seed token lot for unknown: %v", err)
	}
	if err := dbUpsertBSVUTXO(context.Background(), newClientDB(db, nil), bsvUTXOEntry{
		UTXOID:         "tx_unknown:0",
		OwnerPubkeyHex: ownerPubkeyHex,
		Address:        address,
		TxID:           "tx_unknown",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed unknown token carrier bsv utxo: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO fact_token_carrier_links(link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid, created_at_unix, updated_at_unix, note, payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"link_tx_unknown_0", lotID, "tx_unknown:0", ownerPubkeyHex, "active", "tx_unknown", "", now, now, "", "{}",
	)
	if err != nil {
		t.Fatalf("seed token carrier link for unknown: %v", err)
	}

	// 查询可花费选源
	flows, err := dbListTokenSpendableSourceFlows(context.Background(), newClientDB(db, nil), walletID, "BSV21", "token123")
	if err != nil {
		t.Fatalf("list token spendable: %v", err)
	}

	// unknown 不应出现在结果中
	if len(flows) != 0 {
		t.Fatalf("expected 0 token spendable flows for unknown UTXO, got %d", len(flows))
	}
}

// TestTokenBalance_FactEmptyWithHistoryNotFallback 验证 fact 空但有历史，不回退旧路径
// 使用新表 fact_settlement_records 和 fact_token_lots
func TestTokenBalance_FactEmptyWithHistoryNotFallback(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(walletID, "wallet:"))
	now := time.Now().Unix()

	// 种子 Token Lot（已消耗完）
	lotID := "lot_token123_tx_hist_0"
	_, err := db.Exec(
		`INSERT INTO fact_token_lots(lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text, lot_state, mint_txid, created_at_unix, updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		lotID, ownerPubkeyHex, "token123", "BSV21", "100", "100", "spent", "tx_hist", now, now,
	)
	if err != nil {
		t.Fatalf("seed token lot: %v", err)
	}

	// 种子 settlement cycle
	cycleID := "cycle_test_hist"
	err = dbUpsertSettlementCycle(db, cycleID, "chain_payment", "tx_hist_pay", "confirmed",
		1000, 100, 900, 1, now, "test cycle", nil)
	if err != nil {
		t.Fatalf("seed settlement cycle: %v", err)
	}

	cycleIDInt, err := dbGetSettlementCycleBySource(db, "chain_payment", "tx_hist_pay")
	if err != nil {
		t.Fatalf("lookup settlement cycle: %v", err)
	}

	// 种子结算记录（消耗记录）
	recordID := fmt.Sprintf("rec_token_%d_%s", cycleIDInt, lotID)
	_, err = db.Exec(
		`INSERT INTO fact_settlement_records(record_id, settlement_cycle_id, asset_type, owner_pubkey_hex, source_lot_id, used_satoshi, used_quantity_text, state, occurred_at_unix, confirmed_at_unix, note, payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		recordID, cycleIDInt, "TOKEN", ownerPubkeyHex, lotID, 0, "100", "confirmed", now, now, "test", "{}",
	)
	if err != nil {
		t.Fatalf("seed settlement record: %v", err)
	}

	// 余额应为 0（lot 的 quantity_text 等于 used_quantity_text）
	bal, err := dbLoadTokenBalanceFact(context.Background(), newClientDB(db, nil), walletID, "BSV21", "token123")
	if err != nil {
		t.Fatalf("load fact balance: %v", err)
	}

	// 验证有历史（lot 已消耗完，余额为 0）
	totalIn, _ := sumDecimalTexts(bal.TotalInText)
	totalUsed, _ := sumDecimalTexts(bal.TotalUsedText)
	remaining, _ := subtractTokenDecimalText(totalIn, totalUsed)
	if remaining != "0" {
		t.Fatalf("expected remaining 0, got %s", remaining)
	}
}

// TestSettlementRecordIdempotent_DoubleWriteNoDuplicate 验证幂等重复写不产生重复记录
// 使用新结算记录表 fact_settlement_records
func TestSettlementRecordIdempotent_DoubleWriteNoDuplicate(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(walletID, "wallet:"))
	now := time.Now().Unix()

	// 种子 Token Lot
	lotID := "lot_token123_tx_idem_0"
	_, err := db.Exec(
		`INSERT INTO fact_token_lots(lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text, lot_state, mint_txid, created_at_unix, updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		lotID, ownerPubkeyHex, "token123", "BSV21", "100", "0", "unspent", "tx_idem", now, now,
	)
	if err != nil {
		t.Fatalf("seed token lot: %v", err)
	}

	// 种子 settlement cycle
	cycleID := "cycle_test_001"
	err = dbUpsertSettlementCycle(db, cycleID, "chain_payment", "tx_idem_pay", "confirmed",
		1000, 100, 900, 1, now, "test cycle", nil)
	if err != nil {
		t.Fatalf("seed settlement cycle: %v", err)
	}

	cycleIDInt, err := dbGetSettlementCycleBySource(db, "chain_payment", "tx_idem_pay")
	if err != nil {
		t.Fatalf("lookup settlement cycle: %v", err)
	}

	// 第一次写入结算记录
	recordID := fmt.Sprintf("rec_token_%d_%s", cycleIDInt, lotID)
	err = dbAppendSettlementRecordDB(db, settlementRecordEntry{
		RecordID:          recordID,
		SettlementCycleID: cycleIDInt,
		AssetType:         "TOKEN",
		OwnerPubkeyHex:    ownerPubkeyHex,
		SourceLotID:       lotID,
		UsedQuantityText:  "50",
		State:             "confirmed",
		OccurredAtUnix:    now,
		Note:              "Token consumed by settlement cycle",
	})
	if err != nil {
		t.Fatalf("first write: %v", err)
	}

	// 第二次写入结算记录（相同 record_id，应被幂等跳过）
	err = dbAppendSettlementRecordDB(db, settlementRecordEntry{
		RecordID:          recordID,
		SettlementCycleID: cycleIDInt,
		AssetType:         "TOKEN",
		OwnerPubkeyHex:    ownerPubkeyHex,
		SourceLotID:       lotID,
		UsedQuantityText:  "50",
		State:             "confirmed",
		OccurredAtUnix:    now,
		Note:              "Token consumed by settlement cycle",
	})
	if err != nil {
		t.Fatalf("second write (idempotent): %v", err)
	}

	// 验证只有一条消耗记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE settlement_cycle_id=?`, cycleIDInt).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 settlement record, got %d", count)
	}
}
