package clientapp

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func legacyTableName(parts ...string) string {
	return strings.Join(parts, "_")
}

func seedAssetFactSettlementPaymentAttempt(t *testing.T, db *sql.DB, sourceType string, sourceID string, occurredAt int64) int64 {
	t.Helper()
	paymentAttemptID := fmt.Sprintf("cycle_%s_%s", sourceType, sourceID)
	switch sourceType {
	case "chain_quote_pay", "pool_session_quote_pay", "chain_direct_pay", "chain_asset_create":
	default:
		t.Fatalf("unsupported source type %s", sourceType)
	}
	res, err := db.Exec(`INSERT INTO fact_settlement_payment_attempts(
		payment_attempt_id,source_type,source_id,state,
		gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,cycle_index,
		occurred_at_unix,confirmed_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		paymentAttemptID, sourceType, sourceID, "confirmed",
		1000, 0, 1000, 0, occurredAt, occurredAt, "test cycle", "{}",
	)
	if err != nil {
		t.Fatalf("seed settlement payment attempt failed: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("seed settlement payment attempt id failed: %v", err)
	}
	return id
}

func assertTableMissing(t *testing.T, db *sql.DB, parts ...string) {
	t.Helper()
	name := legacyTableName(parts...)
	exists, err := hasTable(db, name)
	if err != nil {
		t.Fatalf("check table %s failed: %v", name, err)
	}
	if exists {
		t.Fatalf("table %s should not exist", name)
	}
}

// TestInitIndexDB_CreatesCurrentAssetFactSchema 验证新的资产事实表结构
// 新五表：fact_bsv_utxos, fact_token_lots, fact_token_carrier_links, fact_settlement_records, fact_settlement_payment_attempts
func TestInitIndexDB_CreatesCurrentAssetFactSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证新五表存在
	for _, table := range []string{
		"fact_bsv_utxos",
		"fact_token_lots",
		"fact_token_carrier_links",
		"fact_settlement_records",
		"fact_settlement_payment_attempts",
	} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	// 验证旧表不存在
	for _, table := range []string{
		"fact_chain_asset_flows",
		"fact_bsv_consumptions",
		"fact_token_consumptions",
		"fact_token_utxo_links",
	} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("check legacy table %s failed: %v", table, err)
		}
		if exists {
			t.Fatalf("legacy table %s should not exist", table)
		}
	}

	// 验证 fact_bsv_utxos 列结构
	bsvUtxoCols, err := tableColumns(db, "fact_bsv_utxos")
	if err != nil {
		t.Fatalf("inspect fact_bsv_utxos columns failed: %v", err)
	}
	for _, col := range []string{
		"utxo_id", "owner_pubkey_hex", "address", "txid", "vout",
		"value_satoshi", "utxo_state", "carrier_type", "spent_by_txid",
		"created_at_unix", "updated_at_unix", "spent_at_unix", "note", "payload_json",
	} {
		if _, ok := bsvUtxoCols[col]; !ok {
			t.Fatalf("fact_bsv_utxos missing column %s", col)
		}
	}

	// 验证 fact_token_lots 列结构
	tokenLotCols, err := tableColumns(db, "fact_token_lots")
	if err != nil {
		t.Fatalf("inspect fact_token_lots columns failed: %v", err)
	}
	for _, col := range []string{
		"lot_id", "owner_pubkey_hex", "token_id", "token_standard",
		"quantity_text", "used_quantity_text", "lot_state",
		"mint_txid", "last_spend_txid", "created_at_unix", "updated_at_unix",
		"note", "payload_json",
	} {
		if _, ok := tokenLotCols[col]; !ok {
			t.Fatalf("fact_token_lots missing column %s", col)
		}
	}

	// 验证 fact_token_carrier_links 列结构
	carrierLinkCols, err := tableColumns(db, "fact_token_carrier_links")
	if err != nil {
		t.Fatalf("inspect fact_token_carrier_links columns failed: %v", err)
	}
	for _, col := range []string{
		"link_id", "lot_id", "carrier_utxo_id", "owner_pubkey_hex",
		"link_state", "bind_txid", "unbind_txid",
		"created_at_unix", "updated_at_unix", "note", "payload_json",
	} {
		if _, ok := carrierLinkCols[col]; !ok {
			t.Fatalf("fact_token_carrier_links missing column %s", col)
		}
	}

	// 验证 fact_settlement_records 列结构
	settlementRecordCols, err := tableColumns(db, "fact_settlement_records")
	if err != nil {
		t.Fatalf("inspect fact_settlement_records columns failed: %v", err)
	}
	for _, col := range []string{
		"record_id", "settlement_payment_attempt_id", "asset_type", "owner_pubkey_hex",
		"source_utxo_id", "source_lot_id", "used_satoshi", "used_quantity_text",
		"state", "occurred_at_unix", "confirmed_at_unix", "note", "payload_json",
	} {
		if _, ok := settlementRecordCols[col]; !ok {
			t.Fatalf("fact_settlement_records missing column %s", col)
		}
	}

	// 验证 fact_settlement_payment_attempts 列结构
	paymentAttemptCols, err := tableColumns(db, "fact_settlement_payment_attempts")
	if err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts columns failed: %v", err)
	}
	for _, col := range []string{
		"id", "payment_attempt_id", "source_type", "source_id", "state",
		"gross_amount_satoshi", "gate_fee_satoshi", "net_amount_satoshi", "cycle_index",
		"occurred_at_unix", "confirmed_at_unix", "note", "payload_json",
	} {
		if _, ok := paymentAttemptCols[col]; !ok {
			t.Fatalf("fact_settlement_payment_attempts missing column %s", col)
		}
	}
}

// TestInitIndexDB_AssetFactIndexesAndConstraints 验证新资产事实表的索引和约束
func TestInitIndexDB_AssetFactIndexesAndConstraints(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证 fact_bsv_utxos 唯一约束 (txid, vout)
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_bsv_utxos", []string{"txid", "vout"}); err != nil {
		t.Fatalf("inspect fact_bsv_utxos unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_bsv_utxos should have unique constraint on (txid, vout)")
	}

	// 验证 fact_token_carrier_links 唯一约束 (lot_id, link_state) 和 (carrier_utxo_id, link_state)
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_token_carrier_links", []string{"lot_id", "link_state"}); err != nil {
		t.Fatalf("inspect fact_token_carrier_links unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_token_carrier_links should have unique constraint on (lot_id, link_state)")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_token_carrier_links", []string{"carrier_utxo_id", "link_state"}); err != nil {
		t.Fatalf("inspect fact_token_carrier_links unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_token_carrier_links should have unique constraint on (carrier_utxo_id, link_state)")
	}

	// 验证 fact_settlement_records 唯一约束 (settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id)
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_records", []string{"settlement_payment_attempt_id", "asset_type", "source_utxo_id", "source_lot_id"}); err != nil {
		t.Fatalf("inspect fact_settlement_records unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_records should have unique constraint on (settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id)")
	}

	// 验证 fact_settlement_payment_attempts 唯一约束 (source_type, source_id)
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_payment_attempts", []string{"source_type", "source_id"}); err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_payment_attempts should have unique constraint on (source_type, source_id)")
	}

	// 验证 fact_settlement_records 外键约束
	if hasFK, err := tableHasForeignKey(db, "fact_settlement_records", "settlement_payment_attempt_id", "fact_settlement_payment_attempts", "id"); err != nil {
		t.Fatalf("inspect fact_settlement_records foreign key failed: %v", err)
	} else if !hasFK {
		t.Fatal("fact_settlement_records should have foreign key on settlement_payment_attempt_id referencing fact_settlement_payment_attempts.id")
	}

	// 验证 NOT NULL 约束
	if notNull, err := tableColumnNotNull(db, "fact_bsv_utxos", "owner_pubkey_hex"); err != nil {
		t.Fatalf("inspect fact_bsv_utxos owner_pubkey_hex failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_bsv_utxos.owner_pubkey_hex should be NOT NULL")
	}

	if notNull, err := tableColumnNotNull(db, "fact_token_lots", "token_standard"); err != nil {
		t.Fatalf("inspect fact_token_lots token_standard failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_token_lots.token_standard should be NOT NULL")
	}

	if notNull, err := tableColumnNotNull(db, "fact_settlement_payment_attempts", "source_type"); err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts source_type failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_settlement_payment_attempts.source_type should be NOT NULL")
	}
}

// TestInitIndexDB_AssetFactWriteOperations 验证新资产事实表的写入操作
func TestInitIndexDB_AssetFactWriteOperations(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 插入 fact_bsv_utxos 测试数据
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
		utxo_state, carrier_type, spent_by_txid, created_at_unix, updated_at_unix, note, payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"utxo_bsv_1", "pubkey_hex_1", "addr1", "tx_bsv_1", 0, 1000,
		"unspent", "plain_bsv", "", 1700000000, 1700000000, "test bsv utxo", "{}",
	)
	if err != nil {
		t.Fatalf("insert fact_bsv_utxos failed: %v", err)
	}

	// 验证插入成功
	var bsvUtxoCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE utxo_id=?`, "utxo_bsv_1").Scan(&bsvUtxoCount); err != nil {
		t.Fatalf("count fact_bsv_utxos failed: %v", err)
	}
	if bsvUtxoCount != 1 {
		t.Fatalf("expected 1 fact_bsv_utxos, got %d", bsvUtxoCount)
	}

	// 验证唯一约束：重复 (txid, vout) 应该失败
	_, err = db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
		utxo_state, carrier_type, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"utxo_bsv_2", "pubkey_hex_2", "addr2", "tx_bsv_1", 0, 2000,
		"unspent", "plain_bsv", 1700000001, 1700000001,
	)
	if err == nil {
		t.Fatal("expected duplicate (txid, vout) insert to fail")
	}

	// 插入 fact_token_lots 测试数据
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text,
		used_quantity_text, lot_state, mint_txid, created_at_unix, updated_at_unix, note
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"lot_token_1", "pubkey_hex_1", "token_1", "BSV20", "1000",
		"0", "unspent", "mint_tx_1", 1700000000, 1700000000, "test token lot",
	)
	if err != nil {
		t.Fatalf("insert fact_token_lots failed: %v", err)
	}

	var tokenLotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_token_lots WHERE lot_id=?`, "lot_token_1").Scan(&tokenLotCount); err != nil {
		t.Fatalf("count fact_token_lots failed: %v", err)
	}
	if tokenLotCount != 1 {
		t.Fatalf("expected 1 fact_token_lots, got %d", tokenLotCount)
	}

	// 插入 fact_token_carrier_links 测试数据
	_, err = db.Exec(`INSERT INTO fact_token_carrier_links(
		link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state,
		bind_txid, created_at_unix, updated_at_unix, note
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		"link_1", "lot_token_1", "utxo_bsv_1", "pubkey_hex_1", "active",
		"bind_tx_1", 1700000000, 1700000000, "test carrier link",
	)
	if err != nil {
		t.Fatalf("insert fact_token_carrier_links failed: %v", err)
	}

	var linkCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_token_carrier_links WHERE link_id=?`, "link_1").Scan(&linkCount); err != nil {
		t.Fatalf("count fact_token_carrier_links failed: %v", err)
	}
	if linkCount != 1 {
		t.Fatalf("expected 1 fact_token_carrier_links, got %d", linkCount)
	}

	// 创建结算周期
	paymentAttemptID := seedAssetFactSettlementPaymentAttempt(t, db, "chain_quote_pay", "payment_1", 1700000000)

	// 插入 fact_settlement_records 测试数据
	_, err = db.Exec(`INSERT INTO fact_settlement_records(
		record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
		source_utxo_id, used_satoshi, state, occurred_at_unix, note
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		"record_bsv_1", paymentAttemptID, "BSV", "pubkey_hex_1",
		"utxo_bsv_1", 1000, "confirmed", 1700000000, "test settlement record",
	)
	if err != nil {
		t.Fatalf("insert fact_settlement_records failed: %v", err)
	}

	var recordCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE record_id=?`, "record_bsv_1").Scan(&recordCount); err != nil {
		t.Fatalf("count fact_settlement_records failed: %v", err)
	}
	if recordCount != 1 {
		t.Fatalf("expected 1 fact_settlement_records, got %d", recordCount)
	}

	// 验证外键约束：非法 settlement_payment_attempt_id 应该失败
	_, err = db.Exec(`INSERT INTO fact_settlement_records(
		record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
		source_utxo_id, used_satoshi, state, occurred_at_unix
	) VALUES(?,?,?,?,?,?,?,?)`,
		"record_invalid", 99999, "BSV", "pubkey_hex_1",
		"utxo_bsv_1", 1000, "confirmed", 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid settlement_payment_attempt_id insert to fail due to foreign key constraint")
	}
}

// TestInitIndexDB_AssetFactCheckConstraints 验证 CHECK 约束
func TestInitIndexDB_AssetFactCheckConstraints(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 测试 fact_bsv_utxos.utxo_state CHECK 约束
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
		utxo_state, carrier_type, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"utxo_check_1", "pubkey_hex_1", "addr1", "tx_check_1", 0, 1000,
		"invalid_state", "plain_bsv", 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid utxo_state to fail CHECK constraint")
	}

	// 测试 fact_bsv_utxos.carrier_type CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
		utxo_state, carrier_type, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"utxo_check_2", "pubkey_hex_1", "addr1", "tx_check_2", 0, 1000,
		"unspent", "invalid_carrier", 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid carrier_type to fail CHECK constraint")
	}

	// 测试 fact_token_lots.token_standard CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text,
		lot_state, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?)`,
		"lot_check_1", "pubkey_hex_1", "token_1", "INVALID_STD", "1000",
		"unspent", 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid token_standard to fail CHECK constraint")
	}

	// 测试 fact_token_lots.lot_state CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text,
		lot_state, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?)`,
		"lot_check_2", "pubkey_hex_1", "token_1", "BSV20", "1000",
		"invalid_state", 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid lot_state to fail CHECK constraint")
	}

	// 测试 fact_token_carrier_links.link_state CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_token_carrier_links(
		link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state,
		created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?)`,
		"link_check_1", "lot_check_1", "utxo_check_1", "pubkey_hex_1", "invalid_state",
		1700000000, 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid link_state to fail CHECK constraint")
	}

	// 测试 fact_settlement_records.asset_type CHECK 约束
	paymentAttemptID := seedAssetFactSettlementPaymentAttempt(t, db, "chain_quote_pay", "payment_check_1", 1700000000)
	_, err = db.Exec(`INSERT INTO fact_settlement_records(
		record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
		state, occurred_at_unix
	) VALUES(?,?,?,?,?,?)`,
		"record_check_1", paymentAttemptID, "INVALID_ASSET", "pubkey_hex_1",
		"confirmed", 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid asset_type to fail CHECK constraint")
	}

	// 测试 fact_settlement_records.state CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_settlement_records(
		record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
		state, occurred_at_unix
	) VALUES(?,?,?,?,?,?)`,
		"record_check_2", paymentAttemptID, "BSV", "pubkey_hex_1",
		"invalid_state", 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid state to fail CHECK constraint")
	}

	// 测试 fact_settlement_payment_attempts.source_type CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_settlement_payment_attempts(
		payment_attempt_id, source_type, source_id, state, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		"cycle_check_1", "invalid_source", "source_1", "confirmed", 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid source_type to fail CHECK constraint")
	}

	// 测试 fact_settlement_payment_attempts.state CHECK 约束
	_, err = db.Exec(`INSERT INTO fact_settlement_payment_attempts(
		payment_attempt_id, source_type, source_id, state, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		"cycle_check_2", "chain_quote_pay", "source_2", "invalid_state", 1700000000,
	)
	if err == nil {
		t.Fatal("expected invalid state to fail CHECK constraint")
	}
}

// TestInitIndexDB_AssetFactUpdateOperations 验证更新操作
func TestInitIndexDB_AssetFactUpdateOperations(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 插入测试数据
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
		utxo_state, carrier_type, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"utxo_update_1", "pubkey_hex_1", "addr1", "tx_update_1", 0, 1000,
		"unspent", "plain_bsv", 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert test data failed: %v", err)
	}

	// 更新 utxo_state 为 spent
	_, err = db.Exec(`UPDATE fact_bsv_utxos SET 
		utxo_state=?, spent_by_txid=?, spent_at_unix=?, updated_at_unix=? 
		WHERE utxo_id=?`,
		"spent", "spend_tx_1", 1700000100, 1700000100, "utxo_update_1",
	)
	if err != nil {
		t.Fatalf("update fact_bsv_utxos failed: %v", err)
	}

	// 验证更新
	var utxoState, spentByTxid string
	var spentAtUnix int64
	if err := db.QueryRow(`SELECT utxo_state, spent_by_txid, spent_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, "utxo_update_1").
		Scan(&utxoState, &spentByTxid, &spentAtUnix); err != nil {
		t.Fatalf("query updated fact_bsv_utxos failed: %v", err)
	}
	if utxoState != "spent" || spentByTxid != "spend_tx_1" || spentAtUnix != 1700000100 {
		t.Fatalf("unexpected update result: state=%s, spent_by=%s, spent_at=%d", utxoState, spentByTxid, spentAtUnix)
	}

	// 插入 token lot 并更新 used_quantity_text
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text,
		used_quantity_text, lot_state, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		"lot_update_1", "pubkey_hex_1", "token_1", "BSV20", "1000",
		"0", "unspent", 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert token lot failed: %v", err)
	}

	_, err = db.Exec(`UPDATE fact_token_lots SET 
		used_quantity_text=?, lot_state=?, last_spend_txid=?, updated_at_unix=? 
		WHERE lot_id=?`,
		"500", "spent", "spend_tx_2", 1700000100, "lot_update_1",
	)
	if err != nil {
		t.Fatalf("update fact_token_lots failed: %v", err)
	}

	var usedQty, lotState string
	if err := db.QueryRow(`SELECT used_quantity_text, lot_state FROM fact_token_lots WHERE lot_id=?`, "lot_update_1").
		Scan(&usedQty, &lotState); err != nil {
		t.Fatalf("query updated fact_token_lots failed: %v", err)
	}
	if usedQty != "500" || lotState != "spent" {
		t.Fatalf("unexpected update result: used_quantity=%s, lot_state=%s", usedQty, lotState)
	}
}

func TestInitIndexDB_RejectsLegacyAssetSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	legacyName := legacyTableName("fact", "asset", "consumptions")
	if _, err := db.Exec(`CREATE TABLE ` + legacyName + `(
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`); err != nil {
		t.Fatalf("create legacy table failed: %v", err)
	}

	if err := initIndexDB(db); err == nil {
		t.Fatal("expected initIndexDB to reject legacy asset schema")
	} else if !strings.Contains(err.Error(), "rebuild DB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitIndexDB_RejectsOldAssetFlowTable(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	// 创建旧表 fact_chain_asset_flows
	if _, err := db.Exec(`CREATE TABLE fact_chain_asset_flows(
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`); err != nil {
		t.Fatalf("create legacy fact_chain_asset_flows table failed: %v", err)
	}

	if err := initIndexDB(db); err == nil {
		t.Fatal("expected initIndexDB to reject legacy fact_chain_asset_flows table")
	} else if !strings.Contains(err.Error(), "rebuild DB") {
		t.Fatalf("unexpected error: %v", err)
	}
}
