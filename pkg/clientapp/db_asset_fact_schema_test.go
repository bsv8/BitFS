package clientapp

import (
	"testing"
)

// TestInitIndexDB_CreatesAssetFactSchema 验证资产事实层两张表创建成功
func TestInitIndexDB_CreatesAssetFactSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证两张表存在
	for _, table := range []string{"fact_chain_asset_flows", "fact_asset_consumptions"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	// 验证 fact_chain_asset_flows 核心字段
	flowCols, err := tableColumns(db, "fact_chain_asset_flows")
	if err != nil {
		t.Fatalf("inspect fact_chain_asset_flows columns failed: %v", err)
	}
	requiredFlowCols := []string{
		"id", "flow_id", "wallet_id", "address", "direction", "asset_kind",
		"token_id", "utxo_id", "txid", "vout", "amount_satoshi", "quantity_text",
		"occurred_at_unix", "updated_at_unix", "evidence_source", "note", "payload_json",
	}
	for _, col := range requiredFlowCols {
		if _, ok := flowCols[col]; !ok {
			t.Fatalf("fact_chain_asset_flows missing column %s", col)
		}
	}

	// 验证 fact_asset_consumptions 核心字段
	consCols, err := tableColumns(db, "fact_asset_consumptions")
	if err != nil {
		t.Fatalf("inspect fact_asset_consumptions columns failed: %v", err)
	}
	requiredConsCols := []string{
		"id", "source_flow_id", "chain_payment_id", "pool_allocation_id",
		"used_satoshi", "used_quantity_text", "occurred_at_unix", "note", "payload_json",
	}
	for _, col := range requiredConsCols {
		if _, ok := consCols[col]; !ok {
			t.Fatalf("fact_asset_consumptions missing column %s", col)
		}
	}
}

// TestInitIndexDB_CreatesAssetFactIndexes 验证索引全部创建
func TestInitIndexDB_CreatesAssetFactIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// fact_chain_asset_flows 索引
	flowIndexes := []string{
		"idx_fact_chain_asset_flows_wallet_asset",
		"idx_fact_chain_asset_flows_wallet_direction",
		"idx_fact_chain_asset_flows_txid_vout",
		"idx_fact_chain_asset_flows_occurred",
	}
	for _, idx := range flowIndexes {
		has, err := tableHasIndex(db, "fact_chain_asset_flows", idx)
		if err != nil {
			t.Fatalf("inspect index %s failed: %v", idx, err)
		}
		if !has {
			t.Fatalf("missing index %s on fact_chain_asset_flows", idx)
		}
	}

	// fact_asset_consumptions 索引
	consIndexes := []string{
		"idx_fact_asset_consumptions_source",
		"idx_fact_asset_consumptions_payment",
		"idx_fact_asset_consumptions_allocation",
		"uq_fact_asset_consumptions_flow_payment",
		"uq_fact_asset_consumptions_flow_allocation",
	}
	for _, idx := range consIndexes {
		has, err := tableHasIndex(db, "fact_asset_consumptions", idx)
		if err != nil {
			t.Fatalf("inspect index %s failed: %v", idx, err)
		}
		if !has {
			t.Fatalf("missing index %s on fact_asset_consumptions", idx)
		}
	}
}

// TestInitIndexDB_AssetFactConstraints 验证约束生效
func TestInitIndexDB_AssetFactConstraints(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 先插入一个 wallet_utxo 作为外键依赖
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_1", "wallet_1", "addr1", "tx1", 0, 1000, "confirmed",
		"plain_bsv", "", "tx1", "", 1700000000, 1700000000, 0,
	)
	if err != nil {
		t.Fatalf("insert test utxo failed: %v", err)
	}

	// 1. 验证 asset_kind='UNKNOWN' 插入失败
	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_1", "wallet_1", "addr1", "IN", "UNKNOWN", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatalf("asset_kind='UNKNOWN' insert should fail")
	}

	// 2. 验证 direction 非 IN/OUT 插入失败
	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_2", "wallet_1", "addr1", "INVALID", "BSV", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatalf("direction='INVALID' insert should fail")
	}

	// 3. 验证有效数据可以插入
	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_3", "wallet_1", "addr1", "IN", "BSV", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("valid flow insert failed: %v", err)
	}

	// 4. 验证 flow_id 唯一约束
	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_3", "wallet_1", "addr1", "OUT", "BSV20", "test_utxo_1", "tx2", 1,
		1000, 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatalf("duplicate flow_id insert should fail")
	}

	// 5. 验证 wallet_id + utxo_id + direction 唯一约束
	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_4", "wallet_1", "addr1", "IN", "BSV21", "test_utxo_1", "tx3", 2,
		1000, 1700000000, 1700000000,
	)
	if err == nil {
		t.Fatalf("duplicate (wallet_id, utxo_id, direction) insert should fail")
	}
}

// TestInitIndexDB_AssetConsumptionXorConstraint 验证二选一约束生效
func TestInitIndexDB_AssetConsumptionXorConstraint(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 准备依赖数据
	// 1. UTXO
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_1", "wallet_1", "addr1", "tx1", 0, 1000, "confirmed",
		"plain_bsv", "", "tx1", "", 1700000000, 1700000000, 0,
	)
	if err != nil {
		t.Fatalf("insert test utxo failed: %v", err)
	}

	// 2. fact_chain_asset_flows 记录
	res, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_1", "wallet_1", "addr1", "OUT", "BSV", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert flow failed: %v", err)
	}
	flowID, _ := res.LastInsertId()

	// 3. fact_chain_payments 记录
	res, err = db.Exec(`INSERT INTO fact_chain_payments(
		txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi,
		net_amount_satoshi, block_height, occurred_at_unix, from_party_id, to_party_id,
		payload_json, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"payment_tx_1", "transfer", "confirmed", 1000, 0, 1000, 100,
		1700000000, "from_1", "to_1", "{}", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert payment failed: %v", err)
	}
	paymentID, _ := res.LastInsertId()

	// 4. fact_pool_sessions 记录
	_, err = db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex,
		arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi,
		fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"pool_sess_1", "2of3", "", "", "", "", 1000, 100,
		1.0, 10, "", "active", 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert pool session failed: %v", err)
	}

	// 5. fact_pool_session_events 记录
	res, err = db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"alloc_1", "pool_sess_1", 1, "open", 1, 0, 1000, "tx_alloc", "hex", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert pool allocation failed: %v", err)
	}
	allocationID, _ := res.LastInsertId()

	// 测试：二选一约束 - 两个都为空应该失败
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID, nil, nil, 100, 1700000000,
	)
	if err == nil {
		t.Fatalf("consumption with both NULL should fail")
	}

	// 测试：二选一约束 - 两个都非空应该失败
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID, paymentID, allocationID, 100, 1700000000,
	)
	if err == nil {
		t.Fatalf("consumption with both non-NULL should fail")
	}

	// 测试：只有 chain_payment_id 应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID, paymentID, nil, 100, 1700000000,
	)
	if err != nil {
		t.Fatalf("consumption with only chain_payment_id failed: %v", err)
	}

	// 准备第二个 utxo 和 flow 用于 pool_allocation 测试
	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_2", "wallet_1", "addr1", "tx2", 0, 500, "confirmed",
		"plain_bsv", "", "tx2", "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("insert second test utxo failed: %v", err)
	}

	res, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_2", "wallet_1", "addr1", "OUT", "BSV20", "test_utxo_2", "tx2", 0,
		500, 1700000001, 1700000001,
	)
	if err != nil {
		t.Fatalf("insert second flow failed: %v", err)
	}
	flowID2, _ := res.LastInsertId()

	// 测试：只有 pool_allocation_id 应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID2, nil, allocationID, 100, 1700000000,
	)
	if err != nil {
		t.Fatalf("consumption with only pool_allocation_id failed: %v", err)
	}
}

// TestInitIndexDB_AssetFactForeignKeys 验证外键生效
func TestInitIndexDB_AssetFactForeignKeys(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 验证外键关系
	fks, err := tableForeignKeys(db, "fact_chain_asset_flows")
	if err != nil {
		t.Fatalf("inspect fact_chain_asset_flows foreign keys failed: %v", err)
	}
	wantFKs := map[string]bool{
		"utxo_id->wallet_utxo.utxo_id": true,
	}
	for _, fk := range fks {
		delete(wantFKs, fk)
	}
	if len(wantFKs) != 0 {
		t.Fatalf("missing foreign keys on fact_chain_asset_flows: %v", wantFKs)
	}

	// 验证 fact_asset_consumptions 外键
	consFKs, err := tableForeignKeys(db, "fact_asset_consumptions")
	if err != nil {
		t.Fatalf("inspect fact_asset_consumptions foreign keys failed: %v", err)
	}
	wantConsFKs := map[string]bool{
		"source_flow_id->fact_chain_asset_flows.id":    true,
		"chain_payment_id->fact_chain_payments.id":     true,
		"pool_allocation_id->fact_pool_session_events.id": true,
	}
	for _, fk := range consFKs {
		delete(wantConsFKs, fk)
	}
	if len(wantConsFKs) != 0 {
		t.Fatalf("missing foreign keys on fact_asset_consumptions: %v", wantConsFKs)
	}
}

// TestInitIndexDB_AssetFactIdempotent 验证幂等性
func TestInitIndexDB_AssetFactIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)

	// 第一次初始化
	if err := initIndexDB(db); err != nil {
		t.Fatalf("first initIndexDB failed: %v", err)
	}

	// 准备测试数据
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_1", "wallet_1", "addr1", "tx1", 0, 1000, "confirmed",
		"plain_bsv", "", "tx1", "", 1700000000, 1700000000, 0,
	)
	if err != nil {
		t.Fatalf("insert test utxo failed: %v", err)
	}

	_, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_idem", "wallet_1", "addr1", "IN", "BSV", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert test flow failed: %v", err)
	}

	// 记录第一次初始化后的数据量
	var flowCount1 int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM fact_chain_asset_flows`).Scan(&flowCount1); err != nil {
		t.Fatalf("count flows after first init failed: %v", err)
	}

	// 第二次初始化（幂等测试）
	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	// 验证数据没有被重复或丢失
	var flowCount2 int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM fact_chain_asset_flows`).Scan(&flowCount2); err != nil {
		t.Fatalf("count flows after second init failed: %v", err)
	}
	if flowCount1 != flowCount2 {
		t.Fatalf("flow count changed after repeated init: got=%d want=%d", flowCount2, flowCount1)
	}

	// 验证表结构仍然正确
	exists, err := hasTable(db, "fact_chain_asset_flows")
	if err != nil {
		t.Fatalf("check table existence failed: %v", err)
	}
	if !exists {
		t.Fatalf("fact_chain_asset_flows table missing after repeated init")
	}
}

// TestInitIndexDB_AssetConsumptionPartialUnique 验证部分唯一索引生效
// 这是关键测试：SQLite 的普通 UNIQUE 对 NULL 不可靠，必须用部分唯一索引
func TestInitIndexDB_AssetConsumptionPartialUnique(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 准备基础数据
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_1", "wallet_1", "addr1", "tx1", 0, 1000, "confirmed",
		"plain_bsv", "", "tx1", "", 1700000000, 1700000000, 0,
	)
	if err != nil {
		t.Fatalf("insert test utxo failed: %v", err)
	}

	// 准备两个 flow（不同 utxo 避免 UNIQUE(wallet_id, utxo_id, direction) 冲突）
	res, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_1", "wallet_1", "addr1", "OUT", "BSV", "test_utxo_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert flow_1 failed: %v", err)
	}
	flowID1, _ := res.LastInsertId()

	// 需要第二个 utxo 来创建第二个 OUT flow
	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"test_utxo_2", "wallet_1", "addr1", "tx2", 0, 1000, "confirmed",
		"plain_bsv", "", "tx2", "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("insert second test utxo failed: %v", err)
	}

	res, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_2", "wallet_1", "addr1", "OUT", "BSV", "test_utxo_2", "tx2", 0,
		1000, 1700000001, 1700000001,
	)
	if err != nil {
		t.Fatalf("insert flow_2 failed: %v", err)
	}
	flowID2, _ := res.LastInsertId()

	// 准备两个 payment
	res, err = db.Exec(`INSERT INTO fact_chain_payments(
		txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi,
		net_amount_satoshi, block_height, occurred_at_unix, from_party_id, to_party_id,
		payload_json, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"payment_tx_1", "transfer", "confirmed", 1000, 0, 1000, 100,
		1700000000, "from_1", "to_1", "{}", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert payment_1 failed: %v", err)
	}
	paymentID1, _ := res.LastInsertId()

	res, err = db.Exec(`INSERT INTO fact_chain_payments(
		txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi,
		net_amount_satoshi, block_height, occurred_at_unix, from_party_id, to_party_id,
		payload_json, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"payment_tx_2", "transfer", "confirmed", 1000, 0, 1000, 100,
		1700000000, "from_2", "to_2", "{}", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert payment_2 failed: %v", err)
	}
	paymentID2, _ := res.LastInsertId()

	// 准备 pool session 和 allocation
	_, err = db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex,
		arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi,
		fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"pool_sess_1", "2of3", "", "", "", "", 1000, 100,
		1.0, 10, "", "active", 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert pool session failed: %v", err)
	}

	res, err = db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"alloc_1", "pool_sess_1", 1, "open", 1, 0, 1000, "tx_alloc1", "hex1", 1700000000,
	)
	if err != nil {
		t.Fatalf("insert allocation_1 failed: %v", err)
	}
	var allocationID1 int64
	if err := db.QueryRow(`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`, "alloc_1").Scan(&allocationID1); err != nil {
		t.Fatalf("lookup allocation_1 id failed: %v", err)
	}

	res, err = db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"alloc_2", "pool_sess_1", 2, "pay", 2, 100, 900, "tx_alloc2", "hex2", 1700000001,
	)
	if err != nil {
		t.Fatalf("insert allocation_2 failed: %v", err)
	}
	var allocationID2 int64
	if err := db.QueryRow(`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`, "alloc_2").Scan(&allocationID2); err != nil {
		t.Fatalf("lookup allocation_2 id failed: %v", err)
	}

	// ========== 测试部分唯一索引：source_flow_id + chain_payment_id ==========

	// 第一次插入：flow_1 -> payment_1，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, paymentID1, nil, 100, 1700000000,
	)
	if err != nil {
		t.Fatalf("first insert flow_1->payment_1 failed: %v", err)
	}

	// 重复插入：flow_1 -> payment_1，应该失败（部分唯一索引）
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, paymentID1, nil, 200, 1700000001,
	)
	if err == nil {
		t.Fatalf("duplicate insert flow_1->payment_1 should fail")
	}

	// 不同 flow 同一 payment：flow_2 -> payment_1，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID2, paymentID1, nil, 150, 1700000002,
	)
	if err != nil {
		t.Fatalf("insert flow_2->payment_1 failed: %v", err)
	}

	// 同一 flow 不同 payment：flow_1 -> payment_2，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, paymentID2, nil, 300, 1700000003,
	)
	if err != nil {
		t.Fatalf("insert flow_1->payment_2 failed: %v", err)
	}

	// ========== 测试部分唯一索引：source_flow_id + pool_allocation_id ==========

	// flow_1 -> allocation_1，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, nil, allocationID1, 100, 1700000004,
	)
	if err != nil {
		t.Fatalf("first insert flow_1->allocation_1 failed: %v", err)
	}

	// 重复插入：flow_1 -> allocation_1，应该失败（部分唯一索引）
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, nil, allocationID1, 200, 1700000005,
	)
	if err == nil {
		t.Fatalf("duplicate insert flow_1->allocation_1 should fail")
	}

	// 不同 flow 同一 allocation：flow_2 -> allocation_1，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID2, nil, allocationID1, 150, 1700000006,
	)
	if err != nil {
		t.Fatalf("insert flow_2->allocation_1 failed: %v", err)
	}

	// 同一 flow 不同 allocation：flow_1 -> allocation_2，应该成功
	_, err = db.Exec(`INSERT INTO fact_asset_consumptions(
		source_flow_id, chain_payment_id, pool_allocation_id, used_satoshi, occurred_at_unix
	) VALUES(?,?,?,?,?)`,
		flowID1, nil, allocationID2, 300, 1700000007,
	)
	if err != nil {
		t.Fatalf("insert flow_1->allocation_2 failed: %v", err)
	}
}
