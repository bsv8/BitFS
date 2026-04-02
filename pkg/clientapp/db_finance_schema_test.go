package clientapp

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func openSchemaTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	return db
}

func createLegacyFinanceTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// 创建真实老库结构（第五轮迭代前），用于测试迁移逻辑
	// 注意：不包含新口径字段（source_type/source_id/accounting_scene/accounting_subtype）
	stmts := []string{
		`CREATE TABLE fin_business(
			business_id TEXT PRIMARY KEY,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE fin_process_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			process_id TEXT NOT NULL,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			event_type TEXT NOT NULL,
			status TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy table failed: %v", err)
		}
	}
}

func createLegacyDirectTransferPoolTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE direct_transfer_pools(
		session_id TEXT PRIMARY KEY,
		deal_id TEXT NOT NULL,
		buyer_pubkey_hex TEXT NOT NULL,
		seller_pubkey_hex TEXT NOT NULL,
		arbiter_pubkey_hex TEXT NOT NULL,
		pool_amount INTEGER NOT NULL,
		spend_tx_fee INTEGER NOT NULL,
		sequence_num INTEGER NOT NULL,
		seller_amount INTEGER NOT NULL,
		buyer_amount INTEGER NOT NULL,
		current_tx_hex TEXT NOT NULL,
		base_tx_hex TEXT NOT NULL,
		base_txid TEXT NOT NULL,
		status TEXT NOT NULL,
		fee_rate_sat_byte REAL NOT NULL,
		lock_blocks INTEGER NOT NULL,
		created_at_unix INTEGER NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create legacy direct_transfer_pools failed: %v", err)
	}
}

func TestInitIndexDB_MigratesFinanceColumns(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyFinanceTables(t, db)

	if _, err := db.Exec(`INSERT INTO fin_business(
		business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_legacy_1", "fee_pool", "open", "client:self", "pool:peer", "sess_1", "posted", 1700000001, "idem_biz_1", "旧业务", "{}",
	); err != nil {
		t.Fatalf("insert legacy fin_business failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fin_process_events(
		process_id,scene_type,scene_subtype,event_type,status,ref_id,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"proc_legacy_1", "fee_pool", "cycle_pay", "update", "applied", "sess_1", 1700000002, "idem_evt_1", "旧事件", "{}",
	); err != nil {
		t.Fatalf("insert legacy fin_process_events failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantCols := []string{"source_type", "source_id", "accounting_scene", "accounting_subtype"}
	for _, table := range []string{"fin_business", "fin_process_events"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		for _, col := range wantCols {
			if _, ok := cols[col]; !ok {
				t.Fatalf("missing column %s on %s", col, table)
			}
		}
	}

	var businessSourceType, businessSourceID, businessAccountingScene, businessAccountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM fin_business WHERE business_id=?`,
		"biz_legacy_1",
	).Scan(&businessSourceType, &businessSourceID, &businessAccountingScene, &businessAccountingSubtype); err != nil {
		t.Fatalf("query migrated fin_business failed: %v", err)
	}
	if businessSourceType != "" || businessSourceID != "" || businessAccountingScene != "" || businessAccountingSubtype != "" {
		t.Fatalf("unexpected migrated fin_business defaults: %q %q %q %q", businessSourceType, businessSourceID, businessAccountingScene, businessAccountingSubtype)
	}

	var eventSourceType, eventSourceID, eventAccountingScene, eventAccountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM fin_process_events WHERE process_id=?`,
		"proc_legacy_1",
	).Scan(&eventSourceType, &eventSourceID, &eventAccountingScene, &eventAccountingSubtype); err != nil {
		t.Fatalf("query migrated fin_process_events failed: %v", err)
	}
	if eventSourceType != "" || eventSourceID != "" || eventAccountingScene != "" || eventAccountingSubtype != "" {
		t.Fatalf("unexpected migrated fin_process_events defaults: %q %q %q %q", eventSourceType, eventSourceID, eventAccountingScene, eventAccountingSubtype)
	}
}

func TestInitIndexDB_FreshSchemaKeepsFinanceColumns(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantCols := []string{"source_type", "source_id", "accounting_scene", "accounting_subtype"}
	for _, table := range []string{"fin_business", "fin_process_events"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		for _, col := range wantCols {
			if _, ok := cols[col]; !ok {
				t.Fatalf("missing column %s on %s", col, table)
			}
		}
	}

	// 第六次迭代：只测试主口径字段
	if _, err := db.Exec(`INSERT INTO fin_business(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_fresh_1", "pool_allocation", "alloc_1", "c2c_transfer", "open", "client:self", "pool:peer", "posted", 1700000003, "idem_biz_2", "新业务", "{}",
	); err != nil {
		t.Fatalf("insert fresh fin_business failed: %v", err)
	}

	var sourceType, sourceID, accountingScene, accountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM fin_business WHERE business_id=?`,
		"biz_fresh_1",
	).Scan(&sourceType, &sourceID, &accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query fresh fin_business failed: %v", err)
	}
	if sourceType != "pool_allocation" || sourceID != "alloc_1" || accountingScene != "c2c_transfer" || accountingSubtype != "open" {
		t.Fatalf("unexpected fresh fin_business values: %q %q %q %q", sourceType, sourceID, accountingScene, accountingSubtype)
	}
}

func TestInitIndexDB_FinalizesFinTxBreakdown(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	hasOldTable, err := hasTable(db, "fin_business_txs")
	if err != nil {
		t.Fatalf("check fin_business_txs failed: %v", err)
	}
	if hasOldTable {
		t.Fatalf("fin_business_txs should be dropped after finalization")
	}

	cols, err := tableColumns(db, "fin_tx_breakdown")
	if err != nil {
		t.Fatalf("inspect fin_tx_breakdown columns failed: %v", err)
	}
	if _, ok := cols["tx_role"]; !ok {
		t.Fatalf("fin_tx_breakdown missing tx_role")
	}
	if notNull, err := tableColumnNotNull(db, "fin_tx_breakdown", "tx_role"); err != nil {
		t.Fatalf("inspect fin_tx_breakdown tx_role notnull failed: %v", err)
	} else if !notNull {
		t.Fatalf("fin_tx_breakdown.tx_role should be NOT NULL after finalization")
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "fin_tx_breakdown", []string{"business_id", "txid"}); err != nil {
		t.Fatalf("inspect fin_tx_breakdown unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("fin_tx_breakdown should keep unique constraint on (business_id, txid)")
	}

	for _, indexName := range []string{
		"idx_fin_tx_breakdown_business",
		"idx_fin_tx_breakdown_txid",
		"idx_fin_tx_breakdown_business_txid",
	} {
		hasIndex, err := tableHasIndex(db, "fin_tx_breakdown", indexName)
		if err != nil {
			t.Fatalf("inspect %s failed: %v", indexName, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on fin_tx_breakdown", indexName)
		}
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}
}

func TestInitIndexDB_CreatesFinanceReadIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 第六次迭代：只检查主口径索引
	wantIndexes := map[string][]string{
		"fin_business": {
			"idx_fin_business_source",
			"idx_fin_business_accounting",
		},
		"fin_process_events": {
			"idx_fin_process_events_source",
			"idx_fin_process_events_accounting",
		},
	}
	for table, wants := range wantIndexes {
		got, err := tableIndexNamesForTable(db, table)
		if err != nil {
			t.Fatalf("inspect %s indexes failed: %v", table, err)
		}
		for _, want := range wants {
			if !containsString(got, want) {
				t.Fatalf("missing index %s on %s", want, table)
			}
		}
	}
}

func tableIndexNamesForTable(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA index_list(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 8)
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
