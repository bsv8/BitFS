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

	if _, err := db.Exec(`INSERT INTO fin_business(
		business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_fresh_1", "fee_pool", "open", "client:self", "pool:peer", "sess_2", "posted", 1700000003, "idem_biz_2", "新业务", "{}",
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
	if sourceType != "" || sourceID != "" || accountingScene != "" || accountingSubtype != "" {
		t.Fatalf("unexpected fresh fin_business defaults: %q %q %q %q", sourceType, sourceID, accountingScene, accountingSubtype)
	}
}

func TestInitIndexDB_MigratesPoolFactTables(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyDirectTransferPoolTable(t, db)

	if _, err := db.Exec(`INSERT INTO direct_transfer_pools(
		session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_legacy", "deal_legacy", "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		990, 10, 1, 0, 990, "ctx", "btx", "base_tx", "active", 0.5, 6, 1700000001, 1700000002,
	); err != nil {
		t.Fatalf("insert legacy direct_transfer_pools failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{"pool_sessions", "pool_allocations"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	var status, openBaseTxID, txHex string
	var poolAmount, spendTxFee, openCount, allocCount int64
	var createdAtUnix, updatedAtUnix int64
	if err := db.QueryRow(
		`SELECT status,open_base_txid,pool_amount_satoshi,spend_tx_fee_satoshi,created_at_unix,updated_at_unix
		   FROM pool_sessions WHERE pool_session_id=?`,
		"sess_legacy",
	).Scan(&status, &openBaseTxID, &poolAmount, &spendTxFee, &createdAtUnix, &updatedAtUnix); err != nil {
		t.Fatalf("query migrated pool session failed: %v", err)
	}
	if status != "active" || openBaseTxID != "base_tx" || poolAmount != 990 || spendTxFee != 10 || createdAtUnix != 1700000001 || updatedAtUnix != 1700000002 {
		t.Fatalf("unexpected migrated pool session: status=%s open_base_txid=%s pool_amount=%d spend_tx_fee=%d created_at_unix=%d updated_at_unix=%d", status, openBaseTxID, poolAmount, spendTxFee, createdAtUnix, updatedAtUnix)
	}

	var allocationKind string
	var sequenceNum, payeeAmountAfter, payerAmountAfter, allocationCreatedAtUnix int64
	if err := db.QueryRow(
		`SELECT allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
		   FROM pool_allocations WHERE pool_session_id=? ORDER BY allocation_no ASC LIMIT 1`,
		"sess_legacy",
	).Scan(&allocationKind, &sequenceNum, &payeeAmountAfter, &payerAmountAfter, &openBaseTxID, &txHex, &allocationCreatedAtUnix); err != nil {
		t.Fatalf("query migrated pool allocation failed: %v", err)
	}
	if allocationKind != "open" || sequenceNum != 1 || payeeAmountAfter != 0 || payerAmountAfter != 990 || openBaseTxID != "base_tx" || txHex != "btx" || allocationCreatedAtUnix != 1700000001 {
		t.Fatalf("unexpected migrated pool allocation: kind=%s seq=%d payee=%d payer=%d txid=%s tx_hex=%s created_at_unix=%d", allocationKind, sequenceNum, payeeAmountAfter, payerAmountAfter, openBaseTxID, txHex, allocationCreatedAtUnix)
	}

	if err := db.QueryRow(`SELECT COUNT(*) FROM pool_sessions WHERE pool_session_id=?`, "sess_legacy").Scan(&openCount); err != nil {
		t.Fatalf("count pool_sessions failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pool_allocations WHERE pool_session_id=?`, "sess_legacy").Scan(&allocCount); err != nil {
		t.Fatalf("count pool_allocations failed: %v", err)
	}
	if openCount != 1 || allocCount != 1 {
		t.Fatalf("unexpected migrated row counts: pool_sessions=%d pool_allocations=%d", openCount, allocCount)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB second run failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pool_sessions WHERE pool_session_id=?`, "sess_legacy").Scan(&openCount); err != nil {
		t.Fatalf("count pool_sessions after second run failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pool_allocations WHERE pool_session_id=?`, "sess_legacy").Scan(&allocCount); err != nil {
		t.Fatalf("count pool_allocations after second run failed: %v", err)
	}
	if openCount != 1 || allocCount != 1 {
		t.Fatalf("duplicate rows after second initIndexDB: pool_sessions=%d pool_allocations=%d", openCount, allocCount)
	}
}

func TestInitIndexDB_FreshSchemaKeepsPoolFactTables(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{"pool_sessions", "pool_allocations"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}
}

func TestInitIndexDB_CreatesFinanceReadIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantIndexes := map[string][]string{
		"fin_business": {
			"idx_fin_business_scene",
			"idx_fin_business_source",
			"idx_fin_business_accounting",
		},
		"fin_process_events": {
			"idx_fin_process_events_scene",
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
