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
