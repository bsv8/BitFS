package clientapp

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func newLegacyCommandFactMigrationTestDB(t *testing.T) *sql.DB {
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

func createLegacyCommandFactTablesWithoutSourceColumns(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE command_journal(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			requested_at_unix INTEGER NOT NULL,
			accepted INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL,
			error_message TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_key TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE state_snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			state TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy table failed: %v", err)
		}
	}
}

func createLegacyCommandFactTablesWithSourceColumns(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE command_journal(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			requested_at_unix INTEGER NOT NULL,
			accepted INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL,
			error_message TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_key TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			source_kind TEXT NOT NULL DEFAULT '',
			source_ref TEXT NOT NULL DEFAULT '',
			observed_at_unix INTEGER NOT NULL DEFAULT 0,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE state_snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			source_kind TEXT NOT NULL DEFAULT '',
			source_ref TEXT NOT NULL DEFAULT '',
			observed_at_unix INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy table failed: %v", err)
		}
	}
}

func TestCommandFactSourceMigrationAddsMissingColumnsAndBackfillsValues(t *testing.T) {
	t.Parallel()

	db := newLegacyCommandFactMigrationTestDB(t)
	createLegacyCommandFactTablesWithoutSourceColumns(t, db)
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000200, "cmd_domain_legacy", "feepool.open", "gw1", "agg-domain", "tester", 1700000200, 1, "applied", "", "", "idle", "open", 1, "", `{"x":0}`, `{"ok":true}`,
	); err != nil {
		t.Fatalf("insert legacy command journal failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000200, "cmd_state_legacy", "feepool.open", "gw1", "agg-state", "tester", 1700000200, 1, "applied", "", "", "idle", "open", 1, "", `{"x":0}`, `{"ok":true}`,
	); err != nil {
		t.Fatalf("insert legacy command journal failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?)`,
		1700000201, "cmd_domain_legacy", "gw1", "fee_pool_paused_insufficient", "idle", "paused_insufficient", `{"x":1}`,
	); err != nil {
		t.Fatalf("insert legacy domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		1700000202, "cmd_state_legacy", "gw1", "paused_insufficient", "wallet_insufficient", 100000, 12345, "not enough balance", `{"x":2}`,
	); err != nil {
		t.Fatalf("insert legacy state snapshot failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{"domain_events", "state_snapshots"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		for _, col := range []string{"source_kind", "source_ref", "observed_at_unix"} {
			if _, ok := cols[col]; !ok {
				t.Fatalf("%s missing %s", table, col)
			}
		}
	}

	var sourceKind, sourceRef string
	var observedAt int64
	if err := db.QueryRow(`SELECT source_kind,source_ref,observed_at_unix FROM domain_events WHERE command_id=?`, "cmd_domain_legacy").Scan(&sourceKind, &sourceRef, &observedAt); err != nil {
		t.Fatalf("load domain event backfill failed: %v", err)
	}
	if sourceKind != "command" || sourceRef != "cmd_domain_legacy" || observedAt != 1700000201 {
		t.Fatalf("unexpected domain event backfill: kind=%s ref=%s observed=%d", sourceKind, sourceRef, observedAt)
	}
	if err := db.QueryRow(`SELECT source_kind,source_ref,observed_at_unix FROM state_snapshots WHERE command_id=?`, "cmd_state_legacy").Scan(&sourceKind, &sourceRef, &observedAt); err != nil {
		t.Fatalf("load state snapshot backfill failed: %v", err)
	}
	if sourceKind != "command" || sourceRef != "cmd_state_legacy" || observedAt != 1700000202 {
		t.Fatalf("unexpected state snapshot backfill: kind=%s ref=%s observed=%d", sourceKind, sourceRef, observedAt)
	}

	cmdEvents, err := dbListDomainEvents(nil, newClientDB(db, nil), domainEventFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list domain events failed: %v", err)
	}
	if cmdEvents.Total != 1 || len(cmdEvents.Items) != 1 || cmdEvents.Items[0].CommandID != "cmd_domain_legacy" {
		t.Fatalf("unexpected domain event list: %+v", cmdEvents)
	}
	cmdStates, err := dbListStateSnapshots(nil, newClientDB(db, nil), stateSnapshotFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list state snapshots failed: %v", err)
	}
	if cmdStates.Total != 1 || len(cmdStates.Items) != 1 || cmdStates.Items[0].CommandID != "cmd_state_legacy" {
		t.Fatalf("unexpected state snapshot list: %+v", cmdStates)
	}
}

func TestCommandFactSourceMigrationPreservesExistingValues(t *testing.T) {
	t.Parallel()

	db := newLegacyCommandFactMigrationTestDB(t)
	createLegacyCommandFactTablesWithSourceColumns(t, db)
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000300, "cmd_domain_keep", "feepool.open", "gw1", "agg-domain", "tester", 1700000300, 1, "applied", "", "", "idle", "open", 1, "", `{"x":0}`, `{"ok":true}`,
	); err != nil {
		t.Fatalf("insert legacy command journal failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000300, "cmd_state_keep", "feepool.open", "gw1", "agg-state", "tester", 1700000300, 1, "applied", "", "", "idle", "open", 1, "", `{"x":0}`, `{"ok":true}`,
	); err != nil {
		t.Fatalf("insert legacy command journal failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		1700000301, "cmd_domain_keep", "gw1", "", "manual_ref", 0, "fee_pool_paused_insufficient", "idle", "paused_insufficient", `{"x":1}`,
	); err != nil {
		t.Fatalf("insert domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000302, "cmd_state_keep", "gw1", "command", "keep_ref", 0, "paused_insufficient", "wallet_insufficient", 100000, 12345, "not enough balance", `{"x":2}`,
	); err != nil {
		t.Fatalf("insert state snapshot failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	var sourceKind, sourceRef string
	var observedAt int64
	if err := db.QueryRow(`SELECT source_kind,source_ref,observed_at_unix FROM domain_events WHERE command_id=?`, "cmd_domain_keep").Scan(&sourceKind, &sourceRef, &observedAt); err != nil {
		t.Fatalf("load domain event failed: %v", err)
	}
	if sourceKind != "command" || sourceRef != "manual_ref" || observedAt != 1700000301 {
		t.Fatalf("domain event was overwritten: kind=%s ref=%s observed=%d", sourceKind, sourceRef, observedAt)
	}
	if err := db.QueryRow(`SELECT source_kind,source_ref,observed_at_unix FROM state_snapshots WHERE command_id=?`, "cmd_state_keep").Scan(&sourceKind, &sourceRef, &observedAt); err != nil {
		t.Fatalf("load state snapshot failed: %v", err)
	}
	if sourceKind != "command" || sourceRef != "keep_ref" || observedAt != 1700000302 {
		t.Fatalf("state snapshot was overwritten: kind=%s ref=%s observed=%d", sourceKind, sourceRef, observedAt)
	}

	if err := backfillCommandFactSourceColumns(db, "domain_events"); err != nil {
		t.Fatalf("second backfill domain_events failed: %v", err)
	}
	if err := backfillCommandFactSourceColumns(db, "state_snapshots"); err != nil {
		t.Fatalf("second backfill state_snapshots failed: %v", err)
	}
	if err := db.QueryRow(`SELECT source_kind,source_ref,observed_at_unix FROM domain_events WHERE command_id=?`, "cmd_domain_keep").Scan(&sourceKind, &sourceRef, &observedAt); err != nil {
		t.Fatalf("reload domain event failed: %v", err)
	}
	if sourceKind != "command" || sourceRef != "manual_ref" || observedAt != 1700000301 {
		t.Fatalf("domain event changed after second backfill: kind=%s ref=%s observed=%d", sourceKind, sourceRef, observedAt)
	}
}
