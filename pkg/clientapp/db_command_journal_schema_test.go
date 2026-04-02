package clientapp

import (
	"database/sql"
	"strings"
	"testing"
)

func createLegacyCommandJournalSchemaWithTriggerKeyOnly(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`CREATE TABLE command_journal(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_unix INTEGER NOT NULL,
		command_id TEXT NOT NULL,
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
	)`)
	if err != nil {
		t.Fatalf("create legacy command_journal failed: %v", err)
	}
}

func TestInitIndexDB_EnsuresCommandJournalTriggerKeyIndexWhenColumnExists(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyCommandJournalSchemaWithTriggerKeyOnly(t, db)

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	cols, err := tableColumns(db, "command_journal")
	if err != nil {
		t.Fatalf("inspect command_journal columns failed: %v", err)
	}
	if _, ok := cols["trigger_key"]; !ok {
		t.Fatalf("command_journal missing trigger_key after migration")
	}

	hasIndex, err := tableHasIndex(db, "command_journal", "idx_command_journal_trigger_key")
	if err != nil {
		t.Fatalf("inspect trigger_key index failed: %v", err)
	}
	if !hasIndex {
		t.Fatalf("missing idx_command_journal_trigger_key after migration")
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}
	hasIndex, err = tableHasIndex(db, "command_journal", "idx_command_journal_trigger_key")
	if err != nil {
		t.Fatalf("inspect trigger_key index after second init failed: %v", err)
	}
	if !hasIndex {
		t.Fatalf("missing idx_command_journal_trigger_key after second init")
	}

	unique, err := tableHasUniqueIndexOnColumns(db, "command_journal", []string{"command_id"})
	if err != nil {
		t.Fatalf("inspect command_journal unique failed: %v", err)
	}
	if !unique {
		t.Fatalf("command_journal missing unique constraint on command_id")
	}
}

func TestInitIndexDB_RejectsDuplicateCommandJournalCommandID(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyCommandJournalSchemaWithTriggerKeyOnly(t, db)
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000001, "dup_cmd", "fee_pool.open", "gw1", "agg-1", "tester", 1700000001, 1, "applied", "", "", "before", "after", 10, "", "{}", "{}",
	); err != nil {
		t.Fatalf("insert first command_journal row failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000002, "dup_cmd", "fee_pool.open", "gw1", "agg-2", "tester", 1700000002, 1, "applied", "", "", "before", "after", 10, "", "{}", "{}",
	); err != nil {
		t.Fatalf("insert second command_journal row failed: %v", err)
	}

	err := initIndexDB(db)
	if err == nil {
		t.Fatalf("expected initIndexDB to fail on duplicate command_id")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "duplicate command_id") {
		t.Fatalf("expected duplicate command_id error, got=%v", err)
	}
}
