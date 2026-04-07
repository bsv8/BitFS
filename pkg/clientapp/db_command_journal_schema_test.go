package clientapp

import "testing"

func TestInitIndexDB_CreatesCommandJournalTriggerKeyIndexOnFreshDB(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	cols, err := tableColumns(db, "proc_command_journal")
	if err != nil {
		t.Fatalf("inspect proc_command_journal columns failed: %v", err)
	}
	if _, ok := cols["trigger_key"]; !ok {
		t.Fatalf("proc_command_journal missing trigger_key")
	}

	hasIndex, err := tableHasIndex(db, "proc_command_journal", "idx_proc_command_journal_trigger_key")
	if err != nil {
		t.Fatalf("inspect trigger_key index failed: %v", err)
	}
	if !hasIndex {
		t.Fatalf("missing idx_proc_command_journal_trigger_key")
	}

	unique, err := tableHasUniqueIndexOnColumns(db, "proc_command_journal", []string{"command_id"})
	if err != nil {
		t.Fatalf("inspect proc_command_journal unique failed: %v", err)
	}
	if !unique {
		t.Fatalf("proc_command_journal missing unique constraint on command_id")
	}
}

func TestProcCommandJournalRejectsDuplicateCommandIDOnFreshSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO proc_command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000001, "dup_cmd", "fee_pool.open", "gw1", "agg-1", "tester", 1700000001, 1, "applied", "", "", "before", "after", 10, "", "{}", "{}",
	); err != nil {
		t.Fatalf("insert first proc_command_journal row failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO proc_command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000002, "dup_cmd", "fee_pool.open", "gw1", "agg-2", "tester", 1700000002, 1, "applied", "", "", "before", "after", 10, "", "{}", "{}",
	); err == nil {
		t.Fatalf("expected duplicate command_id to fail on insert")
	}
}
