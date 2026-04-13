package clientapp

import (
	"context"
	"testing"
)

func TestCommandLinkedTablesFinalSchemaHasIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	wantIndexByTable := map[string]string{
		"proc_domain_events":   "idx_proc_domain_events_cmd_id",
		"proc_state_snapshots": "idx_proc_state_snapshots_cmd_id",
		"proc_effect_logs":     "idx_proc_effect_logs_cmd_id",
	}
	for _, table := range []string{"proc_domain_events", "proc_state_snapshots", "proc_effect_logs"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		if _, ok := cols["command_id"]; !ok {
			t.Fatalf("%s missing command_id", table)
		}
		for _, col := range []string{"source_kind", "source_ref", "observed_at_unix"} {
			if _, ok := cols[col]; ok {
				t.Fatalf("%s should not keep %s", table, col)
			}
		}
		notNull, err := tableColumnNotNull(db, table, "command_id")
		if err != nil {
			t.Fatalf("inspect %s command_id notnull failed: %v", table, err)
		}
		if !notNull {
			t.Fatalf("%s.command_id should be NOT NULL", table)
		}
		hasCheck, err := tableHasCreateSQLContains(db, table, "CHECK(trim(command_id) <> '')")
		if err != nil {
			t.Fatalf("inspect %s check failed: %v", table, err)
		}
		if !hasCheck {
			t.Fatalf("%s missing CHECK(trim(command_id) <> '')", table)
		}
		hasIndex, err := tableHasIndex(db, table, wantIndexByTable[table])
		if err != nil {
			t.Fatalf("inspect %s index failed: %v", table, err)
		}
		if !hasIndex {
			t.Fatalf("%s missing %s", table, wantIndexByTable[table])
		}
	}
}

func TestCommandLinkedTablesRejectBlankWrites(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	db.SetMaxOpenConns(1)
	store := newClientDB(db, nil)
	if err := dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "cmd-linked-1",
		CommandType:   "feepool.open",
		GatewayPeerID: "gw1",
		AggregateID:   "agg-1",
		RequestedBy:   "tester",
		RequestedAt:   1700002001,
		Accepted:      true,
		Status:        "applied",
	}); err != nil {
		t.Fatalf("append proc_command_journal failed: %v", err)
	}

	testCases := []struct {
		table        string
		blankSQL     string
		requiredArgs []any
	}{
		{
			table: "proc_domain_events",
			blankSQL: `INSERT INTO proc_domain_events(
				created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
			) VALUES(?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700002002), "   ", "gw1", "fee_pool_opened", "idle", "open", `{"scene":"reject"}`},
		},
		{
			table: "proc_state_snapshots",
			blankSQL: `INSERT INTO proc_state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700002003), "   ", "gw1", "paused_insufficient", "wallet_insufficient", int64(100000), int64(12345), "not enough balance", `{"scene":"reject"}`},
		},
		{
			table: "proc_effect_logs",
			blankSQL: `INSERT INTO proc_effect_logs(
				created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			) VALUES(?,?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700002004), "   ", "gw1", "chain", "fee_pool_open", "paused", "blocked", `{"scene":"reject"}`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.table+"/blank", func(t *testing.T) {
			_, err := db.Exec(tc.blankSQL, tc.requiredArgs...)
			if err == nil {
				t.Fatalf("expected %s blank command_id write to fail", tc.table)
			}
		})
	}
}
