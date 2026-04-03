package clientapp

import (
	"database/sql"
	"strings"
	"testing"
)

func TestCommandLinkedTablesSchemaHasCheckFKAndIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantIndexByTable := map[string]string{
		"domain_events":   "idx_domain_events_cmd_id",
		"state_snapshots": "idx_state_snapshots_cmd_id",
		"effect_logs":     "idx_effect_logs_cmd_id",
	}
	for _, table := range []string{"domain_events", "state_snapshots", "effect_logs"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		if _, ok := cols["command_id"]; !ok {
			t.Fatalf("%s missing command_id", table)
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
		if table != "effect_logs" {
			hasSourceKindCheck, err := tableHasCreateSQLContains(db, table, "CHECK(source_kind = 'command')")
			if err != nil {
				t.Fatalf("inspect %s source kind check failed: %v", table, err)
			}
			if !hasSourceKindCheck {
				t.Fatalf("%s missing CHECK(source_kind = 'command')", table)
			}
		}

		hasFK, err := tableHasForeignKey(db, table, "command_id", "command_journal", "command_id")
		if err != nil {
			t.Fatalf("inspect %s foreign key failed: %v", table, err)
		}
		if !hasFK {
			t.Fatalf("%s missing foreign key to command_journal.command_id", table)
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

func TestCommandLinkedTablesLegacyMigrationCleansDirtyRowsAndAddsConstraints(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyCommandJournalSchemaForGatewayTest(t, db)
	createLegacyCommandLinkedTablesForStage3Test(t, db)

	insertGatewayEventTestCommandJournal(t, db, "cmd_keep_domain", "gw1")
	insertGatewayEventTestCommandJournal(t, db, "cmd_keep_state", "gw1")
	insertGatewayEventTestCommandJournal(t, db, "cmd_keep_effect", "gw1")

	insertLegacyCommandLinkedDomainEventRow(t, db, "cmd_keep_domain", "domain_keep", "cmd_keep_domain")
	insertLegacyCommandLinkedDomainEventRowWithSourceKind(t, db, "cmd_keep_domain", "domain_observed", "observed_gateway_state", "cmd_keep_domain")
	insertLegacyCommandLinkedDomainEventRow(t, db, "", "domain_blank", "")
	insertLegacyCommandLinkedDomainEventRow(t, db, "", "domain_null", nil)
	insertLegacyCommandLinkedDomainEventRow(t, db, "cmd_missing_domain", "domain_orphan", "cmd_missing_domain")

	insertLegacyCommandLinkedStateSnapshotRow(t, db, "cmd_keep_state", "state_keep", "cmd_keep_state")
	insertLegacyCommandLinkedStateSnapshotRowWithSourceKind(t, db, "cmd_keep_state", "state_observed", "observed_gateway_state", "cmd_keep_state")
	insertLegacyCommandLinkedStateSnapshotRow(t, db, "", "state_blank", "")
	insertLegacyCommandLinkedStateSnapshotRow(t, db, "", "state_null", nil)
	insertLegacyCommandLinkedStateSnapshotRow(t, db, "cmd_missing_state", "state_orphan", "cmd_missing_state")

	insertLegacyCommandLinkedEffectLogRow(t, db, "cmd_keep_effect", "effect_keep", "cmd_keep_effect")
	insertLegacyCommandLinkedEffectLogRow(t, db, "", "effect_blank", "")
	insertLegacyCommandLinkedEffectLogRow(t, db, "", "effect_null", nil)
	insertLegacyCommandLinkedEffectLogRow(t, db, "cmd_missing_effect", "effect_orphan", "cmd_missing_effect")

	for _, table := range []string{"domain_events", "state_snapshots", "effect_logs"} {
		report, err := auditCommandLinkedTableRows(db, table)
		if err != nil {
			t.Fatalf("audit %s failed: %v", table, err)
		}
		if report.NullCommandIDRows != 1 || report.BlankCommandIDRows != 1 || report.OrphanCommandIDRows != 1 {
			t.Fatalf("unexpected audit for %s: %s", table, report.String())
		}
	}
	for _, table := range []string{"domain_events", "state_snapshots"} {
		report, err := auditCommandFactSourceKindRows(db, table)
		if err != nil {
			t.Fatalf("audit source kind %s failed: %v", table, err)
		}
		if report.NonCommandSourceKindRows != 1 {
			t.Fatalf("unexpected source kind audit for %s: %s", table, report.String())
		}
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	expectCommandLinkedTableClean(t, db, "domain_events", "cmd_keep_domain", "idx_domain_events_cmd_id")
	expectCommandLinkedTableClean(t, db, "state_snapshots", "cmd_keep_state", "idx_state_snapshots_cmd_id")
	expectCommandLinkedTableClean(t, db, "effect_logs", "cmd_keep_effect", "idx_effect_logs_cmd_id")
	expectCommandFactSourceKindClean(t, db, "domain_events")
	expectCommandFactSourceKindClean(t, db, "state_snapshots")
}

func TestCommandFactSourceKindRejectedAtDatabaseLayer(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if err := dbAppendCommandJournal(nil, newClientDB(db, nil), commandJournalEntry{
		CommandID:     "cmd-source-kind-reject",
		CommandType:   "feepool.open",
		GatewayPeerID: "gw1",
		AggregateID:   "agg-source-kind",
		RequestedBy:   "tester",
		RequestedAt:   1700002501,
		Accepted:      true,
		Status:        "applied",
	}); err != nil {
		t.Fatalf("append command_journal failed: %v", err)
	}

	for _, tc := range []struct {
		table string
		sql   string
		args  []any
	}{
		{
			table: "domain_events",
			sql: `INSERT INTO domain_events(
				created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			args: []any{int64(1700002502), "cmd-source-kind-reject", "gw1", "observed_gateway_state", "cmd-source-kind-reject", int64(1700002502), "fee_pool_opened", "idle", "open", `{"scene":"reject"}`},
		},
		{
			table: "state_snapshots",
			sql: `INSERT INTO state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			args: []any{int64(1700002503), "cmd-source-kind-reject", "gw1", "observed_gateway_state", "cmd-source-kind-reject", int64(1700002503), "paused_insufficient", "wallet_insufficient", int64(100000), int64(12345), "not enough balance", `{"scene":"reject"}`},
		},
	} {
		t.Run(tc.table, func(t *testing.T) {
			if _, err := db.Exec(tc.sql, tc.args...); err == nil {
				t.Fatalf("expected %s observed source_kind write to fail", tc.table)
			}
		})
	}
}

func TestCommandLinkedTablesRejectWhitespaceNullAndOrphanWritesAtDatabaseLayer(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	db.SetMaxOpenConns(1)
	if err := enableForeignKeys(db); err != nil {
		t.Fatalf("enable foreign keys failed: %v", err)
	}

	testCases := []struct {
		table        string
		blankSQL     string
		nullSQL      string
		orphanSQL    string
		requiredArgs []any
	}{
		{
			table: "domain_events",
			blankSQL: `INSERT INTO domain_events(
				created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
			) VALUES(?,?,?,?,?,?,?)`,
			nullSQL: `INSERT INTO domain_events(
				created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
			) VALUES(?,?,?,?,?,?,?)`,
			orphanSQL: `INSERT INTO domain_events(
				created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
			) VALUES(?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700001001), "   ", "gw1", "fee_pool_paused_insufficient", "idle", "paused_insufficient", `{"x":1}`},
		},
		{
			table: "state_snapshots",
			blankSQL: `INSERT INTO state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?)`,
			nullSQL: `INSERT INTO state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?)`,
			orphanSQL: `INSERT INTO state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700001002), "   ", "gw1", "paused_insufficient", "wallet_insufficient", int64(100000), int64(12345), "not enough balance", `{"x":2}`},
		},
		{
			table: "effect_logs",
			blankSQL: `INSERT INTO effect_logs(
				created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			) VALUES(?,?,?,?,?,?,?,?)`,
			nullSQL: `INSERT INTO effect_logs(
				created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			) VALUES(?,?,?,?,?,?,?,?)`,
			orphanSQL: `INSERT INTO effect_logs(
				created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			) VALUES(?,?,?,?,?,?,?,?)`,
			requiredArgs: []any{int64(1700001003), "   ", "gw1", "chain", "fee_pool_open", "paused", "blocked", `{"x":3}`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.table+"/blank", func(t *testing.T) {
			_, err := db.Exec(tc.blankSQL, tc.requiredArgs...)
			if err == nil {
				t.Fatalf("expected %s blank command_id write to fail", tc.table)
			}
		})
		t.Run(tc.table+"/null", func(t *testing.T) {
			args := make([]any, len(tc.requiredArgs))
			copy(args, tc.requiredArgs)
			args[1] = nil
			_, err := db.Exec(tc.nullSQL, args...)
			if err == nil {
				t.Fatalf("expected %s null command_id write to fail", tc.table)
			}
		})
		t.Run(tc.table+"/orphan", func(t *testing.T) {
			args := make([]any, len(tc.requiredArgs))
			copy(args, tc.requiredArgs)
			switch tc.table {
			case "domain_events":
				args[1] = "missing_parent_domain"
			case "state_snapshots":
				args[1] = "missing_parent_state"
			case "effect_logs":
				args[1] = "missing_parent_effect"
			}
			args[2] = "gw1"
			_, err := db.Exec(tc.orphanSQL, args...)
			if err == nil {
				t.Fatalf("expected %s orphan command_id write to fail", tc.table)
			}
		})
	}
}

func TestCommandLinkedTablesRealChainWritesRequireParentCommand(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	db.SetMaxOpenConns(1)
	if err := enableForeignKeys(db); err != nil {
		t.Fatalf("enable foreign keys failed: %v", err)
	}
	store := newClientDB(db, nil)

	if err := dbAppendCommandJournal(nil, store, commandJournalEntry{
		CommandID:     "cmd-real-chain",
		CommandType:   "feepool.open",
		GatewayPeerID: "gw1",
		AggregateID:   "agg-1",
		RequestedBy:   "tester",
		RequestedAt:   1700002001,
		Accepted:      true,
		Status:        "applied",
	}); err != nil {
		t.Fatalf("append command_journal failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO domain_events(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		int64(1700002002),
		"cmd-real-chain",
		"gw1",
		"command",
		"cmd-real-chain",
		int64(1700002002),
		"fee_pool_opened",
		"idle",
		"open",
		`{"scene":"real_chain"}`,
	); err != nil {
		t.Fatalf("insert domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		int64(1700002003),
		"cmd-real-chain",
		"gw1",
		"command",
		"cmd-real-chain",
		int64(1700002003),
		"open",
		"",
		int64(0),
		int64(200000),
		"",
		`{"scene":"real_chain"}`,
	); err != nil {
		t.Fatalf("insert state snapshot failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO effect_logs(
		created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
	) VALUES(?,?,?,?,?,?,?,?)`,
		int64(1700002004),
		"cmd-real-chain",
		"gw1",
		"chain",
		"fee_pool_open",
		"ok",
		"",
		`{"scene":"real_chain"}`,
	); err != nil {
		t.Fatalf("insert effect log failed: %v", err)
	}

	for _, table := range []string{"command_journal", "domain_events", "state_snapshots", "effect_logs"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s failed: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("unexpected %s count: got=%d want=1", table, count)
		}
	}

	for _, table := range []string{"domain_events", "state_snapshots", "effect_logs"} {
		report, err := auditCommandLinkedTableRows(db, table)
		if err != nil {
			t.Fatalf("audit %s failed: %v", table, err)
		}
		if report.DirtyRows() != 0 {
			t.Fatalf("unexpected dirty rows in %s: %s", table, report.String())
		}
	}

	if _, err := db.Exec(`INSERT INTO domain_events(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		int64(1700002005),
		"missing-parent-chain",
		"gw1",
		"command",
		"missing-parent-chain",
		int64(1700002005),
		"fee_pool_opened",
		"idle",
		"open",
		`{"scene":"missing_parent"}`,
	); err == nil {
		t.Fatalf("expected missing parent domain event write to fail")
	}
	if _, err := db.Exec(`INSERT INTO effect_logs(
		created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
	) VALUES(?,?,?,?,?,?,?,?)`,
		int64(1700002006),
		"missing-parent-chain",
		"gw1",
		"chain",
		"fee_pool_open",
		"ok",
		"",
		`{"scene":"missing_parent"}`,
	); err == nil {
		t.Fatalf("expected missing parent effect log write to fail")
	}
}

func createLegacyCommandLinkedTablesForStage3Test(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT,
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
			command_id TEXT,
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
		`CREATE TABLE effect_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT,
			gateway_pubkey_hex TEXT NOT NULL,
			effect_type TEXT NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy command linked table failed: %v", err)
		}
	}
}

func insertLegacyCommandLinkedDomainEventRow(t *testing.T, db *sql.DB, commandID, eventName string, commandIDValue any) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO domain_events(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		int64(1700003001),
		commandIDValue,
		"gw1",
		"command",
		strings.TrimSpace(commandID),
		int64(1700003001),
		eventName,
		"idle",
		"open",
		`{"scene":"legacy"}`,
	); err != nil {
		t.Fatalf("insert legacy domain event failed: %v", err)
	}
}

func insertLegacyCommandLinkedDomainEventRowWithSourceKind(t *testing.T, db *sql.DB, commandID, eventName, sourceKind string, commandIDValue any) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO domain_events(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		int64(1700003001),
		commandIDValue,
		"gw1",
		sourceKind,
		strings.TrimSpace(commandID),
		int64(1700003001),
		eventName,
		"idle",
		"open",
		`{"scene":"legacy"}`,
	); err != nil {
		t.Fatalf("insert legacy domain event failed: %v", err)
	}
}

func insertLegacyCommandLinkedStateSnapshotRow(t *testing.T, db *sql.DB, commandID, state string, commandIDValue any) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO state_snapshots(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		int64(1700003002),
		commandIDValue,
		"gw1",
		"command",
		strings.TrimSpace(commandID),
		int64(1700003002),
		state,
		"",
		int64(0),
		int64(1000),
		"",
		`{"scene":"legacy"}`,
	); err != nil {
		t.Fatalf("insert legacy state snapshot failed: %v", err)
	}
}

func insertLegacyCommandLinkedStateSnapshotRowWithSourceKind(t *testing.T, db *sql.DB, commandID, state, sourceKind string, commandIDValue any) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO state_snapshots(
		created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		int64(1700003002),
		commandIDValue,
		"gw1",
		sourceKind,
		strings.TrimSpace(commandID),
		int64(1700003002),
		state,
		"",
		int64(0),
		int64(1000),
		"",
		`{"scene":"legacy"}`,
	); err != nil {
		t.Fatalf("insert legacy state snapshot failed: %v", err)
	}
}

func insertLegacyCommandLinkedEffectLogRow(t *testing.T, db *sql.DB, commandID, effectType string, commandIDValue any) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO effect_logs(
		created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
	) VALUES(?,?,?,?,?,?,?,?)`,
		int64(1700003003),
		commandIDValue,
		"gw1",
		effectType,
		"fee_pool_open",
		"ok",
		"",
		`{"scene":"legacy"}`,
	); err != nil {
		t.Fatalf("insert legacy effect log failed: %v", err)
	}
}

func expectCommandLinkedTableClean(t *testing.T, db *sql.DB, table, wantCommandID, wantIndex string) {
	t.Helper()

	cols, err := tableColumns(db, table)
	if err != nil {
		t.Fatalf("inspect %s columns failed: %v", table, err)
	}
	if _, ok := cols["command_id"]; !ok {
		t.Fatalf("%s missing command_id after migration", table)
	}

	notNull, err := tableColumnNotNull(db, table, "command_id")
	if err != nil {
		t.Fatalf("inspect %s command_id notnull failed: %v", table, err)
	}
	if !notNull {
		t.Fatalf("%s.command_id should be NOT NULL after migration", table)
	}

	hasCheck, err := tableHasCreateSQLContains(db, table, "CHECK(trim(command_id) <> '')")
	if err != nil {
		t.Fatalf("inspect %s check failed: %v", table, err)
	}
	if !hasCheck {
		t.Fatalf("%s missing CHECK(trim(command_id) <> '') after migration", table)
	}

	hasFK, err := tableHasForeignKey(db, table, "command_id", "command_journal", "command_id")
	if err != nil {
		t.Fatalf("inspect %s foreign key failed: %v", table, err)
	}
	if !hasFK {
		t.Fatalf("%s missing foreign key after migration", table)
	}

	hasIndex, err := tableHasIndex(db, table, wantIndex)
	if err != nil {
		t.Fatalf("inspect %s index failed: %v", table, err)
	}
	if !hasIndex {
		t.Fatalf("%s missing %s after migration", table, wantIndex)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count %s failed: %v", table, err)
	}
	if count != 1 {
		t.Fatalf("unexpected %s count after migration: got=%d want=1", table, count)
	}

	var gotCommandID string
	if err := db.QueryRow(`SELECT command_id FROM ` + table + ` ORDER BY id ASC LIMIT 1`).Scan(&gotCommandID); err != nil {
		t.Fatalf("load %s row failed: %v", table, err)
	}
	if gotCommandID != wantCommandID {
		t.Fatalf("%s command_id mismatch: got=%s want=%s", table, gotCommandID, wantCommandID)
	}
	if wantCommandID == "" {
		t.Fatalf("%s command_id should be kept", table)
	}
}

func expectCommandFactSourceKindClean(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	hasCheck, err := tableHasCreateSQLContains(db, table, "CHECK(source_kind = 'command')")
	if err != nil {
		t.Fatalf("inspect %s source kind check failed: %v", table, err)
	}
	if !hasCheck {
		t.Fatalf("%s missing CHECK(source_kind = 'command') after migration", table)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table + ` WHERE source_kind <> 'command'`).Scan(&count); err != nil {
		t.Fatalf("count %s source_kind failed: %v", table, err)
	}
	if count != 0 {
		t.Fatalf("unexpected %s non-command source_kind rows after migration: got=%d want=0", table, count)
	}

	var gotSourceKind string
	if err := db.QueryRow(`SELECT source_kind FROM ` + table + ` ORDER BY id ASC LIMIT 1`).Scan(&gotSourceKind); err != nil {
		t.Fatalf("load %s source_kind failed: %v", table, err)
	}
	if gotSourceKind != "command" {
		t.Fatalf("%s source_kind mismatch: got=%s want=command", table, gotSourceKind)
	}
}
