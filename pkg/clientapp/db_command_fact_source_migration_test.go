package clientapp

import (
	"database/sql"
	"encoding/json"
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

func createLegacyCommandFactTablesWithTransitionColumns(t *testing.T, db *sql.DB) {
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

func TestCommandFactFinalMigrationRemovesTransitionColumnsAndDropsObservedRows(t *testing.T) {
	t.Parallel()

	db := newLegacyCommandFactMigrationTestDB(t)
	createLegacyCommandFactTablesWithTransitionColumns(t, db)

	if _, err := db.Exec(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000200, "cmd-command-1", "feepool.open", "gw1", "agg-1", "tester", 1700000200, 1, "applied", "", "", "idle", "open", 1, "", `{"x":0}`, `{"ok":true}`,
	); err != nil {
		t.Fatalf("insert legacy command journal failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		1700000201, "cmd-command-1", "gw1", "command", "cmd-command-1", 1700000201, "fee_pool_paused_insufficient", "idle", "paused_insufficient", `{"x":1}`,
	); err != nil {
		t.Fatalf("insert command domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		1700000202, "cmd-observed-1", "gw1", "observed_gateway_state", "gw1", 1700000202, "fee_pool_resumed_by_wallet_probe", "paused_insufficient", "idle", `{"x":2}`,
	); err != nil {
		t.Fatalf("insert observed domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000203, "cmd-command-1", "gw1", "command", "cmd-command-1", 1700000203, "paused_insufficient", "wallet_insufficient", 100000, 12345, "not enough balance", `{"x":3}`,
	); err != nil {
		t.Fatalf("insert command state snapshot failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000204, "cmd-observed-state-1", "gw1", "observed_gateway_state", "gw1", 1700000204, "idle", "", 0, 999999, "", `{"x":4}`,
	); err != nil {
		t.Fatalf("insert observed state snapshot failed: %v", err)
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
			if _, ok := cols[col]; ok {
				t.Fatalf("%s should not keep %s", table, col)
			}
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM domain_events`).Scan(&count); err != nil {
		t.Fatalf("count domain_events failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected domain_events count after migration: got=%d want=1", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM state_snapshots`).Scan(&count); err != nil {
		t.Fatalf("count state_snapshots failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected state_snapshots count after migration: got=%d want=1", count)
	}

	var commandID string
	if err := db.QueryRow(`SELECT command_id FROM domain_events ORDER BY id ASC LIMIT 1`).Scan(&commandID); err != nil {
		t.Fatalf("load migrated domain event failed: %v", err)
	}
	if commandID != "cmd-command-1" {
		t.Fatalf("unexpected migrated domain command_id: %s", commandID)
	}
	if err := db.QueryRow(`SELECT command_id FROM state_snapshots ORDER BY id ASC LIMIT 1`).Scan(&commandID); err != nil {
		t.Fatalf("load migrated state snapshot failed: %v", err)
	}
	if commandID != "cmd-command-1" {
		t.Fatalf("unexpected migrated state command_id: %s", commandID)
	}
}

func TestObservedGatewayStatePayloadMigrationUsesFinalEnvelope(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO observed_gateway_states(
		created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000301, "gw1", "gw1", 1700000301, "fee_pool_resumed_by_wallet_probe", "paused_insufficient", "idle", "", 0, 999999, "", `{"source_table":"state_snapshots","snapshot_payload":{"legacy":true}}`,
	); err != nil {
		t.Fatalf("insert legacy observed payload failed: %v", err)
	}

	if err := migrateObservedGatewayStatePayloads(db); err != nil {
		t.Fatalf("migrate observed payloads failed: %v", err)
	}
	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM observed_gateway_states WHERE gateway_pubkey_hex=?`, "gw1").Scan(&payloadJSON); err != nil {
		t.Fatalf("load migrated payload failed: %v", err)
	}
	var payload struct {
		ObservedReason       string         `json:"observed_reason"`
		WalletBalanceSatoshi uint64         `json:"wallet_balance_satoshi"`
		Extra                map[string]any `json:"extra"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode migrated payload failed: %v", err)
	}
	if payload.ObservedReason != "wallet_probe" || payload.WalletBalanceSatoshi != 999999 {
		t.Fatalf("unexpected migrated payload top-level: %+v", payload)
	}
	if payload.Extra == nil || payload.Extra["source_table"] != "state_snapshots" {
		t.Fatalf("unexpected migrated payload extra: %+v", payload.Extra)
	}
}
