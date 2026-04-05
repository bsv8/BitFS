package clientapp

import (
	"database/sql"
	"testing"
)

func createHistoricalPoolAllocationsSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE fact_pool_sessions(
			pool_session_id TEXT PRIMARY KEY,
			pool_scheme TEXT NOT NULL,
			counterparty_pubkey_hex TEXT NOT NULL DEFAULT '',
			seller_pubkey_hex TEXT NOT NULL DEFAULT '',
			arbiter_pubkey_hex TEXT NOT NULL DEFAULT '',
			gateway_pubkey_hex TEXT NOT NULL DEFAULT '',
			pool_amount_satoshi INTEGER NOT NULL,
			spend_tx_fee_satoshi INTEGER NOT NULL,
			fee_rate_sat_byte REAL NOT NULL DEFAULT 0,
			lock_blocks INTEGER NOT NULL DEFAULT 0,
			open_base_txid TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE fact_pool_allocations(
			allocation_id TEXT PRIMARY KEY,
			pool_session_id TEXT NOT NULL,
			allocation_no INTEGER NOT NULL,
			allocation_kind TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			payee_amount_after INTEGER NOT NULL,
			payer_amount_after INTEGER NOT NULL,
			txid TEXT NOT NULL,
			tx_hex TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create historical pool schema failed: %v", err)
		}
	}
}

func insertHistoricalPoolAllocation(t *testing.T, db *sql.DB, allocationID, sessionID string, allocationNo int64, kind string, sequenceNum int64, payeeAfter int64, payerAfter int64, txid, txHex string, createdAtUnix int64) {
	t.Helper()

	if _, err := db.Exec(`INSERT INTO fact_pool_allocations(
		allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		allocationID, sessionID, allocationNo, kind, sequenceNum, payeeAfter, payerAfter, txid, txHex, createdAtUnix,
	); err != nil {
		t.Fatalf("insert historical pool allocation failed: %v", err)
	}
}

func insertHistoricalPoolSession(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()

	if _, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
		pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sessionID, "2of3", "", "", "", "", int64(0), int64(0), float64(0), int64(0), "", "active", int64(1700000000), int64(1700000000),
	); err != nil {
		t.Fatalf("insert historical pool session failed: %v", err)
	}
}

func TestInitIndexDB_CreatesPoolSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{"fact_pool_sessions", "fact_pool_session_events", "fact_chain_payments", "settle_tx_utxo_links"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	allocCols, err := tableColumns(db, "fact_pool_session_events")
	if err != nil {
		t.Fatalf("inspect fact_pool_session_events columns failed: %v", err)
	}
	for _, col := range []string{"id", "allocation_id", "pool_session_id", "allocation_no", "allocation_kind", "sequence_num", "payee_amount_after", "payer_amount_after", "txid", "tx_hex", "created_at_unix"} {
		if _, ok := allocCols[col]; !ok {
			t.Fatalf("fact_pool_session_events missing column %s", col)
		}
	}
	if notNull, err := tableColumnNotNull(db, "fact_pool_session_events", "allocation_id"); err != nil {
		t.Fatalf("inspect fact_pool_session_events allocation_id notnull failed: %v", err)
	} else if !notNull {
		t.Fatalf("fact_pool_session_events.allocation_id should be NOT NULL")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_pool_session_events", []string{"allocation_id"}); err != nil {
		t.Fatalf("inspect fact_pool_session_events unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("fact_pool_session_events should keep unique constraint on allocation_id")
	}

	for _, table := range []string{"fact_chain_payments", "settle_tx_utxo_links"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		if _, ok := cols["id"]; !ok {
			t.Fatalf("%s missing id", table)
		}
	}
}

func TestInitIndexDB_MigratesHistoricalPoolAllocations(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createHistoricalPoolAllocationsSchema(t, db)
	insertHistoricalPoolSession(t, db, "sess_b")
	insertHistoricalPoolSession(t, db, "sess_a")
	insertHistoricalPoolAllocation(t, db, "alloc_b_2", "sess_b", 2, "pay", 2, 150, 850, "tx_b_2", "hex_b_2", 1700000003)
	insertHistoricalPoolAllocation(t, db, "alloc_a_2", "sess_a", 2, "pay", 2, 200, 800, "tx_a_2", "hex_a_2", 1700000002)
	insertHistoricalPoolAllocation(t, db, "alloc_a_1", "sess_a", 1, "open", 1, 0, 1000, "tx_a_1", "hex_a_1", 1700000001)

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	cols, err := tableColumns(db, "fact_pool_session_events")
	if err != nil {
		t.Fatalf("inspect migrated fact_pool_session_events columns failed: %v", err)
	}
	if _, ok := cols["id"]; !ok {
		t.Fatalf("fact_pool_session_events should have id after migration")
	}

	type row struct {
		id           int64
		allocationID string
		sessionID    string
		allocationNo int64
	}
	rows, err := db.Query(`SELECT id,allocation_id,pool_session_id,allocation_no FROM fact_pool_session_events ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query migrated fact_pool_session_events failed: %v", err)
	}
	defer rows.Close()

	got := make([]row, 0, 4)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.allocationID, &r.sessionID, &r.allocationNo); err != nil {
			t.Fatalf("scan migrated fact_pool_session_events failed: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated fact_pool_session_events failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("unexpected migrated row count: got=%d want=3", len(got))
	}
	wantOrder := []row{
		{id: 1, allocationID: "alloc_a_1", sessionID: "sess_a", allocationNo: 1},
		{id: 2, allocationID: "alloc_a_2", sessionID: "sess_a", allocationNo: 2},
		{id: 3, allocationID: "alloc_b_2", sessionID: "sess_b", allocationNo: 2},
	}
	for i := range wantOrder {
		if got[i] != wantOrder[i] {
			t.Fatalf("migrated row order mismatch at %d: got=%+v want=%+v", i, got[i], wantOrder[i])
		}
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_pool_session_events", []string{"allocation_id"}); err != nil {
		t.Fatalf("inspect allocation_id unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("allocation_id unique constraint should exist after migration")
	}

	if _, err := db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"alloc_a_1", "sess_a", 3, "pay", 3, 1, 999, "tx_dup", "hex_dup", 1700000004,
	); err == nil {
		t.Fatalf("duplicate allocation_id insert should fail")
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	rows, err = db.Query(`SELECT id,allocation_id,pool_session_id,allocation_no FROM fact_pool_session_events ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query fact_pool_session_events after second init failed: %v", err)
	}
	defer rows.Close()

	got = got[:0]
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.allocationID, &r.sessionID, &r.allocationNo); err != nil {
			t.Fatalf("scan fact_pool_session_events after second init failed: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate fact_pool_session_events after second init failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("row count changed after second init: got=%d want=3", len(got))
	}
	for i := range wantOrder {
		if got[i] != wantOrder[i] {
			t.Fatalf("row changed after second init at %d: got=%+v want=%+v", i, got[i], wantOrder[i])
		}
	}
}

func TestInitIndexDB_CreatesPoolSchemaIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, indexName := range []string{
		"uq_fact_pool_session_events_session_kind_seq",
		"idx_fact_pool_session_events_session_no",
		"idx_fact_pool_session_events_txid",
		"idx_fact_chain_payments_occurred",
		"idx_fact_chain_payments_subtype",
		"idx_fact_chain_payments_status",
		"idx_settle_tx_utxo_links_business",
		"idx_settle_tx_utxo_links_utxo",
		"idx_settle_tx_utxo_links_txid",
	} {
		tableName := "fact_pool_session_events"
		if indexName == "idx_fact_chain_payments_occurred" || indexName == "idx_fact_chain_payments_subtype" || indexName == "idx_fact_chain_payments_status" {
			tableName = "fact_chain_payments"
		}
		if indexName == "idx_settle_tx_utxo_links_business" || indexName == "idx_settle_tx_utxo_links_utxo" || indexName == "idx_settle_tx_utxo_links_txid" {
			tableName = "settle_tx_utxo_links"
		}
		hasIndex, err := tableHasIndex(db, tableName, indexName)
		if err != nil {
			t.Fatalf("inspect %s failed: %v", indexName, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on %s", indexName, tableName)
		}
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_chain_payments", []string{"txid"}); err != nil {
		t.Fatalf("inspect fact_chain_payments unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("fact_chain_payments should keep unique constraint on txid")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "settle_tx_utxo_links", []string{"business_id", "txid", "utxo_id", "io_side", "utxo_role"}); err != nil {
		t.Fatalf("inspect settle_tx_utxo_links unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("settle_tx_utxo_links should keep unique constraint on (business_id, txid, utxo_id, io_side, utxo_role)")
	}

	if exists, err := hasTable(db, "fact_chain_payment_utxo_links"); err != nil {
		t.Fatalf("check historical table absence failed: %v", err)
	} else if exists {
		t.Fatal("fact_chain_payment_utxo_links should not exist anymore")
	}
}

func TestInitIndexDB_PoolSchemaIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createHistoricalPoolAllocationsSchema(t, db)
	insertHistoricalPoolSession(t, db, "sess_idem")
	insertHistoricalPoolAllocation(t, db, "alloc_idem_1", "sess_idem", 1, "open", 1, 0, 1000, "tx_idem_1", "hex_idem_1", 1700000101)

	if err := initIndexDB(db); err != nil {
		t.Fatalf("first initIndexDB failed: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM fact_pool_session_events WHERE pool_session_id=?`, "sess_idem").Scan(&count); err != nil {
		t.Fatalf("count fact_pool_session_events failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("fact_pool_session_events row count changed after repeated init: got=%d want=1", count)
	}

	var id int64
	if err := db.QueryRow(`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`, "alloc_idem_1").Scan(&id); err != nil {
		t.Fatalf("query migrated allocation id failed: %v", err)
	}
	if id != 1 {
		t.Fatalf("unexpected migrated allocation id: got=%d want=1", id)
	}
}

func tableForeignKeys(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA foreign_key_list(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 4)
	for rows.Next() {
		var (
			id       int
			seq      int
			refTable string
			from     string
			to       string
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		out = append(out, from+"->"+refTable+"."+to)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
