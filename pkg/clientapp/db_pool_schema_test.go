package clientapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestInitIndexDB_CreatesCurrentPoolSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{
		"biz_pool",
		"biz_pool_allocations",
		"fact_settlement_channel_pool_session_quote_pay",
		"fact_pool_session_events",
		"fact_settlement_cycles",
		"fact_settlement_channel_chain_quote_pay",
		"settle_records",
		"wallet_local_broadcast_txs",
	} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	for _, table := range []string{
		legacyTableName("fact", "pool", "allocations"),
		legacyTableName("fact", "tx", "history"),
	} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("check legacy table %s failed: %v", table, err)
		}
		if exists {
			t.Fatalf("legacy table %s should not exist", table)
		}
	}

	for _, check := range []struct {
		table string
		cols  []string
	}{
		{
			table: "biz_pool",
			cols:  []string{"pool_session_id", "pool_scheme", "pool_amount_satoshi", "spend_tx_fee_satoshi", "allocated_satoshi", "cycle_fee_satoshi", "available_satoshi", "next_sequence_num", "status"},
		},
		{
			table: "biz_pool_allocations",
			cols:  []string{"allocation_id", "pool_session_id", "allocation_no", "allocation_kind", "sequence_num", "payee_amount_after", "payer_amount_after", "txid", "tx_hex", "created_at_unix"},
		},
		{
			table: "fact_pool_session_events",
			cols:  []string{"id", "allocation_id", "pool_session_id", "allocation_kind", "event_kind", "sequence_num", "created_at_unix"},
		},
		{
			table: "fact_settlement_cycles",
			cols:  []string{"id", "cycle_id", "source_type", "source_id", "state", "gross_amount_satoshi", "gate_fee_satoshi", "net_amount_satoshi"},
		},
		{
			table: "settle_records",
			cols:  []string{"business_id", "settlement_id", "business_role", "source_type", "source_id", "accounting_scene", "accounting_subtype", "from_party_id", "to_party_id", "status", "occurred_at_unix", "idempotency_key", "note", "payload_json", "settlement_method", "settlement_status", "target_type", "target_id", "error_message", "settlement_payload_json", "created_at_unix", "updated_at_unix"},
		},
		{
			table: "wallet_local_broadcast_txs",
			cols:  []string{"txid", "wallet_id", "address", "tx_hex", "created_at_unix", "updated_at_unix", "observed_at_unix"},
		},
	} {
		cols, err := tableColumns(db, check.table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", check.table, err)
		}
		for _, col := range check.cols {
			if _, ok := cols[col]; !ok {
				t.Fatalf("%s missing column %s", check.table, col)
			}
		}
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "biz_pool", []string{"pool_session_id"}); err != nil {
		t.Fatalf("inspect biz_pool unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("biz_pool should keep unique constraint on pool_session_id")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "biz_pool_allocations", []string{"pool_session_id", "allocation_kind", "sequence_num"}); err != nil {
		t.Fatalf("inspect biz_pool_allocations unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("biz_pool_allocations should keep unique constraint on (pool_session_id, allocation_kind, sequence_num)")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_pool_session_events", []string{"allocation_id"}); err != nil {
		t.Fatalf("inspect fact_pool_session_events unique constraint failed: %v", err)
	} else if !unique {
		t.Fatal("fact_pool_session_events should keep unique constraint on allocation_id")
	}
	if ok, err := tableHasIndex(db, "wallet_local_broadcast_txs", "idx_wallet_local_broadcast_txs_wallet_observed"); err != nil {
		t.Fatalf("inspect wallet_local_broadcast_txs index failed: %v", err)
	} else if !ok {
		t.Fatal("wallet_local_broadcast_txs should keep wallet/observed index")
	}
}

func TestInitIndexDB_PoolFactsWriteCurrentTables(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if err := dbUpsertDirectTransferPoolSessionTx(context.Background(), nil, directTransferPoolSessionFactInput{}); err == nil {
		t.Fatal("nil tx should be rejected")
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := dbUpsertDirectTransferPoolSessionTx(context.Background(), tx, directTransferPoolSessionFactInput{
		SessionID:          "sess_pool_schema_1",
		PoolScheme:         "2of3",
		CounterpartyPubHex: "11",
		SellerPubHex:       "22",
		ArbiterPubHex:      "33",
		GatewayPubHex:      "44",
		PoolAmountSat:      1000,
		SpendTxFeeSat:      10,
		FeeRateSatByte:     1,
		LockBlocks:         10,
		OpenBaseTxID:       "base_tx_1",
		Status:             "active",
	}); err != nil {
		t.Fatalf("write pool session failed: %v", err)
	}
	if err := dbUpsertDirectTransferBizPoolSnapshotTx(context.Background(), tx, directTransferBizPoolSnapshotInput{
		SessionID:          "sess_pool_schema_1",
		PoolScheme:         "2of3",
		CounterpartyPubHex: "11",
		SellerPubHex:       "22",
		ArbiterPubHex:      "33",
		GatewayPubHex:      "44",
		PoolAmountSat:      1000,
		SpendTxFeeSat:      10,
		AllocatedSat:       0,
		CycleFeeSat:        0,
		AvailableSat:       990,
		NextSequenceNum:    1,
		Status:             "active",
		OpenBaseTxID:       "base_tx_1",
		CreatedAtUnix:      1700000100,
		UpdatedAtUnix:      1700000100,
	}); err != nil {
		t.Fatalf("write biz pool snapshot failed: %v", err)
	}
	if err := dbUpsertDirectTransferBizPoolAllocationTx(context.Background(), tx, directTransferBizPoolAllocationInput{
		SessionID:        "sess_pool_schema_1",
		AllocationID:     "alloc_pool_schema_1",
		AllocationNo:     1,
		AllocationKind:   "open",
		SequenceNum:      1,
		PayeeAmountAfter: 0,
		PayerAmountAfter: 990,
		TxID:             "tx_pool_schema_1",
		TxHex:            "deadbeef",
		CreatedAtUnix:    1700000100,
	}); err != nil {
		t.Fatalf("write biz pool allocation failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx failed: %v", err)
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_pool WHERE pool_session_id=?`, "sess_pool_schema_1").Scan(&count); err != nil {
		t.Fatalf("count biz_pool failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("biz_pool count mismatch: got=%d want=1", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_pool_allocations WHERE pool_session_id=?`, "sess_pool_schema_1").Scan(&count); err != nil {
		t.Fatalf("count biz_pool_allocations failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("biz_pool_allocations count mismatch: got=%d want=1", count)
	}
}

func TestInitIndexDB_RejectsLegacyPoolSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	legacyName := legacyTableName("fact", "pool", "allocations")
	if _, err := db.Exec(`CREATE TABLE ` + legacyName + `(
		allocation_id TEXT PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create legacy table failed: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + legacyTableName("fact", "tx", "history") + `(
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`); err != nil {
		t.Fatalf("create legacy tx table failed: %v", err)
	}

	if err := initIndexDB(db); err == nil {
		t.Fatal("expected initIndexDB to reject legacy pool schema")
	} else if !strings.Contains(err.Error(), "rebuild DB") {
		t.Fatalf("unexpected error: %v", err)
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
