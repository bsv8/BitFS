package clientapp

import (
	"testing"
)

// TestEnsureSettlementCyclesSchema_BackfillsPartialCycles 验证迁移不会因为已有部分 cycle 就中断。
func TestEnsureSettlementCyclesSchema_BackfillsPartialCycles(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)

	// 只搭迁移需要的最小老库结构。
	stmts := []string{
		`CREATE TABLE fact_settlement_cycles(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			cycle_id TEXT NOT NULL UNIQUE,
			channel TEXT NOT NULL,
			state TEXT NOT NULL,
			pool_session_event_id INTEGER,
			chain_payment_id INTEGER,
			gross_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			gate_fee_satoshi INTEGER NOT NULL DEFAULT 0,
			net_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			cycle_index INTEGER NOT NULL DEFAULT 0,
			occurred_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE fact_chain_payments(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			txid TEXT NOT NULL UNIQUE,
			payment_subtype TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT '',
			wallet_input_satoshi INTEGER NOT NULL DEFAULT 0,
			wallet_output_satoshi INTEGER NOT NULL DEFAULT 0,
			net_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			block_height INTEGER NOT NULL DEFAULT 0,
			occurred_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL DEFAULT 0,
			wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			from_party_id TEXT NOT NULL DEFAULT '',
			to_party_id TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE fact_pool_session_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_kind TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL DEFAULT 0,
			cycle_index INTEGER NOT NULL DEFAULT 0,
			created_at_unix INTEGER NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE fact_asset_consumptions(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_flow_id INTEGER,
			chain_payment_id INTEGER,
			pool_allocation_id INTEGER,
			used_satoshi INTEGER NOT NULL DEFAULT 0,
			occurred_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy table failed: %v", err)
		}
	}

	now := int64(1700000000)

	// 先放一条已存在的 cycle，模拟“库里已经有部分 cycle 数据”。
	if _, err := db.Exec(`INSERT INTO fact_chain_payments(
		id,txid,wallet_input_satoshi,net_amount_satoshi,occurred_at_unix,payload_json
	) VALUES(?,?,?,?,?,?)`, 1, "tx_chain_1", 1000, 900, now, "{}"); err != nil {
		t.Fatalf("seed chain payment failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_pool_session_events(
		id,event_kind,amount_satoshi,cycle_index,created_at_unix,payload_json
	) VALUES(?,?,?,?,?,?)`, 1, "pool_event", 800, 7, now, "{}"); err != nil {
		t.Fatalf("seed pool event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_settlement_cycles(
		cycle_id,channel,state,chain_payment_id,gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,
		cycle_index,occurred_at_unix,confirmed_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"cycle_chain_1", "chain", "confirmed", 1, 1000, 0, 900, 0, now, now, "seed chain cycle", "{}",
	); err != nil {
		t.Fatalf("seed existing cycle failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_asset_consumptions(
		id,chain_payment_id,pool_allocation_id,used_satoshi,occurred_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?)`, 1, 1, nil, 1000, now, "chain consumption", "{}"); err != nil {
		t.Fatalf("seed chain consumption failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_asset_consumptions(
		id,chain_payment_id,pool_allocation_id,used_satoshi,occurred_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?)`, 2, nil, 1, 800, now, "pool consumption", "{}"); err != nil {
		t.Fatalf("seed pool consumption failed: %v", err)
	}

	if err := ensureSettlementCyclesSchema(db); err != nil {
		t.Fatalf("ensureSettlementCyclesSchema failed: %v", err)
	}

	cols, err := tableColumns(db, "fact_asset_consumptions")
	if err != nil {
		t.Fatalf("inspect fact_asset_consumptions columns failed: %v", err)
	}
	if _, ok := cols["settlement_cycle_id"]; !ok {
		t.Fatal("fact_asset_consumptions missing settlement_cycle_id after migration")
	}
	notNull, err := tableColumnNotNull(db, "fact_asset_consumptions", "settlement_cycle_id")
	if err != nil {
		t.Fatalf("inspect settlement_cycle_id nullability failed: %v", err)
	}
	if !notNull {
		t.Fatal("fact_asset_consumptions.settlement_cycle_id should be NOT NULL after migration")
	}

	var chainCycleID int64
	if err := db.QueryRow(`SELECT settlement_cycle_id FROM fact_asset_consumptions WHERE chain_payment_id=1`).Scan(&chainCycleID); err != nil {
		t.Fatalf("query chain settlement_cycle_id failed: %v", err)
	}
	if chainCycleID <= 0 {
		t.Fatal("chain consumption settlement_cycle_id should be backfilled")
	}

	var poolCycleID int64
	if err := db.QueryRow(`SELECT settlement_cycle_id FROM fact_asset_consumptions WHERE pool_allocation_id=1`).Scan(&poolCycleID); err != nil {
		t.Fatalf("query pool settlement_cycle_id failed: %v", err)
	}
	if poolCycleID <= 0 {
		t.Fatal("pool consumption settlement_cycle_id should be backfilled")
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_cycles", []string{"chain_payment_id"}); err != nil {
		t.Fatalf("inspect chain unique index failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_cycles should keep unique index on chain_payment_id")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_cycles", []string{"pool_session_event_id"}); err != nil {
		t.Fatalf("inspect pool unique index failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_cycles should keep unique index on pool_session_event_id")
	}

	cycleCols, err := tableColumns(db, "fact_settlement_cycles")
	if err != nil {
		t.Fatalf("inspect fact_settlement_cycles columns failed: %v", err)
	}
	if _, ok := cycleCols["pool_session_id"]; !ok {
		t.Fatal("fact_settlement_cycles missing pool_session_id after migration")
	}
	if notNull, err := tableColumnNotNull(db, "fact_settlement_cycles", "pool_session_id"); err != nil {
		t.Fatalf("inspect fact_settlement_cycles pool_session_id notnull failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_settlement_cycles.pool_session_id should be NOT NULL after migration")
	}
	if hasIndex, err := tableHasIndex(db, "fact_settlement_cycles", "idx_fact_settlement_cycles_pool_session"); err != nil {
		t.Fatalf("inspect fact_settlement_cycles pool_session index failed: %v", err)
	} else if !hasIndex {
		t.Fatal("fact_settlement_cycles should keep index on pool_session_id")
	}
}
