package clientapp

import (
	"testing"
)

func TestEnsureSettlementCyclesSchema_UsesCurrentTablesOnly(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBBaseSchema(db); err != nil {
		t.Fatalf("setup base schema failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO fact_chain_payments(
		txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,block_height,
		occurred_at_unix,submitted_at_unix,wallet_observed_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_chain_cycle_1", "transfer", "confirmed", 1000, 0, 1000, 100, 1700000000, 1700000000, 1700000000, "from", "to", "{}", 1700000000,
	); err != nil {
		t.Fatalf("seed chain payment failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
		pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_pool_cycle_1", "2of3", "", "", "", "", 1000, 0, 0, 0, "", "active", 1700000000, 1700000000,
	); err != nil {
		t.Fatalf("seed pool session failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id,pool_session_id,allocation_no,allocation_kind,event_kind,sequence_num,state,direction,
		amount_satoshi,purpose,note,msg_id,cycle_index,payee_amount_after,payer_amount_after,txid,tx_hex,
		gateway_pubkey_hex,created_at_unix,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"alloc_pool_cycle_1", "sess_pool_cycle_1", 1, "pay", PoolFactEventKindPoolEvent, 1, "confirmed", "",
		1000, "pay", "seed", "", 1, 1000, 0, "tx_pool_cycle_1", "hex", "", 1700000000, "{}",
	); err != nil {
		t.Fatalf("seed pool event failed: %v", err)
	}

	if err := ensureSettlementCyclesSchema(db); err != nil {
		t.Fatalf("ensureSettlementCyclesSchema failed: %v", err)
	}

	for _, table := range []string{"fact_chain_payments", "fact_pool_session_events", "fact_settlement_cycles"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}
	if exists, err := hasTable(db, legacyTableName("fact", "asset", "consumptions")); err != nil {
		t.Fatalf("check legacy table failed: %v", err)
	} else if exists {
		t.Fatal("legacy asset consumption table should not exist")
	}

	cols, err := tableColumns(db, "fact_settlement_cycles")
	if err != nil {
		t.Fatalf("inspect fact_settlement_cycles columns failed: %v", err)
	}
	for _, col := range []string{"id", "cycle_id", "channel", "pool_session_id", "pool_session_event_id", "chain_payment_id"} {
		if _, ok := cols[col]; !ok {
			t.Fatalf("fact_settlement_cycles missing column %s", col)
		}
	}
	if notNull, err := tableColumnNotNull(db, "fact_settlement_cycles", "pool_session_id"); err != nil {
		t.Fatalf("inspect pool_session_id failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_settlement_cycles.pool_session_id should be NOT NULL")
	}
	if hasIndex, err := tableHasIndex(db, "fact_settlement_cycles", "idx_fact_settlement_cycles_pool_session"); err != nil {
		t.Fatalf("inspect pool session index failed: %v", err)
	} else if !hasIndex {
		t.Fatal("fact_settlement_cycles should keep index on pool_session_id")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_cycles", []string{"pool_session_event_id"}); err != nil {
		t.Fatalf("inspect pool unique index failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_cycles should keep unique index on pool_session_event_id")
	}
	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_cycles", []string{"chain_payment_id"}); err != nil {
		t.Fatalf("inspect chain unique index failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_cycles should keep unique index on chain_payment_id")
	}
}
