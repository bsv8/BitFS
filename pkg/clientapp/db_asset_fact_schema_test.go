package clientapp

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func legacyTableName(parts ...string) string {
	return strings.Join(parts, "_")
}

func seedAssetFactSettlementCycle(t *testing.T, db *sql.DB, channel string, refID int64, occurredAt int64) int64 {
	t.Helper()
	cycleID := fmt.Sprintf("cycle_%s_%d", channel, refID)
	var (
		poolSessionEventID any
		chainPaymentID     any
	)
	switch channel {
	case "chain":
		chainPaymentID = refID
		poolSessionEventID = nil
	case "pool":
		poolSessionEventID = refID
		chainPaymentID = nil
	default:
		t.Fatalf("unsupported channel %s", channel)
	}
	res, err := db.Exec(`INSERT INTO fact_settlement_cycles(
		cycle_id,channel,state,pool_session_id,pool_session_event_id,chain_payment_id,
		gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,cycle_index,
		occurred_at_unix,confirmed_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		cycleID, channel, "confirmed", "sess_"+channel, poolSessionEventID, chainPaymentID,
		1000, 0, 1000, 0, occurredAt, occurredAt, "test cycle", "{}",
	)
	if err != nil {
		t.Fatalf("seed settlement cycle failed: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("seed settlement cycle id failed: %v", err)
	}
	return id
}

func assertTableMissing(t *testing.T, db *sql.DB, parts ...string) {
	t.Helper()
	name := legacyTableName(parts...)
	exists, err := hasTable(db, name)
	if err != nil {
		t.Fatalf("check table %s failed: %v", name, err)
	}
	if exists {
		t.Fatalf("table %s should not exist", name)
	}
}

func TestInitIndexDB_CreatesCurrentAssetFactSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	for _, table := range []string{
		"fact_chain_asset_flows",
		"fact_bsv_consumptions",
		"fact_token_consumptions",
		"fact_token_utxo_links",
		"fact_settlement_cycles",
	} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	assertTableMissing(t, db, "fact", "asset", "consumptions")

	flowCols, err := tableColumns(db, "fact_chain_asset_flows")
	if err != nil {
		t.Fatalf("inspect fact_chain_asset_flows columns failed: %v", err)
	}
	for _, col := range []string{"id", "flow_id", "wallet_id", "direction", "asset_kind", "utxo_id", "txid", "vout", "amount_satoshi", "occurred_at_unix", "updated_at_unix"} {
		if _, ok := flowCols[col]; !ok {
			t.Fatalf("fact_chain_asset_flows missing column %s", col)
		}
	}

	bsvCols, err := tableColumns(db, "fact_bsv_consumptions")
	if err != nil {
		t.Fatalf("inspect fact_bsv_consumptions columns failed: %v", err)
	}
	for _, col := range []string{"id", "consumption_id", "source_flow_id", "source_utxo_id", "chain_payment_id", "pool_allocation_id", "settlement_cycle_id", "used_satoshi", "occurred_at_unix"} {
		if _, ok := bsvCols[col]; !ok {
			t.Fatalf("fact_bsv_consumptions missing column %s", col)
		}
	}
	if notNull, err := tableColumnNotNull(db, "fact_bsv_consumptions", "settlement_cycle_id"); err != nil {
		t.Fatalf("inspect fact_bsv_consumptions settlement_cycle_id failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_bsv_consumptions.settlement_cycle_id should be NOT NULL")
	}

	tokenCols, err := tableColumns(db, "fact_token_consumptions")
	if err != nil {
		t.Fatalf("inspect fact_token_consumptions columns failed: %v", err)
	}
	for _, col := range []string{"id", "consumption_id", "source_flow_id", "token_id", "token_standard", "settlement_cycle_id", "used_quantity_text"} {
		if _, ok := tokenCols[col]; !ok {
			t.Fatalf("fact_token_consumptions missing column %s", col)
		}
	}
}

func TestInitIndexDB_AssetFactConstraints(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"utxo_bsv_1", "wallet_1", "addr1", "tx1", 0, 1000, "confirmed",
		"plain_bsv", "", "tx1", "", 1700000000, 1700000000, 0,
	)
	if err != nil {
		t.Fatalf("insert wallet_utxo failed: %v", err)
	}

	res, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_bsv_1", "wallet_1", "addr1", "IN", "BSV", "utxo_bsv_1", "tx1", 0,
		1000, 1700000000, 1700000000,
	)
	if err != nil {
		t.Fatalf("insert flow failed: %v", err)
	}
	flowID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("lookup flow id failed: %v", err)
	}

	paymentID, err := dbUpsertChainPaymentWithSettlementCycleDB(db, chainPaymentEntry{
		TxID:                "payment_bsv_1",
		PaymentSubType:      "transfer",
		Status:              "confirmed",
		WalletInputSatoshi:  1000,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    1000,
		BlockHeight:         100,
		OccurredAtUnix:      1700000001,
		Payload:             map[string]any{"seed": true},
	})
	if err != nil {
		t.Fatalf("insert payment failed: %v", err)
	}
	chainCycleID, err := dbGetSettlementCycleByChainPayment(db, paymentID)
	if err != nil {
		t.Fatalf("lookup chain settlement cycle failed: %v", err)
	}

	_, err = db.Exec(`INSERT INTO fact_bsv_consumptions(
		source_flow_id, source_utxo_id, chain_payment_id, settlement_cycle_id,
		state, used_satoshi, occurred_at_unix, confirmed_at_unix, note, payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		flowID, "utxo_bsv_1", paymentID, chainCycleID, "confirmed", 1000, 1700000001, 1700000001, "bsv consume", "{}",
	)
	if err != nil {
		t.Fatalf("insert bsv consumption failed: %v", err)
	}

	_, err = db.Exec(`INSERT INTO fact_bsv_consumptions(
		source_flow_id, source_utxo_id, chain_payment_id, settlement_cycle_id,
		state, used_satoshi, occurred_at_unix, confirmed_at_unix, note, payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		flowID, "utxo_bsv_1", paymentID, chainCycleID, "confirmed", 1000, 1700000001, 1700000001, "dup", "{}",
	)
	if err == nil {
		t.Fatal("duplicate bsv consumption should fail")
	}

	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state,
		allocation_class, allocation_reason, created_txid, spent_txid,
		created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"utxo_tok_1", "wallet_2", "addr2", "tx2", 0, 1, "confirmed",
		"unknown", "", "tx2", "", 1700000002, 1700000002, 0,
	)
	if err != nil {
		t.Fatalf("insert token utxo failed: %v", err)
	}
	res, err = db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id, wallet_id, address, direction, asset_kind, utxo_id, txid, vout,
		amount_satoshi, occurred_at_unix, updated_at_unix, token_id, quantity_text
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"flow_tok_1", "wallet_2", "addr2", "IN", "BSV20", "utxo_tok_1", "tx2", 0,
		1, 1700000002, 1700000002, "token_1", "10",
	)
	if err != nil {
		t.Fatalf("insert token flow failed: %v", err)
	}
	tokenFlowID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("lookup token flow id failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
		pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_pool_1", "2of3", "", "", "", "", 1000, 0, 0, 0, "", "active", 1700000002, 1700000002,
	); err != nil {
		t.Fatalf("insert pool session failed: %v", err)
	}
	res, err = db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id,pool_session_id,allocation_no,allocation_kind,event_kind,sequence_num,state,direction,
		amount_satoshi,purpose,note,msg_id,cycle_index,payee_amount_after,payer_amount_after,txid,tx_hex,
		gateway_pubkey_hex,created_at_unix,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"alloc_tok_1", "sess_pool_1", 1, "pay", PoolFactEventKindPoolEvent, 1, "confirmed", "",
		1000, "pay", "token allocation", "", 0, 1000, 0, "tx_tok_1", "hex_tok_1",
		"", 1700000002, "{}",
	)
	if err != nil {
		t.Fatalf("insert pool event failed: %v", err)
	}
	poolEventID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("lookup pool event id failed: %v", err)
	}
	tokenCycleID := seedAssetFactSettlementCycle(t, db, "pool", poolEventID, 1700000002)

	_, err = db.Exec(`INSERT INTO fact_token_consumptions(
		source_flow_id, source_utxo_id, token_id, token_standard, pool_allocation_id, settlement_cycle_id,
		state, used_quantity_text, occurred_at_unix, confirmed_at_unix, note, payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenFlowID, "utxo_tok_1", "token_1", "BSV20", poolEventID, tokenCycleID, "confirmed", "10", 1700000002, 1700000002, "token consume", "{}",
	)
	if err != nil {
		t.Fatalf("insert token consumption failed: %v", err)
	}

	_, err = db.Exec(`INSERT INTO fact_token_consumptions(
		source_flow_id, source_utxo_id, token_id, token_standard, pool_allocation_id, settlement_cycle_id,
		state, used_quantity_text, occurred_at_unix, confirmed_at_unix, note, payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenFlowID, "utxo_tok_1", "token_1", "BSV20", poolEventID, tokenCycleID, "confirmed", "10", 1700000002, 1700000002, "dup", "{}",
	)
	if err == nil {
		t.Fatal("duplicate token consumption should fail")
	}
}

func TestInitIndexDB_RejectsLegacyAssetSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	legacyName := legacyTableName("fact", "asset", "consumptions")
	if _, err := db.Exec(`CREATE TABLE ` + legacyName + `(
		id INTEGER PRIMARY KEY AUTOINCREMENT
	)`); err != nil {
		t.Fatalf("create legacy table failed: %v", err)
	}

	if err := initIndexDB(db); err == nil {
		t.Fatal("expected initIndexDB to reject legacy asset schema")
	} else if !strings.Contains(err.Error(), "rebuild DB") {
		t.Fatalf("unexpected error: %v", err)
	}
}
