package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func openSchemaTestDB(t *testing.T) *sql.DB {
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

func createLegacyFinanceTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// 创建真实老库结构（第五轮迭代前），用于测试迁移逻辑
	// 注意：不包含新口径字段（source_type/source_id/accounting_scene/accounting_subtype）
	stmts := []string{
		`CREATE TABLE settle_businesses(
			business_id TEXT PRIMARY KEY,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE settle_process_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			process_id TEXT NOT NULL,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			event_type TEXT NOT NULL,
			status TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy table failed: %v", err)
		}
	}
}

func createLegacyDirectTransferPoolTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE proc_direct_transfer_pools(
		session_id TEXT PRIMARY KEY,
		deal_id TEXT NOT NULL,
		buyer_pubkey_hex TEXT NOT NULL,
		seller_pubkey_hex TEXT NOT NULL,
		arbiter_pubkey_hex TEXT NOT NULL,
		pool_amount INTEGER NOT NULL,
		spend_tx_fee INTEGER NOT NULL,
		sequence_num INTEGER NOT NULL,
		seller_amount INTEGER NOT NULL,
		buyer_amount INTEGER NOT NULL,
		current_tx_hex TEXT NOT NULL,
		base_tx_hex TEXT NOT NULL,
		base_txid TEXT NOT NULL,
		status TEXT NOT NULL,
		fee_rate_sat_byte REAL NOT NULL,
		lock_blocks INTEGER NOT NULL,
		created_at_unix INTEGER NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create legacy proc_direct_transfer_pools failed: %v", err)
	}
}

func TestInitIndexDB_MigratesFinanceColumns(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	createLegacyFinanceTables(t, db)

	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_legacy_1", "fee_pool", "open", "client:self", "pool:peer", "sess_1", "posted", 1700000001, "idem_biz_1", "旧业务", "{}",
	); err != nil {
		t.Fatalf("insert legacy settle_businesses failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_process_events(
		process_id,scene_type,scene_subtype,event_type,status,ref_id,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"proc_legacy_1", "fee_pool", "cycle_pay", "update", "applied", "sess_1", 1700000002, "idem_evt_1", "旧事件", "{}",
	); err != nil {
		t.Fatalf("insert legacy settle_process_events failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantCols := []string{"source_type", "source_id", "accounting_scene", "accounting_subtype"}
	for _, table := range []string{"settle_businesses", "settle_process_events"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		for _, col := range wantCols {
			if _, ok := cols[col]; !ok {
				t.Fatalf("missing column %s on %s", col, table)
			}
		}
	}

	var businessSourceType, businessSourceID, businessAccountingScene, businessAccountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM settle_businesses WHERE business_id=?`,
		"biz_legacy_1",
	).Scan(&businessSourceType, &businessSourceID, &businessAccountingScene, &businessAccountingSubtype); err != nil {
		t.Fatalf("query migrated settle_businesses failed: %v", err)
	}
	if businessSourceType != "" || businessSourceID != "" || businessAccountingScene != "" || businessAccountingSubtype != "" {
		t.Fatalf("unexpected migrated settle_businesses defaults: %q %q %q %q", businessSourceType, businessSourceID, businessAccountingScene, businessAccountingSubtype)
	}

	var eventSourceType, eventSourceID, eventAccountingScene, eventAccountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM settle_process_events WHERE process_id=?`,
		"proc_legacy_1",
	).Scan(&eventSourceType, &eventSourceID, &eventAccountingScene, &eventAccountingSubtype); err != nil {
		t.Fatalf("query migrated settle_process_events failed: %v", err)
	}
	if eventSourceType != "" || eventSourceID != "" || eventAccountingScene != "" || eventAccountingSubtype != "" {
		t.Fatalf("unexpected migrated settle_process_events defaults: %q %q %q %q", eventSourceType, eventSourceID, eventAccountingScene, eventAccountingSubtype)
	}
}

func TestInitIndexDB_FreshSchemaKeepsFinanceColumns(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	wantCols := []string{"source_type", "source_id", "accounting_scene", "accounting_subtype"}
	for _, table := range []string{"settle_businesses", "settle_process_events"} {
		cols, err := tableColumns(db, table)
		if err != nil {
			t.Fatalf("inspect %s columns failed: %v", table, err)
		}
		for _, col := range wantCols {
			if _, ok := cols[col]; !ok {
				t.Fatalf("missing column %s on %s", col, table)
			}
		}
	}

	// 第六次迭代：只测试主口径字段
	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_fresh_1", "settlement_cycle", "1", "c2c_transfer", "open", "client:self", "pool:peer", "posted", 1700000003, "idem_biz_2", "新业务", "{}",
	); err != nil {
		t.Fatalf("insert fresh settle_businesses failed: %v", err)
	}

	var sourceType, sourceID, accountingScene, accountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM settle_businesses WHERE business_id=?`,
		"biz_fresh_1",
	).Scan(&sourceType, &sourceID, &accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query fresh settle_businesses failed: %v", err)
	}
	if sourceType != "settlement_cycle" || sourceID != "1" || accountingScene != "c2c_transfer" || accountingSubtype != "open" {
		t.Fatalf("unexpected fresh settle_businesses values: %q %q %q %q", sourceType, sourceID, accountingScene, accountingSubtype)
	}
}

func TestInitIndexDB_BackfillsLegacyPoolAllocationFinanceSources(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE fact_pool_allocations(
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
	)`); err != nil {
		t.Fatalf("create legacy pool allocation table failed: %v", err)
	}
	sessionID := "sess_backfill_pool_1"
	allocationID := "poolalloc_" + sessionID + "_open_1"
	insertLegacyPoolSession(t, db, sessionID)
	insertLegacyPoolAllocation(t, db, allocationID, sessionID, 1, "open", 1, 0, 1000, "tx_backfill_pool_1", "hex_backfill_pool_1", 1700000101)
	if _, err := db.Exec(`INSERT INTO fact_settlement_cycles(
		cycle_id,channel,state,pool_session_event_id,gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,
		cycle_index,occurred_at_unix,confirmed_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"cycle_pool_backfill_1", "pool", "confirmed", 1, 1000, 0, 1000, 1, 1700000101, 1700000101, "legacy pool settlement", "{}",
	); err != nil {
		t.Fatalf("seed legacy settlement cycle failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_pool_backfill_1", "pool_allocation", allocationID, "fee_pool", "open", "client:self", "pool:peer", "posted", 1700000102, "idem_pool_backfill_1", "legacy pool allocation", "{}",
	); err != nil {
		t.Fatalf("seed legacy settle_businesses failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_process_events(
		process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"proc_pool_backfill_1", "pool_allocation", allocationID, "fee_pool", "open", "accounting", "applied", 1700000102, "idem_pool_evt_backfill_1", "legacy pool allocation event", "{}",
	); err != nil {
		t.Fatalf("seed legacy settle_process_events failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	var expectedID int64
	if err := db.QueryRow(`SELECT id FROM fact_pool_session_events WHERE allocation_id=?`, allocationID).Scan(&expectedID); err != nil {
		t.Fatalf("lookup pool allocation id failed: %v", err)
	}
	settlementCycleID, err := dbGetSettlementCycleByPoolEvent(db, expectedID)
	if err != nil {
		t.Fatalf("lookup pool settlement cycle id failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", settlementCycleID)

	var businessSourceType, businessSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_businesses WHERE business_id=?`, "biz_pool_backfill_1").Scan(&businessSourceType, &businessSourceID); err != nil {
		t.Fatalf("query backfilled settle_businesses failed: %v", err)
	}
	if businessSourceType != "settlement_cycle" || businessSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled business source: %s %s", businessSourceType, businessSourceID)
	}

	var eventSourceType, eventSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_process_events WHERE process_id=?`, "proc_pool_backfill_1").Scan(&eventSourceType, &eventSourceID); err != nil {
		t.Fatalf("query backfilled settle_process_events failed: %v", err)
	}
	if eventSourceType != "settlement_cycle" || eventSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled process source: %s %s", eventSourceType, eventSourceID)
	}
}

func TestInitIndexDB_BackfillsLegacyFeePoolFinanceSources(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	txid := "tx_fee_pool_backfill_1"
	chainPaymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_in",
		Status:              "posted",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 5000,
		NetAmountSatoshi:    5000,
		OccurredAtUnix:      1700000501,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             map[string]any{"txid": txid},
	})
	if err != nil {
		t.Fatalf("seed chain payment failed: %v", err)
	}
	settlementCycleID, err := dbGetSettlementCycleByChainPayment(db, chainPaymentID)
	if err != nil {
		t.Fatalf("lookup settlement cycle failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", settlementCycleID)

	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_fee_pool_backfill_1", "process", "fee_pool", txid, "fee_pool", "open", "client:self", "gateway:gw1", "posted", 1700000502, "idem_fee_pool_biz_1", "legacy fee pool business", "{}",
	); err != nil {
		t.Fatalf("seed legacy fee_pool business failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_process_events(
		process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"proc_fee_pool_backfill_1", "fee_pool", txid, "fee_pool", "cycle_pay", "update", "applied", 1700000503, "idem_fee_pool_evt_1", "legacy fee pool event", "{}",
	); err != nil {
		t.Fatalf("seed legacy fee_pool process failed: %v", err)
	}

	report, err := backfillLegacyFinanceSources(db)
	if err != nil {
		t.Fatalf("backfillLegacyFinanceSources failed: %v", err)
	}
	if report.RewrittenRows < 2 {
		t.Fatalf("expected fee_pool rows to be rewritten, got %d", report.RewrittenRows)
	}
	if report.UnmappedRows != 0 {
		t.Fatalf("unexpected unmapped rows: %d", report.UnmappedRows)
	}

	var businessSourceType, businessSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_businesses WHERE business_id=?`, "biz_fee_pool_backfill_1").Scan(&businessSourceType, &businessSourceID); err != nil {
		t.Fatalf("query backfilled fee_pool business failed: %v", err)
	}
	if businessSourceType != "settlement_cycle" || businessSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled fee_pool business source: %s %s", businessSourceType, businessSourceID)
	}

	var eventSourceType, eventSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_process_events WHERE process_id=?`, "proc_fee_pool_backfill_1").Scan(&eventSourceType, &eventSourceID); err != nil {
		t.Fatalf("query backfilled fee_pool process failed: %v", err)
	}
	if eventSourceType != "settlement_cycle" || eventSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled fee_pool process source: %s %s", eventSourceType, eventSourceID)
	}

	report2, err := backfillLegacyFinanceSources(db)
	if err != nil {
		t.Fatalf("second backfillLegacyFinanceSources failed: %v", err)
	}
	if report2.RewrittenRows != 0 || report2.UnmappedRows != 0 {
		t.Fatalf("expected second backfill to be idempotent, got rewritten=%d unmapped=%d", report2.RewrittenRows, report2.UnmappedRows)
	}
}

func TestInitIndexDB_BackfillsLegacyWalletChainFinanceSourcesFromBreakdown(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	txid := "tx_backfill_wallet_chain_1"
	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_wallet_chain_"+txid, "wallet_chain", txid, "wallet_transfer", "external_in", "external:unknown", "wallet:self", "posted", 1700000201, "wallet_chain:"+txid, "legacy wallet chain business", `{"txid":"`+txid+`"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet business failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_process_events(
		process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"proc_wallet_chain_"+txid, "wallet_chain", txid, "wallet_transfer", "external_in", "accounting", "applied", 1700000202, "wallet_chain_event:"+txid, "legacy wallet chain process", `{"txid":"`+txid+`"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet process failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_tx_breakdown(
		business_id,txid,tx_role,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_wallet_chain_"+txid, txid, "external_in", 0, 0, 5000, 0, 0, 0, 5000, 1700000201, "legacy wallet chain breakdown", `{"txid":"`+txid+`"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet breakdown failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	var chainPaymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&chainPaymentID); err != nil {
		t.Fatalf("query chain_payment failed: %v", err)
	}
	if chainPaymentID <= 0 {
		t.Fatalf("unexpected chain_payment id: %d", chainPaymentID)
	}

	var paymentSubType, status string
	var walletInput, walletOutput, netAmount int64
	if err := db.QueryRow(
		`SELECT payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi
		   FROM fact_chain_payments WHERE id=?`,
		chainPaymentID,
	).Scan(&paymentSubType, &status, &walletInput, &walletOutput, &netAmount); err != nil {
		t.Fatalf("query chain_payment fields failed: %v", err)
	}
	if paymentSubType != "external_in" {
		t.Fatalf("unexpected payment_subtype: %s", paymentSubType)
	}
	if status != "posted" {
		t.Fatalf("unexpected status: %s", status)
	}
	if walletInput != 0 || walletOutput != 5000 || netAmount != 5000 {
		t.Fatalf("unexpected chain_payment amounts: input=%d output=%d net=%d", walletInput, walletOutput, netAmount)
	}

	settlementCycleID, err := dbGetSettlementCycleByChainPayment(db, chainPaymentID)
	if err != nil {
		t.Fatalf("lookup wallet settlement cycle id failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", settlementCycleID)
	var businessSourceType, businessSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_businesses WHERE business_id=?`, "biz_wallet_chain_"+txid).Scan(&businessSourceType, &businessSourceID); err != nil {
		t.Fatalf("query backfilled wallet business failed: %v", err)
	}
	if businessSourceType != "settlement_cycle" || businessSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled wallet business source: %s %s", businessSourceType, businessSourceID)
	}

	var eventSourceType, eventSourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_process_events WHERE process_id=?`, "proc_wallet_chain_"+txid).Scan(&eventSourceType, &eventSourceID); err != nil {
		t.Fatalf("query backfilled wallet process failed: %v", err)
	}
	if eventSourceType != "settlement_cycle" || eventSourceID != wantSourceID {
		t.Fatalf("unexpected backfilled wallet process source: %s %s", eventSourceType, eventSourceID)
	}
}

func TestInitIndexDB_BackfillsLegacyWalletChainFinanceSourcesFromBusinessPayload(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	txid := "tx_backfill_wallet_chain_payload_1"
	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_wallet_chain_"+txid, "wallet_chain", txid, "wallet_transfer", "external_out", "wallet:self", "external:unknown", "posted", 1700000301, "wallet_chain:"+txid, "legacy wallet chain business payload", `{"wallet_input_satoshi":7200,"wallet_output_satoshi":0,"net_amount_satoshi":-7200,"payment_subtype":"external_out"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet business failed: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}

	var chainPaymentID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&chainPaymentID); err != nil {
		t.Fatalf("query chain_payment failed: %v", err)
	}

	var paymentSubType, status string
	var walletInput, walletOutput, netAmount int64
	if err := db.QueryRow(
		`SELECT payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi
		   FROM fact_chain_payments WHERE id=?`,
		chainPaymentID,
	).Scan(&paymentSubType, &status, &walletInput, &walletOutput, &netAmount); err != nil {
		t.Fatalf("query chain_payment fields failed: %v", err)
	}
	if paymentSubType != "external_out" {
		t.Fatalf("unexpected payment_subtype: %s", paymentSubType)
	}
	if status != "posted" {
		t.Fatalf("unexpected status: %s", status)
	}
	if walletInput != 7200 || walletOutput != 0 || netAmount != -7200 {
		t.Fatalf("unexpected chain_payment amounts: input=%d output=%d net=%d", walletInput, walletOutput, netAmount)
	}

	settlementCycleID, err := dbGetSettlementCycleByChainPayment(db, chainPaymentID)
	if err != nil {
		t.Fatalf("lookup wallet settlement cycle id failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", settlementCycleID)
	var sourceType, sourceID string
	if err := db.QueryRow(`SELECT source_type,source_id FROM settle_businesses WHERE business_id=?`, "biz_wallet_chain_"+txid).Scan(&sourceType, &sourceID); err != nil {
		t.Fatalf("query backfilled wallet business failed: %v", err)
	}
	if sourceType != "settlement_cycle" || sourceID != wantSourceID {
		t.Fatalf("unexpected backfilled source: %s %s", sourceType, sourceID)
	}
}

func TestInitIndexDB_BackfillsLegacyWalletChainFinanceSourcesConflictFails(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	txid := "tx_backfill_wallet_chain_conflict_1"
	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_wallet_chain_"+txid, "wallet_chain", txid, "wallet_transfer", "external_in", "external:unknown", "wallet:self", "posted", 1700000401, "wallet_chain:"+txid, "legacy wallet chain business conflict", `{"wallet_input_satoshi":0,"wallet_output_satoshi":5000,"net_amount_satoshi":5000,"payment_subtype":"external_in"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet business failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_tx_breakdown(
		business_id,txid,tx_role,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_wallet_chain_"+txid, txid, "external_out", 7200, 0, 0, 7200, 0, 7200, 0, 1700000401, "legacy wallet chain breakdown conflict", `{"txid":"`+txid+`"}`,
	); err != nil {
		t.Fatalf("seed legacy wallet breakdown failed: %v", err)
	}

	if err := initIndexDB(db); err == nil {
		t.Fatal("expected initIndexDB to fail for conflicting wallet_chain facts")
	}
}

func TestInitIndexDB_FinalizesFinTxBreakdown(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	hasOldTable, err := hasTable(db, "settle_business_txs")
	if err != nil {
		t.Fatalf("check settle_business_txs failed: %v", err)
	}
	if hasOldTable {
		t.Fatalf("settle_business_txs should be dropped after finalization")
	}

	cols, err := tableColumns(db, "settle_tx_breakdown")
	if err != nil {
		t.Fatalf("inspect settle_tx_breakdown columns failed: %v", err)
	}
	if _, ok := cols["tx_role"]; !ok {
		t.Fatalf("settle_tx_breakdown missing tx_role")
	}
	if notNull, err := tableColumnNotNull(db, "settle_tx_breakdown", "tx_role"); err != nil {
		t.Fatalf("inspect settle_tx_breakdown tx_role notnull failed: %v", err)
	} else if !notNull {
		t.Fatalf("settle_tx_breakdown.tx_role should be NOT NULL after finalization")
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "settle_tx_breakdown", []string{"business_id", "txid"}); err != nil {
		t.Fatalf("inspect settle_tx_breakdown unique constraint failed: %v", err)
	} else if !unique {
		t.Fatalf("settle_tx_breakdown should keep unique constraint on (business_id, txid)")
	}

	for _, indexName := range []string{
		"idx_settle_tx_breakdown_business",
		"idx_settle_tx_breakdown_txid",
		"idx_settle_tx_breakdown_business_txid",
	} {
		hasIndex, err := tableHasIndex(db, "settle_tx_breakdown", indexName)
		if err != nil {
			t.Fatalf("inspect %s failed: %v", indexName, err)
		}
		if !hasIndex {
			t.Fatalf("missing index %s on settle_tx_breakdown", indexName)
		}
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}
}

func TestInitIndexDB_CreatesFinanceReadIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	// 第六次迭代：只检查主口径索引
	wantIndexes := map[string][]string{
		"settle_businesses": {
			"idx_settle_businesses_source",
			"idx_settle_businesses_accounting",
		},
		"settle_process_events": {
			"idx_settle_process_events_source",
			"idx_settle_process_events_accounting",
		},
	}
	for table, wants := range wantIndexes {
		got, err := tableIndexNamesForTable(db, table)
		if err != nil {
			t.Fatalf("inspect %s indexes failed: %v", table, err)
		}
		for _, want := range wants {
			if !containsString(got, want) {
				t.Fatalf("missing index %s on %s", want, table)
			}
		}
	}
}

func TestFinanceDBLayerRejectsLegacySourceType(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)
	store := newClientDB(db, nil)

	_, err := dbListFinanceBusinesses(context.Background(), store, financeBusinessFilter{
		Limit:      10,
		SourceType: "pool_allocation",
		SourceID:   "1",
	})
	if err == nil {
		t.Fatal("expected dbListFinanceBusinesses to reject legacy source_type")
	}
	if !strings.Contains(err.Error(), "source_type must be settlement_cycle") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = dbListFinanceProcessEvents(context.Background(), store, financeProcessEventFilter{
		Limit:      10,
		SourceType: "fee_pool",
		SourceID:   "1",
	})
	if err == nil {
		t.Fatal("expected dbListFinanceProcessEvents to reject legacy source_type")
	}
	if !strings.Contains(err.Error(), "source_type must be settlement_cycle") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureNoLegacyFinanceSourceRowsFailsOnLegacyRows(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_legacy_guard_1", "process", "fee_pool", "tx_guard_1", "fee_pool", "open", "client:self", "gateway:gw1", "posted", 1700000601, "idem_guard_1", "legacy row", "{}",
	); err != nil {
		t.Fatalf("insert legacy business failed: %v", err)
	}

	if err := ensureNoLegacyFinanceSourceRows(db); err == nil {
		t.Fatal("expected legacy source guard to fail")
	}
}

func TestEnsureNoLegacyFinanceSourceRowsFailsOnBothTables(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO settle_businesses(
		business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"biz_legacy_guard_both_1", "process", "fee_pool", "tx_guard_both_1", "fee_pool", "open", "client:self", "gateway:gw1", "posted", 1700000606, "idem_guard_both_biz", "legacy row biz", "{}",
	); err != nil {
		t.Fatalf("insert legacy business failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO settle_process_events(
		process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"proc_legacy_guard_both_1", "wallet_chain", "tx_guard_both_2", "wallet_transfer", "open", "state_change", "applied", 1700000607, "idem_guard_both_evt", "legacy row evt", "{}",
	); err != nil {
		t.Fatalf("insert legacy process event failed: %v", err)
	}

	err := ensureNoLegacyFinanceSourceRows(db)
	if err == nil {
		t.Fatal("expected legacy source guard to fail when both tables have residual rows")
	}
	if !strings.Contains(err.Error(), "settle_businesses") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSettlementCycleWrappersPopulateSettlementSource(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)

	if err := dbAppendSettlementCycleFinBusiness(db, 77, finBusinessEntry{
		BusinessID:        "biz_settlement_guard_1",
		BusinessRole:      "process",
		AccountingScene:   "wallet_transfer",
		AccountingSubType: "open",
		FromPartyID:       "client:self",
		ToPartyID:         "gateway:peer",
		Status:            "posted",
		OccurredAtUnix:    1700000602,
		IdempotencyKey:    "idem_settlement_guard_1",
		Note:              "legacy business",
	}); err != nil {
		t.Fatalf("settlement cycle business write failed: %v", err)
	}

	if err := dbAppendSettlementCycleFinProcessEvent(db, 77, finProcessEventEntry{
		ProcessID:         "proc_settlement_guard_1",
		AccountingScene:   "wallet_transfer",
		AccountingSubType: "open",
		EventType:         "state_change",
		Status:            "applied",
		OccurredAtUnix:    1700000603,
		IdempotencyKey:    "idem_settlement_guard_evt_1",
		Note:              "legacy event",
	}); err != nil {
		t.Fatalf("settlement cycle process event write failed: %v", err)
	}

	var businessSourceType, businessSourceID string
	if err := db.QueryRow(
		`SELECT source_type,source_id FROM settle_businesses WHERE business_id=?`,
		"biz_settlement_guard_1",
	).Scan(&businessSourceType, &businessSourceID); err != nil {
		t.Fatalf("query settlement cycle business failed: %v", err)
	}
	if businessSourceType != "settlement_cycle" || businessSourceID != "77" {
		t.Fatalf("unexpected settlement cycle business source: %s %s", businessSourceType, businessSourceID)
	}

	var processSourceType, processSourceID string
	if err := db.QueryRow(
		`SELECT source_type,source_id FROM settle_process_events WHERE process_id=?`,
		"proc_settlement_guard_1",
	).Scan(&processSourceType, &processSourceID); err != nil {
		t.Fatalf("query settlement cycle process event failed: %v", err)
	}
	if processSourceType != "settlement_cycle" || processSourceID != "77" {
		t.Fatalf("unexpected settlement cycle process source: %s %s", processSourceType, processSourceID)
	}
}

func TestEnsureNoSharedEntryDirectCallsInSettlementWriteFiles(t *testing.T) {
	t.Parallel()

	files := []string{
		"wallet_accounting.go",
		"db_process_writes.go",
	}
	allowed := map[string]map[string]bool{
		"db_process_writes.go": {
			"func dbAppendFinBusiness(db sqlConn, e finBusinessEntry) error {":         true,
			"func dbAppendFinProcessEvent(db sqlConn, e finProcessEventEntry) error {": true,
			"return dbAppendFinBusiness(db, e)":                                        true,
			"return dbAppendFinProcessEvent(db, e)":                                    true,
		},
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s failed: %v", file, err)
		}
		lines := strings.Split(string(data), "\n")
		for idx, line := range lines {
			if !strings.Contains(line, "dbAppendFinBusiness(") && !strings.Contains(line, "dbAppendFinProcessEvent(") {
				continue
			}
			trimmed := strings.TrimSpace(line)
			if allowedLines, ok := allowed[file]; ok && allowedLines[trimmed] {
				continue
			}
			t.Fatalf("forbidden shared finance write call in %s:%d: %s", file, idx+1, trimmed)
		}
	}
}

func TestInitIndexDB_IsIdempotentOnRepeatedRun(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("first initIndexDB failed: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}
}

func tableIndexNamesForTable(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA index_list(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 8)
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
