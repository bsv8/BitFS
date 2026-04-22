package clientapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

)

func openSchemaTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_fk=1")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	return db
}

func TestInitIndexDB_FreshSchemaKeepsFinanceColumns(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	wantCols := []string{"source_type", "source_id", "accounting_scene", "accounting_subtype"}
	for _, table := range []string{"order_settlements", "order_settlement_events"} {
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

	paymentAttemptCols, err := tableColumns(db, "fact_settlement_payment_attempts")
	if err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts columns failed: %v", err)
	}
	for _, col := range []string{"source_type", "source_id", "payment_attempt_id"} {
		if _, ok := paymentAttemptCols[col]; !ok {
			t.Fatalf("missing column %s on fact_settlement_payment_attempts", col)
		}
	}
	if notNull, err := tableColumnNotNull(db, "fact_settlement_payment_attempts", "source_type"); err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts source_type notnull failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_settlement_payment_attempts.source_type should be NOT NULL")
	}
	if hasIndex, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_payment_attempts", []string{"source_type", "source_id"}); err != nil {
		t.Fatalf("inspect fact_settlement_payment_attempts unique index failed: %v", err)
	} else if !hasIndex {
		t.Fatal("fact_settlement_payment_attempts should keep unique index on source_type/source_id")
	}

	// 第六次迭代：只测试主口径字段
	if _, err := db.Exec(`INSERT INTO order_settlements(
		settlement_id,order_id,settlement_no,business_role,source_type,source_id,accounting_scene,accounting_subtype,settlement_method,status,settlement_status,from_party_id,to_party_id,target_type,target_id,note,payload_json,settlement_payload_json,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"set_fresh_1", "ord_fresh_1", 1, "formal", "settlement_payment_attempt", "1", "c2c_transfer", "open", "chain", "posted", "posted", "client:self", "pool:peer", "", "", "新业务", "{}", "{}", 1700000003, 1700000003,
	); err != nil {
		t.Fatalf("insert fresh order_settlements failed: %v", err)
	}

	var sourceType, sourceID, accountingScene, accountingSubtype string
	if err := db.QueryRow(
		`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM order_settlements WHERE order_id=?`,
		"ord_fresh_1",
	).Scan(&sourceType, &sourceID, &accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query fresh order_settlements failed: %v", err)
	}
	if sourceType != "settlement_payment_attempt" || sourceID != "1" || accountingScene != "c2c_transfer" || accountingSubtype != "open" {
		t.Fatalf("unexpected fresh order_settlements values: %q %q %q %q", sourceType, sourceID, accountingScene, accountingSubtype)
	}
}

func TestInitIndexDB_CreatesFinanceReadIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	// 第六次迭代：只检查主口径索引
	wantIndexes := map[string][]string{
		"order_settlements": {
			"idx_order_settlements_order",
			"idx_order_settlements_status",
			"idx_order_settlements_method",
			"idx_order_settlements_target",
		},
		"order_settlement_events": {
			"idx_order_settlement_events_settlement",
			"idx_order_settlement_events_type",
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

func TestFinanceDBLayerRejectsHistoricalSourceType(t *testing.T) {
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
		t.Fatal("expected dbListFinanceBusinesses to reject non-settlement_payment_attempt source_type")
	}
	if !strings.Contains(err.Error(), "source_type must be settlement_payment_attempt, pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = dbListFinanceProcessEvents(context.Background(), store, financeProcessEventFilter{
		Limit:      10,
		SourceType: "pool_allocation",
		SourceID:   "1",
	})
	if err == nil {
		t.Fatal("expected dbListFinanceProcessEvents to reject non-settlement_payment_attempt source_type")
	}
	if !strings.Contains(err.Error(), "source_type must be settlement_payment_attempt, pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestNormalizeFinanceQuerySourceAllowsChainWalletTypes 验证查询层显式接受 chain_bsv / chain_token。
func TestNormalizeFinanceQuerySourceAllowsChainWalletTypes(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)

	sourceType, sourceID, err := normalizeFinanceQuerySource(context.Background(), store, "chain_direct_pay", "tx_bsv_1")
	if err != nil {
		t.Fatalf("normalize chain_bsv failed: %v", err)
	}
	if sourceType != "chain_direct_pay" || sourceID != "tx_bsv_1" {
		t.Fatalf("unexpected chain_bsv normalize result: %s %s", sourceType, sourceID)
	}

	sourceType, sourceID, err = normalizeFinanceQuerySource(context.Background(), store, "chain_direct_pay", "tx_token_1")
	if err != nil {
		t.Fatalf("normalize chain_token failed: %v", err)
	}
	if sourceType != "chain_direct_pay" || sourceID != "tx_token_1" {
		t.Fatalf("unexpected chain_token normalize result: %s %s", sourceType, sourceID)
	}

	if sourceType, sourceID, err = normalizeFinanceQuerySource(context.Background(), store, "chain_quote_pay", "42"); err != nil {
		t.Fatalf("normalize chain_quote_pay failed: %v", err)
	} else if sourceType != "chain_quote_pay" || sourceID != "42" {
		t.Fatalf("unexpected chain_quote_pay normalize result: %s %s", sourceType, sourceID)
	}
	if sourceType, sourceID, err = normalizeFinanceQuerySource(context.Background(), store, "pool_session_quote_pay", "7"); err != nil {
		t.Fatalf("normalize pool_session_quote_pay failed: %v", err)
	} else if sourceType != "pool_session_quote_pay" || sourceID != "7" {
		t.Fatalf("unexpected pool_session_quote_pay normalize result: %s %s", sourceType, sourceID)
	}
}

func TestSettlementPaymentAttemptWrappersPopulateSettlementSource(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)

	if err :=  store.WriteEntTx(context.Background(), func(tx EntWriteRoot) error {
		return dbAppendSettlementPaymentAttemptFinBusiness(context.Background(), tx, 77, finBusinessEntry{
			OrderID:           "biz_settlement_guard_1",
			BusinessRole:      "process",
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "open",
			FromPartyID:       "client:self",
			ToPartyID:         "gateway:peer",
			Status:            "posted",
			OccurredAtUnix:    1700000602,
			IdempotencyKey:    "idem_settlement_guard_1",
			Note:              "历史回填业务",
		})
	}); err != nil {
		t.Fatalf("settlement payment attempt business write failed: %v", err)
	}

	if err :=  store.WriteEntTx(context.Background(), func(tx EntWriteRoot) error {
		return dbAppendSettlementPaymentAttemptFinProcessEvent(context.Background(), tx, 77, finProcessEventEntry{
			ProcessID:         "proc_settlement_guard_1",
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "open",
			EventType:         "state_change",
			Status:            "applied",
			OccurredAtUnix:    1700000603,
			IdempotencyKey:    "idem_settlement_guard_evt_1",
			Note:              "历史回填事件",
		})
	}); err != nil {
		t.Fatalf("settlement payment attempt process event write failed: %v", err)
	}

	var businessSourceType, businessSourceID string
	if err := db.QueryRow(
		`SELECT source_type,source_id FROM order_settlements WHERE order_id=?`,
		"biz_settlement_guard_1",
	).Scan(&businessSourceType, &businessSourceID); err != nil {
		t.Fatalf("query settlement payment attempt business failed: %v", err)
	}
	if businessSourceType != "settlement_payment_attempt" || businessSourceID != "77" {
		t.Fatalf("unexpected settlement payment attempt business source: %s %s", businessSourceType, businessSourceID)
	}

	var processSourceType, processSourceID string
	if err := db.QueryRow(
		`SELECT source_type,source_id FROM order_settlement_events WHERE process_id=?`,
		"proc_settlement_guard_1",
	).Scan(&processSourceType, &processSourceID); err != nil {
		t.Fatalf("query settlement payment attempt process event failed: %v", err)
	}
	if processSourceType != "settlement_payment_attempt" || processSourceID != "77" {
		t.Fatalf("unexpected settlement payment attempt process source: %s %s", processSourceType, processSourceID)
	}
}

// TestBizOrderPayBSVProcessEventUsesSourceSettlementID 验证钱包主流程事件不会把 process_id 误当 settlement_id。
func TestBizOrderPayBSVProcessEventUsesSourceSettlementID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)

	if err := dbAppendFinBusiness(context.Background(), newClientDB(db, nil), finBusinessEntry{
		OrderID:           "biz_wallet_guard_1",
		BusinessRole:      "process",
		SourceType:        "front_order",
		SourceID:          "fo_wallet_guard_1",
		AccountingScene:   "wallet_transfer",
		AccountingSubType: "pay_bsv",
		FromPartyID:       "client:self",
		ToPartyID:         "gateway:peer",
		Status:            "posted",
		OccurredAtUnix:    1700000610,
		IdempotencyKey:    "idem_wallet_guard_1",
		Note:              "钱包主流程业务",
		Payload:           map[string]any{"step": 1},
		SettlementID:      "set_wallet_guard_1",
		SettlementMethod:  "chain",
		SettlementStatus:  "posted",
	}); err != nil {
		t.Fatalf("business write failed: %v", err)
	}

	if err :=  store.WriteEntTx(context.Background(), func(tx EntWriteRoot) error {
		return dbAppendFinProcessEvent(context.Background(), tx, finProcessEventEntry{
			ProcessID:         "proc_wallet_guard_1",
			SourceType:        "biz_order_pay_bsv",
			SourceID:          "set_wallet_guard_1",
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "pay_bsv",
			EventType:         "wallet_transfer_submit",
			Status:            "submitted",
			OccurredAtUnix:    1700000611,
			IdempotencyKey:    "idem_wallet_guard_evt_1",
			Note:              "钱包主流程事件",
			Payload:           map[string]any{"step": 2},
		})
	}); err != nil {
		t.Fatalf("process event write failed: %v", err)
	}

	var settlementID, sourceType, sourceID string
	if err := db.QueryRow(
		`SELECT settlement_id,source_type,source_id FROM order_settlement_events WHERE process_id=?`,
		"proc_wallet_guard_1",
	).Scan(&settlementID, &sourceType, &sourceID); err != nil {
		t.Fatalf("query process event failed: %v", err)
	}
	if settlementID != "set_wallet_guard_1" {
		t.Fatalf("expected settlement_id set_wallet_guard_1, got %s", settlementID)
	}
	if sourceType != "biz_order_pay_bsv" || sourceID != "set_wallet_guard_1" {
		t.Fatalf("unexpected process source: %s %s", sourceType, sourceID)
	}
}

// TestSettlementPaymentAttemptWriteGuard_NoSharedEntryDirectCalls 是收官固定检查：
// 这里只允许结算写入口经过专用封装，不允许新增共享入口直调或手填来源字段。
func TestSettlementPaymentAttemptWriteGuard_NoSharedEntryDirectCalls(t *testing.T) {
	t.Parallel()

	files := []string{
		"db_process_writes.go",
	}
	allowed := map[string]map[string]bool{
		"db_process_writes.go": {
			"func dbAppendFinBusiness(ctx context.Context, store *clientDB, e finBusinessEntry) error {":    true,
			"func dbAppendFinProcessEvent(ctx context.Context, tx EntWriteRoot, e finProcessEventEntry) error {": true,
			"return dbAppendFinBusinessTx(ctx, tx, e)":                                                      true,
			"return dbAppendFinProcessEvent(ctx, tx, e)":                                                    true,
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

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s failed: %v", file, err)
		}
		lines := strings.Split(string(data), "\n")
		for idx, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.Contains(trimmed, "SourceType:") || strings.Contains(trimmed, "SourceID:") {
				t.Fatalf("forbidden manual settlement source field in %s:%d: %s", file, idx+1, trimmed)
			}
		}
	}
}

func TestInitIndexDB_IsIdempotentOnRepeatedRun(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("first schema init failed: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("second schema init failed: %v", err)
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
