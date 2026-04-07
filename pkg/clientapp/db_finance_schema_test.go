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

	cycleCols, err := tableColumns(db, "fact_settlement_cycles")
	if err != nil {
		t.Fatalf("inspect fact_settlement_cycles columns failed: %v", err)
	}
	for _, col := range []string{"pool_session_id", "pool_session_event_id", "chain_payment_id", "cycle_id"} {
		if _, ok := cycleCols[col]; !ok {
			t.Fatalf("missing column %s on fact_settlement_cycles", col)
		}
	}
	if notNull, err := tableColumnNotNull(db, "fact_settlement_cycles", "pool_session_id"); err != nil {
		t.Fatalf("inspect fact_settlement_cycles pool_session_id notnull failed: %v", err)
	} else if !notNull {
		t.Fatal("fact_settlement_cycles.pool_session_id should be NOT NULL")
	}
	if hasIndex, err := tableHasIndex(db, "fact_settlement_cycles", "idx_fact_settlement_cycles_pool_session"); err != nil {
		t.Fatalf("inspect fact_settlement_cycles pool_session index failed: %v", err)
	} else if !hasIndex {
		t.Fatal("fact_settlement_cycles should keep index on pool_session_id")
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
		t.Fatal("expected dbListFinanceBusinesses to reject historical source_type")
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
		t.Fatal("expected dbListFinanceProcessEvents to reject historical source_type")
	}
	if !strings.Contains(err.Error(), "source_type must be settlement_cycle") {
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
		Note:              "历史回填业务",
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
		Note:              "历史回填事件",
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

// TestSettlementCycleWriteGuard_NoSharedEntryDirectCalls 是收官固定检查：
// 这里只允许结算写入口经过专用封装，不允许新增共享入口直调或手填来源字段。
func TestSettlementCycleWriteGuard_NoSharedEntryDirectCalls(t *testing.T) {
	t.Parallel()

	files := []string{
		"wallet_accounting.go",
		"db_process_writes.go",
		"db_init.go",
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
