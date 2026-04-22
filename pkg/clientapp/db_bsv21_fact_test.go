package clientapp

import (
	"context"
	"database/sql"
	"testing"
)

func createHistoricalBSV21CreateStatusSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`CREATE TABLE wallet_bsv21_create_status(
		token_id TEXT PRIMARY KEY,
		create_txid TEXT NOT NULL,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		token_standard TEXT NOT NULL,
		symbol TEXT NOT NULL,
		max_supply TEXT NOT NULL,
		decimals INTEGER NOT NULL,
		icon TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at_unix INTEGER NOT NULL,
		submitted_at_unix INTEGER NOT NULL,
		confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
		last_check_at_unix INTEGER NOT NULL DEFAULT 0,
		next_auto_check_at_unix INTEGER NOT NULL DEFAULT 0,
		updated_at_unix INTEGER NOT NULL,
		last_check_error TEXT NOT NULL DEFAULT ''
	)`)
	if err != nil {
		t.Fatalf("create historical wallet_bsv21_create_status failed: %v", err)
	}
}

func insertHistoricalBSV21CreateStatus(t *testing.T, db *sql.DB, tokenID, createTxID, walletID, address, status string, createdAtUnix, submittedAtUnix, confirmedAtUnix, lastCheckAtUnix, nextAutoCheckAtUnix, updatedAtUnix int64) {
	t.Helper()

	_, err := db.Exec(`INSERT INTO wallet_bsv21_create_status(
		token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,status,created_at_unix,submitted_at_unix,confirmed_at_unix,last_check_at_unix,next_auto_check_at_unix,updated_at_unix,last_check_error
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenID, createTxID, walletID, address, "bsv21", "LEGACY", "100", 0, "icon_hex", status, createdAtUnix, submittedAtUnix, confirmedAtUnix, lastCheckAtUnix, nextAutoCheckAtUnix, updatedAtUnix, "legacy note",
	)
	if err != nil {
		t.Fatalf("insert historical wallet_bsv21_create_status failed: %v", err)
	}
}

func TestInitIndexDB_CreatesBSV21FactSchema(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	for _, table := range []string{"fact_bsv21", "fact_bsv21_events"} {
		exists, err := hasTable(db, table)
		if err != nil {
			t.Fatalf("hasTable %s failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing %s table", table)
		}
	}

	hasOld, err := hasTable(db, "wallet_bsv21_create_status")
	if err != nil {
		t.Fatalf("hasTable wallet_bsv21_create_status failed: %v", err)
	}
	if hasOld {
		t.Fatal("wallet_bsv21_create_status should not exist after init")
	}

	cols, err := tableColumns(db, "fact_bsv21")
	if err != nil {
		t.Fatalf("inspect fact_bsv21 columns failed: %v", err)
	}
	for _, col := range []string{"token_id", "create_txid", "wallet_id", "address", "token_standard", "symbol", "max_supply", "decimals", "icon", "created_at_unix", "submitted_at_unix", "updated_at_unix", "payload_json"} {
		if _, ok := cols[col]; !ok {
			t.Fatalf("fact_bsv21 missing column %s", col)
		}
	}
}

func TestFactBSV21Helpers(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	if err := upsertFactBSV21Create(context.Background(), store, factBSV21CreateItem{
		TokenID:         "tx1_1",
		CreateTxID:      "tx1",
		WalletID:        "wallet_a",
		Address:         "addr_a",
		TokenStandard:   "bsv21",
		Symbol:          "AAA",
		MaxSupply:       "100",
		Decimals:        0,
		Icon:            "icon_a",
		CreatedAtUnix:   1700001000,
		SubmittedAtUnix: 1700001001,
		UpdatedAtUnix:   1700001002,
		PayloadJSON:     `{"note":"first"}`,
	}); err != nil {
		t.Fatalf("upsertFactBSV21Create failed: %v", err)
	}
	if err := upsertFactBSV21Create(context.Background(), store, factBSV21CreateItem{
		TokenID:         "tx1_1",
		CreateTxID:      "tx1",
		WalletID:        "wallet_b",
		Address:         "addr_b",
		TokenStandard:   "bsv21",
		Symbol:          "BBB",
		MaxSupply:       "200",
		Decimals:        2,
		Icon:            "icon_b",
		CreatedAtUnix:   1700001003,
		SubmittedAtUnix: 1700001004,
		UpdatedAtUnix:   1700001005,
		PayloadJSON:     `{"note":"second"}`,
	}); err != nil {
		t.Fatalf("second upsertFactBSV21Create failed: %v", err)
	}

	item, err := getFactBSV21ByTokenID(context.Background(), store, "tx1_1")
	if err != nil {
		t.Fatalf("getFactBSV21ByTokenID failed: %v", err)
	}
	if item.WalletID != "wallet_b" || item.Symbol != "BBB" || item.Decimals != 2 {
		t.Fatalf("unexpected fact item after upsert: %+v", item)
	}

	if err := appendFactBSV21Event(context.Background(), store, factBSV21EventItem{
		TokenID:     "tx1_1",
		EventKind:   "submitted",
		EventAtUnix: 1700001006,
		TxID:        "tx1",
		Note:        "submitted",
		PayloadJSON: `{"ok":true}`,
	}); err != nil {
		t.Fatalf("appendFactBSV21Event failed: %v", err)
	}
}
