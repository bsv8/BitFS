package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

type walletAssetDetectorStub struct {
	items map[string]walletUTXOAssetClassification
	err   error
}

func (s walletAssetDetectorStub) DetectUTXOAssets(_ context.Context, _ string, utxos []poolcore.UTXO) (map[string]walletUTXOAssetClassification, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make(map[string]walletUTXOAssetClassification, len(utxos))
	for _, u := range utxos {
		utxoID := u.TxID + ":" + itoa64(int64(u.Vout))
		if item, ok := s.items[utxoID]; ok {
			out[utxoID] = item
		}
	}
	return out, nil
}

func TestRefreshWalletAssetProjection_ProjectsProtectedAssets(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{DB: db, runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 1, now, "", "chain_utxo_worker", "seed", 0, "round-asset-projection", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	utxoID := txid + ":0"
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, addr, txid, 0, 1, "unspent", walletUTXOAllocationUnknown, "awaiting asset detector confirmation", txid, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}
	rt.WalletAssetDetector = walletAssetDetectorStub{
		items: map[string]walletUTXOAssetClassification{
			utxoID: {
				AllocationClass:  walletUTXOAllocationProtectedAsset,
				AllocationReason: "indexed by external asset api",
				Assets: []walletUTXOAssetBinding{
					{
						UTXOID:        utxoID,
						AssetGroup:    walletAssetGroupToken,
						AssetStandard: "bsv21",
						AssetKey:      "bsv21:test-token",
						AssetSymbol:   "TEST",
						QuantityText:  "42",
						SourceName:    "stub-indexer",
						Payload: map[string]any{
							"ticker": "TEST",
						},
					},
				},
			},
		},
	}

	if err := refreshWalletAssetProjection(context.Background(), rt, addr, "test_asset_projection"); err != nil {
		t.Fatalf("refreshWalletAssetProjection: %v", err)
	}

	var allocationClass, allocationReason string
	if err := db.QueryRow(`SELECT allocation_class,allocation_reason FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&allocationClass, &allocationReason); err != nil {
		t.Fatalf("query wallet_utxo: %v", err)
	}
	if allocationClass != walletUTXOAllocationProtectedAsset {
		t.Fatalf("allocation_class mismatch: got=%s want=%s", allocationClass, walletUTXOAllocationProtectedAsset)
	}
	if allocationReason != "indexed by external asset api" {
		t.Fatalf("allocation_reason mismatch: got=%q", allocationReason)
	}
	assets, err := listWalletUTXOAssetRows(db, utxoID)
	if err != nil {
		t.Fatalf("listWalletUTXOAssetRows: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("asset row count mismatch: got=%d want=1", len(assets))
	}
	if assets[0].AssetStandard != "bsv21" || assets[0].AssetKey != "bsv21:test-token" {
		t.Fatalf("asset row mismatch: %+v", assets[0])
	}
	syncState, err := loadWalletUTXOSyncState(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState: %v", err)
	}
	if syncState.ProtectedUTXOCount != 1 || syncState.ProtectedBalanceSatoshi != 1 {
		t.Fatalf("syncState protected mismatch: %+v", syncState)
	}
	if syncState.PlainBSVUTXOCount != 0 || syncState.PlainBSVBalanceSatoshi != 0 {
		t.Fatalf("syncState plain mismatch: %+v", syncState)
	}
}

func TestRefreshWalletAssetProjection_DegradesToUnknownWhenDetectorFails(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{DB: db, runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	txid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	utxoID := txid + ":1"
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 1, now, "", "chain_utxo_worker", "seed", 0, "round-asset-projection", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, addr, txid, 1, 1, "unspent", walletUTXOAllocationUnknown, "awaiting asset detector confirmation", txid, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}
	rt.WalletAssetDetector = walletAssetDetectorStub{err: errors.New("indexer temporarily unavailable")}

	if err := refreshWalletAssetProjection(context.Background(), rt, addr, "test_asset_projection_fail_safe"); err != nil {
		t.Fatalf("refreshWalletAssetProjection: %v", err)
	}

	var allocationClass, allocationReason string
	if err := db.QueryRow(`SELECT allocation_class,allocation_reason FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&allocationClass, &allocationReason); err != nil {
		t.Fatalf("query wallet_utxo: %v", err)
	}
	if allocationClass != walletUTXOAllocationUnknown {
		t.Fatalf("allocation_class mismatch: got=%s want=%s", allocationClass, walletUTXOAllocationUnknown)
	}
	if allocationReason != "asset detector unavailable" {
		t.Fatalf("allocation_reason mismatch: got=%q", allocationReason)
	}
	syncState, err := loadWalletUTXOSyncState(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState: %v", err)
	}
	if syncState.UnknownUTXOCount != 1 || syncState.UnknownBalanceSatoshi != 1 {
		t.Fatalf("syncState unknown mismatch: %+v", syncState)
	}
}

func TestRefreshWalletAssetProjection_AllowsPlainBSVOnlyAfterSuccessfulClearCheck(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	rt := &Runtime{DB: db, runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	txid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	utxoID := txid + ":2"
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 1, now, "", "chain_utxo_worker", "seed", 0, "round-asset-clear-check", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, addr, txid, 2, 1, "unspent", walletUTXOAllocationUnknown, "awaiting asset detector confirmation", txid, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}
	rt.WalletAssetDetector = walletAssetDetectorStub{items: map[string]walletUTXOAssetClassification{}}

	if err := refreshWalletAssetProjection(context.Background(), rt, addr, "test_asset_projection_clear_check"); err != nil {
		t.Fatalf("refreshWalletAssetProjection: %v", err)
	}

	var allocationClass, allocationReason string
	if err := db.QueryRow(`SELECT allocation_class,allocation_reason FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&allocationClass, &allocationReason); err != nil {
		t.Fatalf("query wallet_utxo: %v", err)
	}
	if allocationClass != walletUTXOAllocationPlainBSV {
		t.Fatalf("allocation_class mismatch: got=%s want=%s", allocationClass, walletUTXOAllocationPlainBSV)
	}
	if allocationReason != "" {
		t.Fatalf("allocation_reason mismatch: got=%q want empty", allocationReason)
	}
	syncState, err := loadWalletUTXOSyncState(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState: %v", err)
	}
	if syncState.PlainBSVUTXOCount != 1 || syncState.PlainBSVBalanceSatoshi != 1 {
		t.Fatalf("syncState plain mismatch: %+v", syncState)
	}
	if syncState.UnknownUTXOCount != 0 || syncState.UnknownBalanceSatoshi != 0 {
		t.Fatalf("syncState unknown mismatch: %+v", syncState)
	}
}

func TestExternalAssetAPIDetector_ClassifiesOneSatOutputs(t *testing.T) {
	t.Parallel()

	requested := make([]string, 0, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method mismatch: got=%s want=POST", r.Method)
		}
		if r.URL.Path != "/txo/outpoints" {
			t.Fatalf("path mismatch: got=%s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("authorization mismatch: got=%q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSON := []map[string]any{
			{
				"outpoint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.0",
				"satoshis": 1,
				"data": map[string]any{
					"inscription": map[string]any{
						"origin":      "origin-tx_0",
						"contentType": "image/png",
					},
					"ordlock": map[string]any{
						"price": 1234,
					},
				},
			},
			{
				"outpoint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.1",
				"satoshis": 1,
				"data": map[string]any{
					"bsv21": map[string]any{
						"id":  "token-origin_0",
						"sym": "TOK",
						"amt": "42",
					},
				},
			},
			{
				"outpoint": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.2",
				"satoshis": 1,
				"data": map[string]any{
					"bsv20": map[string]any{
						"tick": "DOGE",
						"amt":  "99",
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(writeJSON); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	detector := &externalAssetAPIDetector{
		baseURL:    srv.URL,
		sourceName: "external-asset-api",
		client:     srv.Client(),
		auth:       walletAssetAuthConfigFromTest("bearer", "", "secret-token"),
	}
	utxos := []poolcore.UTXO{
		{TxID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Vout: 0, Value: 1},
		{TxID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Vout: 1, Value: 1},
		{TxID: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Vout: 2, Value: 1},
		{TxID: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Vout: 3, Value: 500},
	}

	got, err := detector.DetectUTXOAssets(context.Background(), "ignored-address", utxos)
	if err != nil {
		t.Fatalf("DetectUTXOAssets: %v", err)
	}
	if slices.Contains(requested, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.3") {
		t.Fatalf("non-1sat output should not be queried: %v", requested)
	}
	if len(requested) != 3 {
		t.Fatalf("requested outpoint count mismatch: got=%d want=3 body=%v", len(requested), requested)
	}
	ordinal := got["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"]
	if ordinal.AllocationClass != walletUTXOAllocationProtectedAsset || len(ordinal.Assets) != 2 {
		t.Fatalf("ordinal classification mismatch: %+v", ordinal)
	}
	if ordinal.Assets[0].AssetGroup != walletAssetGroupListing && ordinal.Assets[1].AssetGroup != walletAssetGroupListing {
		t.Fatalf("ordlock asset missing: %+v", ordinal.Assets)
	}
	token21 := got["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1"]
	if token21.AllocationReason != "indexed bsv21 output" || len(token21.Assets) != 1 || token21.Assets[0].AssetKey != "bsv21:token-origin_0" {
		t.Fatalf("bsv21 classification mismatch: %+v", token21)
	}
	token20 := got["cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:2"]
	if token20.AllocationReason != "indexed bsv20 output" || len(token20.Assets) != 1 || token20.Assets[0].AssetKey != "bsv20:DOGE" {
		t.Fatalf("bsv20 classification mismatch: %+v", token20)
	}
}

func walletAssetAuthConfigFromTest(mode, name, value string) chainbridge.AuthConfig {
	return chainbridge.AuthConfig{Mode: mode, Name: name, Value: value}
}
