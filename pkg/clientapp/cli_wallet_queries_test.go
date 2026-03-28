package clientapp

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestRunWalletAssetCLIQuery_TokenBalancesAndOrdinalDetail(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1"}, now+1)

	tokenJSON, err := RunWalletAssetCLIQuery(dbPath, WalletAssetCLIQuery{
		Kind:     WalletAssetCLIQueryTokenBalances,
		Standard: "bsv20",
		Limit:    50,
		Offset:   0,
	})
	if err != nil {
		t.Fatalf("RunWalletAssetCLIQuery token balances failed: %v", err)
	}
	var tokenResp struct {
		WalletAddress string `json:"wallet_address"`
		Total         int    `json:"total"`
		Items         []struct {
			AssetKey     string `json:"asset_key"`
			QuantityText string `json:"quantity_text"`
		} `json:"items"`
	}
	if err := json.Unmarshal(tokenJSON, &tokenResp); err != nil {
		t.Fatalf("decode token balances failed: %v", err)
	}
	if tokenResp.WalletAddress != addr || tokenResp.Total != 1 || len(tokenResp.Items) != 1 {
		t.Fatalf("unexpected token balance payload: %+v", tokenResp)
	}
	if tokenResp.Items[0].AssetKey != "bsv20:demo" || tokenResp.Items[0].QuantityText != "10" {
		t.Fatalf("unexpected token balance item: %+v", tokenResp.Items[0])
	}

	ordinalJSON, err := RunWalletAssetCLIQuery(dbPath, WalletAssetCLIQuery{
		Kind:     WalletAssetCLIQueryOrdinalDetail,
		AssetKey: "ordinal:origin-1",
	})
	if err != nil {
		t.Fatalf("RunWalletAssetCLIQuery ordinal detail failed: %v", err)
	}
	var ordinalResp struct {
		UTXOID   string `json:"utxo_id"`
		AssetKey string `json:"asset_key"`
	}
	if err := json.Unmarshal(ordinalJSON, &ordinalResp); err != nil {
		t.Fatalf("decode ordinal detail failed: %v", err)
	}
	if ordinalResp.UTXOID != "1111111111111111111111111111111111111111111111111111111111111111:0" || ordinalResp.AssetKey != "ordinal:origin-1" {
		t.Fatalf("unexpected ordinal detail payload: %+v", ordinalResp)
	}
}
