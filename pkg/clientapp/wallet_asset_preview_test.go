package clientapp

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestPreviewWalletTokenSend_SplitsTokenAndBSVUnits 验证预演里 Token 数量和 BSV 链费分开展示。
func TestPreviewWalletTokenSend_SplitsTokenAndBSVUnits(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	now := time.Now().Unix()

	address := "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(address)
	tokenID := "token_preview_001"

	// Token 承载 UTXO。
	tokenUTXOID := "preview_token_tx:0"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenUTXOID, walletID, address, "preview_token_tx", 0, int64(1), "unspent", walletUTXOAllocationProtectedAsset, "",
		"preview_token_tx", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed token utxo failed: %v", err)
	}
	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "preview_token_flow",
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV21",
		TokenID:        tokenID,
		UTXOID:         tokenUTXOID,
		TxID:           "preview_token_tx",
		Vout:           0,
		AmountSatoshi:  1,
		QuantityText:   "1000",
		OccurredAtUnix: now,
	}); err != nil {
		t.Fatalf("seed token flow failed: %v", err)
	}

	// plain BSV 费用 UTXO。
	bsvUTXOID := "preview_bsv_tx:0"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		bsvUTXOID, walletID, address, "preview_bsv_tx", 0, int64(5000), "unspent", walletUTXOAllocationPlainBSV, "",
		"preview_bsv_tx", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed bsv utxo failed: %v", err)
	}
	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "preview_bsv_flow",
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         bsvUTXOID,
		TxID:           "preview_bsv_tx",
		Vout:           0,
		AmountSatoshi:  5000,
		OccurredAtUnix: now,
	}); err != nil {
		t.Fatalf("seed bsv flow failed: %v", err)
	}

	preview, err := previewWalletTokenSend(ctx, store, nil, address, "bsv21", tokenID, "1000", address)
	if err != nil {
		t.Fatalf("preview token send failed: %v", err)
	}
	if !preview.Feasible {
		t.Fatalf("preview should be feasible: %+v", preview)
	}
	if len(preview.SelectedAssetUTXOIDs) != 1 || preview.SelectedAssetUTXOIDs[0] != tokenUTXOID {
		t.Fatalf("token selection mismatch: %+v", preview.SelectedAssetUTXOIDs)
	}
	if len(preview.SelectedFeeUTXOIDs) == 0 {
		t.Fatalf("fee selection should not be empty: %+v", preview)
	}

	var tokenDebit, feeDebit bool
	for _, change := range preview.Changes {
		switch change.AssetGroup {
		case walletAssetGroupToken:
			if change.Direction == "debit" && change.QuantityText == "1000" {
				tokenDebit = true
			}
		case "bsv":
			if change.Direction == "debit" && change.AssetStandard == "native" && strings.TrimSpace(change.QuantityText) != "" {
				feeDebit = true
			}
		}
	}
	if !tokenDebit {
		t.Fatalf("preview should expose token debit in quantity units: %+v", preview.Changes)
	}
	if !feeDebit {
		t.Fatalf("preview should expose bsv fee debit in satoshi: %+v", preview.Changes)
	}
}
