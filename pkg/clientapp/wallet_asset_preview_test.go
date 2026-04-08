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
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(address))
	tokenID := "token_preview_001"

	// Token 承载 UTXO（1 sat） - 使用新 API dbUpsertBSVUTXO
	tokenUTXOID := "preview_token_tx:0"
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         tokenUTXOID,
		OwnerPubkeyHex: ownerPubkeyHex,
		Address:        address,
		TxID:           "preview_token_tx",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token utxo failed: %v", err)
	}

	// Token Lot - 使用新 API dbUpsertTokenLot
	lotID := "preview_token_lot"
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   ownerPubkeyHex,
		TokenID:          tokenID,
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         "preview_token_tx",
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}

	// Token Carrier Link - 关联 lot 和 UTXO carrier
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "preview_token_link",
		LotID:          lotID,
		CarrierUTXOID:  tokenUTXOID,
		OwnerPubkeyHex: ownerPubkeyHex,
		LinkState:      "active",
		BindTxid:       "preview_token_tx",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	// plain BSV 费用 UTXO - 使用新 API dbUpsertBSVUTXO
	bsvUTXOID := "preview_bsv_tx:0"
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         bsvUTXOID,
		OwnerPubkeyHex: ownerPubkeyHex,
		Address:        address,
		TxID:           "preview_bsv_tx",
		Vout:           0,
		ValueSatoshi:   5000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed bsv utxo failed: %v", err)
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
