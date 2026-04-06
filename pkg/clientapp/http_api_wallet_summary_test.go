package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHandleWalletSummary_SplitsBSVAndTokenUsed 验证钱包汇总把 BSV 和 Token 消耗拆开返回。
func TestHandleWalletSummary_SplitsBSVAndTokenUsed(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	srv := &httpAPIServer{db: db, store: store}
	ctx := context.Background()
	now := time.Now().Unix()

	address := "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(address)

	// BSV 入账与消耗。
	bsvUTXOID := "sum_bsv_tx:0"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		bsvUTXOID, walletID, address, "sum_bsv_tx", 0, int64(9000), "unspent", walletUTXOAllocationPlainBSV, "",
		"sum_bsv_tx", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed bsv utxo failed: %v", err)
	}
	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "sum_bsv_flow",
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         bsvUTXOID,
		TxID:           "sum_bsv_tx",
		Vout:           0,
		AmountSatoshi:  9000,
		OccurredAtUnix: now,
	}); err != nil {
		t.Fatalf("seed bsv flow failed: %v", err)
	}
	bsvPaymentID, err := dbUpsertChainPaymentWithSettlementCycleDB(db, chainPaymentEntry{
		TxID:                "sum_bsv_pay",
		PaymentSubType:      "direct_payment",
		Status:              "confirmed",
		WalletInputSatoshi:  600,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -600,
		BlockHeight:         100,
		OccurredAtUnix:      now,
	})
	if err != nil {
		t.Fatalf("seed bsv payment failed: %v", err)
	}
	if err := dbAppendBSVConsumptionsForChainPayment(db, bsvPaymentID, []chainPaymentUTXOLinkEntry{
		{
			ChainPaymentID: bsvPaymentID,
			UTXOID:         bsvUTXOID,
			IOSide:         "input",
			UTXORole:       "wallet_input",
			AmountSatoshi:  600,
			CreatedAtUnix:  now,
		},
	}, now); err != nil {
		t.Fatalf("append bsv consumption failed: %v", err)
	}

	// Token 入账与消耗。
	tokenUTXOID := "sum_token_tx:0"
	tokenID := "token_summary_001"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenUTXOID, walletID, address, "sum_token_tx", 0, int64(1), "unspent", walletUTXOAllocationProtectedAsset, "",
		"sum_token_tx", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed token utxo failed: %v", err)
	}
	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "sum_token_flow",
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV21",
		TokenID:        tokenID,
		UTXOID:         tokenUTXOID,
		TxID:           "sum_token_tx",
		Vout:           0,
		AmountSatoshi:  1,
		QuantityText:   "2500",
		OccurredAtUnix: now,
	}); err != nil {
		t.Fatalf("seed token flow failed: %v", err)
	}
	tokenPaymentID, err := dbUpsertChainPaymentWithSettlementCycleDB(db, chainPaymentEntry{
		TxID:                "sum_token_pay",
		PaymentSubType:      "token_send",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    0,
		BlockHeight:         100,
		OccurredAtUnix:      now,
	})
	if err != nil {
		t.Fatalf("seed token payment failed: %v", err)
	}
	if err := dbAppendTokenConsumptionForChainPaymentByUTXO(db, tokenPaymentID, tokenUTXOID, "2500", now); err != nil {
		t.Fatalf("append token consumption failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletSummary(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("wallet summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode wallet summary: %v", err)
	}
	if _, ok := body["total_used_satoshi"]; ok {
		t.Fatalf("wallet summary should not expose total_used_satoshi anymore: %+v", body)
	}
	if got, ok := body["bsv_used_satoshi"].(float64); !ok || got != 600 {
		t.Fatalf("bsv_used_satoshi mismatch: %+v", body)
	}
	if got, ok := body["net_spent_satoshi"].(float64); !ok || got != 600 {
		t.Fatalf("net_spent_satoshi mismatch: %+v", body)
	}
	tokenUsed, ok := body["token_used"].([]any)
	if !ok {
		t.Fatalf("token_used shape mismatch: %+v", body)
	}
	if len(tokenUsed) != 1 {
		t.Fatalf("token_used count mismatch: %+v", body)
	}
	item, ok := tokenUsed[0].(map[string]any)
	if !ok {
		t.Fatalf("token_used item shape mismatch: %+v", body)
	}
	if item["token_standard"] != "BSV21" || item["token_id"] != tokenID || item["used_quantity_text"] != "2500" {
		t.Fatalf("token_used item mismatch: %+v", item)
	}
}
