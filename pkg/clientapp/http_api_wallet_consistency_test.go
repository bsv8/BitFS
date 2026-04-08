package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func seedWalletDualLineConsistencyPresent(t *testing.T, db *sql.DB, txid string) {
	t.Helper()

	ctx := context.Background()
	store := newClientDB(db, nil)
	address := "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(address)
	tokenID := "token_dual_line_001"
	prevTxID := txid + "_prev"
	inputUTXO := prevTxID + ":0"

	seedWalletUTXO(t, db, inputUTXO, prevTxID, 0, 1)
	// 使用新资产事实表 API：写入 token carrier BSV UTXO
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletID,
		Address:        address,
		TxID:           prevTxID,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
		Note:           "seed token carrier for " + tokenID,
		Payload:        map[string]any{"token_id": tokenID, "quantity_text": "1000"},
	}); err != nil {
		t.Fatalf("seed token carrier utxo failed: %v", err)
	}

	addressScript, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}
	inputs, err := buildWalletChainAccountingInputsFromTxDetail(db, address, whatsonchain.TxDetail{
		TxID: txid,
		Vin: []whatsonchain.TxInput{
			{TxID: prevTxID, Vout: 0},
		},
		Vout: []whatsonchain.TxOutput{
			{N: 0, ValueSatoshi: 1, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: addressScript}},
		},
	})
	if err != nil {
		t.Fatalf("build wallet chain inputs failed: %v", err)
	}
	if len(inputs) != 2 {
		t.Fatalf("expected 2 chain inputs, got %d", len(inputs))
	}
	for i := range inputs {
		if err := recordWalletChainAccounting(db, inputs[i]); err != nil {
			t.Fatalf("record wallet chain accounting failed: %v", err)
		}
	}
}

func TestHandleAdminWalletConsistency_OK(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_wallet_consistency_ok"
	seedWalletDualLineConsistencyPresent(t, db, txid)

	srv := &httpAPIServer{store: newClientDB(db, nil)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/wallet/consistency?txid="+txid, nil)
	rec := httptest.NewRecorder()
	srv.handleAdminWalletConsistency(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body walletConsistencyResp
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Ok {
		t.Fatalf("expected ok=true, got %+v", body)
	}
	if body.TxID != txid {
		t.Fatalf("unexpected txid: got=%s want=%s", body.TxID, txid)
	}
	if !body.Consistency.HasChainBSVCycle || !body.Consistency.HasChainTokenCycle || !body.Consistency.HasCarrierBSVFact || !body.Consistency.HasTokenQuantityFact {
		t.Fatalf("unexpected consistency: %+v", body.Consistency)
	}
	if len(body.Consistency.MissingItems) != 0 {
		t.Fatalf("expected no missing items, got %+v", body.Consistency.MissingItems)
	}
}

func TestHandleAdminWalletConsistency_MissingTxid(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	srv := &httpAPIServer{store: newClientDB(db, nil)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/wallet/consistency", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminWalletConsistency(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "txid is required") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestHandleAdminWalletConsistency_MissingItemsReturned(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	txid := "tx_wallet_consistency_missing"
	address := "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(address)

	seedWalletUTXO(t, db, txid+":0", txid, 0, 1000)
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 900,
		NetSat:          -100,
		Payload:         map[string]any{"wallet_id": walletID},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: txid + ":0", IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000, Note: "carrier"},
		},
	}); err != nil {
		t.Fatalf("seed chain_bsv failed: %v", err)
	}

	srv := &httpAPIServer{store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/wallet/consistency?txid="+txid, nil)
	rec := httptest.NewRecorder()
	srv.handleAdminWalletConsistency(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body walletConsistencyResp
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Ok {
		t.Fatalf("expected ok=false, got %+v", body)
	}
	if !containsString(body.Consistency.MissingItems, "chain_token_cycle") || !containsString(body.Consistency.MissingItems, "token_quantity_fact") {
		t.Fatalf("unexpected missing items: %+v", body.Consistency.MissingItems)
	}
	if !body.Consistency.HasChainBSVCycle || !body.Consistency.HasCarrierBSVFact {
		t.Fatalf("expected bsv line intact, got %+v", body.Consistency)
	}
}
