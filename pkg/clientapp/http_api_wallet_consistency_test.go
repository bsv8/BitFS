package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

	// 写入 token lot 和 carrier link（使 collectTokenUTXOLinkFacts 能找到 token 输入）
	now := time.Now().Unix()
	lotID := "lot_dual_line_" + prevTxID
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   walletID,
		TokenID:          tokenID,
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         prevTxID,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_dual_line_" + prevTxID,
		LotID:          lotID,
		CarrierUTXOID:  inputUTXO,
		OwnerPubkeyHex: walletID,
		LinkState:      "active",
		BindTxid:       prevTxID,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	channelID, err := dbUpsertChainDirectPayWithSettlementPaymentAttempt(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "bsv21_transfer",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    0,
		OccurredAtUnix:      now,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             map[string]any{"kind": "token"},
	})
	if err != nil {
		t.Fatalf("seed chain_direct_pay channel failed: %v", err)
	}
	paymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", fmt.Sprintf("%d", channelID))
	if err != nil {
		t.Fatalf("lookup chain_direct_pay cycle failed: %v", err)
	}
	if err := dbAppendBSVConsumptionsForSettlementPaymentAttempt(db, paymentAttemptID, []chainPaymentUTXOLinkEntry{
		{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1, CreatedAtUnix: now},
	}, now); err != nil {
		t.Fatalf("seed carrier bsv fact failed: %v", err)
	}
	if err := dbAppendTokenConsumptionsForSettlementPaymentAttempt(db, paymentAttemptID, []chainPaymentUTXOLinkEntry{
		{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1, QuantityText: "1000", AssetKind: "BSV21", TokenID: tokenID, TokenStandard: "BSV21", CreatedAtUnix: now},
	}, now); err != nil {
		t.Fatalf("seed token quantity fact failed: %v", err)
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
	if !body.Consistency.HasChainTokenCycle || !body.Consistency.HasCarrierBSVFact || !body.Consistency.HasTokenQuantityFact {
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
	if err := dbUpsertBSVUTXO(context.Background(), store, bsvUTXOEntry{
		UTXOID:         txid + ":0",
		OwnerPubkeyHex: walletID,
		Address:        address,
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed chain_token fact utxo failed: %v", err)
	}
	channelID, err := dbUpsertChainDirectPayWithSettlementPaymentAttempt(context.Background(), store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "bsv21_transfer",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    0,
		OccurredAtUnix:      time.Now().Unix(),
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
	})
	if err != nil {
		t.Fatalf("seed chain_direct_pay channel failed: %v", err)
	}
	if _, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", fmt.Sprintf("%d", channelID)); err != nil {
		t.Fatalf("lookup chain_direct_pay cycle failed: %v", err)
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
	if !containsString(body.Consistency.MissingItems, "carrier_bsv_fact") || !containsString(body.Consistency.MissingItems, "token_quantity_fact") {
		t.Fatalf("unexpected missing items: %+v", body.Consistency.MissingItems)
	}
	if !body.Consistency.HasChainTokenCycle {
		t.Fatalf("expected chain_token line intact, got %+v", body.Consistency)
	}
}

func TestCheckConfirmedBSVSpendConsistency_RepairByCycle(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	txid := "tx_wallet_consistency_repair"
	utxoID := txid + ":0"
	now := time.Now().Unix()

	seedWalletUTXO(t, db, utxoID, txid, 0, 1200)
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"),
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1200,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed bsv utxo failed: %v", err)
	}
	channelID, err := dbUpsertChainDirectPayWithSettlementPaymentAttempt(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "wallet_transfer",
		Status:              "confirmed",
		WalletInputSatoshi:  1200,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -1200,
		OccurredAtUnix:      now,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
	})
	if err != nil {
		t.Fatalf("seed chain_direct_pay channel failed: %v", err)
	}
	paymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", fmt.Sprintf("%d", channelID))
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	if err := dbAppendSettlementRecord(ctx, store, settlementRecordEntry{
		RecordID:                   "rec_repair_" + txid,
		SettlementPaymentAttemptID: paymentAttemptID,
		AssetType:                  "BSV",
		OwnerPubkeyHex:             walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"),
		SourceUTXOID:               utxoID,
		UsedSatoshi:                1200,
		State:                      "confirmed",
		OccurredAtUnix:             now,
		Note:                       "repair seed",
	}); err != nil {
		t.Fatalf("seed settlement record failed: %v", err)
	}

	check1, err := CheckConfirmedBSVSpendConsistency(ctx, store, txid)
	if err != nil {
		t.Fatalf("check consistency failed: %v", err)
	}
	if !check1.HasConfirmedCycle || !check1.HasBSVSettlementFact {
		t.Fatalf("expected confirmed cycle and settlement fact, got %+v", check1)
	}
	if check1.HasSpentUTXO {
		t.Fatalf("expected spent utxo missing before repair, got %+v", check1)
	}
	if !check1.Repairable || !containsString(check1.MissingItems, "spent_bsv_utxo") {
		t.Fatalf("expected repairable missing spent_bsv_utxo, got %+v", check1)
	}

	if err := RepairConfirmedBSVSpendConsistency(ctx, store, txid); err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	check2, err := CheckConfirmedBSVSpendConsistency(ctx, store, txid)
	if err != nil {
		t.Fatalf("recheck consistency failed: %v", err)
	}
	if !check2.HasSpentUTXO || len(check2.MissingItems) != 0 {
		t.Fatalf("expected repaired consistency, got %+v", check2)
	}

	got, err := dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("query repaired utxo failed: %v", err)
	}
	if got == nil || got.SpentByTxid != txid || got.UTXOState != "spent" {
		t.Fatalf("expected repaired spent utxo, got %+v", got)
	}
}
