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
	pubkeyHex := walletID // walletID 就是公钥 hex

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
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         bsvUTXOID,
		OwnerPubkeyHex: pubkeyHex,
		Address:        address,
		TxID:           "sum_bsv_tx",
		Vout:           0,
		ValueSatoshi:   9000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed bsv utxo fact failed: %v", err)
	}
	if err := dbUpsertSettlementPaymentAttempt(db, "payment_attempt_chain_bsv_sum_bsv_pay", "chain_direct_pay", "sum_bsv_pay", "confirmed", 600, 0, -600, 0, now, "summary test", map[string]any{"kind": "bsv"}); err != nil {
		t.Fatalf("seed bsv settlement payment attempt failed: %v", err)
	}
	bsvPaymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", "sum_bsv_pay")
	if err != nil {
		t.Fatalf("lookup bsv settlement payment attempt failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_settlement_channel_chain_direct_pay(settlement_payment_attempt_id, txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi, net_amount_satoshi, block_height, occurred_at_unix, submitted_at_unix, wallet_observed_at_unix, from_party_id, to_party_id, payload_json, updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		bsvPaymentAttemptID, "sum_bsv_pay", "summary", "confirmed", 600, 0, -600, 0, now, now, 0, "wallet:self", "external:unknown", `{"kind":"bsv"}`, now,
	); err != nil {
		t.Fatalf("seed bsv channel row failed: %v", err)
	}
	if err := dbAppendBSVConsumptionsForSettlementPaymentAttempt(db, bsvPaymentAttemptID, []chainPaymentUTXOLinkEntry{
		{
			UTXOID:        bsvUTXOID,
			IOSide:        "input",
			UTXORole:      "wallet_input",
			AmountSatoshi: 600,
			CreatedAtUnix: now,
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
	// 写入 token carrier 的 BSV UTXO（外键约束要求）
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         tokenUTXOID,
		OwnerPubkeyHex: pubkeyHex,
		Address:        address,
		TxID:           "sum_token_tx",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier utxo failed: %v", err)
	}
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            "sum_token_lot_001",
		OwnerPubkeyHex:   pubkeyHex,
		TokenID:          tokenID,
		TokenStandard:    "BSV21",
		QuantityText:     "2500",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         "sum_token_tx",
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot fact failed: %v", err)
	}
	// 写入 carrier link（绑定 lot 到 UTXO）
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "sum_token_link_001",
		LotID:          "sum_token_lot_001",
		CarrierUTXOID:  tokenUTXOID,
		OwnerPubkeyHex: pubkeyHex,
		LinkState:      "active",
		BindTxid:       "sum_token_tx",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}
	if err := dbUpsertSettlementPaymentAttempt(db, "payment_attempt_chain_token_sum_token_pay", "chain_direct_pay", "sum_token_pay", "confirmed", 0, 0, 0, 0, now, "summary test", map[string]any{"kind": "token"}); err != nil {
		t.Fatalf("seed token settlement payment attempt failed: %v", err)
	}
	tokenPaymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", "sum_token_pay")
	if err != nil {
		t.Fatalf("lookup token settlement payment attempt failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_settlement_channel_chain_direct_pay(settlement_payment_attempt_id, txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi, net_amount_satoshi, block_height, occurred_at_unix, submitted_at_unix, wallet_observed_at_unix, from_party_id, to_party_id, payload_json, updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tokenPaymentAttemptID, "sum_token_pay", "summary", "confirmed", 0, 0, 0, 0, now, now, 0, "wallet:self", "external:unknown", `{"kind":"token"}`, now,
	); err != nil {
		t.Fatalf("seed token channel row failed: %v", err)
	}
	if err := dbAppendTokenConsumptionsForSettlementPaymentAttempt(db, tokenPaymentAttemptID, []chainPaymentUTXOLinkEntry{
		{
			UTXOID:        tokenUTXOID,
			IOSide:        "input",
			UTXORole:      "wallet_input",
			AssetKind:     "BSV21",
			TokenID:       tokenID,
			TokenStandard: "BSV21",
			AmountSatoshi: 1,
			QuantityText:  "2500",
			CreatedAtUnix: now,
		},
	}, now); err != nil {
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
