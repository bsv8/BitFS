package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

func TestApplyDomainRegisterSubmitResultFailureClearsOkAndUppercasesCode(t *testing.T) {
	t.Parallel()

	base := domainRegisterNameResultFromPrepared(TriggerDomainPrepareRegisterResult{
		Ok:                         true,
		Code:                       "PREPARED",
		Name:                       "example.eth",
		OwnerPubkeyHex:             "owner",
		TargetPubkeyHex:            "target",
		RegisterTxID:               "prepared_txid",
		QueryFeeChargedSatoshi:     1,
		RegisterLockChargedSatoshi: 1,
		RegisterPriceSatoshi:       1,
		RegisterSubmitFeeSatoshi:   1,
		TotalRegisterPaySatoshi:    2,
	})

	got := applyDomainRegisterSubmitResult(base, TriggerDomainSubmitPreparedRegisterResult{
		Ok:      false,
		Code:    "broadcast_failed",
		Message: "forced reject",
	})

	if got.Ok {
		t.Fatalf("submit failure should clear ok flag")
	}
	if got.Code != "BROADCAST_FAILED" {
		t.Fatalf("code mismatch: got=%s want=BROADCAST_FAILED", got.Code)
	}
	if got.Message != "forced reject" {
		t.Fatalf("message mismatch: got=%s want=forced reject", got.Message)
	}
	if got.RegisterTxID != "prepared_txid" {
		t.Fatalf("prepared register txid should be preserved on submit failure: got=%s", got.RegisterTxID)
	}
}

func TestApplyDomainRegisterSubmitResultSuccessMarksOk(t *testing.T) {
	t.Parallel()

	base := domainRegisterNameResultFromPrepared(TriggerDomainPrepareRegisterResult{
		Ok:              true,
		Code:            "PREPARED",
		Name:            "example.eth",
		OwnerPubkeyHex:  "owner_before",
		TargetPubkeyHex: "target_before",
		RegisterTxID:    "prepared_txid",
	})

	got := applyDomainRegisterSubmitResult(base, TriggerDomainSubmitPreparedRegisterResult{
		Ok:              true,
		Name:            "example.eth",
		OwnerPubkeyHex:  "owner_after",
		TargetPubkeyHex: "target_after",
		ExpireAtUnix:    123,
		RegisterTxID:    "final_txid",
	})

	if !got.Ok {
		t.Fatalf("submit success should mark ok")
	}
	if got.Code != "OK" {
		t.Fatalf("code mismatch: got=%s want=OK", got.Code)
	}
	if got.OwnerPubkeyHex != "owner_after" {
		t.Fatalf("owner mismatch: got=%s want=owner_after", got.OwnerPubkeyHex)
	}
	if got.TargetPubkeyHex != "target_after" {
		t.Fatalf("target mismatch: got=%s want=target_after", got.TargetPubkeyHex)
	}
	if got.RegisterTxID != "final_txid" {
		t.Fatalf("register txid mismatch: got=%s want=final_txid", got.RegisterTxID)
	}
	if got.ExpireAtUnix != 123 {
		t.Fatalf("expire_at mismatch: got=%d want=123", got.ExpireAtUnix)
	}
}

func TestRecordDomainRegisterAccountingAfterBroadcast_WritesChainPaymentFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("4", 64)
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)

	txHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	registerTxRaw, err := hex.DecodeString(txHex)
	if err != nil {
		t.Fatalf("decode tx hex failed: %v", err)
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		t.Fatalf("parse tx hex failed: %v", err)
	}
	for _, input := range parsed.Inputs {
		if input == nil || input.SourceTXID == nil {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
		if _, err := db.Exec(`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			utxoID, "wallet1", "addr1", strings.ToLower(strings.TrimSpace(input.SourceTXID.String())), int64(input.SourceTxOutIndex), 1000, "confirmed", "plain_bsv", "",
			strings.ToLower(strings.TrimSpace(input.SourceTXID.String())), "", 1700000001, 1700000001, 0,
		); err != nil {
			t.Fatalf("seed wallet_utxo failed: %v", err)
		}
	}

	if err := recordDomainRegisterAccountingAfterBroadcast(ctx, store, rt, registerTxRaw, parsed.TxID().String(), "02resolverpubkey0000000000000000000000000000000000000000000000000000"); err != nil {
		t.Fatalf("record domain register accounting failed: %v", err)
	}

	var paymentCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, strings.ToLower(parsed.TxID().String())).Scan(&paymentCount); err != nil {
		t.Fatalf("query fact_settlement_channel_chain_quote_pay failed: %v", err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 fact_settlement_channel_chain_quote_pay row, got %d", paymentCount)
	}

	var paymentAttemptCount int
	var channelID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, strings.ToLower(parsed.TxID().String())).Scan(&channelID); err != nil {
		t.Fatalf("query channel id failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_payment_attempts WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", channelID)).Scan(&paymentAttemptCount); err != nil {
		t.Fatalf("query fact_settlement_payment_attempts failed: %v", err)
	}
	if paymentAttemptCount != 1 {
		t.Fatalf("expected 1 settlement payment attempt, got %d", paymentAttemptCount)
	}
}
