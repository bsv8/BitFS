package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

type walletBSVTransferMockChain struct{}

func (walletBSVTransferMockChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	return nil, nil
}

func (walletBSVTransferMockChain) GetTipHeight() (uint32, error) {
	return 0, nil
}

func (walletBSVTransferMockChain) Broadcast(txHex string) (string, error) {
	return "", nil
}

type walletBSVTransferFailChain struct{}

func (walletBSVTransferFailChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	return nil, nil
}

func (walletBSVTransferFailChain) GetTipHeight() (uint32, error) {
	return 0, nil
}

func (walletBSVTransferFailChain) Broadcast(txHex string) (string, error) {
	return "", fmt.Errorf("broadcast failed")
}

func TestTriggerWalletBSVTransfer_Success(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 10); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	res, err := TriggerWalletBSVTransfer(context.Background(), newClientDB(db, nil), rt, WalletBSVTransferRequest{
		ToAddress:     "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi: 6,
	})
	if err != nil {
		t.Fatalf("TriggerWalletBSVTransfer failed: %v", err)
	}
	if !res.Ok {
		t.Fatalf("transfer should succeed: %+v", res)
	}
	if res.TxID == "" {
		t.Fatalf("txid should not be empty: %+v", res)
	}
	if len(res.SelectedUTXOIDs) != 1 {
		t.Fatalf("selected utxo count mismatch: %+v", res)
	}
	if res.MinerFeeSatoshi == 0 {
		t.Fatalf("miner fee should be non-zero")
	}
	if res.ChangeSatoshi != 3 {
		t.Fatalf("change mismatch: got=%d want=3", res.ChangeSatoshi)
	}
	if len(res.DetailLines) == 0 {
		t.Fatalf("detail lines should not be empty")
	}

	var spentTxID string
	if err := db.QueryRow(`SELECT spent_txid FROM wallet_utxo WHERE utxo_id=?`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0").Scan(&spentTxID); err != nil {
		t.Fatalf("query spent utxo failed: %v", err)
	}
	if spentTxID == "" {
		t.Fatalf("input should be marked spent")
	}
	var changeState string
	var changeValue uint64
	if err := db.QueryRow(`SELECT state,value_satoshi FROM wallet_utxo WHERE txid=? AND vout=1`, spentTxID).Scan(&changeState, &changeValue); err != nil {
		t.Fatalf("query change output failed: %v", err)
	}
	if changeState != "unspent" || changeValue != 3 {
		t.Fatalf("change output mismatch: state=%s value=%d", changeState, changeValue)
	}
}

func TestTriggerWalletBSVTransfer_InsufficientBalance(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 1); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRowsWithAmount: %v", err)
	}

	res, err := TriggerWalletBSVTransfer(context.Background(), newClientDB(db, nil), rt, WalletBSVTransferRequest{
		ToAddress:     "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi: 6,
	})
	if err == nil {
		t.Fatal("expected insufficient balance error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "insufficient plain bsv balance") {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Ok {
		t.Fatalf("result should fail: %+v", res)
	}
	if len(res.DetailLines) == 0 {
		t.Fatalf("failure should keep detail lines")
	}
}

func TestTriggerWalletBSVTransfer_OwnerStoredAsAddressFallback(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	// 回归场景：fact_bsv_utxos.owner_pubkey_hex 被写成地址小写，而不是公钥 hex。
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(fromAddress)), 10); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	res, err := TriggerWalletBSVTransfer(context.Background(), newClientDB(db, nil), rt, WalletBSVTransferRequest{
		ToAddress:     "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi: 6,
	})
	if err != nil {
		t.Fatalf("TriggerWalletBSVTransfer failed with address-owner fallback: %v", err)
	}
	if !res.Ok {
		t.Fatalf("transfer should succeed with address-owner fallback: %+v", res)
	}
	if res.TxID == "" {
		t.Fatalf("txid should not be empty")
	}
}

func TestTriggerWalletBSVTransfer_InvalidAddress(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 5, 6); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	_, err = TriggerWalletBSVTransfer(context.Background(), newClientDB(db, nil), rt, WalletBSVTransferRequest{
		ToAddress:     "not-an-address",
		AmountSatoshi: 6,
	})
	if err == nil {
		t.Fatal("expected invalid address error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "to_address invalid") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTriggerWalletBSVTransfer_RuntimeAndStoreMissing(t *testing.T) {
	t.Parallel()

	if _, err := TriggerWalletBSVTransfer(context.Background(), nil, nil, WalletBSVTransferRequest{
		ToAddress:     "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi: 6,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "store not initialized") {
		t.Fatalf("expected store not initialized error, got=%v", err)
	}

	db := newWalletAccountingTestDB(t)
	if _, err := TriggerWalletBSVTransfer(context.Background(), newClientDB(db, nil), nil, WalletBSVTransferRequest{
		ToAddress:     "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi: 6,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "runtime not initialized") {
		t.Fatalf("expected runtime not initialized error, got=%v", err)
	}
}

func TestTriggerBizOrderPayBSV_Success(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 5, 6); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	resp, err := TriggerBizOrderPayBSV(context.Background(), newClientDB(db, nil), rt, BizOrderPayBSVRequest{
		OrderID:        "order_biz_pay_bsv_1",
		ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi:  6,
		IdempotencyKey: "idem_biz_pay_bsv_1",
	})
	if err != nil {
		t.Fatalf("TriggerBizOrderPayBSV failed: %v", err)
	}
	if resp.BusinessID == "" || resp.SettlementID == "" {
		t.Fatalf("business ids should not be empty: %+v", resp)
	}
	if resp.TxID == "" {
		t.Fatalf("txid should not be empty: %+v", resp)
	}
	if resp.Status != "submitted" && resp.Status != "submitted_unknown_projection" {
		t.Fatalf("unexpected status: %+v", resp)
	}
	if resp.FrontOrderSummary == nil {
		t.Fatalf("summary should not be nil: %+v", resp)
	}
	if resp.FrontOrderSummary.Summary.TotalBusinessCount != 1 {
		t.Fatalf("expected one business, got %+v", resp.FrontOrderSummary.Summary)
	}
	assertBizOrderPayBSVSettlementFacts(t, db, resp.BusinessID, 2)
}

func TestTriggerBizOrderPayBSV_Idempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 5, 6); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	req := BizOrderPayBSVRequest{
		OrderID:        "order_biz_pay_bsv_2",
		ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi:  6,
		IdempotencyKey: "idem_biz_pay_bsv_2",
	}
	first, err := TriggerBizOrderPayBSV(context.Background(), newClientDB(db, nil), rt, req)
	if err != nil {
		t.Fatalf("first trigger failed: %v", err)
	}
	second, err := TriggerBizOrderPayBSV(context.Background(), newClientDB(db, nil), rt, req)
	if err != nil {
		t.Fatalf("second trigger failed: %v", err)
	}
	if first.BusinessID != second.BusinessID || first.SettlementID != second.SettlementID {
		t.Fatalf("idempotency mismatch: first=%+v second=%+v", first, second)
	}
	if first.TxID == "" || second.TxID == "" || first.TxID != second.TxID {
		t.Fatalf("expected stable txid on idempotent retry: first=%+v second=%+v", first, second)
	}
	assertBizOrderPayBSVSettlementFacts(t, db, first.BusinessID, 2)
	var chainCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_channel_chain_direct_pay WHERE settlement_payment_attempt_id=(SELECT settlement_payment_attempt_id FROM fact_settlement_channel_chain_direct_pay WHERE id=(SELECT target_id FROM settle_records WHERE business_id=?))`, first.BusinessID).Scan(&chainCount); err != nil {
		t.Fatalf("count fact_settlement_channel_chain_direct_pay failed: %v", err)
	}
	if chainCount != 1 {
		t.Fatalf("expected one chain_direct_pay row, got %d", chainCount)
	}
}

func TestTriggerBizOrderPayBSV_InsufficientBalance(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 1); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	resp, err := TriggerBizOrderPayBSV(context.Background(), newClientDB(db, nil), rt, BizOrderPayBSVRequest{
		OrderID:        "order_biz_pay_bsv_3",
		ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi:  6,
		IdempotencyKey: "idem_biz_pay_bsv_3",
	})
	if err != nil {
		t.Fatalf("TriggerBizOrderPayBSV should not return technical error: %v", err)
	}
	if resp.Status != "waiting_fund" {
		t.Fatalf("unexpected status: %+v", resp)
	}
	if resp.Ok {
		t.Fatalf("insufficient balance should not be ok: %+v", resp)
	}
	if resp.FrontOrderSummary == nil || resp.FrontOrderSummary.Summary.OverallStatus != "waiting_fund" {
		t.Fatalf("summary should report waiting_fund: %+v", resp.FrontOrderSummary)
	}
}

func TestTriggerBizOrderPayBSV_WaitingFundCanRetry(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 1); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	store := newClientDB(db, nil)
	req := BizOrderPayBSVRequest{
		OrderID:        "order_biz_pay_bsv_retry_waiting_fund",
		ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi:  6,
		IdempotencyKey: "idem_biz_pay_bsv_retry_waiting_fund",
	}
	first, err := TriggerBizOrderPayBSV(context.Background(), store, rt, req)
	if err != nil {
		t.Fatalf("first trigger failed: %v", err)
	}
	if first.Status != "waiting_fund" {
		t.Fatalf("expected waiting_fund first, got %+v", first)
	}

	retryTxID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		retryTxID+":0",
		walletIDByAddress(fromAddress),
		fromAddress,
		retryTxID,
		int64(0),
		int64(10),
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		retryTxID,
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("insert wallet_utxo retry failed: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO fact_bsv_utxos(
			utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		retryTxID+":0",
		strings.ToLower(strings.TrimSpace(actor.PubHex)),
		fromAddress,
		retryTxID,
		int64(0),
		int64(10),
		"unspent",
		"plain_bsv",
		"",
		now,
		now,
		int64(0),
		"retry wallet transfer test utxo",
		"{}",
	); err != nil {
		t.Fatalf("insert fact_bsv_utxos retry failed: %v", err)
	}

	second, err := TriggerBizOrderPayBSV(context.Background(), store, rt, req)
	if err != nil {
		t.Fatalf("second trigger failed: %v", err)
	}
	if second.Status != "submitted" && second.Status != "submitted_unknown_projection" {
		t.Fatalf("expected retry success, got %+v", second)
	}
	if second.TxID == "" {
		t.Fatalf("retry should return txid: %+v", second)
	}
}

func TestTriggerBizOrderPayBSV_BroadcastFailed(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := &Runtime{
		runIn:       newWalletBSVTransferTestRuntime(t).runIn,
		ActionChain: walletBSVTransferFailChain{},
	}
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 10); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	resp, err := TriggerBizOrderPayBSV(context.Background(), newClientDB(db, nil), rt, BizOrderPayBSVRequest{
		OrderID:        "order_biz_pay_bsv_4",
		ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		AmountSatoshi:  6,
		IdempotencyKey: "idem_biz_pay_bsv_4",
	})
	if err != nil {
		t.Fatalf("TriggerBizOrderPayBSV should not return technical error: %v", err)
	}
	if resp.Status != "failed" {
		t.Fatalf("unexpected status: %+v", resp)
	}
	if resp.Ok {
		t.Fatalf("broadcast failure should not be ok: %+v", resp)
	}
}

func TestTriggerBizOrderPayBSV_MultiplePaymentsSameOrder(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := newWalletBSVTransferTestRuntime(t)
	fromAddress, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		t.Fatalf("buildClientActorFromRunInput: %v", err)
	}
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 20); err != nil {
		t.Fatalf("seedWalletBSVTransferTestRows: %v", err)
	}

	store := newClientDB(db, nil)
	orderID := "order_biz_pay_bsv_5"
	for i := 0; i < 2; i++ {
		if _, err := TriggerBizOrderPayBSV(context.Background(), store, rt, BizOrderPayBSVRequest{
			OrderID:        orderID,
			ToAddress:      "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
			AmountSatoshi:  6,
			IdempotencyKey: fmt.Sprintf("idem_biz_pay_bsv_5_%d", i),
		}); err != nil {
			t.Fatalf("trigger %d failed: %v", i+1, err)
		}
	}

	summary, err := GetFrontOrderSettlementSummary(context.Background(), store, orderID)
	if err != nil {
		t.Fatalf("GetFrontOrderSettlementSummary failed: %v", err)
	}
	if len(summary.Businesses) != 2 {
		t.Fatalf("expected two businesses, got %d", len(summary.Businesses))
	}
	if summary.Summary.TotalTargetSatoshi != 12 {
		t.Fatalf("expected total target 12, got %d", summary.Summary.TotalTargetSatoshi)
	}
}

func newWalletBSVTransferTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	const privHex = "1111111111111111111111111111111111111111111111111111111111111111"
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = privHex
	return &Runtime{
		runIn:       NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex),
		ActionChain: walletBSVTransferMockChain{},
	}
}

func seedWalletBSVTransferTestRows(t *testing.T, db *sql.DB, address string, ownerPubkeyHex string, amounts ...uint64) error {
	t.Helper()
	if len(amounts) == 0 {
		return fmt.Errorf("amounts are required")
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	for i, amount := range amounts {
		txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if len(amounts) > 1 {
			txid = fmt.Sprintf("%064x", i+1)
		}
		if _, err := db.Exec(
			`INSERT INTO wallet_utxo(
				utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			txid+":0",
			walletID,
			address,
			txid,
			int64(0),
			int64(amount),
			"unspent",
			walletUTXOAllocationPlainBSV,
			"",
			txid,
			"",
			now,
			now,
			0,
		); err != nil {
			return fmt.Errorf("seed wallet_utxo failed: %w", err)
		}
		if _, err := db.Exec(
			`INSERT INTO fact_bsv_utxos(
				utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			txid+":0",
			ownerPubkeyHex,
			address,
			txid,
			int64(0),
			int64(amount),
			"unspent",
			"plain_bsv",
			"",
			now,
			now,
			int64(0),
			"seed wallet transfer test utxo",
			"{}",
		); err != nil {
			return fmt.Errorf("seed fact_bsv_utxos failed: %w", err)
		}
	}
	return nil
}

func assertBizOrderPayBSVSettlementFacts(t *testing.T, db *sql.DB, businessID string, wantCount int) {
	t.Helper()
	var targetID string
	if err := db.QueryRow(`SELECT target_id FROM settle_records WHERE business_id=?`, businessID).Scan(&targetID); err != nil {
		t.Fatalf("query settle_records target_id failed: %v", err)
	}
	if strings.TrimSpace(targetID) == "" {
		t.Fatalf("settle_records target_id should not be empty for business %s", businessID)
	}
	var chainCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_channel_chain_direct_pay WHERE id=?`, targetID).Scan(&chainCount); err != nil {
		t.Fatalf("count fact_settlement_channel_chain_direct_pay failed: %v", err)
	}
	if chainCount != 1 {
		t.Fatalf("expected one chain_direct_pay row, got %d", chainCount)
	}
	var paymentAttemptID int64
	if err := db.QueryRow(`SELECT settlement_payment_attempt_id FROM fact_settlement_channel_chain_direct_pay WHERE id=?`, targetID).Scan(&paymentAttemptID); err != nil {
		t.Fatalf("query settlement_payment_attempt_id failed: %v", err)
	}
	rows, err := db.Query(`SELECT source_utxo_id, used_satoshi FROM fact_settlement_records WHERE settlement_payment_attempt_id=? AND asset_type='BSV' ORDER BY source_utxo_id ASC`, paymentAttemptID)
	if err != nil {
		t.Fatalf("query fact_settlement_records failed: %v", err)
	}
	defer rows.Close()
	type factRow struct {
		utxoID string
		used   int64
	}
	var got []factRow
	for rows.Next() {
		var row factRow
		if err := rows.Scan(&row.utxoID, &row.used); err != nil {
			t.Fatalf("scan fact_settlement_records failed: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate fact_settlement_records failed: %v", err)
	}
	if len(got) != wantCount {
		t.Fatalf("expected %d fact_settlement_records rows, got %d", wantCount, len(got))
	}
	wantUTXOIDs := make([]string, 0, wantCount)
	wantUsed := make([]int64, 0, wantCount)
	for i := 0; i < wantCount; i++ {
		txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if wantCount > 1 {
			txid = fmt.Sprintf("%064x", i+1)
		}
		wantUTXOIDs = append(wantUTXOIDs, txid+":0")
		wantUsed = append(wantUsed, 5)
	}
	if wantCount > 1 {
		wantUsed[1] = 6
	}
	for i := range got {
		if got[i].utxoID != wantUTXOIDs[i] {
			t.Fatalf("source_utxo_id mismatch at %d: want=%s got=%s", i, wantUTXOIDs[i], got[i].utxoID)
		}
		if got[i].used != wantUsed[i] {
			t.Fatalf("used_satoshi mismatch at %d: want=%d got=%d", i, wantUsed[i], got[i].used)
		}
	}
}
