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
	if err := seedWalletBSVTransferTestRows(t, db, fromAddress, strings.ToLower(strings.TrimSpace(actor.PubHex)), 10); err != nil {
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

func seedWalletBSVTransferTestRows(t *testing.T, db *sql.DB, address string, ownerPubkeyHex string, amount uint64) error {
	t.Helper()
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	txid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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
	return nil
}
