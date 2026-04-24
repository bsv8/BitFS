package poolcore

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
)

type closePathTestChain struct {
	tip            uint32
	broadcastTxID  string
	broadcastCalls int
	utxoCalls      int
}

func (c *closePathTestChain) GetUTXOs(address string) ([]UTXO, error) {
	c.utxoCalls++
	return nil, nil
}

func (c *closePathTestChain) GetTipHeight() (uint32, error) {
	return c.tip, nil
}

func (c *closePathTestChain) Broadcast(txHex string) (string, error) {
	c.broadcastCalls++
	return c.broadcastTxID, nil
}

func TestCloseLegacySubmittedSessionReturnsClosedImmediately(t *testing.T) {
	db := openTimelineTestDB(t)
	client := mustTestActor(t, "client", "1111111111111111111111111111111111111111111111111111111111111111")
	server := mustTestActor(t, "server", "2222222222222222222222222222222222222222222222222222222222222222")

	row := GatewaySessionRow{
		SpendTxID:                 "tx_legacy_close",
		ClientID:                  client.PubHex,
		ClientBSVCompressedPubHex: client.PubHex,
		ServerBSVCompressedPubHex: server.PubHex,
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  3,
		ServerAmountSat:           100,
		ClientAmountSat:           899,
		BaseTxID:                  "base_legacy_close",
		FinalTxID:                 "final_legacy_close",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            lifecycleCloseSubmitted,
	}
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB:    db,
		Chain: &closePathTestChain{tip: 100, broadcastTxID: "unused"},
	}
	resp, err := svc.Close(CloseReq{SpendTxID: row.SpendTxID})
	if err != nil {
		t.Fatalf("close returned unexpected error: %v", err)
	}
	if !resp.Success || resp.Status != lifecycleClosed {
		t.Fatalf("close status mismatch: %+v", resp)
	}
	if resp.FinalSpendTxID != row.FinalTxID {
		t.Fatalf("final txid mismatch: got=%s want=%s", resp.FinalSpendTxID, row.FinalTxID)
	}

	after, found, err := LoadSessionBySpendTxID(db, row.SpendTxID)
	if err != nil {
		t.Fatalf("reload session failed: %v", err)
	}
	if !found {
		t.Fatalf("session should exist")
	}
	if after.LifecycleState != lifecycleClosed {
		t.Fatalf("lifecycle mismatch: got=%s want=%s", after.LifecycleState, lifecycleClosed)
	}
	if after.CloseObserveStatus != "accepted" {
		t.Fatalf("close observe status mismatch: got=%s", after.CloseObserveStatus)
	}
	if after.CloseObserveReason != "legacy_close_submitted" {
		t.Fatalf("close observe reason mismatch: got=%s", after.CloseObserveReason)
	}
}

func TestStateLegacySubmittedSessionReturnsClosedImmediately(t *testing.T) {
	db := openTimelineTestDB(t)
	client := mustTestActor(t, "client", "3333333333333333333333333333333333333333333333333333333333333333")
	server := mustTestActor(t, "server", "4444444444444444444444444444444444444444444444444444444444444444")

	row := GatewaySessionRow{
		SpendTxID:                 "tx_legacy_state",
		ClientID:                  client.PubHex,
		ClientBSVCompressedPubHex: client.PubHex,
		ServerBSVCompressedPubHex: server.PubHex,
		InputAmountSat:            2000,
		PoolAmountSat:             2000,
		SpendTxFeeSat:             2,
		Sequence:                  4,
		ServerAmountSat:           200,
		ClientAmountSat:           1798,
		BaseTxID:                  "base_legacy_state",
		FinalTxID:                 "final_legacy_state",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            lifecycleCloseSubmitted,
	}
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB:    db,
		Chain: &closePathTestChain{tip: 100, broadcastTxID: "unused"},
	}
	resp, err := svc.State(StateReq{
		ClientID:  client.PubHex,
		SpendTxID: row.SpendTxID,
	})
	if err != nil {
		t.Fatalf("state returned unexpected error: %v", err)
	}
	if resp.Status != lifecycleClosed {
		t.Fatalf("state status mismatch: got=%s want=%s", resp.Status, lifecycleClosed)
	}
	if resp.FinalTxID != row.FinalTxID {
		t.Fatalf("state final txid mismatch: got=%s want=%s", resp.FinalTxID, row.FinalTxID)
	}
	if !resp.OutpointSpent {
		t.Fatalf("state should mark outpoint spent after accepted close")
	}
}

func TestPassiveCloseExpiredOnceMarksAcceptedBroadcastAsClosed(t *testing.T) {
	db := openTimelineTestDB(t)
	client := mustTestActor(t, "client", "5555555555555555555555555555555555555555555555555555555555555555")
	server := mustTestActor(t, "server", "6666666666666666666666666666666666666666666666666666666666666666")
	chain := &closePathTestChain{
		tip:           100,
		broadcastTxID: "final_passive_close",
	}

	row := GatewaySessionRow{
		SpendTxID:                 "tx_passive_close",
		ClientID:                  client.PubHex,
		ClientBSVCompressedPubHex: client.PubHex,
		ServerBSVCompressedPubHex: server.PubHex,
		InputAmountSat:            1500,
		PoolAmountSat:             1500,
		SpendTxFeeSat:             1,
		Sequence:                  2,
		ServerAmountSat:           120,
		ClientAmountSat:           1379,
		BaseTxID:                  "base_passive_close",
		BaseTxHex:                 "00",
		CurrentTxHex:              mustTestSpendTxHex(t, 99),
		LifecycleState:            lifecycleActive,
	}
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB:    db,
		Chain: chain,
		Params: GatewayParams{
			PayRejectBeforeExpiryBlocks: 1,
		},
	}
	if err := svc.PassiveCloseExpiredOnce(); err != nil {
		t.Fatalf("passive close returned unexpected error: %v", err)
	}
	if chain.broadcastCalls != 1 {
		t.Fatalf("broadcast call mismatch: got=%d want=1", chain.broadcastCalls)
	}
	if chain.utxoCalls != 0 {
		t.Fatalf("utxo query should not happen after accepted close: got=%d", chain.utxoCalls)
	}

	after, found, err := LoadSessionBySpendTxID(db, row.SpendTxID)
	if err != nil {
		t.Fatalf("reload session failed: %v", err)
	}
	if !found {
		t.Fatalf("session should exist")
	}
	if after.LifecycleState != lifecycleClosed {
		t.Fatalf("lifecycle mismatch: got=%s want=%s", after.LifecycleState, lifecycleClosed)
	}
	if after.FinalTxID != chain.broadcastTxID {
		t.Fatalf("final txid mismatch: got=%s want=%s", after.FinalTxID, chain.broadcastTxID)
	}
	if after.CloseObserveStatus != "accepted" {
		t.Fatalf("close observe status mismatch: got=%s", after.CloseObserveStatus)
	}
	if after.CloseObserveReason != "passive_close_accepted" {
		t.Fatalf("close observe reason mismatch: got=%s", after.CloseObserveReason)
	}
}

func mustTestActor(t *testing.T, name string, privHex string) *Actor {
	t.Helper()
	actor, err := BuildActor(name, privHex, false)
	if err != nil {
		t.Fatalf("build actor failed: %v", err)
	}
	return actor
}

func mustTestSpendTxHex(t *testing.T, lockTime uint32) string {
	t.Helper()
	lockingScript, err := script.NewFromHex("51")
	if err != nil {
		t.Fatalf("build locking script failed: %v", err)
	}
	prevHash, err := chainhash.NewHashFromHex(strings.Repeat("11", 32))
	if err != nil {
		t.Fatalf("build prev hash failed: %v", err)
	}
	spendTx := tx.NewTransaction()
	spendTx.LockTime = lockTime
	spendTx.Inputs = append(spendTx.Inputs, &tx.TransactionInput{
		SourceTXID:       prevHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   1,
	})
	spendTx.Outputs = append(spendTx.Outputs, &tx.TransactionOutput{
		Satoshis:      1000,
		LockingScript: lockingScript,
	})
	return spendTx.Hex()
}
