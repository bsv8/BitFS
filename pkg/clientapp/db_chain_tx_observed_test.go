package clientapp

import (
	"context"
	"strings"
	"testing"
)

const testWalletScriptPlainTxHex = "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

type testWalletScriptEvidenceSource struct {
	txHex string
}

func (s *testWalletScriptEvidenceSource) GetTxHex(context.Context, string) (string, error) {
	return strings.TrimSpace(s.txHex), nil
}

func (s *testWalletScriptEvidenceSource) GetAddressBSV20TokenUnspent(context.Context, string) ([]walletBSV20WOCCandidate, error) {
	return []walletBSV20WOCCandidate{}, nil
}

func (s *testWalletScriptEvidenceSource) GetAddressBSV21TokenUnspent(context.Context, string) ([]walletBSV21WOCCandidate, error) {
	return []walletBSV21WOCCandidate{}, nil
}

func TestInferObservedWalletLocalBroadcastTxIDs_ByLiveTxID(t *testing.T) {
	t.Parallel()

	rows := []walletLocalBroadcastRow{
		{TxID: "tx_a", TxHex: testWalletScriptPlainTxHex, ObservedAtUnix: 0},
	}
	liveUTXOIDs := map[string]struct{}{}
	liveTxIDs := map[string]struct{}{"tx_a": {}}
	out := inferObservedWalletLocalBroadcastTxIDs(rows, liveUTXOIDs, liveTxIDs)
	if _, ok := out["tx_a"]; !ok {
		t.Fatal("tx_a should be inferred as observed by live txid")
	}
}

func TestInferObservedWalletLocalBroadcastTxIDs_ByExternalInputMissing(t *testing.T) {
	t.Parallel()

	txid := "tx_ext_input_missing"
	rows := []walletLocalBroadcastRow{
		{TxID: txid, TxHex: testWalletScriptPlainTxHex, ObservedAtUnix: 0},
	}
	liveUTXOIDs := map[string]struct{}{}
	liveTxIDs := map[string]struct{}{}
	out := inferObservedWalletLocalBroadcastTxIDs(rows, liveUTXOIDs, liveTxIDs)
	if _, ok := out[txid]; !ok {
		t.Fatal("tx should be inferred as observed when external inputs are missing from live set")
	}
}
