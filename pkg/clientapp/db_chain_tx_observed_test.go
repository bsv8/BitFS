package clientapp

import (
	"testing"
)

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
