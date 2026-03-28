package clientapp

import (
	"testing"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
)

func TestMergeOpenedFeePoolCurrentTx(t *testing.T) {
	client, err := poolcore.BuildActor("client", "a18f841f787e152b4721f32c6f49df517892a77eda85ec104aba5502ec37d143", false)
	if err != nil {
		t.Fatalf("BuildActor(client) error = %v", err)
	}
	server, err := poolcore.BuildActor("server", "ff67863e7b524f1e72e6fca95306ae7acdfd71211a3b4b8ac6cf433398270b1c", false)
	if err != nil {
		t.Fatalf("BuildActor(server) error = %v", err)
	}
	utxos := []kmlibs.UTXO{{
		TxID:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Vout:  0,
		Value: 2000,
	}}
	baseResp, err := ce.BuildDualFeePoolBaseTx(&utxos, 1000, client.PrivKey, server.PubKey, false, 0.5)
	if err != nil {
		t.Fatalf("BuildDualFeePoolBaseTx() error = %v", err)
	}
	spendTx, clientSig, _, err := ce.BuildDualFeePoolSpendTX(baseResp.Tx, 1000, 1, 100, client.PrivKey, server.PubKey, false, 0.5)
	if err != nil {
		t.Fatalf("BuildDualFeePoolSpendTX() error = %v", err)
	}
	serverSig, err := ce.SpendTXServerSign(spendTx, baseResp.Amount, server.PrivKey, client.PubKey)
	if err != nil {
		t.Fatalf("SpendTXServerSign() error = %v", err)
	}
	want, err := ce.MergeDualPoolSigForSpendTx(spendTx.Hex(), serverSig, clientSig)
	if err != nil {
		t.Fatalf("MergeDualPoolSigForSpendTx() error = %v", err)
	}

	got, err := mergeOpenedFeePoolCurrentTx(spendTx.Hex(), *serverSig, *clientSig)
	if err != nil {
		t.Fatalf("mergeOpenedFeePoolCurrentTx() error = %v", err)
	}
	if got != want.Hex() {
		t.Fatalf("merged current tx mismatch")
	}
}
