package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	bsvscript "github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
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

func buildTestWalletScriptTokenEnvelopeTxHex(t *testing.T) (string, string, string) {
	t.Helper()

	rt := newWalletBSV21LocalCandidateTestRuntime(t, nil, "http://127.0.0.1:1")
	identity := rt.mustIdentity()
	if identity == nil || identity.Actor == nil {
		t.Fatal("runtime identity not initialized")
	}
	walletAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(identity.Actor.Addr))
	if err != nil {
		t.Fatalf("NewAddressFromString: %v", err)
	}
	lockScript, err := p2pkh.Lock(walletAddr)
	if err != nil {
		t.Fatalf("p2pkh.Lock: %v", err)
	}
	tx := txsdk.NewTransaction()
	if err := tx.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: lockScript,
		ContentType:   walletTokenContentType,
		Data:          []byte(`{"p":"bsv-20","op":"transfer","id":"test-token","amt":"1","sym":"T"}`),
	}); err != nil {
		t.Fatalf("tx.Inscribe: %v", err)
	}
	txHex := strings.ToLower(strings.TrimSpace(tx.Hex()))
	scriptHexes, err := extractOutputScriptsFromTxHex(txHex)
	if err != nil {
		t.Fatalf("extractOutputScriptsFromTxHex: %v", err)
	}
	if len(scriptHexes) == 0 {
		t.Fatal("expected token envelope output")
	}
	return txHex, scriptHexes[0], strings.ToLower(strings.TrimSpace(tx.TxID().String()))
}

func buildTestWalletScriptScriptHash(scriptHex string) string {
	scriptBytes, err := hex.DecodeString(strings.TrimSpace(scriptHex))
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(scriptBytes)
	return strings.ToLower(hex.EncodeToString(hash[:]))
}
