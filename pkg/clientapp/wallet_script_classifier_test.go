package clientapp

import (
	"context"
	"strings"
	"testing"
)

func TestDefaultWalletScriptClassifierModuleOrder(t *testing.T) {
	t.Parallel()

	classifier := defaultWalletScriptClassifier(nil)
	if classifier == nil {
		t.Fatal("classifier is nil")
	}
	if len(classifier.modules) != 3 {
		t.Fatalf("module count mismatch: got=%d want=3", len(classifier.modules))
	}
	got := []string{
		moduleName(classifier.modules[0]),
		moduleName(classifier.modules[1]),
		moduleName(classifier.modules[2]),
	}
	if !strings.Contains(got[0], "walletScriptModule1SatToken") {
		t.Fatalf("first module mismatch: %+v", got)
	}
	if !strings.Contains(got[1], "walletScriptModuleP2PKH") {
		t.Fatalf("second module mismatch: %+v", got)
	}
	if !strings.Contains(got[2], "walletScriptModuleP2PK") {
		t.Fatalf("third module mismatch: %+v", got)
	}
}

func TestClassifyWalletUTXOWithoutScriptStaysUnknown(t *testing.T) {
	t.Parallel()

	result := classifyWalletUTXOWithClassifier(context.Background(), defaultWalletScriptClassifier(nil), nil, "wallet-addr", "utxo-1", "tx-1", 0, 2, "", "")
	if result.ScriptType != walletScriptTypeUnknown {
		t.Fatalf("script type mismatch: got=%s want=%s", result.ScriptType, walletScriptTypeUnknown)
	}
}

func TestWalletScriptClassifierValue1PlainP2PKHDoesNotBecomeToken(t *testing.T) {
	t.Parallel()

	scriptHexes, err := extractOutputScriptsFromTxHex(testWalletScriptPlainTxHex)
	if err != nil {
		t.Fatalf("extractOutputScriptsFromTxHex: %v", err)
	}
	if len(scriptHexes) == 0 {
		t.Fatal("expected at least one output script")
	}
	source := &countingWalletScriptEvidenceSource{
		bsv20ByAddress: map[string][]walletBSV20WOCCandidate{},
		bsv21ByAddress: map[string][]walletBSV21WOCCandidate{},
	}
	result := classifyWalletUTXOWithClassifier(context.Background(), defaultWalletScriptClassifier(source), source, "wallet-addr", "utxo-1", "tx-1", 0, 1, scriptHexes[0], "")
	if result.ScriptType != walletScriptTypeP2PKH {
		t.Fatalf("script type mismatch: got=%s want=%s", result.ScriptType, walletScriptTypeP2PKH)
	}
	if source.getTxHexCount != 0 {
		t.Fatalf("unexpected tx hex fetch count: %d", source.getTxHexCount)
	}
}

type countingWalletScriptEvidenceSource struct {
	getTxHexCount  int
	getBSV20Count  int
	getBSV21Count  int
	bsv20ByAddress map[string][]walletBSV20WOCCandidate
	bsv21ByAddress map[string][]walletBSV21WOCCandidate
	txHexByTxID    map[string]string
}

func (s *countingWalletScriptEvidenceSource) GetTxHex(ctx context.Context, txid string) (string, error) {
	s.getTxHexCount++
	if s.txHexByTxID == nil {
		return "", nil
	}
	return s.txHexByTxID[strings.ToLower(strings.TrimSpace(txid))], nil
}

func (s *countingWalletScriptEvidenceSource) GetAddressBSV20TokenUnspent(ctx context.Context, address string) ([]walletBSV20WOCCandidate, error) {
	s.getBSV20Count++
	if s.bsv20ByAddress == nil {
		return []walletBSV20WOCCandidate{}, nil
	}
	return s.bsv20ByAddress[strings.ToLower(strings.TrimSpace(address))], nil
}

func (s *countingWalletScriptEvidenceSource) GetAddressBSV21TokenUnspent(ctx context.Context, address string) ([]walletBSV21WOCCandidate, error) {
	s.getBSV21Count++
	if s.bsv21ByAddress == nil {
		return []walletBSV21WOCCandidate{}, nil
	}
	return s.bsv21ByAddress[strings.ToLower(strings.TrimSpace(address))], nil
}

func TestWalletScriptClassifierTokenEvidenceAndTxHexCache(t *testing.T) {
	t.Parallel()

	txHex, scriptHex, txid := buildTestWalletScriptTokenEnvelopeTxHex(t)
	scriptHash := buildTestWalletScriptScriptHash(scriptHex)
	address := "mkir1imD121rwQxxzmgGEA2F4oR3L3GVvv"

	source := &countingWalletScriptEvidenceSource{
		bsv20ByAddress: map[string][]walletBSV20WOCCandidate{
			strings.ToLower(address): []walletBSV20WOCCandidate{
				{
					ScriptHash: scriptHash,
					Current: struct {
						TxID string `json:"txid"`
					}{TxID: txid},
				},
			},
		},
		bsv21ByAddress: map[string][]walletBSV21WOCCandidate{
			strings.ToLower(address): []walletBSV21WOCCandidate{},
		},
		txHexByTxID: map[string]string{
			txid: txHex,
		},
	}
	classifier := defaultWalletScriptClassifier(source)

	first := classifyWalletUTXOWithClassifier(context.Background(), classifier, source, address, "utxo-1", txid, 0, 1, scriptHex, "")
	second := classifyWalletUTXOWithClassifier(context.Background(), classifier, source, address, "utxo-2", txid, 0, 1, scriptHex, "")

	if first.ScriptType != walletScriptTypeBSV20 {
		t.Fatalf("first classification mismatch: got=%s want=%s", first.ScriptType, walletScriptTypeBSV20)
	}
	if second.ScriptType != walletScriptTypeBSV20 {
		t.Fatalf("second classification mismatch: got=%s want=%s", second.ScriptType, walletScriptTypeBSV20)
	}
	if source.getBSV20Count != 1 || source.getBSV21Count != 1 {
		t.Fatalf("prefetch count mismatch: bsv20=%d bsv21=%d", source.getBSV20Count, source.getBSV21Count)
	}
	if source.getTxHexCount != 1 {
		t.Fatalf("unexpected tx hex fetch count: %d", source.getTxHexCount)
	}
}

type failingWalletScriptEvidenceSource struct {
	getBSV20Count int
	getBSV21Count int
}

func (s *failingWalletScriptEvidenceSource) GetTxHex(context.Context, string) (string, error) {
	return "", nil
}

func (s *failingWalletScriptEvidenceSource) GetAddressBSV20TokenUnspent(context.Context, string) ([]walletBSV20WOCCandidate, error) {
	s.getBSV20Count++
	return nil, nil
}

func (s *failingWalletScriptEvidenceSource) GetAddressBSV21TokenUnspent(context.Context, string) ([]walletBSV21WOCCandidate, error) {
	s.getBSV21Count++
	return nil, context.DeadlineExceeded
}

func TestWalletScriptModule1SatTokenFailureReturnsUnknown(t *testing.T) {
	t.Parallel()

	txHex, scriptHex, txid := buildTestWalletScriptTokenEnvelopeTxHex(t)
	source := &failingWalletScriptEvidenceSource{}
	classifier := defaultWalletScriptClassifier(source)
	result := classifyWalletUTXOWithClassifier(context.Background(), classifier, source, "mkir1imD121rwQxxzmgGEA2F4oR3L3GVvv", "utxo-1", txid, 0, 1, scriptHex, txHex)
	if result.ScriptType != walletScriptTypeUnknown {
		t.Fatalf("script type mismatch: got=%s want=%s", result.ScriptType, walletScriptTypeUnknown)
	}
	if source.getBSV21Count != 1 {
		t.Fatalf("unexpected bsv21 count: %d", source.getBSV21Count)
	}
	if source.getBSV20Count != 0 {
		t.Fatalf("unexpected bsv20 count: %d", source.getBSV20Count)
	}
}
