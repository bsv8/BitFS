package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
)

type walletScriptModule1SatToken struct {
	source            walletScriptEvidenceSource
	prefetchedAddress string
	prefetchErr       error
	prefetchedBSV20   []walletBSV20WOCCandidate
	prefetchedBSV21   []walletBSV21WOCCandidate
	txHexByTxID       map[string]string
	txHexErrByTxID    map[string]error
}

func newWalletScriptModule1SatToken(source walletScriptEvidenceSource) walletScriptModule {
	return &walletScriptModule1SatToken{source: source}
}

func (m *walletScriptModule1SatToken) RoundStart(ctx context.Context, round walletScriptClassifierRound) error {
	if m == nil {
		return nil
	}
	if round.Value != 1 {
		return nil
	}
	address := strings.TrimSpace(round.Address)
	if address == "" || m.source == nil {
		return nil
	}
	if strings.EqualFold(m.prefetchedAddress, address) {
		return nil
	}
	bsv21Items, bsv21Err := m.source.GetAddressBSV21TokenUnspent(ctx, address)
	if bsv21Err != nil {
		m.prefetchedAddress = address
		m.prefetchErr = bsv21Err
		m.prefetchedBSV20 = nil
		m.prefetchedBSV21 = nil
		return nil
	}
	bsv20Items, bsv20Err := m.source.GetAddressBSV20TokenUnspent(ctx, address)
	if bsv20Err != nil {
		m.prefetchedAddress = address
		m.prefetchErr = bsv20Err
		m.prefetchedBSV20 = nil
		m.prefetchedBSV21 = nil
		return nil
	}
	m.prefetchedAddress = address
	m.prefetchErr = nil
	m.prefetchedBSV20 = bsv20Items
	m.prefetchedBSV21 = bsv21Items
	return nil
}

func (m *walletScriptModule1SatToken) Match(ctx context.Context, round walletScriptClassifierRound) (walletScriptType, string, bool, error) {
	if round.Value != 1 {
		return walletScriptTypeUnknown, "", false, nil
	}
	scriptHex := strings.TrimSpace(round.ScriptHex)
	if scriptHex == "" {
		return walletScriptTypeUnknown, "locking script missing", false, fmt.Errorf("locking script missing")
	}
	parsedScript, err := script.NewFromHex(scriptHex)
	if err != nil {
		return walletScriptTypeUnknown, "locking script parse failed", false, fmt.Errorf("locking script parse failed")
	}
	payload, ok := decodeWalletTokenEnvelopePayload(parsedScript)
	if !ok {
		return walletScriptTypeUnknown, "locking script is not token envelope", false, nil
	}
	if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
		return walletScriptTypeUnknown, "locking script is not token envelope", false, nil
	}
	op := strings.ToLower(strings.TrimSpace(firstNonEmptyStringField(payload, "op")))
	if op != "transfer" && op != "deploy+mint" {
		return walletScriptTypeUnknown, "locking script is not token envelope", false, nil
	}
	if m == nil || m.source == nil {
		return walletScriptTypeUnknown, "token evidence unavailable", false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(m.prefetchedAddress), strings.TrimSpace(round.Address)) {
		return walletScriptTypeUnknown, "token evidence unavailable", false, nil
	}
	if m.prefetchErr != nil {
		return walletScriptTypeUnknown, "token evidence unavailable", false, m.prefetchErr
	}
	txHex := strings.TrimSpace(round.TxHex)
	if txHex == "" {
		fetchedTxHex, fetchErr := m.loadTxHex(ctx, round.TxID)
		if fetchErr != nil {
			return walletScriptTypeUnknown, "token evidence unavailable", false, fetchErr
		}
		txHex = strings.TrimSpace(fetchedTxHex)
	}
	if txHex == "" {
		return walletScriptTypeUnknown, "token evidence unavailable", false, nil
	}
	matchedBSV21 := false
	matchedBSV20 := false
	for _, item := range m.prefetchedBSV21 {
		if strings.ToLower(strings.TrimSpace(item.Current.TxID)) != round.TxID {
			continue
		}
		ok, err := matchBSV21VoutByScriptHash(ctx, txHex, item.ScriptHash, round.Vout)
		if err != nil || !ok {
			continue
		}
		matchedBSV21 = true
		break
	}
	for _, item := range m.prefetchedBSV20 {
		if strings.ToLower(strings.TrimSpace(item.Current.TxID)) != round.TxID {
			continue
		}
		ok, err := matchBSV20VoutByScriptHash(ctx, txHex, item.ScriptHash, round.Vout)
		if err != nil || !ok {
			continue
		}
		matchedBSV20 = true
		break
	}
	switch {
	case matchedBSV20 && matchedBSV21:
		return walletScriptTypeBSV20OrBSV21, "matched both bsv20 and bsv21 evidence", true, nil
	case matchedBSV20:
		return walletScriptTypeBSV20, "matched bsv20 evidence", true, nil
	case matchedBSV21:
		return walletScriptTypeBSV21, "matched bsv21 evidence", true, nil
	default:
		return walletScriptTypeUnknown, "token evidence not found", false, nil
	}
}

func (m *walletScriptModule1SatToken) RoundEnd(context.Context, walletScriptClassifierRound, walletScriptClassificationResult) error {
	return nil
}

func (m *walletScriptModule1SatToken) loadTxHex(ctx context.Context, txid string) (string, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" || m == nil || m.source == nil {
		return "", nil
	}
	if txHex, ok := m.txHexByTxID[txid]; ok {
		if err, exists := m.txHexErrByTxID[txid]; exists {
			return txHex, err
		}
		return txHex, nil
	}
	rawTxHex, err := m.source.GetTxHex(ctx, txid)
	txHex := strings.ToLower(strings.TrimSpace(rawTxHex))
	if m.txHexByTxID == nil {
		m.txHexByTxID = make(map[string]string)
	}
	if m.txHexErrByTxID == nil {
		m.txHexErrByTxID = make(map[string]error)
	}
	m.txHexByTxID[txid] = txHex
	m.txHexErrByTxID[txid] = err
	return txHex, err
}
