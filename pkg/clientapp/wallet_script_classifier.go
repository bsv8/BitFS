package clientapp

import (
	"context"
	"fmt"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

type walletScriptClassifierRound struct {
	WalletID  string
	Address   string
	UTXOID    string
	TxID      string
	Vout      uint32
	Value     uint64
	TxHex     string
	ScriptHex string
}

type walletScriptClassificationResult struct {
	ScriptType   walletScriptType
	Reason       string
	MatchedBy    string
	ScriptHex    string
	TxHex        string
	ScriptParsed bool
}

type walletScriptModule interface {
	RoundStart(ctx context.Context, round walletScriptClassifierRound) error
	Match(ctx context.Context, round walletScriptClassifierRound) (walletScriptType, string, bool, error)
	RoundEnd(ctx context.Context, round walletScriptClassifierRound, result walletScriptClassificationResult) error
}

type walletScriptClassifier struct {
	modules []walletScriptModule
}

func newWalletScriptClassifier(modules ...walletScriptModule) *walletScriptClassifier {
	out := &walletScriptClassifier{
		modules: make([]walletScriptModule, 0, len(modules)),
	}
	for _, module := range modules {
		if module == nil {
			continue
		}
		out.modules = append(out.modules, module)
	}
	return out
}

func defaultWalletScriptClassifier(source walletScriptEvidenceSource) *walletScriptClassifier {
	return newWalletScriptClassifier(
		newWalletScriptModule1SatToken(source),
		newWalletScriptModuleP2PKH(),
		newWalletScriptModuleP2PK(),
	)
}

func classifyWalletUTXO(ctx context.Context, source walletScriptEvidenceSource, address string, utxoID string, txid string, vout uint32, value uint64, txHex string) walletScriptClassificationResult {
	return classifyWalletUTXOWithScriptHex(ctx, source, address, utxoID, txid, vout, value, "", txHex)
}

func classifyWalletUTXOWithScriptHex(ctx context.Context, source walletScriptEvidenceSource, address string, utxoID string, txid string, vout uint32, value uint64, scriptHex string, txHex string) walletScriptClassificationResult {
	return classifyWalletUTXOWithClassifier(ctx, defaultWalletScriptClassifier(source), source, address, utxoID, txid, vout, value, scriptHex, txHex)
}

func classifyWalletUTXOWithClassifier(ctx context.Context, classifier *walletScriptClassifier, source walletScriptEvidenceSource, address string, utxoID string, txid string, vout uint32, value uint64, scriptHex string, txHex string) walletScriptClassificationResult {
	round := walletScriptClassifierRound{
		Address:   strings.TrimSpace(address),
		UTXOID:    strings.TrimSpace(utxoID),
		TxID:      strings.ToLower(strings.TrimSpace(txid)),
		Vout:      vout,
		Value:     value,
		TxHex:     strings.ToLower(strings.TrimSpace(txHex)),
		ScriptHex: strings.ToLower(strings.TrimSpace(scriptHex)),
	}
	if round.ScriptHex == "" && round.TxHex == "" && round.Value != 1 && source != nil && round.TxID != "" {
		if fetched, err := source.GetTxHex(ctx, round.TxID); err == nil {
			round.TxHex = strings.ToLower(strings.TrimSpace(fetched))
		}
	}
	if round.ScriptHex == "" && round.TxHex != "" {
		if parsed, err := txsdk.NewTransactionFromHex(round.TxHex); err == nil {
			if scriptHex, err := walletOutputLockingScriptHexFromTx(parsed, vout); err == nil {
				round.ScriptHex = strings.ToLower(strings.TrimSpace(scriptHex))
			}
		}
	}
	if classifier == nil {
		classifier = defaultWalletScriptClassifier(nil)
	}
	return classifier.Classify(ctx, round)
}

func (c *walletScriptClassifier) Classify(ctx context.Context, round walletScriptClassifierRound) walletScriptClassificationResult {
	if c == nil || len(c.modules) == 0 {
		return walletScriptClassificationResult{
			ScriptType: walletScriptTypeUnknown,
			Reason:     "script classifier not initialized",
			MatchedBy:  "",
			ScriptHex:  strings.ToLower(strings.TrimSpace(round.ScriptHex)),
			TxHex:      strings.ToLower(strings.TrimSpace(round.TxHex)),
		}
	}
	for _, module := range c.modules {
		_ = module.RoundStart(ctx, round)
	}
	result := walletScriptClassificationResult{
		ScriptType: walletScriptTypeUnknown,
		Reason:     "no script module matched",
		MatchedBy:  "",
		ScriptHex:  strings.ToLower(strings.TrimSpace(round.ScriptHex)),
		TxHex:      strings.ToLower(strings.TrimSpace(round.TxHex)),
	}
	lastReason := ""
	for _, module := range c.modules {
		scriptType, reason, matched, err := module.Match(ctx, round)
		if err != nil {
			if reason != "" {
				lastReason = strings.TrimSpace(reason)
			}
			if round.Value == 1 && isWalletScriptModule1SatToken(module) {
				if strings.TrimSpace(lastReason) == "" {
					lastReason = err.Error()
				}
				result.Reason = lastReason
				break
			}
			continue
		}
		if !matched {
			if strings.TrimSpace(reason) != "" {
				lastReason = strings.TrimSpace(reason)
			}
			continue
		}
		result.ScriptType = normalizeWalletScriptType(string(scriptType))
		result.Reason = strings.TrimSpace(reason)
		result.MatchedBy = moduleName(module)
		result.ScriptParsed = round.ScriptHex != ""
		break
	}
	for i := len(c.modules) - 1; i >= 0; i-- {
		_ = c.modules[i].RoundEnd(ctx, round, result)
	}
	if result.Reason == "" {
		if strings.TrimSpace(lastReason) != "" {
			result.Reason = lastReason
		} else {
			result.Reason = "no script module matched"
		}
	}
	return result
}

func moduleName(module walletScriptModule) string {
	if module == nil {
		return ""
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", module), "*")
}

func isWalletScriptModule1SatToken(module walletScriptModule) bool {
	return strings.Contains(moduleName(module), "walletScriptModule1SatToken")
}
