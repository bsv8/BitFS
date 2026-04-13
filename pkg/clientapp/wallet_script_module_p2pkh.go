package clientapp

import (
	"context"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
)

type walletScriptModuleP2PKH struct{}

func newWalletScriptModuleP2PKH() walletScriptModule {
	return walletScriptModuleP2PKH{}
}

func (walletScriptModuleP2PKH) RoundStart(context.Context, walletScriptClassifierRound) error {
	return nil
}

func (walletScriptModuleP2PKH) Match(_ context.Context, round walletScriptClassifierRound) (walletScriptType, string, bool, error) {
	scriptHex := strings.TrimSpace(round.ScriptHex)
	if scriptHex == "" {
		return walletScriptTypeUnknown, "", false, nil
	}
	parsed, err := script.NewFromHex(scriptHex)
	if err != nil {
		return walletScriptTypeUnknown, "script parse failed", false, nil
	}
	if !parsed.IsP2PKH() {
		return walletScriptTypeUnknown, "", false, nil
	}
	return walletScriptTypeP2PKH, "pure P2PKH script", true, nil
}

func (walletScriptModuleP2PKH) RoundEnd(context.Context, walletScriptClassifierRound, walletScriptClassificationResult) error {
	return nil
}
