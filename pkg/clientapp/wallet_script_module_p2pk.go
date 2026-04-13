package clientapp

import (
	"context"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
)

type walletScriptModuleP2PK struct{}

func newWalletScriptModuleP2PK() walletScriptModule {
	return walletScriptModuleP2PK{}
}

func (walletScriptModuleP2PK) RoundStart(context.Context, walletScriptClassifierRound) error {
	return nil
}

func (walletScriptModuleP2PK) Match(_ context.Context, round walletScriptClassifierRound) (walletScriptType, string, bool, error) {
	scriptHex := strings.TrimSpace(round.ScriptHex)
	if scriptHex == "" {
		return walletScriptTypeUnknown, "", false, nil
	}
	parsed, err := script.NewFromHex(scriptHex)
	if err != nil {
		return walletScriptTypeUnknown, "script parse failed", false, nil
	}
	if !parsed.IsP2PK() {
		return walletScriptTypeUnknown, "", false, nil
	}
	return walletScriptTypeP2PK, "pure P2PK script", true, nil
}

func (walletScriptModuleP2PK) RoundEnd(context.Context, walletScriptClassifierRound, walletScriptClassificationResult) error {
	return nil
}
