package clientapp

import "strings"

type walletScriptType string

const (
	walletScriptTypeP2PKH        walletScriptType = "P2PKH"
	walletScriptTypeP2PK         walletScriptType = "P2PK"
	walletScriptTypeBSV20        walletScriptType = "bsv20"
	walletScriptTypeBSV21        walletScriptType = "bsv21"
	walletScriptTypeBSV20OrBSV21 walletScriptType = "bsv20orbsv21"
	walletScriptTypeUnknown      walletScriptType = "unknown"
)

func (t walletScriptType) String() string {
	return string(t)
}

func normalizeWalletScriptType(raw string) walletScriptType {
	switch walletScriptType(strings.TrimSpace(raw)) {
	case walletScriptTypeP2PKH:
		return walletScriptTypeP2PKH
	case walletScriptTypeP2PK:
		return walletScriptTypeP2PK
	case walletScriptTypeBSV20:
		return walletScriptTypeBSV20
	case walletScriptTypeBSV21:
		return walletScriptTypeBSV21
	case walletScriptTypeBSV20OrBSV21:
		return walletScriptTypeBSV20OrBSV21
	default:
		return walletScriptTypeUnknown
	}
}

func walletScriptTypeAllocationClass(scriptType string) string {
	switch normalizeWalletScriptType(scriptType) {
	case walletScriptTypeP2PKH, walletScriptTypeP2PK:
		return walletUTXOAllocationPlainBSV
	case walletScriptTypeBSV20, walletScriptTypeBSV21, walletScriptTypeBSV20OrBSV21:
		return walletUTXOAllocationProtectedAsset
	default:
		return walletUTXOAllocationUnknown
	}
}

func walletScriptTypeIsPlain(scriptType string) bool {
	switch normalizeWalletScriptType(scriptType) {
	case walletScriptTypeP2PKH, walletScriptTypeP2PK:
		return true
	default:
		return false
	}
}
