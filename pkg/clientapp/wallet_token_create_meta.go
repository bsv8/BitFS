package clientapp

import (
	"fmt"
	"strconv"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

type walletBSV21CreateMeta struct {
	Symbol    string
	MaxSupply string
	Decimals  int
	Icon      string
}

// 说明：这里只负责从交易里提取创建元数据，不带任何状态语义。
// 这样后面删旧状态表时，不会把创建链路一起误伤。
func extractBSV21DeployMintStatusMetaFromTx(tx *txsdk.Transaction) walletBSV21CreateMeta {
	meta := walletBSV21CreateMeta{}
	if tx == nil || len(tx.Outputs) == 0 {
		return meta
	}
	var iconRef string
	for _, output := range tx.Outputs {
		if output == nil || output.LockingScript == nil {
			continue
		}
		payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript)
		if !ok {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "op"), "deploy+mint") {
			continue
		}
		meta.Symbol = strings.TrimSpace(firstNonEmptyStringField(payload, "sym"))
		meta.MaxSupply = strings.TrimSpace(firstNonEmptyStringField(payload, "amt"))
		iconRef = strings.TrimSpace(firstNonEmptyStringField(payload, "icon"))
		if decText := strings.TrimSpace(firstNonEmptyStringField(payload, "dec")); decText != "" {
			if parsed, err := strconv.Atoi(decText); err == nil && parsed >= 0 {
				meta.Decimals = parsed
			}
		}
		break
	}
	if iconRef == fmt.Sprintf("_%d", walletBSV21CreateIconOutputVout) && len(tx.Outputs) > int(walletBSV21CreateIconOutputVout) {
		output := tx.Outputs[walletBSV21CreateIconOutputVout]
		if output != nil && output.LockingScript != nil {
			if payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript); ok &&
				strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bitfs") &&
				strings.EqualFold(firstNonEmptyStringField(payload, "type"), "hash") {
				meta.Icon = strings.ToLower(strings.TrimSpace(firstNonEmptyStringField(payload, "hash")))
			}
		}
	}
	return meta
}
