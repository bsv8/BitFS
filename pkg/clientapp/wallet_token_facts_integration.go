package clientapp

import (
	"context"
	"strings"
)

// loadWalletTokenBalanceWithFallback 统一 token 余额读取入口
// 设计说明：
// - 先查 fact 口径，有数据则使用 fact
// - fact 无数据时回退到旧口径（loadWalletTokenSpendableCandidates 汇总）
// - unknown 不参与 fact，不影响余额
func loadWalletTokenBalanceWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 8：先尝试 fact 口径
	bal, err := dbLoadTokenBalanceFact(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败，回退旧口径
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}
	if bal.TotalInText != "" {
		// fact 有数据，直接返回（由调用方做小数运算）
		return bal.TotalInText, nil
	}

	// fact 无数据，回退旧口径
	return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
}

// loadWalletTokenBalanceFromOldPath 旧口径：从 wallet_utxo_assets + local broadcast 汇总
func loadWalletTokenBalanceFromOldPath(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	candidates, err := loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	if err != nil {
		return "", err
	}
	var acc decimalTextAccumulator
	for _, c := range candidates {
		if err := acc.Add(c.Item.QuantityText); err != nil {
			return "", err
		}
	}
	return acc.String(), nil
}

// loadWalletTokenSpendableCandidatesWithFallback 统一 token 可花费来源入口
// 设计说明：
// - 先尝试 fact 口径（dbListTokenSpendableSourceFlows）
// - fact 有候选则返回；fact 无候选时回退旧路径
// - 返回格式统一为 walletTokenPreviewCandidate，方便上层消费
func loadWalletTokenSpendableCandidatesWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 8：先尝试 fact 口径
	flows, err := dbListTokenSpendableSourceFlows(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败，回退旧路径
		return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	}
	if len(flows) > 0 {
		// fact 有数据，转换成旧格式返回
		out := make([]walletTokenPreviewCandidate, 0, len(flows))
		for _, f := range flows {
			parsedQty, _ := parseDecimalText(f.QuantityText)
			out = append(out, walletTokenPreviewCandidate{
				Item: walletTokenOutputItem{
					UTXOID:       f.UTXOID,
					TxID:         f.TxID,
					Vout:         f.Vout,
					QuantityText: f.QuantityText,
					AssetKey:     strings.TrimSpace(f.TokenID),
				},
				CreatedAtUnix: f.OccurredAtUnix,
				Quantity:      parsedQty,
			})
		}
		return out, nil
	}

	// fact 无数据，回退旧路径
	return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
}
