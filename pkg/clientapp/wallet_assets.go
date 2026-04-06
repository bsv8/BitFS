package clientapp

import (
	"context"
	"fmt"
)

const (
	walletAssetGroupToken   = "token"
	walletAssetGroupOrdinal = "ordinal"
)

// defaultWalletUTXOProtectionForValue 按当前 `1 sat` 风险模型给新 UTXO 一个默认保护分类。
// 设计说明：
// - 当前不再在本地判定 token / ordinal 是否成立；
// - 但 1 聪输出仍然不能直接当普通零钱放行，否则会把潜在资产输出误花；
// - 因此只有 value=1 的输出，先进入 unknown，等待后续业务显式按证据面处理；
// - 非 1 聪输出默认仍按 plain_bsv 处理，避免把保护范围错误放大。
func defaultWalletUTXOProtectionForValue(value uint64) (string, string) {
	if value == 1 {
		return walletUTXOAllocationUnknown, "awaiting external token evidence"
	}
	return walletUTXOAllocationPlainBSV, ""
}

// refreshWalletAssetProjection 在链同步后把 `1 sat` 输出维持在受保护状态。
// 设计说明：
// - 这里只负责把未花费 UTXO 按 1 聪规则重新归类保护分类。
func refreshWalletAssetProjection(ctx context.Context, store *clientDB, address string, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	if err := dbRefreshWalletAssetProjection(ctx, store, address, trigger); err != nil {
		return err
	}
	return nil
}
