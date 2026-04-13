package clientapp

import (
	"context"
	"fmt"
)

const (
	walletAssetGroupToken   = "token"
	walletAssetGroupOrdinal = "ordinal"
)

// refreshWalletAssetProjection 在链同步后把脚本语义汇总回写到运行态投影。
// 设计说明：
// - 这里只负责刷新由 script_type 派生的运行态统计，不再按金额做归一化；
// - 脚本语义是唯一真相，余额只是脚本语义的汇总结果。
func refreshWalletAssetProjection(ctx context.Context, store *clientDB, address string, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	if err := dbRefreshWalletAssetProjection(ctx, store, address, trigger); err != nil {
		return err
	}
	return nil
}
