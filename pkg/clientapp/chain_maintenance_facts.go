package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

func getWalletUTXOsFromDB(ctx context.Context, store ClientStore, rt walletIdentityCaps) ([]poolcore.UTXO, error) {
	return listEligiblePlainBSVWalletUTXOsFact(ctx, store, rt)
}

// listEligiblePlainBSVWalletUTXOsFact 从 fact 口径获取可花费的 plain_bsv UTXO
// 设计说明：
// - 按 spendable source flow 返回，剩余 > 0 的才返回
// - 返回的是原始金额，实际使用由上层决定
func listEligiblePlainBSVWalletUTXOsFact(ctx context.Context, store ClientStore, rt walletIdentityCaps) ([]poolcore.UTXO, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	walletID := walletIDByAddress(addr)
	if store == nil {
		return nil, fmt.Errorf("store not initialized")
	}

	flows, err := dbListSpendableSourceFlows(ctx, store, walletID, "BSV", "")
	if err != nil {
		return nil, err
	}

	out := make([]poolcore.UTXO, 0, len(flows))
	for _, f := range flows {
		out = append(out, poolcore.UTXO{
			TxID:  strings.ToLower(strings.TrimSpace(f.TxID)),
			Vout:  f.Vout,
			Value: uint64(f.Remaining),
		})
	}
	return out, nil
}

func getWalletBalanceFromDB(ctx context.Context, store *clientDB, rt *Runtime) (string, uint64, error) {
	if store == nil {
		return "", 0, fmt.Errorf("store not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return "", 0, err
	}
	walletID := walletIDByAddress(addr)

	// Step 5：fact 优先读余额（严格口径：查询成功就返回，含 0 值）
	// 这里读的是 fact 口径，所以 wallet_fact_orchestrator 和 dbUpsertBSVUTXODB
	// 必须保证状态单调一致，不能让旧链视图把 spent 回刷成 unspent。
	bal, err := dbLoadWalletAssetBalanceFact(ctx, store, walletID, "BSV", "")
	if err == nil {
		return addr, uint64(bal.Remaining), nil
	}

	// fact 查询失败时回退 wallet_utxo_sync_state
	s, err := dbLoadWalletUTXOSyncState(ctx, store, addr)
	if err != nil {
		return "", 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return "", 0, fmt.Errorf("wallet utxo sync state not ready")
	}
	if isWalletUTXOSyncStateStaleForRuntime(rt, s) {
		return "", 0, fmt.Errorf("wallet utxo sync state stale for current runtime")
	}
	stats, err := dbLoadWalletUTXOAggregate(ctx, store, addr)
	if err != nil {
		return "", 0, err
	}
	if strings.TrimSpace(s.LastError) != "" && stats.PlainBSVBalanceSatoshi == 0 {
		return "", 0, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	// 设计说明：
	// - wallet_utxo 表是链上未花费输出的本地投影；
	// - 只要这张表里已经有余额，就不应该因为后续某个 history 游标请求失败而把余额清零；
	// - 同步错误仍然保留在 wallet_utxo_sync_last_error 等诊断字段里，前端可以继续展示告警。
	return addr, stats.PlainBSVBalanceSatoshi, nil
}
