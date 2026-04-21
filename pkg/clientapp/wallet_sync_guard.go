package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// walletSyncGuard 是钱包花费入口的前置校验层。
// 设计说明：
// - 这里只做“能不能继续往下走”的判断，不做等待；
// - tip / utxo 同步状态一旦过期、失败或没准备好，业务入口就直接返回；
// - 这样上层就能明确选择重试或主动触发 sync-once，而不是卡在请求里。

func requireFreshTipState(ctx context.Context, store *clientDB) (chainTipState, error) {
	if store == nil {
		return chainTipState{}, fmt.Errorf("store not initialized")
	}
	s, err := dbLoadChainTipState(ctx, store)
	if err != nil {
		return chainTipState{}, err
	}
	return validateFreshTipState(s)
}

func requireHealthyUTXOSyncState(ctx context.Context, store *clientDB, rt walletIdentityCaps) (walletUTXOSyncState, error) {
	if store == nil {
		return walletUTXOSyncState{}, fmt.Errorf("store not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return walletUTXOSyncState{}, err
	}
	s, err := dbLoadWalletUTXOSyncState(ctx, store, addr)
	if err != nil {
		return walletUTXOSyncState{}, err
	}
	return validateHealthyUTXOSyncState(s)
}

func validateFreshTipState(s chainTipState) (chainTipState, error) {
	now := time.Now().Unix()
	maxAge := syncStateMaxAge(chainTipWorkerInterval)
	if s.UpdatedAtUnix <= 0 {
		return s, fmt.Errorf("sync state not ready: updated_at_unix=%d now=%d max_age_sec=%d", s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	if strings.TrimSpace(s.LastError) != "" {
		return s, fmt.Errorf("tip sync unhealthy: last_error=%q updated_at_unix=%d now=%d max_age_sec=%d", strings.TrimSpace(s.LastError), s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	if syncStateIsStale(s.UpdatedAtUnix, now, maxAge) {
		return s, fmt.Errorf("sync state stale: updated_at_unix=%d now=%d max_age_sec=%d", s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	return s, nil
}

func validateHealthyUTXOSyncState(s walletUTXOSyncState) (walletUTXOSyncState, error) {
	now := time.Now().Unix()
	maxAge := syncStateMaxAge(chainUTXOWorkerInterval)
	if s.UpdatedAtUnix <= 0 {
		return s, fmt.Errorf("sync state not ready: updated_at_unix=%d now=%d max_age_sec=%d", s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	if strings.TrimSpace(s.LastError) != "" {
		return s, fmt.Errorf("utxo sync unhealthy: last_error=%q updated_at_unix=%d now=%d max_age_sec=%d", strings.TrimSpace(s.LastError), s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	if syncStateIsStale(s.UpdatedAtUnix, now, maxAge) {
		return s, fmt.Errorf("sync state stale: updated_at_unix=%d now=%d max_age_sec=%d", s.UpdatedAtUnix, now, int64(maxAge/time.Second))
	}
	return s, nil
}

func syncStateMaxAge(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return interval + interval/5
}

func syncStateIsStale(updatedAtUnix, nowUnix int64, maxAge time.Duration) bool {
	if updatedAtUnix <= 0 || maxAge <= 0 {
		return true
	}
	ageSec := nowUnix - updatedAtUnix
	if ageSec < 0 {
		return false
	}
	return ageSec > int64(maxAge/time.Second)
}
