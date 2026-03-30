package clientapp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type walletSyncOnceResult struct {
	WalletAddress   string `json:"wallet_address"`
	TipHeight       uint32 `json:"tip_height"`
	UTXOCount       int    `json:"utxo_count"`
	BalanceSatoshi  uint64 `json:"balance_satoshi"`
	SyncRoundID     string `json:"sync_round_id"`
	Trigger         string `json:"trigger"`
	CompletedAtUnix int64  `json:"completed_at_unix"`
}

// TriggerWalletSyncOnce 触发一次同步钱包链视图与资产投影。
// 设计说明：
// - 这不是“请求后台稍后刷新”，而是明确的一次同步执行；
// - 先刷新 tip，再刷新 utxo/asset projection，保证本地诊断表与钱包视图一起推进；
// - 这条入口主要给 e2e 与手动调试使用，避免把“等待后台定时任务”误当业务契约。
func TriggerWalletSyncOnce(ctx context.Context, rt *Runtime, trigger string) (walletSyncOnceResult, error) {
	if rt == nil {
		return walletSyncOnceResult{}, fmt.Errorf("runtime not initialized")
	}
	maint := getChainMaintainer(rt)
	if maint == nil {
		return walletSyncOnceResult{}, fmt.Errorf("chain maintainer not initialized")
	}
	trigger = strings.TrimSpace(trigger)
	if trigger == "" {
		trigger = "wallet_sync_once"
	}
	triggeredAt := time.Now().Unix()
	tipResult, err := maint.runTaskSync(ctx, chainTask{
		TaskType:      chainTaskTip,
		TriggerSource: trigger,
		TriggeredAt:   triggeredAt,
	})
	if err != nil {
		return walletSyncOnceResult{}, err
	}
	utxoResult, err := maint.runTaskSync(ctx, chainTask{
		TaskType:      chainTaskUTXO,
		TriggerSource: trigger,
		TriggeredAt:   time.Now().Unix(),
	})
	if err != nil {
		return walletSyncOnceResult{}, err
	}
	return walletSyncOnceResult{
		WalletAddress:   strings.TrimSpace(anyToString(utxoResult["address"])),
		TipHeight:       uint32(anyToInt64(tipResult["tip_to"])),
		UTXOCount:       int(anyToInt64(utxoResult["utxo_count"])),
		BalanceSatoshi:  uint64(anyToInt64(utxoResult["balance_satoshi"])),
		SyncRoundID:     strings.TrimSpace(anyToString(utxoResult["sync_round_id"])),
		Trigger:         trigger,
		CompletedAtUnix: time.Now().Unix(),
	}, nil
}

func (s *httpAPIServer) handleWalletSyncOnce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	out, err := TriggerWalletSyncOnce(r.Context(), s.rt, "http_wallet_sync_once")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"wallet_address":     out.WalletAddress,
		"tip_height":         out.TipHeight,
		"utxo_count":         out.UTXOCount,
		"balance_satoshi":    out.BalanceSatoshi,
		"sync_round_id":      out.SyncRoundID,
		"trigger":            out.Trigger,
		"completed_at_unix":  out.CompletedAtUnix,
	})
}
