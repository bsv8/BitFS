package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

const (
	walletUTXOAllocationPlainBSV       = string(fundalloc.ProtectionPlainBSV)
	walletUTXOAllocationProtectedAsset = string(fundalloc.ProtectionProtectedAsset)
	walletUTXOAllocationUnknown        = string(fundalloc.ProtectionUnknown)
)

type walletFundingCandidate struct {
	UTXOID           string
	UTXO             poolcore.UTXO
	CreatedAtUnix    int64
	AllocationClass  string
	AllocationReason string
}

// listWalletFundingCandidates 走显式 store 读取钱包快照，返回候选输出与保护信息。
// 设计说明：
// - 这里保留“识别结果 + 金额”一起读出的方式，让后续分配器先看保护分类，再谈选币；
// - 不再从 Runtime 里回退取 DB，依赖必须由上游显式传入。
func listWalletFundingCandidates(ctx context.Context, store *clientDB, rt *Runtime) ([]walletFundingCandidate, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("store not initialized")
	}
	items, err := dbListWalletFundingCandidates(ctx, store, addr)
	if err != nil {
		return nil, err
	}
	s, err := dbLoadWalletUTXOSyncState(ctx, store, addr)
	if err != nil {
		return nil, err
	}
	if isWalletUTXOSyncStateStaleForRuntime(rt, s) {
		return nil, fmt.Errorf("wallet utxo sync state stale for current runtime")
	}
	out := make([]walletFundingCandidate, 0, len(items))
	for _, item := range items {
		out = append(out, walletFundingCandidate{
			UTXOID:           item.UTXOID,
			UTXO:             item.UTXO,
			CreatedAtUnix:    item.CreatedAtUnix,
			AllocationClass:  item.AllocationClass,
			AllocationReason: item.AllocationReason,
		})
	}
	return out, nil
}

// listEligiblePlainBSVWalletUTXOs 走显式 store 读取钱包快照，再按同一套规则筛出可花的普通 BSV 输出。
// 设计说明：
// - 这里只换数据入口，不改普通 BSV 的筛选规则；
// - 这样费用池和其他流程可以逐步脱离 Runtime 上的旧 DB 入口。
func listEligiblePlainBSVWalletUTXOs(ctx context.Context, store *clientDB, rt *Runtime) ([]poolcore.UTXO, error) {
	candidates, err := listWalletFundingCandidates(ctx, store, rt)
	if err != nil {
		return nil, err
	}
	input := make([]fundalloc.Candidate, 0, len(candidates))
	for _, item := range candidates {
		input = append(input, fundalloc.Candidate{
			ID:               item.UTXOID,
			TxID:             item.UTXO.TxID,
			Vout:             item.UTXO.Vout,
			ValueSatoshi:     item.UTXO.Value,
			CreatedAtUnix:    item.CreatedAtUnix,
			ProtectionClass:  fundalloc.ProtectionClass(item.AllocationClass),
			ProtectionReason: item.AllocationReason,
		})
	}
	filtered := fundalloc.FilterEligiblePlainBSV(input)
	out := make([]poolcore.UTXO, 0, len(filtered.Selected))
	for _, item := range filtered.Selected {
		out = append(out, poolcore.UTXO{
			TxID:  strings.ToLower(strings.TrimSpace(item.TxID)),
			Vout:  item.Vout,
			Value: item.ValueSatoshi,
		})
	}
	return out, nil
}

// allocatePlainBSVWalletUTXOs 为普通 BSV 支出选择一组输入。
func allocatePlainBSVWalletUTXOs(ctx context.Context, store *clientDB, rt *Runtime, purpose string, target uint64) ([]poolcore.UTXO, error) {
	_ = strings.TrimSpace(purpose)
	candidates, err := listWalletFundingCandidates(ctx, store, rt)
	if err != nil {
		return nil, err
	}
	input := make([]fundalloc.Candidate, 0, len(candidates))
	for _, item := range candidates {
		input = append(input, fundalloc.Candidate{
			ID:               item.UTXOID,
			TxID:             item.UTXO.TxID,
			Vout:             item.UTXO.Vout,
			ValueSatoshi:     item.UTXO.Value,
			CreatedAtUnix:    item.CreatedAtUnix,
			ProtectionClass:  fundalloc.ProtectionClass(item.AllocationClass),
			ProtectionReason: item.AllocationReason,
		})
	}
	selected, err := fundalloc.SelectPlainBSVForTarget(input, target)
	if err != nil {
		return nil, err
	}
	out := make([]poolcore.UTXO, 0, len(selected.Selected))
	for _, item := range selected.Selected {
		out = append(out, poolcore.UTXO{
			TxID:  strings.ToLower(strings.TrimSpace(item.TxID)),
			Vout:  item.Vout,
			Value: item.ValueSatoshi,
		})
	}
	return out, nil
}

func normalizeWalletUTXOAllocationClass(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case walletUTXOAllocationPlainBSV:
		return walletUTXOAllocationPlainBSV
	case walletUTXOAllocationProtectedAsset:
		return walletUTXOAllocationProtectedAsset
	default:
		return walletUTXOAllocationUnknown
	}
}
