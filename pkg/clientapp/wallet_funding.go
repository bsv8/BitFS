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
	ScriptType       string
	ScriptTypeReason string
	AllocationClass  string
	AllocationReason string
}

// listWalletFundingCandidates 走显式 store 读取钱包快照，返回候选输出与保护信息。
// 设计说明：
// - 这里保留“识别结果 + 金额”一起读出的方式，让后续分配器先看保护分类，再谈选币；
// - 不再从 Runtime 里回退取 DB，依赖必须由上游显式传入。

// allocatePlainBSVWalletUTXOs 为普通 BSV 支出选择一组输入。
// Step 6：改为从 fact 口径选源，不再扫 wallet_utxo
func allocatePlainBSVWalletUTXOs(ctx context.Context, store *clientDB, rt *Runtime, purpose string, target uint64) ([]poolcore.UTXO, error) {
	_ = strings.TrimSpace(purpose)
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	walletID := walletIDByAddress(addr)

	// Step 6：从 fact 口径选源
	selected, err := dbSelectSourceFlowsForTarget(ctx, store, walletID, "BSV", "", target)
	if err != nil {
		return nil, err
	}

	out := make([]poolcore.UTXO, 0, len(selected))
	for _, s := range selected {
		out = append(out, poolcore.UTXO{
			TxID:  strings.ToLower(strings.TrimSpace(s.TxID)),
			Vout:  s.Vout,
			Value: uint64(s.UseAmount),
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
