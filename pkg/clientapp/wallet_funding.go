package clientapp

import (
	"context"
	"database/sql"
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

// listWalletFundingCandidates 从本地钱包快照读取“可用于资金决策”的原始候选输出。
// 设计说明：
// - 这里故意把“识别结果”和“金额”一起读出来，让后续分配器始终先看保护分类，再谈选币；
// - 当前只为普通 BSV 分配服务，未来 tokens / ordinals 接进来后，仍可复用同一批候选元数据。
func listWalletFundingCandidates(rt *Runtime) ([]walletFundingCandidate, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	load := func(db *sql.DB) ([]walletFundingCandidate, error) {
		s, err := loadWalletUTXOSyncState(db, addr)
		if err != nil {
			return nil, err
		}
		if s.UpdatedAtUnix <= 0 {
			return nil, fmt.Errorf("wallet utxo sync state not ready")
		}
		if isWalletUTXOSyncStateStaleForRuntime(rt, s) {
			return nil, fmt.Errorf("wallet utxo sync state stale for current runtime")
		}
		if strings.TrimSpace(s.LastError) != "" {
			return nil, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
		}
		walletID := walletIDByAddress(addr)
		rows, err := db.Query(
			`SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,allocation_class,allocation_reason
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent'
			 ORDER BY created_at_unix ASC,value_satoshi ASC,txid ASC,vout ASC`,
			walletID,
			addr,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletFundingCandidate, 0, s.UTXOCount)
		for rows.Next() {
			var item walletFundingCandidate
			if err := rows.Scan(
				&item.UTXOID,
				&item.UTXO.TxID,
				&item.UTXO.Vout,
				&item.UTXO.Value,
				&item.CreatedAtUnix,
				&item.AllocationClass,
				&item.AllocationReason,
			); err != nil {
				return nil, err
			}
			item.UTXOID = strings.ToLower(strings.TrimSpace(item.UTXOID))
			item.UTXO.TxID = strings.ToLower(strings.TrimSpace(item.UTXO.TxID))
			item.AllocationClass = normalizeWalletUTXOAllocationClass(item.AllocationClass)
			item.AllocationReason = strings.TrimSpace(item.AllocationReason)
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}
	// 设计说明：
	// - 正式运行时必须走 sqliteactor；
	// - 这里保留 DB 直读回退，只为了最小测试夹具不必把整个 actor 运行时一并拉起来。
	if rt.DBActor != nil {
		return runtimeDBValue(rt, context.Background(), load)
	}
	if rt.DB != nil {
		return load(rt.DB)
	}
	return nil, fmt.Errorf("runtime not initialized")
}

// listEligiblePlainBSVWalletUTXOs 返回“允许普通 BSV 业务花费”的输出集合。
func listEligiblePlainBSVWalletUTXOs(rt *Runtime) ([]poolcore.UTXO, error) {
	candidates, err := listWalletFundingCandidates(rt)
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
func allocatePlainBSVWalletUTXOs(rt *Runtime, purpose string, target uint64) ([]poolcore.UTXO, error) {
	_ = strings.TrimSpace(purpose)
	candidates, err := listWalletFundingCandidates(rt)
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
