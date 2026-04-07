package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// TokenTxDualLineConsistency 是 token 交易双线一致性检查结果。
// 设计说明：
// - 这里只做核对，不补账；
// - 结果同时给出两条线和两类事实的命中情况，方便 API / e2e / 运维统一读口径。
type TokenTxDualLineConsistency struct {
	TxID                 string   `json:"txid"`
	HasChainBSVCycle     bool     `json:"has_chain_bsv_cycle"`
	HasChainTokenCycle   bool     `json:"has_chain_token_cycle"`
	HasCarrierBSVFact    bool     `json:"has_carrier_bsv_fact"`
	HasTokenQuantityFact bool     `json:"has_token_quantity_fact"`
	MissingItems         []string `json:"missing_items,omitempty"`
}

// CheckTokenTxDualLineConsistency 检查 token tx 的双线是否齐全。
// 设计说明：
// - chain_bsv 负责 carrier 这一层的本币消耗；
// - chain_token 负责 token 数量这一层的消耗；
// - 少一条线时直接返回缺失项，调用方决定是否报错。
func CheckTokenTxDualLineConsistency(ctx context.Context, store *clientDB, txid string) (TokenTxDualLineConsistency, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return TokenTxDualLineConsistency{}, fmt.Errorf("txid is required")
	}
	if store == nil {
		return TokenTxDualLineConsistency{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (TokenTxDualLineConsistency, error) {
		out := TokenTxDualLineConsistency{TxID: txid}
		var bsvCycleID int64
		if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&bsvCycleID); err == nil {
			out.HasChainBSVCycle = true
			var count int
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_consumptions WHERE settlement_cycle_id=?`, bsvCycleID).Scan(&count); err != nil {
				return TokenTxDualLineConsistency{}, err
			}
			if count > 0 {
				out.HasCarrierBSVFact = true
			}
		} else if err != sql.ErrNoRows {
			return TokenTxDualLineConsistency{}, err
		}

		var tokenCycleID int64
		if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_token' AND source_id=?`, txid).Scan(&tokenCycleID); err == nil {
			out.HasChainTokenCycle = true
			var count int
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_token_consumptions WHERE settlement_cycle_id=?`, tokenCycleID).Scan(&count); err != nil {
				return TokenTxDualLineConsistency{}, err
			}
			if count > 0 {
				out.HasTokenQuantityFact = true
			}
		} else if err != sql.ErrNoRows {
			return TokenTxDualLineConsistency{}, err
		}

		if !out.HasChainBSVCycle {
			out.MissingItems = append(out.MissingItems, "chain_bsv_cycle")
		}
		if !out.HasChainTokenCycle {
			out.MissingItems = append(out.MissingItems, "chain_token_cycle")
		}
		if !out.HasCarrierBSVFact {
			out.MissingItems = append(out.MissingItems, "carrier_bsv_fact")
		}
		if !out.HasTokenQuantityFact {
			out.MissingItems = append(out.MissingItems, "token_quantity_fact")
		}
		return out, nil
	})
}
