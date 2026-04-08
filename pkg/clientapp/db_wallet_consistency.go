package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
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

// ConfirmedBSVSpendConsistency 是 BSV 支付已确认后的扣账一致性检查结果。
// 设计说明：
// - 只检查 settlement_cycle -> fact_bsv_utxos 这一条主线；
// - 告警只看“cycle 已确认但对应 UTXO 还没 spent”；
// - 修复入口只能重放 settlement_cycle，不允许旁路直改 UTXO。
type ConfirmedBSVSpendConsistency struct {
	TxID                 string   `json:"txid"`
	HasConfirmedCycle    bool     `json:"has_confirmed_cycle"`
	HasBSVSettlementFact bool     `json:"has_bsv_settlement_fact"`
	HasSpentUTXO         bool     `json:"has_spent_utxo"`
	MissingItems         []string `json:"missing_items,omitempty"`
	Repairable           bool     `json:"repairable,omitempty"`
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
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='BSV' AND settlement_cycle_id=?`, bsvCycleID).Scan(&count); err != nil {
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
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='TOKEN' AND settlement_cycle_id=?`, tokenCycleID).Scan(&count); err != nil {
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

// CheckConfirmedBSVSpendConsistency 检查 BSV 已确认支付是否真正落到 UTXO 扣账。
func CheckConfirmedBSVSpendConsistency(ctx context.Context, store *clientDB, txid string) (ConfirmedBSVSpendConsistency, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return ConfirmedBSVSpendConsistency{}, fmt.Errorf("txid is required")
	}
	if store == nil {
		return ConfirmedBSVSpendConsistency{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (ConfirmedBSVSpendConsistency, error) {
		out := ConfirmedBSVSpendConsistency{TxID: txid}
		var cycleID int64
		if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=? AND state='confirmed'`, txid).Scan(&cycleID); err == nil {
			out.HasConfirmedCycle = true
			var count int
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE settlement_cycle_id=? AND asset_type='BSV'`, cycleID).Scan(&count); err != nil {
				return ConfirmedBSVSpendConsistency{}, err
			}
			if count > 0 {
				out.HasBSVSettlementFact = true
			}
			if err := db.QueryRow(`SELECT COUNT(1) FROM fact_bsv_utxos WHERE spent_by_txid=? AND utxo_state='spent'`, txid).Scan(&count); err != nil {
				return ConfirmedBSVSpendConsistency{}, err
			}
			if count > 0 {
				out.HasSpentUTXO = true
			}
		} else if err != sql.ErrNoRows {
			return ConfirmedBSVSpendConsistency{}, err
		}

		if !out.HasConfirmedCycle {
			out.MissingItems = append(out.MissingItems, "confirmed_cycle")
		}
		if out.HasConfirmedCycle && !out.HasBSVSettlementFact {
			out.MissingItems = append(out.MissingItems, "bsv_settlement_fact")
		}
		if out.HasConfirmedCycle && out.HasBSVSettlementFact && !out.HasSpentUTXO {
			out.MissingItems = append(out.MissingItems, "spent_bsv_utxo")
			out.Repairable = true
		}
		return out, nil
	})
}

// RepairConfirmedBSVSpendConsistency 通过 settlement_cycle 重放 BSV 扣账。
// 设计说明：
// - 只从既有 settlement_cycle / settlement_record 回放；
// - 不直接改 fact_bsv_utxos，避免旁路修复把账修歪；
// - 重放本身是幂等的，已 spent 的 UTXO 会直接跳过。
func RepairConfirmedBSVSpendConsistency(ctx context.Context, store *clientDB, txid string) error {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return fmt.Errorf("txid is required")
	}
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		var cycleID int64
		if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=? AND state='confirmed'`, txid).Scan(&cycleID); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("confirmed settlement cycle not found for txid=%s", txid)
			}
			return err
		}
		rows, err := db.Query(`SELECT source_utxo_id, used_satoshi
			FROM fact_settlement_records
			WHERE settlement_cycle_id=? AND asset_type='BSV' AND state='confirmed' AND source_utxo_id<>''`,
			cycleID)
		if err != nil {
			return err
		}
		defer rows.Close()

		facts := make([]chainPaymentUTXOLinkEntry, 0, 8)
		for rows.Next() {
			var utxoID string
			var usedSatoshi int64
			if err := rows.Scan(&utxoID, &usedSatoshi); err != nil {
				return err
			}
			facts = append(facts, chainPaymentUTXOLinkEntry{
				UTXOID:        strings.ToLower(strings.TrimSpace(utxoID)),
				IOSide:        "input",
				UTXORole:      "wallet_input",
				AmountSatoshi: usedSatoshi,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(facts) == 0 {
			return fmt.Errorf("no bsv settlement records found for txid=%s", txid)
		}
		return dbAppendBSVConsumptionsForSettlementCycle(db, cycleID, facts, time.Now().Unix())
	})
}
