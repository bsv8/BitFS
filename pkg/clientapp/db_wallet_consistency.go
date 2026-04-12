package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func resolveSettlementCycleIDByChannelTxID(ctx context.Context, db *sql.DB, sourceType, channelTable, txid string, requireConfirmed bool) (int64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("db is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	channelTable = strings.TrimSpace(channelTable)
	txid = strings.ToLower(strings.TrimSpace(txid))
	if sourceType == "" || channelTable == "" || txid == "" {
		return 0, false, fmt.Errorf("source_type, channel_table and txid are required")
	}
	query := fmt.Sprintf(
		`SELECT sc.id
		   FROM fact_settlement_cycles sc
		   JOIN %s ch ON ch.settlement_cycle_id=sc.id
		  WHERE sc.source_type=? AND ch.txid=?`,
		channelTable,
	)
	args := []any{sourceType, txid}
	if requireConfirmed {
		query += " AND sc.state='confirmed'"
	}
	query += " LIMIT 1"
	var cycleID int64
	if err := QueryRowContext(ctx, db, query, args...).Scan(&cycleID); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}
	return cycleID, true, nil
}

// TokenTxDualLineConsistency 是 token 交易一致性检查结果。
// 设计说明：
// - 这里只做核对，不补账；
// - 结果只看显式业务 settlement 和钱包事实，不再把 chain_bsv 主线当成 token 业务锚点。
type TokenTxDualLineConsistency struct {
	TxID                 string   `json:"txid"`
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

// CheckTokenTxDualLineConsistency 检查 token tx 的主线是否齐全。
// 设计说明：
// - 现在只认显式的 chain_token settlement；
// - carrier BSV 事实和 token 数量事实从钱包事实里核对，不再借 chain_bsv cycle 做锚点。
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
		var count int
		if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_bsv_utxos WHERE spent_by_txid=? AND utxo_state='spent'`, txid).Scan(&count); err != nil {
			return TokenTxDualLineConsistency{}, err
		}
		if count > 0 {
			out.HasCarrierBSVFact = true
		}

		tokenCycleID, found, err := resolveSettlementCycleIDByChannelTxID(ctx, db, "chain_direct_pay", "fact_settlement_channel_chain_direct_pay", txid, false)
		if err != nil {
			return TokenTxDualLineConsistency{}, err
		}
		if found {
			out.HasChainTokenCycle = true
			if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='TOKEN' AND settlement_cycle_id=?`, tokenCycleID).Scan(&count); err != nil {
				return TokenTxDualLineConsistency{}, err
			}
			if count > 0 {
				out.HasTokenQuantityFact = true
			}
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
		cycleID, found, err := resolveSettlementCycleIDByChannelTxID(ctx, db, "chain_direct_pay", "fact_settlement_channel_chain_direct_pay", txid, true)
		if err != nil {
			return ConfirmedBSVSpendConsistency{}, err
		}
		if found {
			out.HasConfirmedCycle = true
			var count int
			if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_settlement_records WHERE settlement_cycle_id=? AND asset_type='BSV'`, cycleID).Scan(&count); err != nil {
				return ConfirmedBSVSpendConsistency{}, err
			}
			if count > 0 {
				out.HasBSVSettlementFact = true
			}
			if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_bsv_utxos WHERE spent_by_txid=? AND utxo_state='spent'`, txid).Scan(&count); err != nil {
				return ConfirmedBSVSpendConsistency{}, err
			}
			if count > 0 {
				out.HasSpentUTXO = true
			}
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
		cycleID, found, err := resolveSettlementCycleIDByChannelTxID(ctx, db, "chain_direct_pay", "fact_settlement_channel_chain_direct_pay", txid, true)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("confirmed settlement cycle not found for txid=%s", txid)
		}
		rows, err := QueryContext(ctx, db, `SELECT source_utxo_id, used_satoshi
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
		return dbAppendBSVConsumptionsForSettlementCycleCtx(ctx, db, cycleID, facts, time.Now().Unix())
	})
}
