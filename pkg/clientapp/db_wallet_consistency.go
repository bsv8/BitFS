package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factbsvutxos"
)

// settlementAttemptLookupResult 结果结构（已废弃链上直接查询路径）
type settlementAttemptLookupResult struct {
	ID    int64
	Found bool
}

// resolveSettlementPaymentAttemptIDByChannelTxID 已废弃
// 第九阶段整改：fact_settlement_channel_chain_direct_pay 和 fact_settlement_payment_attempts 已删除
// 链上直接支付查询路径不再可用，返回错误
func resolveSettlementPaymentAttemptIDByChannelTxID(ctx context.Context, store *clientDB, sourceType, txid string, requireConfirmed bool) (int64, bool, error) {
	if store == nil {
		return 0, false, fmt.Errorf("client db is nil")
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return 0, false, fmt.Errorf("txid is required")
	}
	// 第九阶段：链上直接支付查询路径已删除
	return 0, false, fmt.Errorf("chain_direct_pay lookup is no longer available: schema fact_settlement_channel_chain_direct_pay has been removed")
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
// - 只检查 settlement_payment_attempt -> fact_bsv_utxos 这一条主线；
// - 告警只看"cycle 已确认但对应 UTXO 还没 spent"；
// - 修复入口只能重放 settlement_payment_attempt，不允许旁路直改 UTXO。
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
	out := TokenTxDualLineConsistency{TxID: txid}
	count, err := readEntValue(ctx, store, func(root EntReadRoot) (int, error) {
		return root.FactBsvUtxos.Query().
			Where(factbsvutxos.SpentByTxidEQ(txid), factbsvutxos.UtxoStateEQ("spent")).
			Count(ctx)
	})
	if err != nil {
		return TokenTxDualLineConsistency{}, err
	}
	out.HasCarrierBSVFact = count > 0

	// 第九阶段整改：链上直接支付路径已删除，无法再通过 channel txid 反查 settlement_payment_attempt
	// HasChainTokenCycle 和 HasTokenQuantityFact 暂时无法确定，标记缺失项
	out.HasChainTokenCycle = false
	out.HasTokenQuantityFact = false

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
	out := ConfirmedBSVSpendConsistency{TxID: txid}

	// 第九阶段整改：链上直接支付路径已删除，无法通过 channel txid 反查 payment attempt
	// 只能检查 UTXO 是否存在，但无法验证完整的 settlement cycle
	count, err := readEntValue(ctx, store, func(root EntReadRoot) (int, error) {
		return root.FactBsvUtxos.Query().
			Where(factbsvutxos.SpentByTxidEQ(txid), factbsvutxos.UtxoStateEQ("spent")).
			Count(ctx)
	})
	if err != nil {
		return ConfirmedBSVSpendConsistency{}, err
	}
	out.HasSpentUTXO = count > 0

	// 链上直接支付路径已删除，无法验证 confirmed_cycle 和 bsv_settlement_fact
	out.HasConfirmedCycle = false
	out.HasBSVSettlementFact = false

	if !out.HasConfirmedCycle {
		out.MissingItems = append(out.MissingItems, "confirmed_cycle")
	}
	if !out.HasBSVSettlementFact {
		out.MissingItems = append(out.MissingItems, "bsv_settlement_fact")
	}
	if out.HasSpentUTXO {
		out.MissingItems = append(out.MissingItems, "utxo_spent_but_cycle_unknown")
	}
	return out, nil
}

// RepairConfirmedBSVSpendConsistency 通过 settlement_payment_attempt 重放 BSV 扣账。
// 设计说明：
// - 只从既有 settlement_payment_attempt / settlement_record 回放；
// - 不直接改 fact_bsv_utxos，避免旁路修复把账修歪；
// - 重放本身是幂等的，已 spent 的 UTXO 会直接跳过。
//
// 第九阶段整改：此函数已废弃，链上直接支付路径已删除
func RepairConfirmedBSVSpendConsistency(ctx context.Context, store *clientDB, txid string) error {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return fmt.Errorf("txid is required")
	}
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	// 第九阶段：链上直接支付查询路径已删除，无法进行重放修复
	return fmt.Errorf("RepairConfirmedBSVSpendConsistency is no longer available: chain_direct_pay schema has been removed")
}
