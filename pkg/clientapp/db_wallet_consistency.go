package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factbsvutxos"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelchaindirectpay"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementpaymentattempts"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementrecords"
)

type settlementAttemptLookupResult struct {
	ID    int64
	Found bool
}

func resolveSettlementPaymentAttemptIDByChannelTxID(ctx context.Context, store *clientDB, sourceType, txid string, requireConfirmed bool) (int64, bool, error) {
	if store == nil {
		return 0, false, fmt.Errorf("client db is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	txid = strings.ToLower(strings.TrimSpace(txid))
	if sourceType == "" || txid == "" {
		return 0, false, fmt.Errorf("source_type and txid are required")
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (settlementAttemptLookupResult, error) {
		channel, err := root.FactSettlementChannelChainDirectPay.Query().
			Where(factsettlementchannelchaindirectpay.TxidEQ(txid)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return settlementAttemptLookupResult{}, nil
			}
			return settlementAttemptLookupResult{}, err
		}
		q := channel.QuerySettlementPaymentAttempt().
			Where(factsettlementpaymentattempts.SourceTypeEQ(sourceType))
		if requireConfirmed {
			q = q.Where(factsettlementpaymentattempts.StateEQ("confirmed"))
		}
		attempt, err := q.Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return settlementAttemptLookupResult{}, nil
			}
			return settlementAttemptLookupResult{}, err
		}
		return settlementAttemptLookupResult{ID: int64(attempt.ID), Found: true}, nil
	})
	if err != nil {
		return 0, false, err
	}
	return out.ID, out.Found, nil
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

	tokenPaymentAttemptID, found, err := resolveSettlementPaymentAttemptIDByChannelTxID(ctx, store, "chain_direct_pay", txid, false)
	if err != nil {
		return TokenTxDualLineConsistency{}, err
	}
	if found {
		out.HasChainTokenCycle = true
		count, err = readEntValue(ctx, store, func(root EntReadRoot) (int, error) {
			return root.FactSettlementRecords.Query().
				Where(factsettlementrecords.SettlementPaymentAttemptIDEQ(tokenPaymentAttemptID), factsettlementrecords.AssetTypeEQ("TOKEN")).
				Count(ctx)
		})
		if err != nil {
			return TokenTxDualLineConsistency{}, err
		}
		out.HasTokenQuantityFact = count > 0
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
	paymentAttemptID, found, err := resolveSettlementPaymentAttemptIDByChannelTxID(ctx, store, "chain_direct_pay", txid, true)
	if err != nil {
		return ConfirmedBSVSpendConsistency{}, err
	}
	if found {
		out.HasConfirmedCycle = true
		count, err := readEntValue(ctx, store, func(root EntReadRoot) (int, error) {
			return root.FactSettlementRecords.Query().
				Where(factsettlementrecords.SettlementPaymentAttemptIDEQ(paymentAttemptID), factsettlementrecords.AssetTypeEQ("BSV")).
				Count(ctx)
		})
		if err != nil {
			return ConfirmedBSVSpendConsistency{}, err
		}
		out.HasBSVSettlementFact = count > 0
		count, err = readEntValue(ctx, store, func(root EntReadRoot) (int, error) {
			return root.FactBsvUtxos.Query().
				Where(factbsvutxos.SpentByTxidEQ(txid), factbsvutxos.UtxoStateEQ("spent")).
				Count(ctx)
		})
		if err != nil {
			return ConfirmedBSVSpendConsistency{}, err
		}
		out.HasSpentUTXO = count > 0
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
}

// RepairConfirmedBSVSpendConsistency 通过 settlement_payment_attempt 重放 BSV 扣账。
// 设计说明：
// - 只从既有 settlement_payment_attempt / settlement_record 回放；
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
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		paymentAttemptID, found, err := resolveSettlementPaymentAttemptIDByChannelTxID(ctx, store, "chain_direct_pay", txid, true)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("confirmed settlement payment attempt not found for txid=%s", txid)
		}
		rows, err := wtx.QueryContext(ctx, `SELECT source_utxo_id, used_satoshi
			FROM fact_settlement_records
			WHERE settlement_payment_attempt_id=? AND asset_type='BSV' AND state='confirmed' AND source_utxo_id<>''`,
			paymentAttemptID)
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
		return nil
	})
}
