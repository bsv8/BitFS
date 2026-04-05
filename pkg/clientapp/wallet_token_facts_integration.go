package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// subtractDecimalText 计算 a - b（高精度字符串小数减法）
// 设计说明：
// - 用于 fact 余额计算：remaining = total_in - total_used
// - 支持不同 scale 的小数，自动对齐精度
func subtractTokenDecimalText(a string, b string) (string, error) {
	pa, err := parseDecimalText(a)
	if err != nil {
		return "", err
	}
	pb, err := parseDecimalText(b)
	if err != nil {
		return "", err
	}
	// 对齐 scale
	scale := pa.scale
	if pb.scale > scale {
		scale = pb.scale
	}
	va := pa.intValue
	vb := pb.intValue
	if va == nil || vb == nil {
		return "0", nil
	}
	// 对齐
	for vaScale := pa.scale; vaScale < scale; vaScale++ {
		va = new(big.Int).Mul(va, big.NewInt(10))
	}
	for vbScale := pb.scale; vbScale < scale; vbScale++ {
		vb = new(big.Int).Mul(vb, big.NewInt(10))
	}
	// 减法
	diff := new(big.Int).Sub(va, vb)
	if diff.Sign() < 0 {
		return "0", nil
	}
	return formatDecimalText(diff, scale), nil
}

// sumDecimalTexts 对一组逗号分隔的 quantity_text 求和
func sumDecimalTexts(csv string) (string, error) {
	if strings.TrimSpace(csv) == "" {
		return "0", nil
	}
	var acc decimalTextAccumulator
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if err := acc.Add(part); err != nil {
			return "", err
		}
	}
	return acc.String(), nil
}

// hasFactTokenHistory 检查钱包是否有该 token 的 fact 历史记录
func hasFactTokenHistory(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (bool, error) {
	bal, err := dbLoadTokenBalanceFact(ctx, store, walletID, assetKind, tokenID)
	if err != nil {
		return false, err
	}
	// 有流入或消耗记录即认为有历史
	return bal.TotalInText != "" || bal.TotalUsedText != "", nil
}

// loadWalletTokenBalanceWithFallback 统一 token 余额读取入口
// 设计说明：
// - 先查 fact 口径，有数据则计算 remaining = in - used
// - fact 无数据时回退到旧口径（loadWalletTokenSpendableCandidates 汇总）
// - unknown 不参与 fact，不影响余额
func loadWalletTokenBalanceWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 8：先尝试 fact 口径
	bal, err := dbLoadTokenBalanceFact(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败，回退旧口径
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}
	if bal.TotalInText != "" {
		// fact 有数据，计算 remaining = in - used
		totalIn, err := sumDecimalTexts(bal.TotalInText)
		if err != nil {
			return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
		}
		totalUsed, err := sumDecimalTexts(bal.TotalUsedText)
		if err != nil {
			return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
		}
		remaining, err := subtractTokenDecimalText(totalIn, totalUsed)
		if err != nil {
			return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
		}
		return remaining, nil
	}

	// fact 无数据，回退旧口径
	return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
}

// loadWalletTokenBalanceFromOldPath 旧口径：从 wallet_utxo_assets + local broadcast 汇总
func loadWalletTokenBalanceFromOldPath(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	candidates, err := loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	if err != nil {
		return "", err
	}
	var acc decimalTextAccumulator
	for _, c := range candidates {
		if err := acc.Add(c.Item.QuantityText); err != nil {
			return "", err
		}
	}
	return acc.String(), nil
}

// loadWalletTokenSpendableCandidatesWithFallback 统一 token 可花费来源入口
// 设计说明：
// - 先尝试 fact 口径（dbListTokenSpendableSourceFlows）
// - fact 有候选则返回
// - fact 无候选但有历史记录时，说明已花完，直接返回空（不回退旧路径）
// - fact 完全无数据时才回退旧路径
// - 返回格式统一为 walletTokenPreviewCandidate，方便上层消费
func loadWalletTokenSpendableCandidatesWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 8：先尝试 fact 口径
	flows, err := dbListTokenSpendableSourceFlows(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败，回退旧路径
		return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	}
	if len(flows) > 0 {
		// fact 有可花费候选，转换成旧格式返回
		out := make([]walletTokenPreviewCandidate, 0, len(flows))
		for _, f := range flows {
			parsedQty, _ := parseDecimalText(f.QuantityText)
			out = append(out, walletTokenPreviewCandidate{
				Item: walletTokenOutputItem{
					UTXOID:       f.UTXOID,
					TxID:         f.TxID,
					Vout:         f.Vout,
					QuantityText: f.QuantityText,
					AssetKey:     strings.TrimSpace(f.TokenID),
				},
				CreatedAtUnix: f.OccurredAtUnix,
				Quantity:      parsedQty,
			})
		}
		return out, nil
	}

	// fact 无候选，检查是否有历史记录
	hasHistory, err := hasFactTokenHistory(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// 查询失败，回退旧路径
		return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	}
	if hasHistory {
		// 有 fact 历史但无可花费：说明已花完，返回空（不回退旧路径）
		return []walletTokenPreviewCandidate{}, nil
	}

	// fact 完全无数据，回退旧路径
	return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
}

// appendTokenConsumptionAfterChainPayment token 发送成功后写入 fact 消耗记录
// 设计说明：
// - 先为 token send tx 写入 chain_payment 事实，再逐条写 token 消耗
// - 幂等：同一 source_flow_id + chain_payment_id 不会重复写
func appendTokenConsumptionAfterChainPayment(ctx context.Context, store *clientDB, txHex string, txID string, tokenUTXOIDs map[string]string, occurredAtUnix int64) error {
	return store.Do(ctx, func(db *sql.DB) error {
		// 先写入 chain_payment 事实（token 发送也需要链上记录）
		chainPaymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
			TxID:                txID,
			PaymentSubType:      "token_send",
			Status:              "confirmed",
			WalletInputSatoshi:  0,
			WalletOutputSatoshi: 0,
			NetAmountSatoshi:    0,
			BlockHeight:         0,
			OccurredAtUnix:      occurredAtUnix,
			FromPartyID:         "wallet:self",
			ToPartyID:           "external:unknown",
			Payload:             map[string]any{"type": "token_send", "token_count": len(tokenUTXOIDs)},
		})
		if err != nil {
			return fmt.Errorf("upsert chain_payment for token send: %w", err)
		}

		// 逐条写 token 消耗
		for utxoID, quantityText := range tokenUTXOIDs {
			if err := dbAppendTokenConsumptionForChainPaymentDB(db, chainPaymentID, utxoID, quantityText, occurredAtUnix); err != nil {
				return fmt.Errorf("append token consumption for utxo %s: %w", utxoID, err)
			}
		}
		return nil
	})
}

// appendTokenConsumptionFromTxHex 从交易 hex 提取 token 输入并写入消耗记录
// 设计说明：
// - 在 token 发送广播成功后调用
// - 通过解析交易 input 的 UTXO，查找对应的 token quantity_text
func appendTokenConsumptionFromTxHex(ctx context.Context, store *clientDB, txHex string, txID string) error {
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return fmt.Errorf("parse tx hex: %w", err)
	}

	// 收集所有 input UTXO 的 quantity_text
	tokenUTXOIDs := make(map[string]string)
	for _, inp := range parsed.Inputs {
		sourceTxID := strings.ToLower(inp.SourceTXID.String())
		utxoID := sourceTxID + ":" + fmt.Sprint(inp.SourceTxOutIndex)

		// 查找该 UTXO 是否有 token 资产记录
		qty, err := dbGetUTXOTokenQuantity(ctx, store, utxoID)
		if err != nil || qty == "" {
			continue
		}
		tokenUTXOIDs[utxoID] = qty
	}

	if len(tokenUTXOIDs) == 0 {
		return nil
	}

	return appendTokenConsumptionAfterChainPayment(ctx, store, txHex, txID, tokenUTXOIDs, time.Now().Unix())
}

// dbGetUTXOTokenQuantity 查询 UTXO 上的 token quantity_text
func dbGetUTXOTokenQuantity(ctx context.Context, store *clientDB, utxoID string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var qty string
		err := db.QueryRow(
			`SELECT quantity_text FROM wallet_utxo_assets WHERE utxo_id=? AND asset_group='token' LIMIT 1`,
			utxoID,
		).Scan(&qty)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return qty, nil
	})
}
