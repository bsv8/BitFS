package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
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
// - Step 12 收口：fact 为主口径
// - Step 13：所有回退路径统一打 `reason` 标签，便于后续统计和下线旧路径
// - fact 查询成功（含空结果/0 值）→ 直接返回，不回退旧路径
// - fact 查询失败 → 记录 reason 后回退旧路径
// - unknown 不参与 fact，不影响余额
func loadWalletTokenBalanceWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 12/13/14：先尝试 fact 口径
	bal, err := dbLoadTokenBalanceFact(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败：记录 reason 后回退（带开关检查）
		logTokenBalanceFallback(ctx, walletID, assetKind, assetKey, "fact_query_error", err.Error())
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}

	// Step 12 收口：fact 查询成功，即使 TotalInText 为空也返回 "0"（不回退旧路径）
	// 设计说明：
	// - fact 已建账但无 IN 记录 = 该 token 余额确实为 0
	// - 不再因 "fact 返回空" 回退到旧路径，避免新旧口径混用
	if bal.TotalInText == "" && bal.TotalUsedText == "" {
		// Step 14：主路径成功日志
		logTokenBalanceSuccess(ctx, walletID, assetKind, assetKey, "0")
		return "0", nil
	}

	// fact 有数据，计算 remaining = in - used
	totalIn, err := sumDecimalTexts(bal.TotalInText)
	if err != nil {
		logTokenBalanceFallback(ctx, walletID, assetKind, assetKey, "total_in_parse_error", err.Error())
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}
	totalUsed, err := sumDecimalTexts(bal.TotalUsedText)
	if err != nil {
		logTokenBalanceFallback(ctx, walletID, assetKind, assetKey, "total_used_parse_error", err.Error())
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}
	remaining, err := subtractTokenDecimalText(totalIn, totalUsed)
	if err != nil {
		logTokenBalanceFallback(ctx, walletID, assetKind, assetKey, "remaining_calc_error", err.Error())
		return loadWalletTokenBalanceFromOldPath(ctx, store, rt, address, standard, assetKey)
	}
	// Step 14：主路径成功日志
	logTokenBalanceSuccess(ctx, walletID, assetKind, assetKey, remaining)
	return remaining, nil
}

// logTokenBalanceFallback 统一记录回退日志，带 reason 枚举
func logTokenBalanceFallback(ctx context.Context, walletID string, assetKind string, tokenID string, reason string, errMsg string) {
	_ = ctx // 预留 context 用于后续 tracing
	obs.Error("bitcast-client", "token_balance_fact_fallback", map[string]any{
		"wallet_id":       walletID,
		"asset_kind":      assetKind,
		"token_id":        tokenID,
		"reason":          reason,
		"error":           errMsg,
		"fallback":        "old_path_runtime_debug_only",
		"source_of_truth": "fact_chain_asset_flows",
	})
}

// logTokenBalanceSuccess 记录主路径成功日志
func logTokenBalanceSuccess(ctx context.Context, walletID string, assetKind string, tokenID string, balance string) {
	_ = ctx
	obs.Info("bitcast-client", "token_balance_fact_ok", map[string]any{
		"wallet_id":       walletID,
		"asset_kind":      assetKind,
		"token_id":        tokenID,
		"balance":         balance,
		"source_of_truth": "fact_chain_asset_flows",
	})
}

// isTokenOldPathEnabled 检查是否允许走旧路径
// 设计说明（Step 14）：
// - 默认禁用，需显式设置 BITFS_ENABLE_TOKEN_OLD_PATH=1
// - 用于过渡期排障，生产环境不应开启
func isTokenOldPathEnabled() bool {
	return os.Getenv("BITFS_ENABLE_TOKEN_OLD_PATH") == "1"
}

// loadWalletTokenBalanceFromOldPath 旧口径：从 wallet_utxo_assets + local broadcast 汇总
// 设计说明（Step 12/14 收口）：
// - runtime_debug_only：不再参与主流程决策，仅在 fact 查询失败时作为兜底
// - Step 14：默认禁用，需显式设置 BITFS_ENABLE_TOKEN_OLD_PATH=1 才允许走旧路径
// - 语义：非主路径，仅供排障和兼容过渡期使用
// - 新代码不应依赖此函数的返回值做业务决策
func loadWalletTokenBalanceFromOldPath(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	// Step 14：旧路径硬降级，默认禁用
	if !isTokenOldPathEnabled() {
		obs.Error("bitcast-client", "token_old_path_disabled", map[string]any{
			"wallet_id":  walletIDByAddress(address),
			"asset_kind": strings.ToUpper(strings.TrimSpace(standard)),
			"token_id":   assetKey,
			"reason":     "BITFS_ENABLE_TOKEN_OLD_PATH not set",
		})
		return "", fmt.Errorf("token old path is disabled, set BITFS_ENABLE_TOKEN_OLD_PATH=1 to enable")
	}
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
// - Step 12/13/14：fact 为主口径
// - fact 查询成功 → 有候选则返回
// - fact 查询成功且无候选但有历史记录 → 说明已花完，返回空（不回退旧路径）
// - fact 查询成功且无候选且无历史 → fact 未建账（空结果），返回空
// - fact 查询失败 → 仅当 BITFS_ENABLE_TOKEN_OLD_PATH=1 时回退旧路径
// - 返回格式统一为 walletTokenPreviewCandidate，方便上层消费
func loadWalletTokenSpendableCandidatesWithFallback(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	walletID := walletIDByAddress(address)
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// Step 12/13/14：先尝试 fact 口径
	flows, err := dbListTokenSpendableSourceFlows(ctx, store, walletID, assetKind, assetKey)
	if err != nil {
		// fact 查询失败，检查旧路径开关
		if !isTokenOldPathEnabled() {
			obs.Error("bitcast-client", "token_spendable_fact_query_failed", map[string]any{
				"wallet_id":  walletID,
				"asset_kind": assetKind,
				"token_id":   assetKey,
				"error":      err.Error(),
				"fallback":   "blocked_old_path_disabled",
			})
			return nil, fmt.Errorf("token old path disabled: %w", err)
		}
		obs.Error("bitcast-client", "token_spendable_fact_query_failed", map[string]any{
			"wallet_id":  walletID,
			"asset_kind": assetKind,
			"token_id":   assetKey,
			"error":      err.Error(),
			"fallback":   "old_path_runtime_debug_only",
		})
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
		// 查询失败，检查旧路径开关
		if !isTokenOldPathEnabled() {
			return nil, fmt.Errorf("token old path disabled: %w", err)
		}
		return loadWalletTokenSpendableCandidates(ctx, store, rt, address, standard, assetKey)
	}
	if hasHistory {
		// 有 fact 历史但无可花费：说明已花完，返回空（不回退旧路径）
		return []walletTokenPreviewCandidate{}, nil
	}

	// Step 13 加强：fact 完全无数据（无候选 + 无历史），说明 fact 未建账
	// 不再回退旧路径，返回空集合，避免临时空结果误回退
	return []walletTokenPreviewCandidate{}, nil
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
