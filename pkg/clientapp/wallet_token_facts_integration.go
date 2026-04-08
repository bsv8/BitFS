package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
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
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots 表
func hasFactTokenHistory(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (bool, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	var count int
	err := store.Do(ctx, func(db *sql.DB) error {
		return db.QueryRow(
			`SELECT COUNT(1) FROM fact_token_lots WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=?`,
			ownerPubkeyHex, assetKind, tokenID,
		).Scan(&count)
	})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// loadWalletTokenBalance 统一 token 余额读取入口
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots 表
// - unknown 不参与 fact，不影响余额
func loadWalletTokenBalance(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) (string, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(address))
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// 直接计算余额
	balance, err := dbCalcTokenBalance(ctx, store, ownerPubkeyHex, assetKind, assetKey)
	if err != nil {
		return "", fmt.Errorf("calc token balance: %w", err)
	}

	if balance == "" || balance == "0" {
		logTokenBalanceSuccess(ctx, "wallet:"+ownerPubkeyHex, assetKind, assetKey, "0")
		return "0", nil
	}

	logTokenBalanceSuccess(ctx, "wallet:"+ownerPubkeyHex, assetKind, assetKey, balance)
	return balance, nil
}

// logTokenBalanceSuccess 记录主路径成功日志
func logTokenBalanceSuccess(ctx context.Context, walletID string, assetKind string, tokenID string, balance string) {
	_ = ctx
	obs.Info("bitcast-client", "token_balance_fact_ok", map[string]any{
		"wallet_id":       walletID,
		"asset_kind":      assetKind,
		"token_id":        tokenID,
		"balance":         balance,
		"source_of_truth": "fact_token_lots",
	})
}

// loadWalletTokenSpendableCandidatesFromFact 从 fact 表加载 token 可花费候选
// 设计说明（硬切版）：
// - 改为查询 fact_token_lots + fact_token_carrier_links
// - 返回格式统一为 walletTokenPreviewCandidate，方便上层消费
func loadWalletTokenSpendableCandidatesFromFact(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(address))
	assetKind := strings.ToUpper(strings.TrimSpace(standard))

	// 查询可花费的 token lots 及其 carrier
	lots, err := dbListSpendableTokenLots(ctx, store, ownerPubkeyHex, assetKind, assetKey)
	if err != nil {
		return nil, fmt.Errorf("list spendable token lots: %w", err)
	}

	if len(lots) == 0 {
		// 检查是否有历史记录
		hasHistory, err := hasFactTokenHistory(ctx, store, "wallet:"+ownerPubkeyHex, assetKind, assetKey)
		if err != nil {
			return nil, fmt.Errorf("check token fact history: %w", err)
		}
		if hasHistory {
			return []walletTokenPreviewCandidate{}, nil
		}
		return []walletTokenPreviewCandidate{}, nil
	}

	out := make([]walletTokenPreviewCandidate, 0, len(lots))
	for _, lot := range lots {
		// 计算剩余数量
		qtyParsed, _ := parseDecimalText(lot.QuantityText)
		usedParsed, _ := parseDecimalText(lot.UsedQuantityText)

		// 对齐精度并计算剩余
		scale := qtyParsed.scale
		if usedParsed.scale > scale {
			scale = usedParsed.scale
		}
		qtyVal := new(big.Int).Set(qtyParsed.intValue)
		usedVal := new(big.Int).Set(usedParsed.intValue)
		for i := qtyParsed.scale; i < scale; i++ {
			qtyVal = new(big.Int).Mul(qtyVal, big.NewInt(10))
		}
		for i := usedParsed.scale; i < scale; i++ {
			usedVal = new(big.Int).Mul(usedVal, big.NewInt(10))
		}
		remainingVal := new(big.Int).Sub(qtyVal, usedVal)
		if remainingVal.Sign() <= 0 {
			continue
		}
		remainingText := formatDecimalText(remainingVal, scale)

		// 查询 carrier UTXO
		link, err := dbGetActiveCarrierForLot(ctx, store, lot.LotID)
		if err != nil || link == nil {
			continue
		}

		out = append(out, walletTokenPreviewCandidate{
			Item: walletTokenOutputItem{
				UTXOID:       link.CarrierUTXOID,
				TxID:         "", // 可以从 UTXOID 解析
				Vout:         0,
				QuantityText: remainingText,
				AssetKey:     strings.TrimSpace(lot.TokenID),
			},
			CreatedAtUnix: lot.CreatedAtUnix,
			Quantity:      decimalTextValue{intValue: remainingVal, scale: scale},
		})
	}

	return out, nil
}

// appendTokenConsumptionAfterBroadcast token 发送成功后写入 fact 消耗记录
// 设计说明（硬切版）：
// - 改为写入 fact_settlement_records 和更新 fact_token_lots
// - 幂等：同一 lot_id 不会重复消耗
func appendTokenConsumptionAfterBroadcast(ctx context.Context, store *clientDB, txHex string, txID string, tokenLotIDs map[string]string, occurredAtUnix int64) error {
	return store.Do(ctx, func(db *sql.DB) error {
		cycleID := fmt.Sprintf("cycle_chain_token_%s", txID)
		if err := dbUpsertSettlementCycle(db, cycleID, "chain_token", txID, "confirmed",
			0, 0, 0, 0, occurredAtUnix, "token send broadcast", map[string]any{"type": "token_send", "token_count": len(tokenLotIDs), "tx_hex": txHex}); err != nil {
			return fmt.Errorf("upsert settlement cycle for token send: %w", err)
		}
		settlementCycleID, err := dbGetSettlementCycleBySource(db, "chain_token", txID)
		if err != nil {
			return fmt.Errorf("resolve settlement cycle for token send: %w", err)
		}

		// 逐条写 token 消耗记录
		for lotID, usedQuantityText := range tokenLotIDs {
			// 获取 lot 信息
			lot, err := dbGetTokenLotDB(db, lotID)
			if err != nil {
				return fmt.Errorf("get token lot %s: %w", lotID, err)
			}
			if lot == nil {
				return fmt.Errorf("token lot %s not found", lotID)
			}

			// 写入结算记录
			recordID := fmt.Sprintf("rec_%s_%s", txID, lotID)
			if err := dbAppendSettlementRecordDB(db, settlementRecordEntry{
				RecordID:          recordID,
				SettlementCycleID: settlementCycleID,
				AssetType:         "TOKEN",
				OwnerPubkeyHex:    lot.OwnerPubkeyHex,
				SourceLotID:       lotID,
				UsedQuantityText:  usedQuantityText,
				State:             "confirmed",
				OccurredAtUnix:    occurredAtUnix,
				Note:              "token consumed by send tx",
				Payload:           map[string]any{"txid": txID},
			}); err != nil {
				return fmt.Errorf("append settlement record for lot %s: %w", lotID, err)
			}

			// 更新 lot 的 used_quantity_text
			newUsedQty, err := sumDecimalTexts(lot.UsedQuantityText + "," + usedQuantityText)
			if err != nil {
				newUsedQty = usedQuantityText
			}

			// 计算是否已花完
			qtyParsed, _ := parseDecimalText(lot.QuantityText)
			usedParsed, _ := parseDecimalText(newUsedQty)
			scale := qtyParsed.scale
			if usedParsed.scale > scale {
				scale = usedParsed.scale
			}
			qtyVal := new(big.Int).Set(qtyParsed.intValue)
			usedVal := new(big.Int).Set(usedParsed.intValue)
			for i := qtyParsed.scale; i < scale; i++ {
				qtyVal = new(big.Int).Mul(qtyVal, big.NewInt(10))
			}
			for i := usedParsed.scale; i < scale; i++ {
				usedVal = new(big.Int).Mul(usedVal, big.NewInt(10))
			}

			lotState := "unspent"
			if usedVal.Cmp(qtyVal) >= 0 {
				lotState = "spent"
			}

			if err := dbUpsertTokenLotDB(db, tokenLotEntry{
				LotID:            lotID,
				OwnerPubkeyHex:   lot.OwnerPubkeyHex,
				TokenID:          lot.TokenID,
				TokenStandard:    lot.TokenStandard,
				QuantityText:     lot.QuantityText,
				UsedQuantityText: newUsedQty,
				LotState:         lotState,
				MintTxid:         lot.MintTxid,
				LastSpendTxid:    txID,
				UpdatedAtUnix:    occurredAtUnix,
			}); err != nil {
				return fmt.Errorf("update token lot %s: %w", lotID, err)
			}
		}
		return nil
	})
}

// appendTokenConsumptionFromTxHex 从交易 hex 提取 token 输入并写入消耗记录
// 设计说明（硬切版）：
// - 改为从 fact_token_carrier_links 查找 lot
func appendTokenConsumptionFromTxHex(ctx context.Context, store *clientDB, txHex string, txID string) error {
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return fmt.Errorf("parse tx hex: %w", err)
	}

	// 收集所有 input UTXO 对应的 lot
	tokenLotIDs := make(map[string]string)
	for _, inp := range parsed.Inputs {
		sourceTxID := strings.ToLower(inp.SourceTXID.String())
		utxoID := sourceTxID + ":" + fmt.Sprint(inp.SourceTxOutIndex)

		// 查找该 UTXO 对应的 lot
		lotID, err := dbGetLotByCarrierUTXO(ctx, store, utxoID)
		if err != nil {
			return fmt.Errorf("lookup lot for utxo %s: %w", utxoID, err)
		}
		if lotID == "" {
			continue
		}

		// 获取 lot 的 quantity
		lot, err := dbGetTokenLot(ctx, store, lotID)
		if err != nil || lot == nil {
			continue
		}

		// 计算剩余数量
		qtyParsed, _ := parseDecimalText(lot.QuantityText)
		usedParsed, _ := parseDecimalText(lot.UsedQuantityText)
		scale := qtyParsed.scale
		if usedParsed.scale > scale {
			scale = usedParsed.scale
		}
		qtyVal := new(big.Int).Set(qtyParsed.intValue)
		usedVal := new(big.Int).Set(usedParsed.intValue)
		for i := qtyParsed.scale; i < scale; i++ {
			qtyVal = new(big.Int).Mul(qtyVal, big.NewInt(10))
		}
		for i := usedParsed.scale; i < scale; i++ {
			usedVal = new(big.Int).Mul(usedVal, big.NewInt(10))
		}
		remainingVal := new(big.Int).Sub(qtyVal, usedVal)
		if remainingVal.Sign() <= 0 {
			continue
		}
		remainingText := formatDecimalText(remainingVal, scale)

		tokenLotIDs[lotID] = remainingText
	}

	if len(tokenLotIDs) == 0 {
		return fmt.Errorf("no token carrier inputs found in txid %s", strings.ToLower(strings.TrimSpace(txID)))
	}

	return appendTokenConsumptionAfterBroadcast(ctx, store, txHex, txID, tokenLotIDs, time.Now().Unix())
}

// dbGetLotByCarrierUTXO 根据 carrier UTXO 查询 lot_id
func dbGetLotByCarrierUTXO(ctx context.Context, store *clientDB, utxoID string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var lotID string
		err := db.QueryRow(
			`SELECT lot_id FROM fact_token_carrier_links WHERE carrier_utxo_id=? AND link_state='active' LIMIT 1`,
			utxoID,
		).Scan(&lotID)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return lotID, nil
	})
}

// dbGetUTXOTokenQuantity 查询 UTXO 上的 token quantity_text（兼容性函数）
// 设计说明（硬切版）：
// - 改为从 fact_token_carrier_links + fact_token_lots 查询
func dbGetUTXOTokenQuantity(ctx context.Context, store *clientDB, utxoID string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		// 先查 carrier link 获取 lot_id
		var lotID string
		err := db.QueryRow(
			`SELECT lot_id FROM fact_token_carrier_links WHERE carrier_utxo_id=? AND link_state='active' LIMIT 1`,
			utxoID,
		).Scan(&lotID)
		if err == sql.ErrNoRows {
			return "", nil
		}
		if err != nil {
			return "", err
		}

		// 再查 lot 获取 quantity
		var qty string
		err = db.QueryRow(
			`SELECT quantity_text FROM fact_token_lots WHERE lot_id=?`,
			lotID,
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
