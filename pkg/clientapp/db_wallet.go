package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

// walletSummaryCounters 钱包统计计数器
// 设计说明：已改为 fact_* 事实表口径
type walletSummaryCounters struct {
	FlowCount         int64 // fact_bsv_utxos 记录数
	TotalIn           int64 // fact_bsv_utxos 未花费总金额
	TotalOut          int64 // fact_bsv_utxos 已花费总金额
	BSVUsedSatoshi    int64 // fact_settlement_records 总消耗
	TokenUsed         []walletTokenUsedSummary
	TotalReturned     int64 // 保留字段，fact 表无直接对应，给 0
	TxCount           int64 // fact_pool_session_events tx_history 记录数
	PurchaseCount     int64 // biz_purchases 完成数
	GatewayEventCount int64 // proc_gateway_events 记录数
}

// walletTokenUsedSummary 钱包 Token 消耗汇总
// 设计说明：Token 消耗不和 satoshi 混算，按标准和 token_id 分组返回。
type walletTokenUsedSummary struct {
	TokenStandard    string `json:"token_standard"`
	TokenID          string `json:"token_id"`
	UsedQuantityText string `json:"used_quantity_text"`
}

func dbLoadWalletSummaryCounters(ctx context.Context, store *clientDB) (walletSummaryCounters, error) {
	if store == nil {
		return walletSummaryCounters{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletSummaryCounters, error) {
		var out walletSummaryCounters
		// 设计说明（硬切版）：
		// - 不再从 fact_chain_asset_flows 统计（表已删除）
		// - 改为从 fact_bsv_utxos 统计本币余额和 UTXO 状态
		// - 从 fact_token_lots 统计 token 消耗
		// - 从 fact_settlement_records 统计结算消耗
		
		// 统计本币UTXO数量和金额
		var utxoCount int64
		var totalUnspent int64
		if err := db.QueryRow(`
			SELECT COUNT(1), COALESCE(SUM(value_satoshi),0) 
			FROM fact_bsv_utxos WHERE utxo_state='unspent'`).Scan(&utxoCount, &totalUnspent); err != nil {
			return walletSummaryCounters{}, err
		}
		out.FlowCount = utxoCount
		out.TotalIn = totalUnspent
		
		// BSV 已花费金额（从结算记录统计实际消耗，而非 spent UTXO 总额）
		var totalSpent int64
		if err := db.QueryRow(`
			SELECT COALESCE(SUM(used_satoshi),0) 
			FROM fact_settlement_records 
			WHERE asset_type='BSV' AND state='confirmed'`).Scan(&totalSpent); err != nil {
			return walletSummaryCounters{}, err
		}
		out.TotalOut = totalSpent
		out.BSVUsedSatoshi = totalSpent
		
		// Token 消耗统计（从 fact_token_lots 计算 used_quantity）
		rows, err := db.Query(`
			SELECT token_standard, token_id, quantity_text, used_quantity_text
			FROM fact_token_lots
			WHERE lot_state IN ('unspent', 'spent') AND used_quantity_text != '0'`)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		defer rows.Close()
		
		tokenUsedMap := make(map[string]*walletTokenUsedSummary)
		for rows.Next() {
			var tokenStandard, tokenID, qtyText, usedText string
			if err := rows.Scan(&tokenStandard, &tokenID, &qtyText, &usedText); err != nil {
				return walletSummaryCounters{}, err
			}
			key := tokenStandard + ":" + tokenID
			if _, ok := tokenUsedMap[key]; !ok {
				tokenUsedMap[key] = &walletTokenUsedSummary{
					TokenStandard:    tokenStandard,
					TokenID:          tokenID,
					UsedQuantityText: "0",
				}
			}
			// 累加 used_quantity
			currentUsed := tokenUsedMap[key].UsedQuantityText
			newUsed, _ := sumDecimalTexts(currentUsed + "," + usedText)
			tokenUsedMap[key].UsedQuantityText = newUsed
		}
		if err := rows.Err(); err != nil {
			return walletSummaryCounters{}, err
		}
		
		out.TokenUsed = make([]walletTokenUsedSummary, 0, len(tokenUsedMap))
		for _, v := range tokenUsedMap {
			out.TokenUsed = append(out.TokenUsed, *v)
		}
		
		out.TotalReturned = 0 // 已废弃
		
		// 交易历史统计
		if err := db.QueryRow(`SELECT COUNT(1) FROM fact_pool_session_events WHERE event_kind=?`, PoolFactEventKindTxHistory).Scan(&out.TxCount); err != nil {
			return walletSummaryCounters{}, err
		}
		// 购买统计
		if err := db.QueryRow(`SELECT COUNT(1) FROM biz_purchases WHERE status='done'`).Scan(&out.PurchaseCount); err != nil {
			return walletSummaryCounters{}, err
		}
		// 网关事件统计
		if err := db.QueryRow(`SELECT COUNT(1) FROM proc_gateway_events`).Scan(&out.GatewayEventCount); err != nil {
			return walletSummaryCounters{}, err
		}
		return out, nil
	})
}
