package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

// walletSummaryCounters 钱包统计计数器
// 设计说明：已改为 fact_* 事实表口径
type walletSummaryCounters struct {
	FlowCount         int64 // fact_chain_asset_flows 记录数
	TotalIn           int64 // fact_chain_asset_flows IN 方向总金额
	TotalOut          int64 // fact_chain_asset_flows OUT 方向总金额
	BSVUsedSatoshi    int64 // fact_bsv_consumptions 总消耗
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
		// 从 fact_chain_asset_flows 统计资金流水
		if err := db.QueryRow(`SELECT COUNT(1),COALESCE(SUM(CASE WHEN direction='IN' THEN amount_satoshi ELSE 0 END),0),COALESCE(SUM(CASE WHEN direction='OUT' THEN amount_satoshi ELSE 0 END),0) FROM fact_chain_asset_flows`).Scan(&out.FlowCount, &out.TotalIn, &out.TotalOut); err != nil {
			return walletSummaryCounters{}, err
		}
		// BSV 消耗只读新表 fact_bsv_consumptions
		if err := db.QueryRow(`SELECT COALESCE(SUM(used_satoshi),0) FROM fact_bsv_consumptions`).Scan(&out.BSVUsedSatoshi); err != nil {
			return walletSummaryCounters{}, err
		}
		rows, err := db.Query(`
			SELECT COALESCE(token_standard, ''), COALESCE(token_id, ''), COALESCE(GROUP_CONCAT(used_quantity_text, ','), '')
			FROM fact_token_consumptions
			GROUP BY token_standard, token_id
			ORDER BY token_standard, token_id`,
		)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		defer rows.Close()
		out.TokenUsed = make([]walletTokenUsedSummary, 0, 8)
		for rows.Next() {
			var tokenStandard, tokenID, usedCSV string
			if err := rows.Scan(&tokenStandard, &tokenID, &usedCSV); err != nil {
				return walletSummaryCounters{}, err
			}
			usedQuantityText, err := sumDecimalTexts(usedCSV)
			if err != nil {
				return walletSummaryCounters{}, err
			}
			out.TokenUsed = append(out.TokenUsed, walletTokenUsedSummary{
				TokenStandard:    tokenStandard,
				TokenID:          tokenID,
				UsedQuantityText: usedQuantityText,
			})
		}
		if err := rows.Err(); err != nil {
			return walletSummaryCounters{}, err
		}
		out.TotalReturned = 0 // fact 表无直接对应字段
		// 交易历史统计
		if err := db.QueryRow(`SELECT COUNT(1) FROM fact_pool_session_events WHERE event_kind='tx_history'`).Scan(&out.TxCount); err != nil {
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
