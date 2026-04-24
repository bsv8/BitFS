package clientapp

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpurchases"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factbsvutxos"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementrecords"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/facttokenlots"
)

// walletSummaryCounters 钱包统计计数器
// 设计说明：已改为 fact_* 事实表口径，统计只看真实链上事实，不看旧汇总表。
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
	// 设计说明：
	// - 这里只做只读聚合，不应额外开启 ent 事务；
	// - e2e 场景里费用池打开后会有持续写入，若这里再开事务，容易把 HTTP 读接口拖进等待链；
	// - 统一走只读查询入口，让 actor 只负责串行，不再叠加事务锁。
	return readEntValue(ctx, store, func(root EntReadRoot) (walletSummaryCounters, error) {
		var out walletSummaryCounters
		// 设计说明（硬切版）：
		// - 不再从旧汇总表读统计；
		// - 只看事实表和真实业务表；
		// - 统计逻辑尽量简单，方便后续直接从代码判断口径。

		// 1. 本币 UTXO 数量和未花费金额
		unspentRows, err := root.FactBsvUtxos.Query().
			Where(factbsvutxos.UtxoStateEQ("unspent")).
			All(ctx)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		for _, row := range unspentRows {
			out.FlowCount++
			out.TotalIn += int64(row.ValueSatoshi)
		}

		// 2. BSV 已花费金额，按已确认结算记录统计
		settlementRows, err := root.FactSettlementRecords.Query().
			Where(
				factsettlementrecords.AssetTypeEQ("BSV"),
				factsettlementrecords.StateEQ("confirmed"),
			).
			All(ctx)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		for _, row := range settlementRows {
			out.TotalOut += int64(row.UsedSatoshi)
		}
		out.BSVUsedSatoshi = out.TotalOut

		// 3. Token 消耗统计，按 token_standard + token_id 分组
		tokenRows, err := root.FactTokenLots.Query().
			Where(
				facttokenlots.LotStateIn("unspent", "spent"),
				facttokenlots.UsedQuantityTextNEQ("0"),
			).
			All(ctx)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		tokenUsedMap := make(map[string]*walletTokenUsedSummary)
		for _, row := range tokenRows {
			key := row.TokenStandard + ":" + row.TokenID
			if _, ok := tokenUsedMap[key]; !ok {
				tokenUsedMap[key] = &walletTokenUsedSummary{
					TokenStandard:    row.TokenStandard,
					TokenID:          row.TokenID,
					UsedQuantityText: "0",
				}
			}
			currentUsed := tokenUsedMap[key].UsedQuantityText
			newUsed, err := sumDecimalTexts(currentUsed + "," + row.UsedQuantityText)
			if err != nil {
				return walletSummaryCounters{}, err
			}
			tokenUsedMap[key].UsedQuantityText = newUsed
		}
		out.TokenUsed = make([]walletTokenUsedSummary, 0, len(tokenUsedMap))
		for _, v := range tokenUsedMap {
			out.TokenUsed = append(out.TokenUsed, *v)
		}

		out.TotalReturned = 0 // 已废弃

		// 4. 交易历史、购买、网关事件的总数
		// 第九阶段整改：fact_pool_session_events 已删除，TxCount 设为 0
		out.TxCount = 0

		purchaseCount, err := root.BizPurchases.Query().
			Where(bizpurchases.StatusEQ("done")).
			Count(ctx)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		out.PurchaseCount = int64(purchaseCount)

		gatewayCount, err := root.ProcGatewayEvents.Query().Count(ctx)
		if err != nil {
			return walletSummaryCounters{}, err
		}
		out.GatewayEventCount = int64(gatewayCount)

		return out, nil
	})
}
