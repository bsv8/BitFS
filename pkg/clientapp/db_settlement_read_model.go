package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ============================================================
// 业务结算聚合读模型（第四步主线）
// 设计原则：
//   - 业务状态优先看 settle_records，不看旧过程表
//   - 汇总状态只根据 settle_records 计算，不参考 proc_direct_transfer_pools/txid/旧表
//   - 混合状态统一归 partial_settled
//   - 本文件只做读取，不做写入
// ============================================================

// FrontOrderSettlementSummary 前台订单聚合视图
// 一个 front_order 对应多条 business，每条 business 对应一条 settlement
type FrontOrderSettlementSummary struct {
	FrontOrder *frontOrderItem          `json:"front_order"`
	Businesses []BusinessSettlementView `json:"businesses"`
	Summary    SettlementSummary        `json:"summary"`
}

// BusinessSettlementView 单条 business + settlement + 底层支付事实
type BusinessSettlementView struct {
	Business       financeBusinessItem    `json:"business"`
	Settlement     BusinessSettlementItem `json:"settlement"`
	ChainPayment   *ChainPaymentItem      `json:"chain_payment,omitempty"`
	PoolAllocation *PoolAllocationItem    `json:"pool_allocation,omitempty"`
}

// SettlementSummary 汇总状态（只根据 settle_records 计算）
type SettlementSummary struct {
	TotalBusinessCount   int    `json:"total_business_count"`
	SettledCount         int    `json:"settled_count"`
	FailedCount          int    `json:"failed_count"`
	PendingCount         int    `json:"pending_count"`
	TotalTargetSatoshi   uint64 `json:"total_target_satoshi"`
	SettledAmountSatoshi uint64 `json:"settled_amount_satoshi"`
	OverallStatus        string `json:"overall_status"` // pending | waiting_fund | partial_settled | settled | failed
}

// GetFrontOrderSettlementSummary 按 front_order_id 查完整聚合视图
// 第四步主入口：前台/后台/调试统一走此函数
func GetFrontOrderSettlementSummary(ctx context.Context, store *clientDB, frontOrderID string) (FrontOrderSettlementSummary, error) {
	var out FrontOrderSettlementSummary

	if store == nil {
		return out, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return out, fmt.Errorf("front_order_id is required")
	}

	// 1. 查 front_order 基本信息
	fo, err := dbGetFrontOrder(ctx, store, frontOrderID)
	if err != nil {
		return out, fmt.Errorf("get front_order: %w", err)
	}
	out.FrontOrder = &fo

	// 2. 查全部 business + settlement
	bizViews, err := ListBusinessesWithSettlementsByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return out, fmt.Errorf("list businesses with settlements: %w", err)
	}
	out.Businesses = bizViews

	// 3. 计算汇总状态（只看 settle_records）
	out.Summary = computeSettlementSummary(bizViews)

	return out, nil
}

// ListBusinessesWithSettlementsByFrontOrderID 按 front_order_id 列全部 business 及各自 settlement
func ListBusinessesWithSettlementsByFrontOrderID(ctx context.Context, store *clientDB, frontOrderID string) ([]BusinessSettlementView, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}

	// 1. 查全部 business
	businesses, err := ListBusinessesByFrontOrderID(ctx, store, frontOrderID)
	if err != nil {
		return nil, fmt.Errorf("list businesses: %w", err)
	}

	var out []BusinessSettlementView
	for _, biz := range businesses {
		view := BusinessSettlementView{
			Business: biz,
		}

		// 2. 查 settlement
		settlement, err := dbGetBusinessSettlementByBusinessID(ctx, store, biz.BusinessID)
		if err != nil {
			// settlement 可能还不存在（pending 初期），不报错，留空
			view.Settlement = BusinessSettlementItem{
				BusinessID: biz.BusinessID,
				Status:     "pending",
			}
		} else {
			view.Settlement = settlement
		}

		// 3. 根据 settlement method 查底层支付事实
		if view.Settlement.SettlementMethod == string(SettlementMethodChain) && view.Settlement.Status == "settled" && view.Settlement.TargetID != "" {
			cp, err := GetChainPaymentBySettlement(ctx, store, view.Settlement)
			if err == nil {
				view.ChainPayment = &cp
			}
		} else if view.Settlement.SettlementMethod == string(SettlementMethodPool) && view.Settlement.Status == "settled" && view.Settlement.TargetID != "" {
			pa, err := GetPoolAllocationByBusinessID(ctx, store, biz.BusinessID)
			if err == nil {
				view.PoolAllocation = &pa
			}
		}

		out = append(out, view)
	}

	return out, nil
}

// computeSettlementSummary 只根据 settle_records 计算汇总状态
// 规则：
//   - 全部 settled → settled
//   - 全部 failed → failed
//   - 全部 pending → pending
//   - 混合 → partial_settled
func computeSettlementSummary(views []BusinessSettlementView) SettlementSummary {
	summary := SettlementSummary{
		TotalBusinessCount: len(views),
	}

	if len(views) == 0 {
		summary.OverallStatus = "pending"
		return summary
	}

	waitingFundCount := 0
	for _, v := range views {
		amount := settlementTargetAmountSatoshi(v)
		summary.TotalTargetSatoshi += amount
		switch v.Settlement.Status {
		case "settled":
			summary.SettledCount++
			summary.SettledAmountSatoshi += amount
		case "failed":
			summary.FailedCount++
		case "waiting_fund":
			waitingFundCount++
		default:
			summary.PendingCount++
		}
	}

	// 判断整体状态
	if summary.SettledCount == len(views) {
		summary.OverallStatus = "settled"
	} else if summary.FailedCount == len(views) {
		summary.OverallStatus = "failed"
	} else if waitingFundCount > 0 && summary.SettledCount == 0 {
		summary.OverallStatus = "waiting_fund"
	} else if summary.TotalTargetSatoshi > 0 && summary.SettledAmountSatoshi >= summary.TotalTargetSatoshi {
		summary.OverallStatus = "settled"
	} else if summary.PendingCount == len(views) {
		summary.OverallStatus = "pending"
	} else {
		if summary.SettledCount > 0 {
			summary.OverallStatus = "partial_settled"
		} else {
			if waitingFundCount > 0 {
				summary.OverallStatus = "waiting_fund"
			} else {
				summary.OverallStatus = "pending"
			}
		}
	}

	return summary
}

// settlementTargetAmountSatoshi 尽量从 settlement/business payload 里抠出目标金额。
// 设计说明：
// - 新的 BSV 订单会把 amount_satoshi 放在 payload 里；
// - 老业务如果没有金额字段，这里就退化成 0，不影响状态判断。
func settlementTargetAmountSatoshi(view BusinessSettlementView) uint64 {
	if amount, ok := jsonAmountSatoshi(view.Settlement.Payload); ok {
		return amount
	}
	if amount, ok := jsonAmountSatoshi(view.Business.Payload); ok {
		return amount
	}
	return 0
}

func jsonAmountSatoshi(raw json.RawMessage) (uint64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, false
	}
	for _, key := range []string{"amount_satoshi", "amount_sat", "target_amount_satoshi"} {
		v, ok := payload[key]
		if !ok {
			continue
		}
		switch n := v.(type) {
		case float64:
			if n >= 0 {
				return uint64(n), true
			}
		case int64:
			if n >= 0 {
				return uint64(n), true
			}
		case uint64:
			return n, true
		case string:
			n = strings.TrimSpace(n)
			if n == "" {
				continue
			}
			if parsed, err := strconv.ParseUint(n, 10, 64); err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

// GetFullPoolSettlementChainByPoolSessionID 按 pool_session_id 直接查完整池结算链。
// 说明：
// - 这是 session 维度的新入口，保留 front_order 入口不变；
// - 这里只负责把 pool_session -> settlement_payment_attempt -> business -> settlement 串起来；
// - 旧的 front_order 读法仍然可用。
func GetFullPoolSettlementChainByPoolSessionID(ctx context.Context, store *clientDB, poolSessionID string) (PoolSettlementChain, error) {
	var out PoolSettlementChain
	if store == nil {
		return out, fmt.Errorf("client db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return out, fmt.Errorf("pool_session_id is required")
	}

	poolSession, err := GetPoolSessionByID(ctx, store, poolSessionID)
	if err != nil {
		return out, fmt.Errorf("find pool_session: %w", err)
	}
	out.PoolSession = &poolSession

	paymentAttemptID, err := dbGetSettlementPaymentAttemptByPoolSessionIDDB(ctx, store.db, poolSessionID)
	if err != nil {
		return out, fmt.Errorf("find settlement payment attempt: %w", err)
	}

	business, err := dbGetLatestBusinessBySettlementPaymentAttemptID(ctx, store, paymentAttemptID)
	if err != nil {
		return out, fmt.Errorf("find business: %w", err)
	}
	out.Business = business

	settlement, err := GetSettlementByBusinessID(ctx, store, business.BusinessID)
	if err != nil {
		return out, fmt.Errorf("find settlement: %w", err)
	}
	out.Settlement = settlement

	if settlement.SettlementMethod == string(SettlementMethodPool) && settlement.Status == "settled" && settlement.TargetID != "" {
		poolAllocationID, err := strconv.ParseInt(strings.TrimSpace(settlement.TargetID), 10, 64)
		if err != nil {
			return out, fmt.Errorf("parse settlement target_id: %w", err)
		}
		poolAllocation, err := GetPoolAllocationByID(ctx, store, poolAllocationID)
		if err != nil {
			return out, fmt.Errorf("find pool_allocation: %w", err)
		}
		out.PoolAllocation = &poolAllocation
	}

	return out, nil
}

// GetSettlementByChainPaymentID 按 chain_payment.id 反查 settlement
func GetSettlementByChainPaymentID(ctx context.Context, store *clientDB, chainPaymentID int64) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := QueryRowContext(ctx, db,
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM settle_records WHERE settlement_method='chain' AND target_id=?`,
			fmt.Sprintf("%d", chainPaymentID),
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return BusinessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}
