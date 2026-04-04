package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// ============================================================
// 业务结算聚合读模型（第四步主线）
// 设计原则：
//   - 业务状态优先看 settle_business_settlements，不看旧过程表
//   - 汇总状态只根据 settle_business_settlements 计算，不参考 proc_direct_transfer_pools/txid/旧表
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
	Business       financeBusinessItem `json:"business"`
	Settlement     BusinessSettlementItem `json:"settlement"`
	ChainPayment   *ChainPaymentItem      `json:"chain_payment,omitempty"`
	PoolAllocation *PoolAllocationItem    `json:"pool_allocation,omitempty"`
}

// SettlementSummary 汇总状态（只根据 settle_business_settlements 计算）
type SettlementSummary struct {
	TotalBusinessCount int    `json:"total_business_count"`
	SettledCount       int    `json:"settled_count"`
	FailedCount        int    `json:"failed_count"`
	PendingCount       int    `json:"pending_count"`
	OverallStatus      string `json:"overall_status"` // pending | partial_settled | settled | failed
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

	// 3. 计算汇总状态（只看 settle_business_settlements）
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

// computeSettlementSummary 只根据 settle_business_settlements 计算汇总状态
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

	for _, v := range views {
		switch v.Settlement.Status {
		case "settled":
			summary.SettledCount++
		case "failed":
			summary.FailedCount++
		default:
			summary.PendingCount++
		}
	}

	// 判断整体状态
	if summary.SettledCount == len(views) {
		summary.OverallStatus = "settled"
	} else if summary.FailedCount == len(views) {
		summary.OverallStatus = "failed"
	} else if summary.PendingCount == len(views) {
		summary.OverallStatus = "pending"
	} else {
		// 混合情况统一归 partial_settled
		summary.OverallStatus = "partial_settled"
	}

	return summary
}

// GetSettlementByChainPaymentID 按 chain_payment.id 反查 settlement
func GetSettlementByChainPaymentID(ctx context.Context, store *clientDB, chainPaymentID int64) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM settle_business_settlements WHERE settlement_method='chain' AND target_id=?`,
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
