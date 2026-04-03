package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// ============================================================
// 第五步：一致性校验
// 职责：验证新旧口径是否一致，找出漂移点
// 原则：
//   - 只报告，不自动修复（防止误修复）
//   - 新模型认定 settled 时，旧表不应显示异常
//   - 冲突时以新模型为准
// ============================================================

// ReconciliationReport 一致性校验报告
type ReconciliationReport struct {
	CheckedAt       time.Time                `json:"checked_at"`
	DomainChecks    []DomainReconcileItem    `json:"domain_checks,omitempty"`
	PoolChecks      []PoolReconcileItem      `json:"pool_checks,omitempty"`
	Inconsistencies []InconsistencyItem      `json:"inconsistencies,omitempty"`
	Summary         ReconciliationSummary    `json:"summary"`
}

// DomainReconcileItem 域名注册对账项
type DomainReconcileItem struct {
	ChainPaymentID int64  `json:"chain_payment_id"`
	TxID           string `json:"txid"`
	NewModelStatus string `json:"new_model_status"` // settlement 状态
	OldModelFound  bool   `json:"old_model_found"`
	Consistent     bool   `json:"consistent"`
}

// PoolReconcileItem 池支付对账项
type PoolReconcileItem struct {
	PoolAllocationID int64  `json:"pool_allocation_id"`
	PoolSessionID    string `json:"pool_session_id"`
	NewModelStatus   string `json:"new_model_status"` // settlement 状态
	OldPoolStatus    string `json:"old_pool_status"`  // direct_transfer_pools.status
	Consistent       bool   `json:"consistent"`
}

// InconsistencyItem 不一致项
type InconsistencyItem struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	NewStatus   string `json:"new_status"`
	OldStatus   string `json:"old_status"`
	Description string `json:"description"`
}

// ReconciliationSummary 对账汇总
type ReconciliationSummary struct {
	TotalChecked      int `json:"total_checked"`
	ConsistentCount   int `json:"consistent_count"`
	InconsistentCount int `json:"inconsistent_count"`
}

// RunDomainRegisterReconciliation 域名注册新旧口径对账
// 校验：每条已 settled 的 domain business，是否都有 chain_payment 目标
func RunDomainRegisterReconciliation(ctx context.Context, store *clientDB) (*ReconciliationReport, error) {
	report := &ReconciliationReport{
		CheckedAt: time.Now(),
	}
	if store == nil {
		return report, fmt.Errorf("client db is nil")
	}

	err := store.Do(ctx, func(db *sql.DB) error {
		// 查询所有 domain 类型的 settlements
		rows, err := db.Query(`
			SELECT bs.settlement_id, bs.business_id, bs.status, bs.target_id,
			       fb.source_type, fb.source_id
			FROM business_settlements bs
			JOIN fin_business fb ON fb.business_id = bs.business_id
			WHERE fb.accounting_scene = 'domain'
			ORDER BY bs.created_at_unix DESC
		`)
		if err != nil {
			return fmt.Errorf("query settlements: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var item struct {
				SettlementID string
				BusinessID   string
				Status       string
				TargetID     string
				SourceType   string
				SourceID     string
			}
			if err := rows.Scan(&item.SettlementID, &item.BusinessID, &item.Status, &item.TargetID,
				&item.SourceType, &item.SourceID); err != nil {
				continue
			}

			report.Summary.TotalChecked++

			// 检查对应的 chain_payment 是否存在
			var chainPaymentID int64
			if item.TargetID != "" {
				fmt.Sscanf(item.TargetID, "%d", &chainPaymentID)
			}

			var txid string
			var cpFound bool
			if chainPaymentID > 0 {
				err := db.QueryRow(`SELECT txid FROM chain_payments WHERE id=?`, chainPaymentID).Scan(&txid)
				if err == nil {
					cpFound = true
				}
			}

			consistent := true
			if item.Status == "settled" && !cpFound {
				consistent = false
				report.Inconsistencies = append(report.Inconsistencies, InconsistencyItem{
					Type:        "domain_register",
					ID:          item.SettlementID,
					NewStatus:   "settled",
					OldStatus:   "missing_chain_payment",
					Description: fmt.Sprintf("settlement settled but chain_payment %s not found", item.TargetID),
				})
			}

			if consistent {
				report.Summary.ConsistentCount++
			} else {
				report.Summary.InconsistentCount++
			}

			report.DomainChecks = append(report.DomainChecks, DomainReconcileItem{
				ChainPaymentID: chainPaymentID,
				TxID:           txid,
				NewModelStatus: item.Status,
				OldModelFound:  cpFound,
				Consistent:     consistent,
			})
		}
		return rows.Err()
	})

	return report, err
}

// RunPoolReconciliation 池支付新旧口径对账
// 校验：每条已 settled 的 pool business，是否都有 pool_allocation 目标
// 同时检查：新模型 settled 时，旧 direct_transfer_pools 不应显示异常状态
func RunPoolReconciliation(ctx context.Context, store *clientDB) (*ReconciliationReport, error) {
	report := &ReconciliationReport{
		CheckedAt: time.Now(),
	}
	if store == nil {
		return report, fmt.Errorf("client db is nil")
	}

	err := store.Do(ctx, func(db *sql.DB) error {
		// 查询所有 pool 类型的 settlements
		rows, err := db.Query(`
			SELECT bs.settlement_id, bs.business_id, bs.status, bs.target_id
			FROM business_settlements bs
			WHERE bs.settlement_method = 'pool'
			ORDER BY bs.created_at_unix DESC
		`)
		if err != nil {
			return fmt.Errorf("query pool settlements: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var item struct {
				SettlementID string
				BusinessID   string
				Status       string
				TargetID     string
			}
			if err := rows.Scan(&item.SettlementID, &item.BusinessID, &item.Status, &item.TargetID); err != nil {
				continue
			}

			report.Summary.TotalChecked++

			// 检查对应的 pool_allocation
			var poolAllocID int64
			if item.TargetID != "" {
				fmt.Sscanf(item.TargetID, "%d", &poolAllocID)
			}

			var paFound bool
			var poolSessionID string
			if poolAllocID > 0 {
				err := db.QueryRow(`SELECT pool_session_id FROM pool_allocations WHERE id=?`, poolAllocID).Scan(&poolSessionID)
				if err == nil {
					paFound = true
				}
			}

			// 检查旧 direct_transfer_pools 状态
			var oldPoolStatus string
			if poolSessionID != "" {
				_ = db.QueryRow(`SELECT status FROM direct_transfer_pools WHERE session_id=?`, poolSessionID).Scan(&oldPoolStatus)
			}

			consistent := true
			if item.Status == "settled" && !paFound {
				consistent = false
				report.Inconsistencies = append(report.Inconsistencies, InconsistencyItem{
					Type:        "pool_allocation",
					ID:          item.SettlementID,
					NewStatus:   "settled",
					OldStatus:   "missing_pool_allocation",
					Description: fmt.Sprintf("settlement settled but pool_allocation %s not found", item.TargetID),
				})
			}

			// 如果新模型认定 settled，但旧模型显示异常，报告但不阻断
			if item.Status == "settled" && oldPoolStatus != "" && oldPoolStatus != "active" && oldPoolStatus != "closing" && oldPoolStatus != "closed" {
				// 旧状态异常，但新模型已 settled，这是允许的（旧表只是过程态）
				// 记录但不标记为不一致
				obs.Info("bitcast-client", "reconcile_pool_old_status_drift", map[string]any{
					"settlement_id":    item.SettlementID,
					"pool_session_id":  poolSessionID,
					"new_status":       "settled",
					"old_pool_status":  oldPoolStatus,
					"note":             "new model settled, old pool has different status (expected)",
				})
			}

			if consistent {
				report.Summary.ConsistentCount++
			} else {
				report.Summary.InconsistentCount++
			}

			report.PoolChecks = append(report.PoolChecks, PoolReconcileItem{
				PoolAllocationID: poolAllocID,
				PoolSessionID:    poolSessionID,
				NewModelStatus:   item.Status,
				OldPoolStatus:    oldPoolStatus,
				Consistent:       consistent,
			})
		}
		return rows.Err()
	})

	return report, err
}

// RunFullReconciliation 运行完整对账
func RunFullReconciliation(ctx context.Context, store *clientDB) (*ReconciliationReport, error) {
	// 先跑域名对账
	report, err := RunDomainRegisterReconciliation(ctx, store)
	if err != nil {
		obs.Error("bitcast-client", "reconciliation_domain_failed", map[string]any{"error": err.Error()})
	}

	// 再跑池对账
	poolReport, err := RunPoolReconciliation(ctx, store)
	if err != nil {
		obs.Error("bitcast-client", "reconciliation_pool_failed", map[string]any{"error": err.Error()})
	} else {
		// 合并结果
		report.PoolChecks = poolReport.PoolChecks
		report.Inconsistencies = append(report.Inconsistencies, poolReport.Inconsistencies...)
		report.Summary.TotalChecked += poolReport.Summary.TotalChecked
		report.Summary.ConsistentCount += poolReport.Summary.ConsistentCount
		report.Summary.InconsistentCount += poolReport.Summary.InconsistentCount
	}

	return report, nil
}

// CheckNewModelDominance 检查新模型是否真正主导
// 验证：所有业务状态判断都基于 settlement，不再依赖旧表
func CheckNewModelDominance(ctx context.Context, store *clientDB) (*DominanceCheckResult, error) {
	result := &DominanceCheckResult{
		CheckedAt: time.Now(),
	}
	if store == nil {
		return result, fmt.Errorf("client db is nil")
	}

	err := store.Do(ctx, func(db *sql.DB) error {
		// 1. 检查是否有代码还在直接查 direct_transfer_pools 判断支付完成
		// 这个检查是静态的，需要在代码审查中完成
		// 这里只检查数据层面：所有 "已完成" 的业务都应该有 settlement

		// 2. 检查 purchases 表中 status='done' 但 settlement 缺失的记录
		rows, err := db.Query(`
			SELECT p.id, p.demand_id, p.seller_pub_hex, p.status
			FROM purchases p
			WHERE p.status = 'done'
				AND NOT EXISTS (
					SELECT 1 FROM business_triggers bt
					JOIN fin_business fb ON fb.business_id = bt.business_id
					WHERE bt.trigger_type = 'purchase' 
					AND bt.trigger_id_value = CAST(p.id AS TEXT)
				)
			LIMIT 100
		`)
		if err != nil {
			return fmt.Errorf("check purchases without settlement: %w", err)
		}
		defer rows.Close()

		var orphanedPurchases []OrphanedRecord
		for rows.Next() {
			var r OrphanedRecord
			if err := rows.Scan(&r.ID, &r.RefID, &r.PartyID, &r.Status); err != nil {
				continue
			}
			r.Table = "purchases"
			r.Description = "purchase marked done but no settlement found"
			orphanedPurchases = append(orphanedPurchases, r)
		}
		result.OrphanedOldRecords = append(result.OrphanedOldRecords, orphanedPurchases...)
		result.NewModelDominant = len(orphanedPurchases) == 0

		return rows.Err()
	})

	return result, err
}

// DominanceCheckResult 新模型主导性检查结果
type DominanceCheckResult struct {
	CheckedAt          time.Time       `json:"checked_at"`
	NewModelDominant   bool            `json:"new_model_dominant"`
	OrphanedOldRecords []OrphanedRecord `json:"orphaned_old_records,omitempty"`
}

// OrphanedRecord 孤立记录（旧表有数据但新模型缺失）
type OrphanedRecord struct {
	Table       string `json:"table"`
	ID          string `json:"id"`
	RefID       string `json:"ref_id"`
	PartyID     string `json:"party_id,omitempty"`
	Status      string `json:"status"`
	Description string `json:"description"`
}

// LogReconciliationReport 输出对账报告到日志
func LogReconciliationReport(report *ReconciliationReport) {
	if report == nil {
		return
	}

	obs.Info("bitcast-client", "reconciliation_report", map[string]any{
		"checked_at":         report.CheckedAt,
		"total_checked":      report.Summary.TotalChecked,
		"consistent":         report.Summary.ConsistentCount,
		"inconsistent":       report.Summary.InconsistentCount,
		"inconsistency_list": report.Inconsistencies,
	})

	if len(report.Inconsistencies) > 0 {
		obs.Error("bitcast-client", "reconciliation_has_inconsistencies", map[string]any{
			"count": len(report.Inconsistencies),
			"note":  "new model is dominant, old model drift detected but not blocking",
		})
	}
}
