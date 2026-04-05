package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// ==================== 资产确认队列运维查询 ====================
// 设计说明：
// - 提供管理端可观测能力：状态分布、失败明细、对账检查
// - 所有查询只读，不改变业务状态
// - 运维动作（重置/重试）单独函数，不影响自动确认流程

const (
	// assetVerificationMaxRetries 最大重试次数，超过后自动转 failed
	// 设计说明：
	// - 避免无限重试占用队列资源
	// - 转 failed 后需人工介入（重置或批量重试）
	assetVerificationMaxRetries = 10
)

// VerificationQueueSummary 队列汇总统计
type VerificationQueueSummary struct {
	Pending           int `json:"pending"`
	ConfirmedBSV20    int `json:"confirmed_bsv20"`
	ConfirmedBSV21    int `json:"confirmed_bsv21"`
	ConfirmedPlainBSV int `json:"confirmed_plain_bsv"`
	Failed            int `json:"failed"`
	Total             int `json:"total"`
}

// VerificationFailureDetail 单条失败明细
type VerificationFailureDetail struct {
	UTXOID          string `json:"utxo_id"`
	WalletID        string `json:"wallet_id"`
	Address         string `json:"address"`
	TxID            string `json:"txid"`
	Vout            uint32 `json:"vout"`
	ValueSatoshi    uint64 `json:"value_satoshi"`
	ErrorMessage    string `json:"error_message"`
	RetryCount      int    `json:"retry_count"`
	NextRetryAtUnix int64  `json:"next_retry_at_unix"`
	LastCheckAtUnix int64  `json:"last_check_at_unix"`
	UpdatedAtUnix   int64  `json:"updated_at_unix"`
}

// VerificationQueueItem 队列项（通用查询返回）
type VerificationQueueItem struct {
	UTXOID          string `json:"utxo_id"`
	WalletID        string `json:"wallet_id"`
	Address         string `json:"address"`
	TxID            string `json:"txid"`
	Vout            uint32 `json:"vout"`
	ValueSatoshi    uint64 `json:"value_satoshi"`
	Status          string `json:"status"`
	ErrorMessage    string `json:"error_message"`
	RetryCount      int    `json:"retry_count"`
	NextRetryAtUnix int64  `json:"next_retry_at_unix"`
	LastCheckAtUnix int64  `json:"last_check_at_unix"`
	WOCResponseJSON string `json:"woc_response_json"`
	UpdatedAtUnix   int64  `json:"updated_at_unix"`
}

// VerificationReconcileReport 对账报告
type VerificationReconcileReport struct {
	// ConfirmedNoFact: verification 已确认但 fact_chain_asset_flows 不存在
	ConfirmedNoFact []VerificationReconcileItem `json:"confirmed_no_fact"`
	// FactNoConfirmation: fact 存在但 verification 仍 pending
	FactNoConfirmation []VerificationReconcileItem `json:"fact_no_confirmation"`
	// 汇总
	Summary map[string]int `json:"summary"`
}

// VerificationReconcileItem 对账异常项
type VerificationReconcileItem struct {
	UTXOID       string `json:"utxo_id"`
	WalletID     string `json:"wallet_id"`
	Address      string `json:"address"`
	Status       string `json:"status"`
	AssetKind    string `json:"asset_kind"` // 来自 fact 或预期值
	ErrorMessage string `json:"error_message"`
}

// ==================== 查询函数 ====================

// dbGetVerificationQueueSummary 获取队列汇总统计
func dbGetVerificationQueueSummary(ctx context.Context, store *clientDB) (*VerificationQueueSummary, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (*VerificationQueueSummary, error) {
		var summary VerificationQueueSummary
		row := db.QueryRow(`
			SELECT
				COUNT(1),
				COUNT(CASE WHEN status='pending' THEN 1 END),
				COUNT(CASE WHEN status='confirmed_bsv20' THEN 1 END),
				COUNT(CASE WHEN status='confirmed_bsv21' THEN 1 END),
				COUNT(CASE WHEN status='confirmed_plain_bsv' THEN 1 END),
				COUNT(CASE WHEN status='failed' THEN 1 END)
			FROM wallet_utxo_token_verification
		`)
		if err := row.Scan(&summary.Total, &summary.Pending,
			&summary.ConfirmedBSV20, &summary.ConfirmedBSV21,
			&summary.ConfirmedPlainBSV, &summary.Failed); err != nil {
			return nil, err
		}
		return &summary, nil
	})
}

// dbListFailedVerificationItems 查询最近 N 条失败项
// 设计说明：
// - 默认只查 status='failed'，与"失败明细"语义一致
// - includePending=true 时也会返回 pending 但带错误信息的项
func dbListFailedVerificationItems(ctx context.Context, store *clientDB, limit int, includePending bool) ([]VerificationFailureDetail, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if limit <= 0 {
		limit = 50
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]VerificationFailureDetail, error) {
		statusClause := "status='failed'"
		if includePending {
			statusClause = "status IN ('pending', 'failed')"
		}
		rows, err := db.Query(`
			SELECT utxo_id, wallet_id, address, txid, vout, value_satoshi,
			       error_message, retry_count, next_retry_at_unix,
			       last_check_at_unix, updated_at_unix
			FROM wallet_utxo_token_verification
			WHERE `+statusClause+` AND error_message != ''
			ORDER BY updated_at_unix DESC
			LIMIT ?
		`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]VerificationFailureDetail, 0, limit)
		for rows.Next() {
			var item VerificationFailureDetail
			if err := rows.Scan(&item.UTXOID, &item.WalletID, &item.Address,
				&item.TxID, &item.Vout, &item.ValueSatoshi,
				&item.ErrorMessage, &item.RetryCount, &item.NextRetryAtUnix,
				&item.LastCheckAtUnix, &item.UpdatedAtUnix); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	})
}

// verificationQueueFilter 通用查询过滤条件
type verificationQueueFilter struct {
	Status        string // 可选：'pending' | 'confirmed_bsv20' | 'confirmed_bsv21' | 'confirmed_plain_bsv' | 'failed'
	WalletID      string // 可选：按钱包过滤
	AfterUnix     int64  // 可选：updated_at_unix >= AfterUnix
	BeforeUnix    int64  // 可选：updated_at_unix <= BeforeUnix
	Limit         int
	OrderByUpdate bool // 按 updated_at_unix DESC 排序，默认按 next_retry_at_unix ASC
}

// dbListVerificationItems 通用队列表查询（支持过滤）
func dbListVerificationItems(ctx context.Context, store *clientDB, f verificationQueueFilter) ([]VerificationQueueItem, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if f.Limit <= 0 {
		f.Limit = 100
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]VerificationQueueItem, error) {
		// 动态构建查询
		where := make([]string, 0, 4)
		args := make([]any, 0, 6)
		if f.Status != "" {
			where = append(where, "status=?")
			args = append(args, f.Status)
		}
		if f.WalletID != "" {
			where = append(where, "wallet_id=?")
			args = append(args, f.WalletID)
		}
		if f.AfterUnix > 0 {
			where = append(where, "updated_at_unix>=?")
			args = append(args, f.AfterUnix)
		}
		if f.BeforeUnix > 0 {
			where = append(where, "updated_at_unix<=?")
			args = append(args, f.BeforeUnix)
		}
		whereSQL := ""
		if len(where) > 0 {
			whereSQL = "WHERE " + strings.Join(where, " AND ")
		}
		orderBy := "next_retry_at_unix ASC"
		if f.OrderByUpdate {
			orderBy = "updated_at_unix DESC"
		}
		query := fmt.Sprintf(`
			SELECT utxo_id, wallet_id, address, txid, vout, value_satoshi, status,
			       error_message, retry_count, next_retry_at_unix, last_check_at_unix,
			       woc_response_json, updated_at_unix
			FROM wallet_utxo_token_verification
			%s
			ORDER BY %s
			LIMIT ?
		`, whereSQL, orderBy)
		args = append(args, f.Limit)

		rows, err := db.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]VerificationQueueItem, 0, f.Limit)
		for rows.Next() {
			var item VerificationQueueItem
			if err := rows.Scan(&item.UTXOID, &item.WalletID, &item.Address,
				&item.TxID, &item.Vout, &item.ValueSatoshi, &item.Status,
				&item.ErrorMessage, &item.RetryCount, &item.NextRetryAtUnix,
				&item.LastCheckAtUnix, &item.WOCResponseJSON, &item.UpdatedAtUnix); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	})
}

// ==================== 对账检查 ====================

// dbCheckVerificationReconciliation 对账检查
// 设计说明：
// 1. confirmed_* 但没有 fact IN 记录的异常项
// 2. fact IN 存在但 verification 仍 pending 的异常项
func dbCheckVerificationReconciliation(ctx context.Context, store *clientDB) (*VerificationReconcileReport, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (*VerificationReconcileReport, error) {
		report := &VerificationReconcileReport{
			Summary: make(map[string]int),
		}

		// 1. confirmed_* 但 fact_chain_asset_flows 不存在
		rows1, err := db.Query(`
			SELECT v.utxo_id, v.wallet_id, v.address, v.status, v.error_message
			FROM wallet_utxo_token_verification v
			LEFT JOIN fact_chain_asset_flows f ON v.utxo_id = f.utxo_id AND f.direction = 'IN'
			WHERE v.status IN ('confirmed_bsv20', 'confirmed_bsv21', 'confirmed_plain_bsv')
			  AND f.flow_id IS NULL
		`)
		if err != nil {
			return nil, fmt.Errorf("query confirmed without fact: %w", err)
		}
		for rows1.Next() {
			var item VerificationReconcileItem
			if err := rows1.Scan(&item.UTXOID, &item.WalletID, &item.Address, &item.Status, &item.ErrorMessage); err != nil {
				rows1.Close()
				return nil, err
			}
			report.ConfirmedNoFact = append(report.ConfirmedNoFact, item)
		}
		rows1.Close()
		report.Summary["confirmed_without_fact"] = len(report.ConfirmedNoFact)

		// 2. fact IN 存在但 verification 仍 pending
		// 设计说明：
		// - 不扫全量 fact_chain_asset_flows，只查 verification 队列范围内的项
		// - 普通 UTXO 的 fact IN 不经过 verification 队列，不应误报
		// - 只检查：verification 队列表中存在（或曾经存在）但状态仍 pending 的项
		// - 用 verification 队列表为驱动，而不是 fact 表
		rows2, err := db.Query(`
			SELECT v.utxo_id, v.wallet_id, v.address,
			       v.status,
			       COALESCE(f.asset_kind, '') as asset_kind
			FROM wallet_utxo_token_verification v
			LEFT JOIN fact_chain_asset_flows f ON v.utxo_id = f.utxo_id AND f.direction = 'IN'
			WHERE v.status = 'pending'
			  AND f.flow_id IS NOT NULL
		`)
		if err != nil {
			return nil, fmt.Errorf("query fact without confirmation: %w", err)
		}
		for rows2.Next() {
			var item VerificationReconcileItem
			if err := rows2.Scan(&item.UTXOID, &item.WalletID, &item.Address, &item.Status, &item.AssetKind); err != nil {
				rows2.Close()
				return nil, err
			}
			report.FactNoConfirmation = append(report.FactNoConfirmation, item)
		}
		rows2.Close()
		report.Summary["fact_pending_or_missing"] = len(report.FactNoConfirmation)

		return report, nil
	})
}

// ==================== 手动运维动作 ====================

// dbResetVerificationToPending 将指定 utxo_id 重置为 pending
// 设计说明：
// - 用于人工介入后手动重试异常项
// - 清空错误信息和重试计数
func dbResetVerificationToPending(ctx context.Context, store *clientDB, utxoID string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		res, err := db.Exec(`
			UPDATE wallet_utxo_token_verification
			SET status='pending', retry_count=0, error_message='',
			    next_retry_at_unix=?, updated_at_unix=?
			WHERE utxo_id=?
		`, now, now, utxoID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		return nil
	})
}

// dbBatchRetryFailed 批量重试 failed 项（按钱包或时间窗）
// 设计说明：
// - 默认只处理 status='failed' 项，与"批量重试失败"语义一致
// - 可按 wallet_id 和时间窗过滤
// - 返回实际重置的数量
func dbBatchRetryFailed(ctx context.Context, store *clientDB, walletID string, afterUnix int64, beforeUnix int64) (int, error) {
	if store == nil {
		return 0, fmt.Errorf("store is nil")
	}
	var result int
	if err := store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		where := make([]string, 0, 4)
		where = append(where, "status='failed'")
		where = append(where, "error_message != ''")
		args := make([]any, 0, 6)
		if walletID != "" {
			where = append(where, "wallet_id=?")
			args = append(args, walletID)
		}
		if afterUnix > 0 {
			where = append(where, "updated_at_unix>=?")
			args = append(args, afterUnix)
		}
		if beforeUnix > 0 {
			where = append(where, "updated_at_unix<=?")
			args = append(args, beforeUnix)
		}
		// 注意：SET 的 ? 在前，WHERE 的 ? 在后
		query := fmt.Sprintf(`UPDATE wallet_utxo_token_verification
			SET status='pending', retry_count=0, error_message='',
			    next_retry_at_unix=?, updated_at_unix=?
			WHERE %s`, strings.Join(where, " AND "))
		// args 必须先放 SET 的两个 now，再放 WHERE 的参数
		fullArgs := append([]any{now, now}, args...)

		res, err := db.Exec(query, fullArgs...)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		result = int(affected)
		return err
	}); err != nil {
		return 0, err
	}
	return result, nil
}

// ==================== 自动转 failed 逻辑 ====================

// dbAutoFailExhaustedRetries 将超过最大重试次数的 pending 项转 failed
// 设计说明：
// - 在 runUnknownAssetVerification 每次执行前调用
// - 先把超重试项转 failed，避免无效查询
// - 返回转 failed 的数量
func dbAutoFailExhaustedRetries(ctx context.Context, store *clientDB) (int, error) {
	if store == nil {
		return 0, fmt.Errorf("store is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int, error) {
		now := time.Now().Unix()
		res, err := db.Exec(`
			UPDATE wallet_utxo_token_verification
			SET status='failed', error_message='max retries exceeded',
			    updated_at_unix=?
			WHERE status='pending' AND retry_count >= ?
		`, now, assetVerificationMaxRetries)
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		return int(affected), err
	})
}

// ==================== 阈值告警 ====================

const (
	// verificationAlertFailedThreshold failed 数量超过此值时产生告警
	verificationAlertFailedThreshold = 10
)

// emitVerificationThresholdAlerts 阈值告警
// 设计说明：
// - 仅日志级别，不阻断业务流程
// - failed 超阈值、confirmed_without_fact 非 0 时产生 Warning/Error 日志
func emitVerificationThresholdAlerts(ctx context.Context, store *clientDB, trigger string) {
	if store == nil {
		return
	}
	summary, err := dbGetVerificationQueueSummary(ctx, store)
	if err != nil {
		return
	}
	// failed 超阈值
	if summary.Failed > verificationAlertFailedThreshold {
		obs.Error("bitcast-client", "verification_alert_failed_threshold", map[string]any{
			"failed_count":   summary.Failed,
			"threshold":      verificationAlertFailedThreshold,
			"trigger":        trigger,
			"alert_severity": "high",
		})
	} else if summary.Failed > 0 {
		obs.Info("bitcast-client", "verification_alert_failed_warning", map[string]any{
			"failed_count":   summary.Failed,
			"threshold":      verificationAlertFailedThreshold,
			"trigger":        trigger,
			"alert_severity": "low",
		})
	}

	// confirmed_without_fact 非 0
	report, err := dbCheckVerificationReconciliation(ctx, store)
	if err != nil {
		return
	}
	if report.Summary["confirmed_without_fact"] > 0 {
		obs.Error("bitcast-client", "verification_alert_confirmed_without_fact", map[string]any{
			"confirmed_without_fact": report.Summary["confirmed_without_fact"],
			"trigger":                trigger,
			"alert_severity":         "high",
			"affected_items":         report.ConfirmedNoFact,
		})
	}
	if report.Summary["fact_pending_or_missing"] > 0 {
		obs.Error("bitcast-client", "verification_alert_fact_pending", map[string]any{
			"fact_pending_count": report.Summary["fact_pending_or_missing"],
			"trigger":            trigger,
			"alert_severity":     "medium",
			"affected_items":     report.FactNoConfirmation,
		})
	}
}

// ==================== 审计日志 ====================

// emitVerificationAuditLog 发射 verification 运维操作审计日志
// 设计说明：
// - 统一入口，记录谁触发、作用范围、影响条数
// - action: "reset" | "batch_retry"
func emitVerificationAuditLog(ctx context.Context, store *clientDB, action string, operator string, scope map[string]any, affected int, err error) {
	fields := map[string]any{
		"action":         action,
		"operator":       operator,
		"scope":          scope,
		"affected_count": affected,
	}
	if err != nil {
		fields["error"] = err.Error()
		obs.Error("bitcast-client", "verification_audit_log", fields)
		return
	}
	obs.Info("bitcast-client", "verification_audit_log", fields)
}

// ==================== 日志发射 ====================

// emitVerificationQueueSummaryLog 发射队列汇总日志事件
// 设计说明：
// - 固定日志事件名 verification_queue_summary
// - 包含各状态数量和失败原因分布
func emitVerificationQueueSummaryLog(ctx context.Context, store *clientDB, trigger string) {
	if store == nil {
		return
	}
	summary, err := dbGetVerificationQueueSummary(ctx, store)
	if err != nil {
		obs.Error("bitcast-client", "verification_queue_summary", map[string]any{
			"error":   err.Error(),
			"trigger": trigger,
		})
		return
	}

	// 附加失败原因分布
	failedItems, _ := dbListFailedVerificationItems(ctx, store, 100, true)
	errorReasons := make(map[string]int)
	for _, item := range failedItems {
		// 截取错误信息前 128 字符作为分类键
		key := item.ErrorMessage
		if len(key) > 128 {
			key = key[:128]
		}
		errorReasons[key]++
	}

	obs.Info("bitcast-client", "verification_queue_summary", map[string]any{
		"pending":             summary.Pending,
		"confirmed_bsv20":     summary.ConfirmedBSV20,
		"confirmed_bsv21":     summary.ConfirmedBSV21,
		"confirmed_plain_bsv": summary.ConfirmedPlainBSV,
		"failed":              summary.Failed,
		"total":               summary.Total,
		"error_reasons":       errorReasons,
		"trigger":             trigger,
	})
}

// emitVerificationReconcileReportLog 发射对账报告日志事件
// 设计说明：
// - 固定日志事件名 verification_reconcile_report
// - 包含异常项列表和汇总计数
// - Step 13：连续 reconcile 异常升级告警（连续 3 次有异常 → Error 级别）
func emitVerificationReconcileReportLog(ctx context.Context, store *clientDB, trigger string) {
	if store == nil {
		return
	}
	report, err := dbCheckVerificationReconciliation(ctx, store)
	if err != nil {
		obs.Error("bitcast-client", "verification_reconcile_report", map[string]any{
			"error":   err.Error(),
			"trigger": trigger,
		})
		return
	}

	anomalyCount := report.Summary["confirmed_without_fact"] + report.Summary["fact_pending_or_missing"]
	isAnomalous := anomalyCount > 0

	// Step 13：连续异常升级
	if isAnomalous {
		consecutive := verificationReconcileConsecutiveAnomaly.Add(1)
		level := "warning"
		if consecutive >= 3 {
			level = "critical"
		}
		obs.Error("bitcast-client", "verification_reconcile_report", map[string]any{
			"confirmed_without_fact":  report.Summary["confirmed_without_fact"],
			"fact_pending_or_missing": report.Summary["fact_pending_or_missing"],
			"confirmed_no_fact_items": report.ConfirmedNoFact,
			"fact_pending_items":      report.FactNoConfirmation,
			"trigger":                 trigger,
			"consecutive_anomalies":   consecutive,
			"alert_level":             level,
		})
	} else {
		verificationReconcileConsecutiveAnomaly.Reset()
		obs.Info("bitcast-client", "verification_reconcile_report", map[string]any{
			"confirmed_without_fact":  report.Summary["confirmed_without_fact"],
			"fact_pending_or_missing": report.Summary["fact_pending_or_missing"],
			"confirmed_no_fact_items": report.ConfirmedNoFact,
			"fact_pending_items":      report.FactNoConfirmation,
			"trigger":                 trigger,
			"consecutive_anomalies":   0,
		})
	}
}

// verificationReconcileConsecutiveAnomaly 连续异常计数器
// 设计说明：
// - 轻量内存计数器，用于升级告警级别
// - 进程重启后重置，不影响长期一致性
var verificationReconcileConsecutiveAnomaly = &consecutiveCounter{mu: sync.Mutex{}}

type consecutiveCounter struct {
	mu    sync.Mutex
	count int
}

func (c *consecutiveCounter) Add(n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count += n
	return c.count
}

func (c *consecutiveCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = 0
}

// ==================== 辅助：短 hex 显示 ====================

// shortHex 取字符串前4...后4，用于日志和 API 显示长 ID
func shortHex(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 8 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}
