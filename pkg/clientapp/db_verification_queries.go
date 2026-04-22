package clientapp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factbsvutxos"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/facttokencarrierlinks"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxotokenverification"
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
	// ConfirmedNoFact: verification 已确认但 fact_bsv_utxos 不存在
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
	return readEntValue(ctx, store, func(root EntReadRoot) (*VerificationQueueSummary, error) {
		var summary VerificationQueueSummary
		total, err := root.WalletUtxoTokenVerification.Query().Count(ctx)
		if err != nil {
			return nil, err
		}
		summary.Total = total
		if summary.Pending, err = root.WalletUtxoTokenVerification.Query().Where(walletutxotokenverification.StatusEQ("pending")).Count(ctx); err != nil {
			return nil, err
		}
		if summary.ConfirmedBSV20, err = root.WalletUtxoTokenVerification.Query().Where(walletutxotokenverification.StatusEQ("confirmed_bsv20")).Count(ctx); err != nil {
			return nil, err
		}
		if summary.ConfirmedBSV21, err = root.WalletUtxoTokenVerification.Query().Where(walletutxotokenverification.StatusEQ("confirmed_bsv21")).Count(ctx); err != nil {
			return nil, err
		}
		if summary.ConfirmedPlainBSV, err = root.WalletUtxoTokenVerification.Query().Where(walletutxotokenverification.StatusEQ("confirmed_plain_bsv")).Count(ctx); err != nil {
			return nil, err
		}
		if summary.Failed, err = root.WalletUtxoTokenVerification.Query().Where(walletutxotokenverification.StatusEQ("failed")).Count(ctx); err != nil {
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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]VerificationFailureDetail, error) {
		q := root.WalletUtxoTokenVerification.Query().
			Where(walletutxotokenverification.ErrorMessageNEQ(""))
		if includePending {
			q = q.Where(walletutxotokenverification.StatusIn("pending", "failed"))
		} else {
			q = q.Where(walletutxotokenverification.StatusEQ("failed"))
		}
		rows, err := q.Order(walletutxotokenverification.ByUpdatedAtUnix(entsql.OrderDesc())).
			Limit(limit).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]VerificationFailureDetail, 0, limit)
		for _, row := range rows {
			item := VerificationFailureDetail{
				UTXOID:          row.UtxoID,
				WalletID:        row.WalletID,
				Address:         row.Address,
				TxID:            row.Txid,
				Vout:            uint32(row.Vout),
				ValueSatoshi:    uint64(row.ValueSatoshi),
				ErrorMessage:    row.ErrorMessage,
				RetryCount:      int(row.RetryCount),
				NextRetryAtUnix: row.NextRetryAtUnix,
				LastCheckAtUnix: row.LastCheckAtUnix,
				UpdatedAtUnix:   row.UpdatedAtUnix,
			}
			out = append(out, item)
		}
		return out, nil
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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]VerificationQueueItem, error) {
		preds := make([]func(*gen.WalletUtxoTokenVerificationQuery), 0, 4)
		if f.Status != "" {
			preds = append(preds, func(q *gen.WalletUtxoTokenVerificationQuery) {
				q.Where(walletutxotokenverification.StatusEQ(f.Status))
			})
		}
		if f.WalletID != "" {
			preds = append(preds, func(q *gen.WalletUtxoTokenVerificationQuery) {
				q.Where(walletutxotokenverification.WalletIDEQ(f.WalletID))
			})
		}
		if f.AfterUnix > 0 {
			preds = append(preds, func(q *gen.WalletUtxoTokenVerificationQuery) {
				q.Where(walletutxotokenverification.UpdatedAtUnixGTE(f.AfterUnix))
			})
		}
		if f.BeforeUnix > 0 {
			preds = append(preds, func(q *gen.WalletUtxoTokenVerificationQuery) {
				q.Where(walletutxotokenverification.UpdatedAtUnixLTE(f.BeforeUnix))
			})
		}
		q := root.WalletUtxoTokenVerification.Query()
		for _, pred := range preds {
			pred(q)
		}
		if f.OrderByUpdate {
			q = q.Order(walletutxotokenverification.ByUpdatedAtUnix(entsql.OrderDesc()))
		} else {
			q = q.Order(walletutxotokenverification.ByNextRetryAtUnix(entsql.OrderAsc()))
		}
		rows, err := q.Limit(f.Limit).All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]VerificationQueueItem, 0, f.Limit)
		for _, row := range rows {
			item := VerificationQueueItem{
				UTXOID:          row.UtxoID,
				WalletID:        row.WalletID,
				Address:         row.Address,
				TxID:            row.Txid,
				Vout:            uint32(row.Vout),
				ValueSatoshi:    uint64(row.ValueSatoshi),
				Status:          row.Status,
				ErrorMessage:    row.ErrorMessage,
				RetryCount:      int(row.RetryCount),
				NextRetryAtUnix: row.NextRetryAtUnix,
				LastCheckAtUnix: row.LastCheckAtUnix,
				WOCResponseJSON: row.WocResponseJSON,
				UpdatedAtUnix:   row.UpdatedAtUnix,
			}
			out = append(out, item)
		}
		return out, nil
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
	return readEntValue(ctx, store, func(root EntReadRoot) (*VerificationReconcileReport, error) {
		report := &VerificationReconcileReport{
			Summary: make(map[string]int),
		}

		confirmedRows, err := root.WalletUtxoTokenVerification.Query().
			Where(walletutxotokenverification.StatusIn("confirmed_bsv20", "confirmed_bsv21", "confirmed_plain_bsv")).
			All(ctx)
		if err != nil {
			return nil, fmt.Errorf("query confirmed verification rows: %w", err)
		}
		confirmedIDs := make([]string, 0, len(confirmedRows))
		for _, row := range confirmedRows {
			confirmedIDs = append(confirmedIDs, row.UtxoID)
		}
		factConfirmedSet := make(map[string]struct{}, len(confirmedIDs))
		if len(confirmedIDs) > 0 {
			factRows, err := root.FactBsvUtxos.Query().
				Where(factbsvutxos.UtxoIDIn(confirmedIDs...)).
				All(ctx)
			if err != nil {
				return nil, fmt.Errorf("query confirmed facts: %w", err)
			}
			for _, row := range factRows {
				factConfirmedSet[strings.ToLower(strings.TrimSpace(row.UtxoID))] = struct{}{}
			}
		}
		for _, row := range confirmedRows {
			utxoID := strings.ToLower(strings.TrimSpace(row.UtxoID))
			if _, ok := factConfirmedSet[utxoID]; ok {
				continue
			}
			report.ConfirmedNoFact = append(report.ConfirmedNoFact, VerificationReconcileItem{
				UTXOID:       row.UtxoID,
				WalletID:     row.WalletID,
				Address:      row.Address,
				Status:       row.Status,
				ErrorMessage: row.ErrorMessage,
			})
		}
		report.Summary["confirmed_without_fact"] = len(report.ConfirmedNoFact)

		pendingRows, err := root.WalletUtxoTokenVerification.Query().
			Where(walletutxotokenverification.StatusEQ("pending")).
			All(ctx)
		if err != nil {
			return nil, fmt.Errorf("query pending verification rows: %w", err)
		}
		pendingIDs := make([]string, 0, len(pendingRows))
		for _, row := range pendingRows {
			pendingIDs = append(pendingIDs, row.UtxoID)
		}
		pendingFactType := make(map[string]string, len(pendingIDs))
		if len(pendingIDs) > 0 {
			factRows, err := root.FactBsvUtxos.Query().
				Where(factbsvutxos.UtxoIDIn(pendingIDs...)).
				All(ctx)
			if err != nil {
				return nil, fmt.Errorf("query pending facts: %w", err)
			}
			for _, row := range factRows {
				utxoID := strings.ToLower(strings.TrimSpace(row.UtxoID))
				if utxoID == "" {
					continue
				}
				pendingFactType[utxoID] = strings.TrimSpace(row.CarrierType)
			}
			carrierRows, err := root.FactTokenCarrierLinks.Query().
				Where(
					facttokencarrierlinks.CarrierUtxoIDIn(pendingIDs...),
					facttokencarrierlinks.LinkStateEQ("active"),
				).
				All(ctx)
			if err != nil {
				return nil, fmt.Errorf("query pending carrier links: %w", err)
			}
			for _, row := range carrierRows {
				utxoID := strings.ToLower(strings.TrimSpace(row.CarrierUtxoID))
				if utxoID == "" {
					continue
				}
				if _, ok := pendingFactType[utxoID]; !ok {
					pendingFactType[utxoID] = row.CarrierUtxoID
				}
			}
		}
		for _, row := range pendingRows {
			utxoID := strings.ToLower(strings.TrimSpace(row.UtxoID))
			assetKind, ok := pendingFactType[utxoID]
			if !ok || strings.TrimSpace(assetKind) == "" {
				continue
			}
			report.FactNoConfirmation = append(report.FactNoConfirmation, VerificationReconcileItem{
				UTXOID:       row.UtxoID,
				WalletID:     row.WalletID,
				Address:      row.Address,
				Status:       row.Status,
				AssetKind:    assetKind,
				ErrorMessage: row.ErrorMessage,
			})
		}
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		now := time.Now().Unix()
		affected, err := tx.WalletUtxoTokenVerification.Update().
			Where(walletutxotokenverification.UtxoIDEQ(utxoID)).
			SetStatus("pending").
			SetRetryCount(0).
			SetErrorMessage("").
			SetNextRetryAtUnix(now).
			SetUpdatedAtUnix(now).
			Save(ctx)
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
	if err := store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		now := time.Now().Unix()
		q := tx.WalletUtxoTokenVerification.Update().
			Where(
				walletutxotokenverification.StatusEQ("failed"),
				walletutxotokenverification.ErrorMessageNEQ(""),
			).
			SetStatus("pending").
			SetRetryCount(0).
			SetErrorMessage("").
			SetNextRetryAtUnix(now).
			SetUpdatedAtUnix(now)
		if walletID != "" {
			q = q.Where(walletutxotokenverification.WalletIDEQ(walletID))
		}
		if afterUnix > 0 {
			q = q.Where(walletutxotokenverification.UpdatedAtUnixGTE(afterUnix))
		}
		if beforeUnix > 0 {
			q = q.Where(walletutxotokenverification.UpdatedAtUnixLTE(beforeUnix))
		}
		affected, err := q.Save(ctx)
		if err != nil {
			return err
		}
		result = affected
		return nil
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

// ==================== 阈值告警 ====================

const (
	// verificationAlertFailedThreshold failed 数量超过此值时产生告警
	verificationAlertFailedThreshold = 10
)

// emitVerificationThresholdAlerts 阈值告警
// 设计说明：
// - 仅日志级别，不阻断业务流程
// - failed 超阈值、confirmed_without_fact 非 0 时产生 Warning/Error 日志

// failed 超阈值

// confirmed_without_fact 非 0
func emitVerificationThresholdAlerts(ctx context.Context, store *clientDB, trigger string) {
	if store == nil {
		return
	}
	summary, err := dbGetVerificationQueueSummary(ctx, store)
	if err != nil {
		obs.Error(ServiceName, "verification_threshold_alert", map[string]any{
			"error":   err.Error(),
			"trigger": trigger,
		})
		return
	}
	report, err := dbCheckVerificationReconciliation(ctx, store)
	if err != nil {
		obs.Error(ServiceName, "verification_threshold_alert", map[string]any{
			"error":   err.Error(),
			"trigger": trigger,
		})
		return
	}
	fields := map[string]any{
		"failed":                 summary.Failed,
		"confirmed_without_fact": report.Summary["confirmed_without_fact"],
		"trigger":                trigger,
	}
	if summary.Failed > verificationAlertFailedThreshold || report.Summary["confirmed_without_fact"] > 0 {
		obs.Error(ServiceName, "verification_threshold_alert", fields)
		return
	}
	obs.Info(ServiceName, "verification_threshold_alert", fields)
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
		obs.Error(ServiceName, "verification_audit_log", fields)
		return
	}
	obs.Info(ServiceName, "verification_audit_log", fields)
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
		obs.Error(ServiceName, "verification_queue_summary", map[string]any{
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

	obs.Info(ServiceName, "verification_queue_summary", map[string]any{
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
		obs.Error(ServiceName, "verification_reconcile_report", map[string]any{
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
		obs.Error(ServiceName, "verification_reconcile_report", map[string]any{
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
		obs.Info(ServiceName, "verification_reconcile_report", map[string]any{
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
