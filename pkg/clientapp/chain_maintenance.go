package clientapp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

const (
	chainTipWorkerInterval  = 60 * time.Second
	chainUTXOWorkerInterval = 10 * time.Second
	chainAPITimeout         = 30 * time.Second
	chainWorkerLogKeepCount = 1000
)

const (
	chainTaskTip  = "tip"
	chainTaskUTXO = "utxo"
)

type chainTask struct {
	TaskType      string
	TriggerSource string
	TriggeredAt   int64
}

type chainSchedulerStatus struct {
	StartedAtUnix     int64  `json:"started_at_unix"`
	QueueLength       int    `json:"queue_length"`
	InFlight          bool   `json:"in_flight"`
	InFlightTaskType  string `json:"in_flight_task_type"`
	TaskAppliedCount  uint64 `json:"task_applied_count"`
	TaskFailedCount   uint64 `json:"task_failed_count"`
	TaskSkippedCount  uint64 `json:"task_skipped_count"`
	LastTaskStartedAt int64  `json:"last_task_started_at_unix"`
	LastTaskEndedAt   int64  `json:"last_task_ended_at_unix"`
	LastError         string `json:"last_error"`
}

type chainMaintainer struct {
	rt    *Runtime
	store *clientDB
	queue chan chainTask

	mu             sync.Mutex
	pendingByType  map[string]bool
	inFlightByType map[string]bool
	status         chainSchedulerStatus
	wg             sync.WaitGroup
}

type chainTipState struct {
	TipHeight      uint32 `json:"tip_height"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
	LastError      string `json:"last_error"`
	LastUpdatedBy  string `json:"last_updated_by"`
	LastTrigger    string `json:"last_trigger"`
	LastDurationMS int64  `json:"last_duration_ms"`
}

type walletUTXOSyncState struct {
	WalletID                string `json:"wallet_id"`
	Address                 string `json:"address"`
	UTXOCount               int    `json:"utxo_count"`
	BalanceSatoshi          uint64 `json:"balance_satoshi"`
	PlainBSVUTXOCount       int    `json:"plain_bsv_utxo_count"`
	PlainBSVBalanceSatoshi  uint64 `json:"plain_bsv_balance_satoshi"`
	ProtectedUTXOCount      int    `json:"protected_utxo_count"`
	ProtectedBalanceSatoshi uint64 `json:"protected_balance_satoshi"`
	UnknownUTXOCount        int    `json:"unknown_utxo_count"`
	UnknownBalanceSatoshi   uint64 `json:"unknown_balance_satoshi"`
	UpdatedAtUnix           int64  `json:"updated_at_unix"`
	LastError               string `json:"last_error"`
	LastUpdatedBy           string `json:"last_updated_by"`
	LastTrigger             string `json:"last_trigger"`
	LastDurationMS          int64  `json:"last_duration_ms"`
	LastSyncRoundID         string `json:"last_sync_round_id"`
	LastFailedStep          string `json:"last_failed_step"`
	LastUpstreamPath        string `json:"last_upstream_path"`
	LastHTTPStatus          int    `json:"last_http_status"`
}

type walletUTXOHistoryCursor struct {
	WalletID            string `json:"wallet_id"`
	Address             string `json:"address"`
	NextConfirmedHeight int64  `json:"next_confirmed_height"`
	NextPageToken       string `json:"next_page_token"`
	AnchorHeight        int64  `json:"anchor_height"`
	RoundTipHeight      int64  `json:"round_tip_height"`
	UpdatedAtUnix       int64  `json:"updated_at_unix"`
	LastError           string `json:"last_error"`
}

type walletUTXOAggregateStats struct {
	UTXOCount               int
	BalanceSatoshi          uint64
	PlainBSVUTXOCount       int
	PlainBSVBalanceSatoshi  uint64
	ProtectedUTXOCount      int
	ProtectedBalanceSatoshi uint64
	UnknownUTXOCount        int
	UnknownBalanceSatoshi   uint64
}

type chainWorkerLogEntry struct {
	TriggeredAtUnix int64
	StartedAtUnix   int64
	EndedAtUnix     int64
	DurationMS      int64
	TriggerSource   string
	Status          string
	ErrorMessage    string
	Result          any
}

type walletSyncRoundMeta struct {
	RoundID        string
	Address        string
	TriggerSource  string
	APIBaseURL     string
	StartedAtUnix  int64
	StepStartedAt  time.Time
	WalletChainTyp string
}

type walletSyncError struct {
	RoundID      string
	Step         string
	UpstreamPath string
	HTTPStatus   int
	Cause        error
}

func (e *walletSyncError) Error() string {
	if e == nil || e.Cause == nil {
		return "wallet sync failed"
	}
	return e.Cause.Error()
}

func (e *walletSyncError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newChainMaintainer(rt *Runtime, store *clientDB) *chainMaintainer {
	if rt == nil {
		return nil
	}
	return &chainMaintainer{
		rt:            rt,
		store:         store,
		queue:         make(chan chainTask, 32),
		pendingByType: map[string]bool{},
		inFlightByType: map[string]bool{
			chainTaskTip:  false,
			chainTaskUTXO: false,
		},
		status: chainSchedulerStatus{
			StartedAtUnix: time.Now().Unix(),
		},
	}
}

func startChainMaintainer(ctx context.Context, rt *Runtime, store *clientDB) {
	if rt == nil {
		return
	}
	if err := resetChainMaintainerStartupState(store); err != nil {
		obs.Error("bitcast-client", "chain_maintainer_startup_reset_failed", map[string]any{"error": err.Error()})
	}
	cm := newChainMaintainer(rt, store)
	if cm == nil {
		return
	}
	rt.chainMaint = cm
	obs.Info("bitcast-client", "chain_maintainer_started", map[string]any{
		"runtime_started_at_unix": runtimeStartedAtUnix(rt),
	})
	cm.start(ctx)
}

func getChainMaintainer(rt *Runtime) *chainMaintainer {
	if rt == nil {
		return nil
	}
	return rt.chainMaint
}

func runtimeStartedAtUnix(rt *Runtime) int64 {
	if rt == nil {
		return 0
	}
	return rt.StartedAtUnix
}

func resetChainMaintainerStartupState(store *clientDB) error {
	if store == nil {
		return nil
	}
	return dbResetWalletUTXOSyncStateOnStartup(context.Background(), store)
}

func (m *chainMaintainer) start(ctx context.Context) {
	if m == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runScheduler(ctx)
	}()
	m.runTipWorker(ctx)
	m.runUTXOWorker(ctx)
	m.enqueue(chainTaskTip, "startup")
	m.enqueue(chainTaskUTXO, "startup")
}

func (m *chainMaintainer) Wait() {
	if m == nil {
		return
	}
	m.wg.Wait()
}

func (m *chainMaintainer) snapshotStatus() chainSchedulerStatus {
	if m == nil {
		return chainSchedulerStatus{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.status
	st.QueueLength = len(m.queue)
	return st
}

func (m *chainMaintainer) runTipWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt, m.store)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_tip_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainTipWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskTip, trigger)
			return map[string]any{"task_type": chainTaskTip, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_tip_task_register_failed", map[string]any{"error": err.Error()})
		return
	}
	obs.Info("bitcast-client", "chain_tip_task_registered", map[string]any{
		"interval_sec": int64(chainTipWorkerInterval / time.Second),
	})
}

func (m *chainMaintainer) runUTXOWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt, m.store)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_utxo_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainUTXOWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskUTXO, trigger)
			return map[string]any{"task_type": chainTaskUTXO, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_utxo_task_register_failed", map[string]any{"error": err.Error()})
		return
	}
	obs.Info("bitcast-client", "chain_utxo_task_registered", map[string]any{
		"interval_sec": int64(chainUTXOWorkerInterval / time.Second),
	})
}

func (m *chainMaintainer) enqueue(taskType string, triggerSource string) {
	if m == nil || m.store == nil {
		return
	}
	taskType = strings.TrimSpace(taskType)
	if taskType != chainTaskTip && taskType != chainTaskUTXO {
		return
	}
	now := time.Now().Unix()
	if strings.TrimSpace(triggerSource) == "" {
		triggerSource = "system"
	}
	m.mu.Lock()
	if m.pendingByType[taskType] || m.inFlightByType[taskType] {
		m.status.TaskSkippedCount++
		m.mu.Unlock()
		m.appendWorkerLog(taskType, chainWorkerLogEntry{
			TriggeredAtUnix: now,
			StartedAtUnix:   now,
			EndedAtUnix:     now,
			DurationMS:      0,
			TriggerSource:   triggerSource,
			Status:          "skipped_running",
			ErrorMessage:    "same task is pending or in flight",
			Result: map[string]any{
				"task_type": taskType,
			},
		})
		return
	}
	m.pendingByType[taskType] = true
	m.mu.Unlock()

	task := chainTask{TaskType: taskType, TriggerSource: triggerSource, TriggeredAt: now}
	select {
	case m.queue <- task:
		if triggerSource == "startup" {
			obs.Info("bitcast-client", "chain_task_startup_enqueued", map[string]any{
				"task_type":      taskType,
				"trigger_source": triggerSource,
			})
		}
	default:
		m.mu.Lock()
		m.pendingByType[taskType] = false
		m.status.TaskFailedCount++
		m.status.LastError = "chain scheduler queue full"
		m.mu.Unlock()
		m.appendWorkerLog(taskType, chainWorkerLogEntry{
			TriggeredAtUnix: now,
			StartedAtUnix:   now,
			EndedAtUnix:     now,
			DurationMS:      0,
			TriggerSource:   triggerSource,
			Status:          "failed",
			ErrorMessage:    "chain scheduler queue full",
			Result: map[string]any{
				"task_type": taskType,
			},
		})
	}
}

func (m *chainMaintainer) runScheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-m.queue:
			obs.Info("bitcast-client", "chain_task_dequeued", map[string]any{
				"task_type":               task.TaskType,
				"trigger_source":          task.TriggerSource,
				"triggered_at_unix":       task.TriggeredAt,
				"runtime_started_at_unix": runtimeStartedAtUnix(m.rt),
			})
			m.runTask(ctx, task)
		}
	}
}

func (m *chainMaintainer) runTask(ctx context.Context, task chainTask) {
	if m == nil || m.rt == nil {
		return
	}
	startedAt := time.Now()
	obs.Info("bitcast-client", "chain_task_started", map[string]any{
		"task_type":               task.TaskType,
		"trigger_source":          task.TriggerSource,
		"triggered_at_unix":       task.TriggeredAt,
		"task_started_at_unix":    startedAt.Unix(),
		"runtime_started_at_unix": runtimeStartedAtUnix(m.rt),
	})
	m.mu.Lock()
	m.pendingByType[task.TaskType] = false
	m.inFlightByType[task.TaskType] = true
	m.status.InFlight = true
	m.status.InFlightTaskType = task.TaskType
	m.status.LastTaskStartedAt = startedAt.Unix()
	m.mu.Unlock()

	defer func() {
		ended := time.Now().Unix()
		m.mu.Lock()
		m.inFlightByType[task.TaskType] = false
		m.status.InFlight = false
		m.status.InFlightTaskType = ""
		m.status.LastTaskEndedAt = ended
		m.mu.Unlock()
	}()

	taskCtx, cancel := context.WithTimeout(ctx, chainAPITimeout)
	defer cancel()

	var err error
	var result map[string]any
	if task.TaskType == chainTaskTip {
		result, err = m.executeTipTask(taskCtx, task)
	} else {
		result, err = m.executeUTXOTask(taskCtx, task)
	}

	endedAt := time.Now()
	entry := chainWorkerLogEntry{
		TriggeredAtUnix: task.TriggeredAt,
		StartedAtUnix:   startedAt.Unix(),
		EndedAtUnix:     endedAt.Unix(),
		DurationMS:      endedAt.Sub(startedAt).Milliseconds(),
		TriggerSource:   task.TriggerSource,
		Result:          result,
	}
	m.mu.Lock()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			entry.Status = "canceled"
			entry.ErrorMessage = err.Error()
			m.status.LastError = ""
		} else {
			m.status.TaskFailedCount++
			m.status.LastError = err.Error()
			entry.Status = "failed"
			entry.ErrorMessage = err.Error()
		}
	} else {
		m.status.TaskAppliedCount++
		entry.Status = "success"
		entry.ErrorMessage = ""
	}
	m.mu.Unlock()
	m.appendWorkerLog(task.TaskType, entry)
}

// runTaskSync 供显式“手动刷新一次”入口使用。
// 设计说明：
// - 手动刷新需要同步语义：HTTP 返回时，这一轮钱包同步已经完成或明确失败；
// - 这里不复用 enqueue，因为 enqueue 只有“排队成功”语义，无法回答“这次刷新有没有真的跑完”；
// - 若后台调度刚好已有同类任务在跑，这里等待其让出执行权，再串行补跑当前手动任务。
func (m *chainMaintainer) runTaskSync(ctx context.Context, task chainTask) (map[string]any, error) {
	if m == nil || m.rt == nil {
		return nil, fmt.Errorf("chain maintainer not initialized")
	}
	if task.TaskType != chainTaskTip && task.TaskType != chainTaskUTXO {
		return nil, fmt.Errorf("unsupported chain task type: %s", strings.TrimSpace(task.TaskType))
	}
	if strings.TrimSpace(task.TriggerSource) == "" {
		task.TriggerSource = "manual_sync"
	}
	if task.TriggeredAt <= 0 {
		task.TriggeredAt = time.Now().Unix()
	}
	startedAt := time.Now()
	for {
		m.mu.Lock()
		if !m.pendingByType[task.TaskType] && !m.inFlightByType[task.TaskType] {
			m.inFlightByType[task.TaskType] = true
			m.status.InFlight = true
			m.status.InFlightTaskType = task.TaskType
			m.status.LastTaskStartedAt = startedAt.Unix()
			m.mu.Unlock()
			break
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	obs.Info("bitcast-client", "chain_task_started", map[string]any{
		"task_type":               task.TaskType,
		"trigger_source":          task.TriggerSource,
		"triggered_at_unix":       task.TriggeredAt,
		"task_started_at_unix":    startedAt.Unix(),
		"runtime_started_at_unix": runtimeStartedAtUnix(m.rt),
	})
	defer func() {
		ended := time.Now().Unix()
		m.mu.Lock()
		m.inFlightByType[task.TaskType] = false
		m.status.InFlight = false
		m.status.InFlightTaskType = ""
		m.status.LastTaskEndedAt = ended
		m.mu.Unlock()
	}()

	taskCtx, cancel := context.WithTimeout(ctx, chainAPITimeout)
	defer cancel()

	var (
		err    error
		result map[string]any
	)
	if task.TaskType == chainTaskTip {
		result, err = m.executeTipTask(taskCtx, task)
	} else {
		result, err = m.executeUTXOTask(taskCtx, task)
	}

	endedAt := time.Now()
	entry := chainWorkerLogEntry{
		TriggeredAtUnix: task.TriggeredAt,
		StartedAtUnix:   startedAt.Unix(),
		EndedAtUnix:     endedAt.Unix(),
		DurationMS:      endedAt.Sub(startedAt).Milliseconds(),
		TriggerSource:   task.TriggerSource,
		Result:          result,
	}
	m.mu.Lock()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			entry.Status = "canceled"
			entry.ErrorMessage = err.Error()
			m.status.LastError = ""
		} else {
			m.status.TaskFailedCount++
			m.status.LastError = err.Error()
			entry.Status = "failed"
			entry.ErrorMessage = err.Error()
		}
	} else {
		m.status.TaskAppliedCount++
		entry.Status = "success"
		entry.ErrorMessage = ""
	}
	m.mu.Unlock()
	m.appendWorkerLog(task.TaskType, entry)
	return result, err
}

func (m *chainMaintainer) executeTipTask(ctx context.Context, task chainTask) (map[string]any, error) {
	if m.rt == nil || m.rt.WalletChain == nil {
		return map[string]any{"task_type": chainTaskTip}, fmt.Errorf("runtime wallet chain not initialized")
	}
	before, _ := dbLoadChainTipState(ctx, m.store)
	tip, err := m.rt.WalletChain.GetChainInfo(ctx)
	if err != nil {
		_ = dbUpdateChainTipStateError(context.Background(), m.store, err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskTip}, err
	}
	if err := dbUpsertChainTipState(ctx, m.store, tip, "", task.TriggerSource, time.Now().Unix(), 0); err != nil {
		return map[string]any{"task_type": chainTaskTip, "tip_height": tip}, err
	}
	emitted := false
	if before.TipHeight > 0 && tip > before.TipHeight {
		if err := scheduleWalletBSV21CreateAutoCheckAfterTipChange(ctx, m.store, m.rt, time.Now().Add(walletBSV21CreateAutoCheckDelay).Unix()); err != nil {
			obs.Error("bitcast-client", "wallet_bsv21_create_auto_check_schedule_failed", map[string]any{
				"error":        err.Error(),
				"tip_from":     before.TipHeight,
				"tip_to":       tip,
				"trigger":      strings.TrimSpace(task.TriggerSource),
				"task_type":    chainTaskTip,
				"runtime_type": fmt.Sprintf("%T", m.rt.WalletChain),
			})
		}
		if m.rt != nil && m.rt.orch != nil {
			m.rt.orch.EmitSignal(orchestratorSignal{
				Source:       "chain_tip_worker",
				Type:         orchestratorSignalChainTip,
				AggregateKey: "chain:tip",
				Payload: map[string]any{
					"tip_from": before.TipHeight,
					"tip_to":   tip,
				},
			})
			emitted = true
		}
	}
	return map[string]any{
		"task_type":   chainTaskTip,
		"tip_from":    before.TipHeight,
		"tip_to":      tip,
		"signal_emit": emitted,
	}, nil
}

func (m *chainMaintainer) executeUTXOTask(ctx context.Context, task chainTask) (map[string]any, error) {
	if m.rt == nil || m.rt.WalletChain == nil {
		return map[string]any{"task_type": chainTaskUTXO}, fmt.Errorf("runtime wallet chain not initialized")
	}
	obs.Info("bitcast-client", "chain_utxo_task_entered", map[string]any{
		"trigger_source":          strings.TrimSpace(task.TriggerSource),
		"triggered_at_unix":       task.TriggeredAt,
		"runtime_started_at_unix": runtimeStartedAtUnix(m.rt),
	})
	meta := walletSyncRoundMeta{
		RoundID:        createWalletSyncRoundID(),
		TriggerSource:  strings.TrimSpace(task.TriggerSource),
		StartedAtUnix:  time.Now().Unix(),
		APIBaseURL:     walletChainBaseURL(m.rt.WalletChain),
		WalletChainTyp: fmt.Sprintf("%T", m.rt.WalletChain),
	}
	chain, err := getWalletChainStateClient(m.rt)
	if err != nil {
		logWalletSyncStepError(meta, "resolve_wallet_chain", err, nil)
		return map[string]any{
			"task_type":         chainTaskUTXO,
			"sync_round_id":     meta.RoundID,
			"wallet_chain_type": meta.WalletChainTyp,
			"api_base_url":      meta.APIBaseURL,
		}, err
	}
	logWalletSyncStepInfo(meta, "resolve_wallet_chain", map[string]any{
		"wallet_chain_type": meta.WalletChainTyp,
		"api_base_url":      meta.APIBaseURL,
	})
	addr, err := clientWalletAddress(m.rt)
	if err != nil {
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, "", meta, wrapWalletSyncStepError(meta, "load_wallet_address", "", err), task.TriggerSource)
		logWalletSyncStepError(meta, "load_wallet_address", err, nil)
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"sync_round_id": meta.RoundID,
		}, err
	}
	meta.Address = addr
	logWalletSyncStepInfo(meta, "load_wallet_address", map[string]any{
		"address": addr,
	})
	startedAt := time.Now()
	stepStart := time.Now()
	tipPath := walletChainTipUpstreamPath()
	tip, err := m.rt.WalletChain.GetChainInfo(ctx)
	if err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "wallet_chain_get_tip", tipPath, err)
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOHistoryCursorError(context.Background(), m.store, addr, err.Error())
		logWalletSyncStepError(meta, "wallet_chain_get_tip", err, map[string]any{
			"upstream_path":    tipPath,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "wallet_chain_get_tip", map[string]any{
		"upstream_path":    tipPath,
		"tip_height":       tip,
		"step_duration_ms": time.Since(stepStart).Milliseconds(),
	})
	snapshot, err := collectCurrentWalletSnapshot(ctx, chain, addr, meta)
	if err != nil {
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, addr, meta, err, task.TriggerSource)
		_ = dbUpdateWalletUTXOHistoryCursorError(context.Background(), m.store, addr, err.Error())
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "collect_wallet_snapshot", map[string]any{
		"utxo_count":              snapshot.Count,
		"balance_satoshi":         snapshot.Balance,
		"observed_mempool_tx_cnt": len(snapshot.ObservedMempoolTxs),
		"confirmed_live_txid_cnt": len(snapshot.ConfirmedLiveTxIDs),
		"oldest_confirmed_height": snapshot.OldestConfirmedHeight,
	})
	stepStart = time.Now()
	cursor, err := dbLoadWalletUTXOHistoryCursor(ctx, m.store, addr)
	if err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "load_wallet_utxo_history_cursor", "", err)
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOHistoryCursorError(context.Background(), m.store, addr, err.Error())
		logWalletSyncStepError(meta, "load_wallet_utxo_history_cursor", err, nil)
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "load_wallet_utxo_history_cursor", map[string]any{
		"next_confirmed_height": cursor.NextConfirmedHeight,
		"next_page_token":       cursor.NextPageToken,
		"anchor_height":         cursor.AnchorHeight,
		"round_tip_height":      cursor.RoundTipHeight,
		"step_duration_ms":      time.Since(stepStart).Milliseconds(),
	})
	cursor.WalletID = walletIDByAddress(addr)
	cursor.Address = addr
	cursor = alignWalletUTXOHistoryCursor(cursor, snapshot.OldestConfirmedHeight, tip)
	stepStart = time.Now()
	history, nextCursor, err := collectConfirmedHistoryRange(ctx, chain, addr, cursor, tip, meta)
	if err != nil {
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, addr, meta, err, task.TriggerSource)
		_ = dbUpdateWalletUTXOHistoryCursorError(context.Background(), m.store, addr, err.Error())
		logWalletSyncStepError(meta, "collect_confirmed_history_range", err, map[string]any{
			"tip_height":           tip,
			"cursor_anchor_height": cursor.AnchorHeight,
			"cursor_next_height":   cursor.NextConfirmedHeight,
			"step_duration_ms":     time.Since(stepStart).Milliseconds(),
		})
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"tip":           tip,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "collect_confirmed_history_range", map[string]any{
		"history_tx_cnt":        len(history),
		"next_confirmed_height": nextCursor.NextConfirmedHeight,
		"next_page_token":       nextCursor.NextPageToken,
		"anchor_height":         nextCursor.AnchorHeight,
		"step_duration_ms":      time.Since(stepStart).Milliseconds(),
	})
	durationMS := time.Since(startedAt).Milliseconds()
	stepStart = time.Now()
	if err := reconcileWalletUTXOSet(ctx, m.store, addr, snapshot, history, nextCursor, meta.RoundID, "", task.TriggerSource, time.Now().Unix(), durationMS); err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "reconcile_wallet_utxo_set", "", err)
		_ = dbUpdateWalletUTXOSyncStateError(context.Background(), m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOHistoryCursorError(context.Background(), m.store, addr, err.Error())
		logWalletSyncStepError(meta, "reconcile_wallet_utxo_set", err, map[string]any{
			"utxo_count":        snapshot.Count,
			"history_tx_cnt":    len(history),
			"step_duration_ms":  time.Since(stepStart).Milliseconds(),
			"total_duration_ms": durationMS,
		})
		return map[string]any{
			"task_type":      chainTaskUTXO,
			"address":        addr,
			"utxo_count":     snapshot.Count,
			"history_tx_cnt": len(history),
			"sync_round_id":  meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "reconcile_wallet_utxo_set", map[string]any{
		"utxo_count":        snapshot.Count,
		"balance_satoshi":   snapshot.Balance,
		"history_tx_cnt":    len(history),
		"step_duration_ms":  time.Since(stepStart).Milliseconds(),
		"total_duration_ms": durationMS,
	})
	stepStart = time.Now()
	if err := refreshWalletAssetProjection(ctx, m.store, addr, task.TriggerSource); err != nil {
		logWalletSyncStepError(meta, "refresh_wallet_asset_projection", err, map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	} else {
		logWalletSyncStepInfo(meta, "refresh_wallet_asset_projection", map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	}
	stepStart = time.Now()
	if err := refreshDueWalletBSV21CreateStatuses(ctx, m.store, m.rt, task.TriggerSource); err != nil {
		logWalletSyncStepError(meta, "refresh_due_wallet_bsv21_create_statuses", err, map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	} else {
		logWalletSyncStepInfo(meta, "refresh_due_wallet_bsv21_create_statuses", map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	}

	// 触发 unknown 1-sat 资产确认流程
	// 设计说明：
	// - 在链同步成功后，异步确认 unknown 资产的类型
	// - 受并发保护，不会与正在执行的确认任务冲突
	// - 失败不影响主同步流程，仅记录日志
	stepStart = time.Now()
	if verifyErr := runUnknownAssetVerification(ctx, m.rt, m.store, addr, task.TriggerSource); verifyErr != nil {
		logWalletSyncStepError(meta, "unknown_asset_verification", verifyErr, map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	} else {
		logWalletSyncStepInfo(meta, "unknown_asset_verification", map[string]any{
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
	}

	select {
	case <-ctx.Done():
		return map[string]any{
			"task_type":       chainTaskUTXO,
			"address":         addr,
			"utxo_count":      snapshot.Count,
			"balance_satoshi": snapshot.Balance,
			"sync_round_id":   meta.RoundID,
		}, ctx.Err()
	default:
	}
	logWalletSyncStepInfo(meta, "wallet_sync_round_completed", map[string]any{
		"utxo_count":        snapshot.Count,
		"balance_satoshi":   snapshot.Balance,
		"history_tx_cnt":    len(history),
		"cursor_height":     nextCursor.NextConfirmedHeight,
		"anchor_height":     nextCursor.AnchorHeight,
		"tip_height":        tip,
		"total_duration_ms": durationMS,
	})
	return map[string]any{
		"task_type":         chainTaskUTXO,
		"address":           addr,
		"utxo_count":        snapshot.Count,
		"balance_satoshi":   snapshot.Balance,
		"history_tx_cnt":    len(history),
		"cursor_height":     nextCursor.NextConfirmedHeight,
		"anchor_height":     nextCursor.AnchorHeight,
		"tip_height":        tip,
		"sync_round_id":     meta.RoundID,
		"api_base_url":      meta.APIBaseURL,
		"wallet_chain_type": meta.WalletChainTyp,
	}, nil
}

func (m *chainMaintainer) appendWorkerLog(taskType string, entry chainWorkerLogEntry) {
	if m == nil || m.store == nil {
		return
	}
	if taskType == chainTaskTip {
		dbAppendChainTipWorkerLog(context.Background(), m.store, entry)
		return
	}
	dbAppendChainUTXOWorkerLog(context.Background(), m.store, entry)
}

func clientWalletAddress(rt *Runtime) (string, error) {
	if rt == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(actor.Addr), nil
}

type utxoStateRow struct {
	UTXOID           string
	TxID             string
	Vout             uint32
	Value            uint64
	State            string
	AllocationClass  string
	AllocationReason string
	CreatedTxID      string
	SpentTxID        string
	CreatedAtUnix    int64
}

func walletIDByAddress(address string) string {
	return "wallet:" + strings.ToLower(strings.TrimSpace(address))
}

type walletHistoryTxRecord struct {
	TxID   string
	Height int64
	Tx     whatsonchain.TxDetail
}

type liveWalletSnapshot struct {
	Live                  map[string]poolcore.UTXO
	ObservedMempoolTxs    []whatsonchain.TxDetail
	ConfirmedLiveTxIDs    map[string]struct{}
	Balance               uint64
	Count                 int
	OldestConfirmedHeight int64
}

type walletLocalBroadcastRow struct {
	TxID           string
	TxHex          string
	CreatedAtUnix  int64
	UpdatedAtUnix  int64
	ObservedAtUnix int64
}

func getWalletChainStateClient(rt *Runtime) (walletChainClient, error) {
	if rt == nil || rt.WalletChain == nil {
		return nil, fmt.Errorf("runtime wallet chain not initialized")
	}
	return rt.WalletChain, nil
}

func createWalletSyncRoundID() string {
	return fmt.Sprintf("wutxo-%d", time.Now().UnixNano())
}

func walletChainBaseURL(chain walletChainClient) string {
	if chain == nil {
		return ""
	}
	return strings.TrimSpace(chain.BaseURL())
}

func logWalletSyncStepInfo(meta walletSyncRoundMeta, step string, fields map[string]any) {
	logWalletSyncStep(obs.LevelInfo, meta, step, fields)
}

func logWalletSyncStepError(meta walletSyncRoundMeta, step string, err error, fields map[string]any) {
	payload := cloneLogFields(fields)
	if err != nil {
		payload["error"] = err.Error()
		if status := walletSyncHTTPStatus(err); status > 0 {
			payload["http_status"] = status
		}
	}
	if errors.Is(err, context.Canceled) {
		logWalletSyncStep(obs.LevelInfo, meta, step, payload)
		return
	}
	logWalletSyncStep(obs.LevelError, meta, step, payload)
}

func logWalletSyncStep(level string, meta walletSyncRoundMeta, step string, fields map[string]any) {
	payload := cloneLogFields(fields)
	payload["sync_round_id"] = strings.TrimSpace(meta.RoundID)
	payload["address"] = strings.TrimSpace(meta.Address)
	payload["trigger_source"] = strings.TrimSpace(meta.TriggerSource)
	payload["api_base_url"] = strings.TrimSpace(meta.APIBaseURL)
	payload["wallet_chain_type"] = strings.TrimSpace(meta.WalletChainTyp)
	payload["step"] = strings.TrimSpace(step)
	if payload["started_at_unix"] == nil && meta.StartedAtUnix > 0 {
		payload["started_at_unix"] = meta.StartedAtUnix
	}
	if level == obs.LevelError {
		obs.Error("bitcast-client", "wallet_utxo_sync_step", payload)
		return
	}
	obs.Info("bitcast-client", "wallet_utxo_sync_step", payload)
}

func cloneLogFields(fields map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range fields {
		out[key] = value
	}
	return out
}

func wrapWalletSyncStepError(meta walletSyncRoundMeta, step string, upstreamPath string, err error) error {
	if err == nil {
		return nil
	}
	var syncErr *walletSyncError
	if errors.As(err, &syncErr) && syncErr != nil {
		if strings.TrimSpace(syncErr.RoundID) == "" {
			syncErr.RoundID = strings.TrimSpace(meta.RoundID)
		}
		if strings.TrimSpace(syncErr.Step) == "" {
			syncErr.Step = strings.TrimSpace(step)
		}
		if strings.TrimSpace(syncErr.UpstreamPath) == "" {
			syncErr.UpstreamPath = strings.TrimSpace(upstreamPath)
		}
		if syncErr.HTTPStatus <= 0 {
			syncErr.HTTPStatus = walletSyncHTTPStatus(syncErr.Cause)
		}
		return syncErr
	}
	return &walletSyncError{
		RoundID:      strings.TrimSpace(meta.RoundID),
		Step:         strings.TrimSpace(step),
		UpstreamPath: strings.TrimSpace(upstreamPath),
		HTTPStatus:   walletSyncHTTPStatus(err),
		Cause:        err,
	}
}

func isWalletChainEmptyConfirmedHistoryPage(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *whatsonchain.HTTPError
	if !errors.As(err, &httpErr) || httpErr == nil || httpErr.HTTPStatus() != 404 {
		return false
	}
	body := strings.TrimSpace(httpErr.HTTPBody())
	return body == "" || strings.EqualFold(body, "not found")
}

func walletSyncFailureDetails(meta walletSyncRoundMeta, err error) (string, string, string, int) {
	roundID := strings.TrimSpace(meta.RoundID)
	failedStep := ""
	upstreamPath := ""
	httpStatus := walletSyncHTTPStatus(err)
	var syncErr *walletSyncError
	if errors.As(err, &syncErr) && syncErr != nil {
		if strings.TrimSpace(syncErr.RoundID) != "" {
			roundID = strings.TrimSpace(syncErr.RoundID)
		}
		failedStep = strings.TrimSpace(syncErr.Step)
		upstreamPath = strings.TrimSpace(syncErr.UpstreamPath)
		if syncErr.HTTPStatus > 0 {
			httpStatus = syncErr.HTTPStatus
		}
	}
	return roundID, failedStep, upstreamPath, httpStatus
}

func walletSyncHTTPStatus(err error) int {
	if err == nil {
		return 0
	}
	var status int
	if _, scanErr := fmt.Sscanf(strings.TrimSpace(err.Error()), "http %d:", &status); scanErr == nil && status > 0 {
		return status
	}
	return 0
}

func walletChainTipUpstreamPath() string {
	return "/chain/info"
}

func walletChainUTXOsUpstreamPath(address string) string {
	return fmt.Sprintf("/address/%s/confirmed/unspent", strings.TrimSpace(address))
}

func walletChainUnconfirmedHistoryUpstreamPath(address string) string {
	return fmt.Sprintf("/address/%s/unconfirmed/history", strings.TrimSpace(address))
}

func walletChainTxDetailUpstreamPath(txid string) string {
	return fmt.Sprintf("/tx/hash/%s", strings.TrimSpace(txid))
}

func walletChainConfirmedHistoryUpstreamPath(address string, q whatsonchain.ConfirmedHistoryQuery) string {
	values := url.Values{}
	if s := strings.TrimSpace(q.Order); s != "" {
		values.Set("order", s)
	}
	if q.Limit > 0 {
		values.Set("limit", fmt.Sprint(q.Limit))
	}
	if q.Height > 0 {
		values.Set("height", fmt.Sprint(q.Height))
	}
	if s := strings.TrimSpace(q.Token); s != "" {
		values.Set("token", s)
	}
	path := fmt.Sprintf("/address/%s/confirmed/history", strings.TrimSpace(address))
	if encoded := values.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}

func collectCurrentWalletSnapshot(ctx context.Context, chain walletChainClient, address string, meta walletSyncRoundMeta) (liveWalletSnapshot, error) {
	stepStart := time.Now()
	utxoPath := walletChainUTXOsUpstreamPath(address)
	confirmedUTXOs, err := chain.GetAddressConfirmedUnspent(ctx, address)
	if err != nil {
		logWalletSyncStepError(meta, "wallet_chain_get_utxos", err, map[string]any{
			"upstream_path":    utxoPath,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return liveWalletSnapshot{}, wrapWalletSyncStepError(meta, "wallet_chain_get_utxos", utxoPath, err)
	}
	logWalletSyncStepInfo(meta, "wallet_chain_get_utxos", map[string]any{
		"upstream_path":      utxoPath,
		"confirmed_utxo_cnt": len(confirmedUTXOs),
		"step_duration_ms":   time.Since(stepStart).Milliseconds(),
	})
	current := map[string]poolcore.UTXO{}
	confirmedLiveTxIDs := map[string]struct{}{}
	for _, u := range confirmedUTXOs {
		txid := strings.ToLower(strings.TrimSpace(u.TxID))
		utxoID := txid + ":" + fmt.Sprint(u.Vout)
		current[utxoID] = poolcore.UTXO{TxID: txid, Vout: u.Vout, Value: u.Value}
		confirmedLiveTxIDs[txid] = struct{}{}
	}
	stepStart = time.Now()
	unconfirmedPath := walletChainUnconfirmedHistoryUpstreamPath(address)
	unconfirmedTxIDs, err := chain.GetAddressUnconfirmedHistory(ctx, address)
	if err != nil {
		logWalletSyncStepError(meta, "wallet_chain_get_unconfirmed_history", err, map[string]any{
			"upstream_path":    unconfirmedPath,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return liveWalletSnapshot{}, wrapWalletSyncStepError(meta, "wallet_chain_get_unconfirmed_history", unconfirmedPath, err)
	}
	logWalletSyncStepInfo(meta, "wallet_chain_get_unconfirmed_history", map[string]any{
		"upstream_path":      unconfirmedPath,
		"unconfirmed_tx_cnt": len(unconfirmedTxIDs),
		"step_duration_ms":   time.Since(stepStart).Milliseconds(),
	})
	scriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	details, err := loadOrderedTxDetails(ctx, chain, unconfirmedTxIDs, meta)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	for _, detail := range details {
		txid := strings.ToLower(strings.TrimSpace(detail.TxID))
		for _, in := range detail.Vin {
			prevID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
			delete(current, prevID)
			delete(confirmedLiveTxIDs, strings.ToLower(strings.TrimSpace(in.TxID)))
		}
		for _, out := range detail.Vout {
			if !walletScriptHexMatchesAddressControl(out.ScriptPubKey.Hex, scriptHex) {
				continue
			}
			utxoID := txid + ":" + fmt.Sprint(out.N)
			current[utxoID] = poolcore.UTXO{
				TxID:  txid,
				Vout:  out.N,
				Value: txOutputValueSatoshi(out),
			}
		}
	}
	oldestConfirmedHeight, err := findOldestCurrentConfirmedHeight(ctx, chain, address, confirmedLiveTxIDs, meta)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	var balance uint64
	for _, u := range current {
		balance += u.Value
	}
	return liveWalletSnapshot{
		Live:                  current,
		ObservedMempoolTxs:    details,
		ConfirmedLiveTxIDs:    confirmedLiveTxIDs,
		Balance:               balance,
		Count:                 len(current),
		OldestConfirmedHeight: oldestConfirmedHeight,
	}, nil
}

func loadOrderedTxDetails(ctx context.Context, chain walletChainClient, txids []string, meta walletSyncRoundMeta) ([]whatsonchain.TxDetail, error) {
	unique := map[string]whatsonchain.TxDetail{}
	for _, txid := range txids {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			continue
		}
		if _, ok := unique[txid]; ok {
			continue
		}
		stepStart := time.Now()
		txPath := walletChainTxDetailUpstreamPath(txid)
		detail, err := chain.GetTxHash(ctx, txid)
		if err != nil {
			logWalletSyncStepError(meta, "wallet_chain_get_tx_detail", err, map[string]any{
				"upstream_path":    txPath,
				"txid":             txid,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return nil, wrapWalletSyncStepError(meta, "wallet_chain_get_tx_detail", txPath, err)
		}
		logWalletSyncStepInfo(meta, "wallet_chain_get_tx_detail", map[string]any{
			"upstream_path":    txPath,
			"txid":             txid,
			"vin_cnt":          len(detail.Vin),
			"vout_cnt":         len(detail.Vout),
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		detail.TxID = txid
		unique[txid] = detail
	}
	pending := map[string]whatsonchain.TxDetail{}
	for txid, detail := range unique {
		pending[txid] = detail
	}
	out := make([]whatsonchain.TxDetail, 0, len(unique))
	for len(pending) > 0 {
		progressed := false
		for txid, detail := range pending {
			ready := true
			for _, in := range detail.Vin {
				prevTxID := strings.ToLower(strings.TrimSpace(in.TxID))
				if prevTxID == "" || prevTxID == txid {
					continue
				}
				if _, waiting := pending[prevTxID]; waiting {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			out = append(out, detail)
			delete(pending, txid)
			progressed = true
		}
		if progressed {
			continue
		}
		for txid, detail := range pending {
			out = append(out, detail)
			delete(pending, txid)
		}
	}
	return out, nil
}

func findOldestCurrentConfirmedHeight(ctx context.Context, chain walletChainClient, address string, txids map[string]struct{}, meta walletSyncRoundMeta) (int64, error) {
	if len(txids) == 0 {
		logWalletSyncStepInfo(meta, "wallet_chain_find_oldest_confirmed_height", map[string]any{
			"confirmed_live_txid_cnt": 0,
			"oldest_confirmed_height": 0,
			"page_cnt":                0,
		})
		return 0, nil
	}
	remaining := map[string]struct{}{}
	for txid := range txids {
		remaining[txid] = struct{}{}
	}
	oldest := int64(0)
	token := ""
	pageCount := 0
	stepStart := time.Now()
	for {
		pagePath := walletChainConfirmedHistoryUpstreamPath(address, whatsonchain.ConfirmedHistoryQuery{
			Order: "desc",
			Limit: 1000,
			Token: token,
		})
		page, err := chain.GetAddressConfirmedHistoryPage(ctx, address, whatsonchain.ConfirmedHistoryQuery{
			Order: "desc",
			Limit: 1000,
			Token: token,
		})
		if err != nil {
			logWalletSyncStepError(meta, "wallet_chain_get_confirmed_history_desc", err, map[string]any{
				"upstream_path":    pagePath,
				"page_cnt":         pageCount,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return 0, wrapWalletSyncStepError(meta, "wallet_chain_get_confirmed_history_desc", pagePath, err)
		}
		pageCount++
		for _, item := range page.Items {
			txid := strings.ToLower(strings.TrimSpace(item.TxID))
			if _, ok := remaining[txid]; !ok {
				continue
			}
			delete(remaining, txid)
			if oldest == 0 || item.Height < oldest {
				oldest = item.Height
			}
		}
		if len(remaining) == 0 || strings.TrimSpace(page.NextPageToken) == "" {
			logWalletSyncStepInfo(meta, "wallet_chain_find_oldest_confirmed_height", map[string]any{
				"confirmed_live_txid_cnt": len(txids),
				"oldest_confirmed_height": oldest,
				"page_cnt":                pageCount,
				"step_duration_ms":        time.Since(stepStart).Milliseconds(),
			})
			return oldest, nil
		}
		token = strings.TrimSpace(page.NextPageToken)
	}
}

func collectConfirmedHistoryRange(ctx context.Context, chain walletChainClient, address string, cursor walletUTXOHistoryCursor, tip uint32, meta walletSyncRoundMeta) ([]walletHistoryTxRecord, walletUTXOHistoryCursor, error) {
	next := cursor
	next.RoundTipHeight = int64(tip)
	next.UpdatedAtUnix = time.Now().Unix()
	if next.NextConfirmedHeight <= 0 {
		if next.AnchorHeight > 0 {
			next.NextConfirmedHeight = next.AnchorHeight
		} else {
			next.NextConfirmedHeight = int64(tip) + 1
		}
	}
	if next.NextConfirmedHeight > int64(tip) && strings.TrimSpace(next.NextPageToken) == "" {
		next.LastError = ""
		return nil, next, nil
	}
	out := make([]walletHistoryTxRecord, 0)
	seen := map[string]struct{}{}
	startHeight := next.NextConfirmedHeight
	pageToken := strings.TrimSpace(next.NextPageToken)
	pageCount := 0
	stepStart := time.Now()
	for {
		pagePath := walletChainConfirmedHistoryUpstreamPath(address, whatsonchain.ConfirmedHistoryQuery{
			Order:  "asc",
			Limit:  1000,
			Height: startHeight,
			Token:  pageToken,
		})
		page, err := chain.GetAddressConfirmedHistoryPage(ctx, address, whatsonchain.ConfirmedHistoryQuery{
			Order:  "asc",
			Limit:  1000,
			Height: startHeight,
			Token:  pageToken,
		})
		if err != nil {
			// 设计说明：
			// - WOC 对“从某个高度开始已经没有 confirmed history”会返回 404 Not Found；
			// - 对钱包同步来说，这表示“历史范围已经到头”，不是致命失败；
			// - 这里把它收敛成空页，避免已同步到 DB 的余额被 last_error 毒化成 0。
			if isWalletChainEmptyConfirmedHistoryPage(err) {
				next.NextConfirmedHeight = int64(tip) + 1
				next.NextPageToken = ""
				next.LastError = ""
				logWalletSyncStepInfo(meta, "wallet_chain_collect_confirmed_history", map[string]any{
					"history_tx_cnt":        len(out),
					"start_height":          startHeight,
					"next_confirmed_height": next.NextConfirmedHeight,
					"anchor_height":         next.AnchorHeight,
					"page_cnt":              pageCount,
					"empty_range_http_404":  true,
					"step_duration_ms":      time.Since(stepStart).Milliseconds(),
				})
				return out, next, nil
			}
			logWalletSyncStepError(meta, "wallet_chain_get_confirmed_history_asc", err, map[string]any{
				"upstream_path":    pagePath,
				"start_height":     startHeight,
				"page_cnt":         pageCount,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return nil, next, wrapWalletSyncStepError(meta, "wallet_chain_get_confirmed_history_asc", pagePath, err)
		}
		pageCount++
		for _, item := range page.Items {
			txid := strings.ToLower(strings.TrimSpace(item.TxID))
			if txid == "" {
				continue
			}
			if _, ok := seen[txid]; ok {
				continue
			}
			seen[txid] = struct{}{}
			txStepStart := time.Now()
			txPath := walletChainTxDetailUpstreamPath(txid)
			detail, err := chain.GetTxHash(ctx, txid)
			if err != nil {
				logWalletSyncStepError(meta, "wallet_chain_get_tx_detail_for_history", err, map[string]any{
					"upstream_path":    txPath,
					"txid":             txid,
					"step_duration_ms": time.Since(txStepStart).Milliseconds(),
				})
				return nil, next, wrapWalletSyncStepError(meta, "wallet_chain_get_tx_detail_for_history", txPath, err)
			}
			logWalletSyncStepInfo(meta, "wallet_chain_get_tx_detail_for_history", map[string]any{
				"upstream_path":    txPath,
				"txid":             txid,
				"height":           item.Height,
				"vin_cnt":          len(detail.Vin),
				"vout_cnt":         len(detail.Vout),
				"step_duration_ms": time.Since(txStepStart).Milliseconds(),
			})
			out = append(out, walletHistoryTxRecord{
				TxID:   txid,
				Height: item.Height,
				Tx:     detail,
			})
		}
		if token := strings.TrimSpace(page.NextPageToken); token != "" {
			pageToken = token
			next.NextConfirmedHeight = startHeight
			next.NextPageToken = token
			continue
		}
		next.NextConfirmedHeight = int64(tip) + 1
		next.NextPageToken = ""
		next.LastError = ""
		logWalletSyncStepInfo(meta, "wallet_chain_collect_confirmed_history", map[string]any{
			"history_tx_cnt":        len(out),
			"start_height":          startHeight,
			"next_confirmed_height": next.NextConfirmedHeight,
			"anchor_height":         next.AnchorHeight,
			"page_cnt":              pageCount,
			"step_duration_ms":      time.Since(stepStart).Milliseconds(),
		})
		return out, next, nil
	}
}

func walletAddressLockScriptHex(address string) (string, error) {
	addr, err := script.NewAddressFromString(strings.TrimSpace(address))
	if err != nil {
		return "", fmt.Errorf("parse wallet address failed: %w", err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		return "", fmt.Errorf("build wallet lock script failed: %w", err)
	}
	return strings.ToLower(hex.EncodeToString(lock.Bytes())), nil
}

// walletScriptHexMatchesAddressControl 判断一个输出脚本是否仍然受当前钱包地址控制。
// 设计说明：
// - 现在钱包不能只认“纯 p2pkh 输出”，因为 1sat token / ordinal 的承载输出通常是“协议前缀 + 钱包 p2pkh 后缀”；
// - 对当前底座来说，我们关心的是“这个输出最终是不是仍然锁给本钱包”，而不是它前面叠了什么协议壳；
// - 因此这里接受两类脚本：
//  1. 纯钱包 p2pkh；
//  2. 以钱包 p2pkh 作为后缀的组合脚本；
//
// - 这样本地投影、链同步、pending local broadcast 叠加三条链才能对 token change 使用同一判断口径。
func walletScriptHexMatchesAddressControl(outputScriptHex string, walletScriptHex string) bool {
	outputScriptHex = strings.TrimSpace(strings.ToLower(outputScriptHex))
	walletScriptHex = strings.TrimSpace(strings.ToLower(walletScriptHex))
	if outputScriptHex == "" || walletScriptHex == "" {
		return false
	}
	if outputScriptHex == walletScriptHex {
		return true
	}
	return strings.HasSuffix(outputScriptHex, walletScriptHex)
}

func buildWalletChainAccountingInputFromTxDetail(db sqlConn, address string, detail whatsonchain.TxDetail) (walletChainAccountingInput, bool, error) {
	txid := strings.ToLower(strings.TrimSpace(detail.TxID))
	if txid == "" {
		return walletChainAccountingInput{}, false, nil
	}
	scriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		return walletChainAccountingInput{}, false, err
	}

	inputFacts := make([]chainPaymentUTXOFact, 0, len(detail.Vin))
	outputFacts := make([]chainPaymentUTXOFact, 0, len(detail.Vout))
	var walletInputSat int64
	var walletOutputSat int64
	hasWalletInput := false
	hasWalletOutput := false

	for _, in := range detail.Vin {
		utxoID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
		amount, ok, err := dbWalletUTXOValueConn(db, utxoID)
		if err != nil {
			return walletChainAccountingInput{}, false, err
		}
		if !ok {
			continue
		}
		hasWalletInput = true
		walletInputSat += amount
		inputFacts = append(inputFacts, chainPaymentUTXOFact{
			UTXOID:        utxoID,
			IOSide:        "input",
			UTXORole:      "wallet_input",
			AmountSatoshi: amount,
			Note:          "wallet input by chain sync",
		})
	}

	for _, out := range detail.Vout {
		if !walletScriptHexMatchesAddressControl(out.ScriptPubKey.Hex, scriptHex) {
			continue
		}
		amount := int64(txOutputValueSatoshi(out))
		if amount <= 0 {
			continue
		}
		hasWalletOutput = true
		walletOutputSat += amount
		role := "external_in"
		if hasWalletInput {
			role = "wallet_change"
		}
		outputFacts = append(outputFacts, chainPaymentUTXOFact{
			UTXOID:        txid + ":" + fmt.Sprint(out.N),
			IOSide:        "output",
			UTXORole:      role,
			AmountSatoshi: amount,
			Note:          "wallet output by chain sync",
		})
	}

	if !hasWalletInput && !hasWalletOutput {
		return walletChainAccountingInput{}, false, nil
	}

	category := "REPAYMENT"
	switch {
	case hasWalletInput && hasWalletOutput:
		category = "CHANGE"
	case hasWalletInput && !hasWalletOutput:
		category = "THIRD_PARTY"
	}

	return walletChainAccountingInput{
		TxID:            txid,
		Category:        category,
		WalletInputSat:  walletInputSat,
		WalletOutputSat: walletOutputSat,
		NetSat:          walletOutputSat - walletInputSat,
		Payload: map[string]any{
			"txid":           txid,
			"wallet_address": strings.TrimSpace(address),
			"source":         "wallet_chain_sync",
		},
		UTXOFacts: append(inputFacts, outputFacts...),
	}, true, nil
}

func matchWalletOutput(txid string, out whatsonchain.TxOutput, scriptHex string) (string, uint64, bool) {
	if strings.TrimSpace(txid) == "" {
		return "", 0, false
	}
	if !walletScriptHexMatchesAddressControl(out.ScriptPubKey.Hex, scriptHex) {
		return "", 0, false
	}
	return strings.ToLower(strings.TrimSpace(txid)) + ":" + fmt.Sprint(out.N), txOutputValueSatoshi(out), true
}

func txOutputValueSatoshi(out whatsonchain.TxOutput) uint64 {
	if out.ValueSatoshi > 0 {
		return out.ValueSatoshi
	}
	return satoshiFromTxOutputValue(out.Value)
}

func satoshiFromTxOutputValue(v float64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v*100000000 + 0.5)
}

// alignWalletUTXOHistoryCursor 把历史游标和“当前仍未花费集合”的最老确认高度重新对齐。
// 设计说明：
// - 钱包开始处理 1sat 后，当前未花费集合不再只是 plain BSV；
// - 如果当前仍未花费集合里出现了更老的输出，必须把同步锚点回退过去重扫；
// - 否则后续资产识别可能拿不到那段历史，导致 UTXO 有了但资产语义丢了。
func alignWalletUTXOHistoryCursor(cursor walletUTXOHistoryCursor, oldestCurrentConfirmedHeight int64, tip uint32) walletUTXOHistoryCursor {
	next := cursor
	if oldestCurrentConfirmedHeight > 0 && (next.AnchorHeight <= 0 || oldestCurrentConfirmedHeight < next.AnchorHeight) {
		next.AnchorHeight = oldestCurrentConfirmedHeight
		next.NextConfirmedHeight = oldestCurrentConfirmedHeight
		next.NextPageToken = ""
	}
	if next.AnchorHeight <= 0 {
		next.AnchorHeight = oldestCurrentConfirmedHeight
	}
	if next.NextConfirmedHeight <= 0 {
		if next.AnchorHeight > 0 {
			next.NextConfirmedHeight = next.AnchorHeight
		} else {
			next.NextConfirmedHeight = int64(tip) + 1
		}
	}
	return next
}

func getTipHeightFromDB(ctx context.Context, store *clientDB) (uint32, error) {
	if store == nil {
		return 0, fmt.Errorf("store not initialized")
	}
	s, err := dbLoadChainTipState(ctx, store)
	if err != nil {
		return 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return 0, fmt.Errorf("chain tip state not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return 0, fmt.Errorf("chain tip state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	return s.TipHeight, nil
}

func isWalletUTXOSyncStateStaleForRuntime(rt *Runtime, s walletUTXOSyncState) bool {
	startedAtUnix := runtimeStartedAtUnix(rt)
	if startedAtUnix <= 0 {
		return false
	}
	return s.UpdatedAtUnix > 0 && s.UpdatedAtUnix < startedAtUnix
}

func getWalletUTXOsFromDB(ctx context.Context, store *clientDB, rt *Runtime) ([]poolcore.UTXO, error) {
	return listEligiblePlainBSVWalletUTXOsFact(ctx, store, rt)
}

// listEligiblePlainBSVWalletUTXOsFact 从 fact 口径获取可花费的 plain_bsv UTXO
// 设计说明：
// - 按 spendable source flow 返回，剩余 > 0 的才返回
// - 返回的是原始金额，实际使用由上层决定
func listEligiblePlainBSVWalletUTXOsFact(ctx context.Context, store *clientDB, rt *Runtime) ([]poolcore.UTXO, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	walletID := walletIDByAddress(addr)
	if store == nil {
		return nil, fmt.Errorf("store not initialized")
	}

	flows, err := dbListSpendableSourceFlows(ctx, store, walletID, "BSV", "")
	if err != nil {
		return nil, err
	}

	out := make([]poolcore.UTXO, 0, len(flows))
	for _, f := range flows {
		out = append(out, poolcore.UTXO{
			TxID:  strings.ToLower(strings.TrimSpace(f.TxID)),
			Vout:  f.Vout,
			Value: uint64(f.Remaining),
		})
	}
	return out, nil
}

func getWalletBalanceFromDB(ctx context.Context, store *clientDB, rt *Runtime) (string, uint64, error) {
	if store == nil {
		return "", 0, fmt.Errorf("store not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return "", 0, err
	}
	walletID := walletIDByAddress(addr)

	// Step 5：fact 优先读余额（严格口径：查询成功就返回，含 0 值）
	bal, err := dbLoadWalletAssetBalanceFact(ctx, store, walletID, "BSV", "")
	if err == nil {
		return addr, uint64(bal.Remaining), nil
	}

	// fact 查询失败时回退 wallet_utxo_sync_state
	s, err := dbLoadWalletUTXOSyncState(ctx, store, addr)
	if err != nil {
		return "", 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return "", 0, fmt.Errorf("wallet utxo sync state not ready")
	}
	if isWalletUTXOSyncStateStaleForRuntime(rt, s) {
		return "", 0, fmt.Errorf("wallet utxo sync state stale for current runtime")
	}
	stats, err := dbLoadWalletUTXOAggregate(ctx, store, addr)
	if err != nil {
		return "", 0, err
	}
	if strings.TrimSpace(s.LastError) != "" && stats.PlainBSVBalanceSatoshi == 0 {
		return "", 0, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	// 设计说明：
	// - wallet_utxo 表是链上未花费输出的本地投影；
	// - 只要这张表里已经有余额，就不应该因为后续某个 history 游标请求失败而把余额清零；
	// - 同步错误仍然保留在 wallet_utxo_sync_last_error 等诊断字段里，前端可以继续展示告警。
	return addr, stats.PlainBSVBalanceSatoshi, nil
}
