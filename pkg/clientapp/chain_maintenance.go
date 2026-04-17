package clientapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	crypto "github.com/bsv-blockchain/go-sdk/primitives/hash"
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

type walletUTXOSyncCursor struct {
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

// walletChainScriptUTXOClient 是可选能力：
// 当底层链客户端支持脚本哈希查询时，钱包同步可补充 p2pk 可见性。
type walletChainScriptUTXOClient interface {
	GetScriptConfirmedUnspent(ctx context.Context, scriptHash string) ([]whatsonchain.UTXO, error)
	GetScriptSpendableUnspent(ctx context.Context, scriptHash string) ([]whatsonchain.UTXO, error)
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
	if err := resetChainMaintainerStartupState(ctx, store); err != nil {
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

func resetChainMaintainerStartupState(ctx context.Context, store *clientDB) error {
	if store == nil {
		return nil
	}
	return dbResetWalletUTXOSyncStateOnStartup(ctx, store)
}

func (m *chainMaintainer) start(ctx context.Context) {
	if m == nil {
		return
	}
	if ctx == nil {
		return
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
		m.appendWorkerLog(runtimeLogContext(m.rt), taskType, chainWorkerLogEntry{
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
		m.appendWorkerLog(runtimeLogContext(m.rt), taskType, chainWorkerLogEntry{
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
	m.appendWorkerLog(context.WithoutCancel(ctx), task.TaskType, entry)
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
	m.appendWorkerLog(context.WithoutCancel(ctx), task.TaskType, entry)
	return result, err
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
	ctx = sqlTraceContextWithMeta(ctx, meta.RoundID, task.TriggerSource, "chain_utxo_round", "chain_maintenance.executeUTXOTask")
	defer flushSQLTraceRoundSummary(meta.RoundID)
	chain, err := getWalletChainStateClient(m.rt)
	if err != nil {
		logWalletSyncStepError(meta, "resolve_wallet_chain", err, nil)
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"sync_round_id": meta.RoundID,
			"chain_type":    meta.WalletChainTyp,
			"api_base_url":  meta.APIBaseURL,
		}, err
	}
	logWalletSyncStepInfo(meta, "resolve_wallet_chain", map[string]any{
		"chain_type":   meta.WalletChainTyp,
		"api_base_url": meta.APIBaseURL,
	})
	addr, err := clientWalletAddress(m.rt)
	if err != nil {
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, "", meta, wrapWalletSyncStepError(meta, "load_wallet_address", "", err), task.TriggerSource)
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
	p2pkScriptHashes, hashErr := walletP2PKScriptHashCandidates(m.rt.ClientID())
	if hashErr != nil {
		logWalletSyncStepError(meta, "build_wallet_p2pk_script_hash", hashErr, nil)
	}
	p2pkhScriptHashes, p2pkhErr := walletP2PKHScriptHashCandidates(addr)
	if p2pkhErr != nil {
		logWalletSyncStepError(meta, "build_wallet_p2pkh_script_hash", p2pkhErr, nil)
	}
	scriptHashes := mergeWalletScriptHashCandidates(p2pkScriptHashes, p2pkhScriptHashes)
	logWalletSyncStepInfo(meta, "build_wallet_script_hashes", map[string]any{
		"p2pk_candidate_count":  len(p2pkScriptHashes),
		"p2pkh_candidate_count": len(p2pkhScriptHashes),
		"candidate_count":       len(scriptHashes),
	})
	if len(scriptHashes) == 0 {
		err = fmt.Errorf("wallet script hash candidates are empty")
		wrappedErr := wrapWalletSyncStepError(meta, "build_wallet_script_hashes", "", err)
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	startedAt := time.Now()
	stepStart := time.Now()
	tipPath := walletChainTipUpstreamPath()
	tip, err := m.rt.WalletChain.GetChainInfo(ctx)
	if err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "chain_get_tip", tipPath, err)
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
		logWalletSyncStepError(meta, "chain_get_tip", err, map[string]any{
			"upstream_path":    tipPath,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "chain_get_tip", map[string]any{
		"upstream_path":    tipPath,
		"tip_height":       tip,
		"step_duration_ms": time.Since(stepStart).Milliseconds(),
	})
	snapshot, err := collectCurrentWalletSnapshot(ctx, chain, addr, scriptHashes, meta)
	if err != nil {
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, err, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
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
	bsv21Path := fmt.Sprintf("/token/bsv21/%s/unspent", strings.TrimSpace(addr))
	bsv21TokenUTXOs, err := chain.GetAddressBSV21TokenUnspent(ctx, addr)
	if err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "chain_get_bsv21_unspent", bsv21Path, err)
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
		logWalletSyncStepError(meta, "chain_get_bsv21_unspent", err, map[string]any{
			"upstream_path":    bsv21Path,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "chain_get_bsv21_unspent", map[string]any{
		"upstream_path":     bsv21Path,
		"bsv21_unspent_cnt": len(bsv21TokenUTXOs),
		"step_duration_ms":  time.Since(stepStart).Milliseconds(),
	})
	stepStart = time.Now()
	bsv21Upserted, bsv21Cleared, err := syncWalletBSV21HoldingsFromUnspent(ctx, m.store, addr, bsv21TokenUTXOs, time.Now().Unix())
	if err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "sync_bsv21_holdings", bsv21Path, err)
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
		logWalletSyncStepError(meta, "sync_bsv21_holdings", err, map[string]any{
			"upstream_path":    bsv21Path,
			"step_duration_ms": time.Since(stepStart).Milliseconds(),
		})
		return map[string]any{
			"task_type":     chainTaskUTXO,
			"address":       addr,
			"sync_round_id": meta.RoundID,
		}, err
	}
	logWalletSyncStepInfo(meta, "sync_bsv21_holdings", map[string]any{
		"upstream_path":    bsv21Path,
		"upserted_count":   bsv21Upserted,
		"cleared_count":    bsv21Cleared,
		"step_duration_ms": time.Since(stepStart).Milliseconds(),
	})
	nextCursor := walletUTXOSyncCursor{
		WalletID:       walletIDByAddress(addr),
		Address:        addr,
		RoundTipHeight: int64(tip),
		LastError:      "",
		UpdatedAtUnix:  time.Now().Unix(),
	}
	history := make([]walletHistoryTxRecord, 0)
	durationMS := time.Since(startedAt).Milliseconds()
	stepStart = time.Now()
	// 编排层：先同步钱包状态，再写入 fact
	// 设计说明：
	// - 两阶段错误处理：同步失败直接返回，fact 失败可重试
	// - fact 写入是幂等的，失败后下次同步会重新尝试
	if err := SyncWalletAndApplyFacts(ctx, m.store, addr, snapshot, history, nextCursor, newRuntimeWalletScriptEvidenceSource(m.rt), meta.RoundID, "", task.TriggerSource, time.Now().Unix(), durationMS); err != nil {
		wrappedErr := wrapWalletSyncStepError(meta, "sync_wallet_and_apply_facts", "", err)
		_ = dbUpdateWalletUTXOSyncStateError(ctx, m.store, addr, meta, wrappedErr, task.TriggerSource)
		_ = dbUpdateWalletUTXOSyncCursorError(ctx, m.store, addr, err.Error())
		logWalletSyncStepError(meta, "sync_wallet_and_apply_facts", err, map[string]any{
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
	logWalletSyncStepInfo(meta, "sync_wallet_and_apply_facts", map[string]any{
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
	// unknown 资产不再自动进入确认流程；这里只保留统计与日志。

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
		"task_type":       chainTaskUTXO,
		"address":         addr,
		"utxo_count":      snapshot.Count,
		"balance_satoshi": snapshot.Balance,
		"history_tx_cnt":  len(history),
		"cursor_height":   nextCursor.NextConfirmedHeight,
		"anchor_height":   nextCursor.AnchorHeight,
		"tip_height":      tip,
		"sync_round_id":   meta.RoundID,
		"api_base_url":    meta.APIBaseURL,
		"chain_type":      meta.WalletChainTyp,
	}, nil
}

func clientWalletAddress(rt *Runtime) (string, error) {
	if rt == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return "", err
	}
	if identity.Actor == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	return strings.TrimSpace(identity.Actor.Addr), nil
}

type utxoStateRow struct {
	UTXOID                  string
	TxID                    string
	Vout                    uint32
	Value                   uint64
	State                   string
	ScriptType              string
	ScriptTypeReason        string
	ScriptTypeUpdatedAtUnix int64
	AllocationClass         string
	AllocationReason        string
	CreatedTxID             string
	SpentTxID               string
	CreatedAtUnix           int64
	SpentAtUnix             int64
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
	payload["chain_type"] = strings.TrimSpace(meta.WalletChainTyp)
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

func walletChainScriptUnspentAllUpstreamPath(scriptHash string) string {
	return fmt.Sprintf("/script/%s/unspent/all", strings.TrimSpace(scriptHash))
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

func collectCurrentWalletSnapshot(ctx context.Context, chain walletChainClient, address string, scriptHashes []string, meta walletSyncRoundMeta) (liveWalletSnapshot, error) {
	stepStart := time.Now()
	spendable, err := collectScriptSpendableUTXOs(ctx, chain, scriptHashes)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	logWalletSyncStepInfo(meta, "chain_get_script_unspent_all", map[string]any{
		"script_hash_count":  len(scriptHashes),
		"spendable_utxo_cnt": len(spendable),
		"step_duration_ms":   time.Since(stepStart).Milliseconds(),
	})
	current := map[string]poolcore.UTXO{}
	liveTxIDs := map[string]struct{}{}
	for _, u := range spendable {
		txid := strings.ToLower(strings.TrimSpace(u.TxID))
		if txid == "" {
			continue
		}
		utxoID := txid + ":" + fmt.Sprint(u.Vout)
		current[utxoID] = poolcore.UTXO{
			TxID:  txid,
			Vout:  u.Vout,
			Value: u.Value,
		}
		liveTxIDs[txid] = struct{}{}
	}
	var balance uint64
	for _, u := range current {
		balance += u.Value
	}
	return liveWalletSnapshot{
		Live:                  current,
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    liveTxIDs,
		Balance:               balance,
		Count:                 len(current),
		OldestConfirmedHeight: 0,
	}, nil
}

func collectScriptSpendableUTXOs(ctx context.Context, chain walletChainClient, scriptHashes []string) ([]whatsonchain.UTXO, error) {
	scriptClient, ok := chain.(walletChainScriptUTXOClient)
	if !ok {
		return nil, fmt.Errorf("wallet chain missing script spendable capability")
	}
	if len(scriptHashes) == 0 {
		return nil, nil
	}
	spendableByID := map[string]whatsonchain.UTXO{}
	for _, hash := range scriptHashes {
		hash = strings.ToLower(strings.TrimSpace(hash))
		if hash == "" {
			continue
		}
		spendable, err := scriptClient.GetScriptSpendableUnspent(ctx, hash)
		if err != nil {
			upstreamPath := walletChainScriptUnspentAllUpstreamPath(hash)
			return nil, fmt.Errorf("query script spendable unspent failed path=%s: %w", upstreamPath, err)
		}
		for _, u := range spendable {
			key := strings.ToLower(strings.TrimSpace(u.TxID)) + ":" + fmt.Sprint(u.Vout)
			if strings.HasPrefix(key, ":") {
				continue
			}
			spendableByID[key] = whatsonchain.UTXO{TxID: strings.ToLower(strings.TrimSpace(u.TxID)), Vout: u.Vout, Value: u.Value}
		}
	}
	spendable := make([]whatsonchain.UTXO, 0, len(spendableByID))
	for _, u := range spendableByID {
		spendable = append(spendable, u)
	}
	return spendable, nil
}

func walletP2PKScriptHashCandidates(pubkeyHex string) ([]string, error) {
	pubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(pubkeyHex))
	if err != nil {
		return nil, err
	}
	lock, err := script.NewFromASM(strings.ToLower(pubkeyHex) + " OP_CHECKSIG")
	if err != nil {
		return nil, fmt.Errorf("build wallet p2pk lock failed: %w", err)
	}
	sum := sha256.Sum256(lock.Bytes())
	normal := strings.ToLower(hex.EncodeToString(sum[:]))
	reversed := reverseHexByByte(normal)
	if reversed == "" || reversed == normal {
		return []string{normal}, nil
	}
	return []string{normal, reversed}, nil
}

func walletP2PKHScriptHashCandidates(address string) ([]string, error) {
	scriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(strings.TrimSpace(scriptHex))
	if err != nil {
		return nil, fmt.Errorf("decode wallet p2pkh lock script hex failed: %w", err)
	}
	sum := sha256.Sum256(raw)
	normal := strings.ToLower(hex.EncodeToString(sum[:]))
	reversed := reverseHexByByte(normal)
	if reversed == "" || reversed == normal {
		return []string{normal}, nil
	}
	return []string{normal, reversed}, nil
}

func mergeWalletScriptHashCandidates(lists ...[]string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 8)
	for _, list := range lists {
		for _, item := range list {
			hash := strings.ToLower(strings.TrimSpace(item))
			if hash == "" {
				continue
			}
			if _, ok := seen[hash]; ok {
				continue
			}
			seen[hash] = struct{}{}
			out = append(out, hash)
		}
	}
	return out
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
			logWalletSyncStepError(meta, "chain_get_tx_detail", err, map[string]any{
				"upstream_path":    txPath,
				"txid":             txid,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return nil, wrapWalletSyncStepError(meta, "chain_get_tx_detail", txPath, err)
		}
		logWalletSyncStepInfo(meta, "chain_get_tx_detail", map[string]any{
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
		logWalletSyncStepInfo(meta, "chain_find_oldest_confirmed_height", map[string]any{
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
			// 空白地址在 WOC 上没有任何 confirmed history 时，返回 404 只是“当前没有数据”，不是失败。
			// 这里直接把它收敛成空结果，避免新空白 key 的首次同步被误判成异常。
			if isWalletChainEmptyConfirmedHistoryPage(err) {
				logWalletSyncStepInfo(meta, "chain_find_oldest_confirmed_height", map[string]any{
					"confirmed_live_txid_cnt": len(txids),
					"oldest_confirmed_height": 0,
					"page_cnt":                pageCount,
					"empty_range_http_404":    true,
					"step_duration_ms":        time.Since(stepStart).Milliseconds(),
				})
				return 0, nil
			}
			logWalletSyncStepError(meta, "chain_get_confirmed_history_desc", err, map[string]any{
				"upstream_path":    pagePath,
				"page_cnt":         pageCount,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return 0, wrapWalletSyncStepError(meta, "chain_get_confirmed_history_desc", pagePath, err)
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
			logWalletSyncStepInfo(meta, "chain_find_oldest_confirmed_height", map[string]any{
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

func collectConfirmedHistoryRange(ctx context.Context, chain walletChainClient, address string, cursor walletUTXOSyncCursor, tip uint32, meta walletSyncRoundMeta) ([]walletHistoryTxRecord, walletUTXOSyncCursor, error) {
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
				logWalletSyncStepInfo(meta, "chain_collect_confirmed_history", map[string]any{
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
			logWalletSyncStepError(meta, "chain_get_confirmed_history_asc", err, map[string]any{
				"upstream_path":    pagePath,
				"start_height":     startHeight,
				"page_cnt":         pageCount,
				"step_duration_ms": time.Since(stepStart).Milliseconds(),
			})
			return nil, next, wrapWalletSyncStepError(meta, "chain_get_confirmed_history_asc", pagePath, err)
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
				logWalletSyncStepError(meta, "chain_get_tx_detail_for_history", err, map[string]any{
					"upstream_path":    txPath,
					"txid":             txid,
					"step_duration_ms": time.Since(txStepStart).Milliseconds(),
				})
				return nil, next, wrapWalletSyncStepError(meta, "chain_get_tx_detail_for_history", txPath, err)
			}
			logWalletSyncStepInfo(meta, "chain_get_tx_detail_for_history", map[string]any{
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
		logWalletSyncStepInfo(meta, "chain_collect_confirmed_history", map[string]any{
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
// - 钱包只认“最终是否仍由本钱包控制”，不再把协议壳当成独立来源；
// - token 两层语义里，carrier 仍然是普通 BSV 输出，只是它同时承载 token 数量；
// - 因此这里接受两类脚本：
//  1. 纯钱包 p2pkh；
//  2. 以钱包 p2pkh 作为后缀的组合脚本；
//
// - 这样本地投影、链同步、pending local broadcast 三条链才能对 token change 使用同一判断口径。
func walletScriptHexMatchesAddressControl(outputScriptHex string, walletScriptHex string) bool {
	outputScriptHex = strings.TrimSpace(strings.ToLower(outputScriptHex))
	walletScriptHex = strings.TrimSpace(strings.ToLower(walletScriptHex))
	if outputScriptHex == "" || walletScriptHex == "" {
		return false
	}
	if outputScriptHex == walletScriptHex {
		return true
	}
	if strings.HasSuffix(outputScriptHex, walletScriptHex) {
		return true
	}
	// 设计说明：
	// - 钱包地址脚本是 p2pkh；当外部打到纯 p2pk（或“前缀变形 + 尾部 p2pk”）时，
	//   需要用“输出公钥哈希是否等于钱包地址哈希”来判定控制权；
	// - 这样标准 p2pk 能进可用余额，而变形脚本仍可按脚本分类落到 unknown。
	walletPKH, ok := walletScriptPublicKeyHash(walletScriptHex)
	if !ok || len(walletPKH) == 0 {
		return false
	}
	return outputScriptHasWalletP2PKTail(outputScriptHex, walletPKH)
}

func walletScriptPublicKeyHash(walletScriptHex string) ([]byte, bool) {
	lock, err := script.NewFromHex(strings.TrimSpace(walletScriptHex))
	if err != nil || lock == nil || !lock.IsP2PKH() {
		return nil, false
	}
	pkh, err := lock.PublicKeyHash()
	if err != nil || len(pkh) == 0 {
		return nil, false
	}
	return pkh, true
}

func outputScriptHasWalletP2PKTail(outputScriptHex string, walletPKH []byte) bool {
	lock, err := script.NewFromHex(strings.TrimSpace(outputScriptHex))
	if err != nil || lock == nil {
		return false
	}
	ops, err := script.DecodeScript(*lock)
	if err != nil || len(ops) < 2 {
		return false
	}
	last := ops[len(ops)-1]
	prev := ops[len(ops)-2]
	if last == nil || prev == nil || last.Op != script.OpCHECKSIG || len(prev.Data) == 0 {
		return false
	}
	return bytes.Equal(crypto.Hash160(prev.Data), walletPKH)
}

// hasBSV21TransferOutput 判断交易里是否包含 BSV21 transfer 输出。
// 设计说明：
// - 命中时表示这笔交易应交给 BSV21 专用事实入口，不再走钱包同步辅助层；
// - 这里只做轻量识别，不展开资产事实。

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

// alignWalletUTXOSyncCursor 把同步游标和“当前仍未花费集合”的最老确认高度重新对齐。
// 设计说明：
// - 钱包开始处理 1sat 后，当前未花费集合不再只是 plain BSV；
// - 如果当前仍未花费集合里出现了更老的输出，必须把同步锚点回退过去重扫；
// - 否则后续资产识别可能拿不到那段历史，导致 UTXO 有了但资产语义丢了。
func alignWalletUTXOSyncCursor(cursor walletUTXOSyncCursor, oldestCurrentConfirmedHeight int64, tip uint32) walletUTXOSyncCursor {
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
	s, err := requireFreshTipState(ctx, store)
	if err != nil {
		return 0, err
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
