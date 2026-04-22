package clientapp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// orchestrator 设计说明：
// - 统一接收 worker 信号，做去重/重试/串行调度；
// - 只把命令交给 kernel 执行，自己不直接改业务状态；
// - 第一版固定并发=1，优先保证一致性与可观测性。
const (
	orchestratorSignalWorkspaceTick = "workspace.tick"
	orchestratorSignalChainTip      = "chain.tip_advanced"
	orchestratorSignalFeePoolTick   = "feepool.billing_tick"

	orchestratorEventStarted          = "orchestrator_started"
	orchestratorEventSignalReceived   = "signal_received"
	orchestratorEventSignalDedupDrop  = "signal_dedup_dropped"
	orchestratorEventSignalReconciled = "signal_reconciled"
	orchestratorEventTaskEnqueued     = "task_enqueued"
	orchestratorEventTaskDequeued     = "task_dequeued"
	orchestratorEventTaskDispatchBeg  = "task_dispatch_started"
	orchestratorEventTaskDispatchRes  = "task_dispatch_result"
	orchestratorEventTaskRetry        = "task_retry_scheduled"
	orchestratorEventTaskFinalDrop    = "task_final_dropped"
)

type orchestratorSignal struct {
	Source       string
	Type         string
	AggregateKey string
	OccurredAt   int64
	Payload      map[string]any
}

type orchestratorTask struct {
	TaskID         string
	TaskType       string
	GatewayPeerID  string
	AggregateID    string
	IdempotencyKey string
	RequestedBy    string
	RequestedAt    int64
	TriggerKey     string
	Payload        map[string]any
	NextRunAt      time.Time
	RetryCount     int
}

type orchestratorTaskResult struct {
	Status       string         `json:"status"`
	ErrorCode    string         `json:"error_code,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
	StateBefore  string         `json:"state_before,omitempty"`
	StateAfter   string         `json:"state_after,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
}

type OrchestratorStatus struct {
	Enabled             bool   `json:"enabled"`
	StartedAtUnix       int64  `json:"started_at_unix"`
	LastSignalAtUnix    int64  `json:"last_signal_at_unix"`
	LastSignalType      string `json:"last_signal_type"`
	LastRunAtUnix       int64  `json:"last_run_at_unix"`
	LastSuccessAtUnix   int64  `json:"last_success_at_unix"`
	LastError           string `json:"last_error"`
	SignalRecvCount     uint64 `json:"signal_recv_count"`
	SignalDroppedCount  uint64 `json:"signal_dropped_count"`
	TaskAppliedCount    uint64 `json:"task_applied_count"`
	TaskRejectedCount   uint64 `json:"task_rejected_count"`
	TaskFailedCount     uint64 `json:"task_failed_count"`
	TaskRetriedCount    uint64 `json:"task_retried_count"`
	QueueLength         int    `json:"queue_length"`
	InFlight            bool   `json:"in_flight"`
	DedupWindowSeconds  int64  `json:"dedup_window_seconds"`
	RetryMax            int    `json:"retry_max"`
	RetryBackoffCapSec  int64  `json:"retry_backoff_cap_seconds"`
	GlobalConcurrency   int    `json:"global_concurrency"`
	WorkspaceTickSecond int64  `json:"workspace_tick_seconds"`
	ChainPollSeconds    int64  `json:"chain_poll_seconds"`
}

type orchestrator struct {
	rt    *Runtime
	store *clientDB

	dedupWindow    time.Duration
	retryMax       int
	backoffCap     time.Duration
	chainPollEvery time.Duration

	signalCh chan orchestratorSignal
	taskCh   chan struct{}

	mu     sync.RWMutex
	status OrchestratorStatus

	queue       []*orchestratorTask
	pendingTask map[string]struct{}
	signalDedup map[string]time.Time
	lastTip     uint32
}

func newOrchestrator(rt *Runtime, store *clientDB) *orchestrator {
	if rt == nil {
		return nil
	}
	snapshot := rt.ConfigSnapshot()
	workspaceTick := int64(snapshot.Scan.RescanIntervalSeconds)
	if workspaceTick <= 0 {
		d, err := networkInitDefaults(snapshot.BSV.Network)
		if err != nil {
			workspaceTick = 300
		} else {
			workspaceTick = int64(d.ScanRescanIntervalSeconds)
		}
	}
	return &orchestrator{
		rt:             rt,
		store:          store,
		dedupWindow:    10 * time.Second,
		retryMax:       5,
		backoffCap:     120 * time.Second,
		chainPollEvery: 0,
		signalCh:       make(chan orchestratorSignal, 128),
		taskCh:         make(chan struct{}, 1),
		pendingTask:    map[string]struct{}{},
		signalDedup:    map[string]time.Time{},
		status: OrchestratorStatus{
			Enabled:             true,
			DedupWindowSeconds:  10,
			RetryMax:            5,
			RetryBackoffCapSec:  120,
			GlobalConcurrency:   1,
			WorkspaceTickSecond: workspaceTick,
			ChainPollSeconds:    0,
		},
	}
}

func (o *orchestrator) Start(ctx context.Context) {
	if o == nil {
		return
	}
	o.setStartedAt(time.Now().Unix())
	o.logEvent(orchestratorLogEntry{
		EventType: orchestratorEventStarted,
		Source:    "orchestrator",
		Payload: map[string]any{
			"dedup_window_seconds":      int64(o.dedupWindow / time.Second),
			"retry_max":                 o.retryMax,
			"retry_backoff_cap_seconds": int64(o.backoffCap / time.Second),
			"chain_poll_seconds":        int64(o.chainPollEvery / time.Second),
		},
	})
	go o.run(ctx)
	// 设计约束：文件存储扫描只走模块 open hook，不再由 orchestrator 额外驱动。
}

func (o *orchestrator) SnapshotStatus() OrchestratorStatus {
	if o == nil {
		return OrchestratorStatus{Enabled: false}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	st := o.status
	st.QueueLength = len(o.queue)
	return st
}

func (o *orchestrator) EmitSignal(sig orchestratorSignal) {
	if o == nil {
		return
	}
	if strings.TrimSpace(sig.Type) == "" {
		return
	}
	if sig.OccurredAt <= 0 {
		sig.OccurredAt = time.Now().Unix()
	}
	select {
	case o.signalCh <- sig:
	default:
		o.mu.Lock()
		o.status.SignalDroppedCount++
		o.mu.Unlock()
		o.logEvent(orchestratorLogEntry{
			EventType:    orchestratorEventSignalDedupDrop,
			Source:       strings.TrimSpace(sig.Source),
			SignalType:   strings.TrimSpace(sig.Type),
			AggregateKey: strings.TrimSpace(sig.AggregateKey),
			TaskStatus:   "dropped_channel_full",
		})
		obs.Error(ServiceName, "orchestrator_signal_dropped", map[string]any{"type": sig.Type, "aggregate": sig.AggregateKey})
	}
}

func (o *orchestrator) run(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-o.signalCh:
			o.handleSignal(sig)
		case <-o.taskCh:
			o.runOneTask(ctx)
		case <-ticker.C:
			o.runOneTask(ctx)
		}
	}
}

func (o *orchestrator) runWorkspaceSignalWorker(ctx context.Context) {
	if o == nil || o.rt == nil {
		return
	}
	snapshot := o.rt.ConfigSnapshot()
	if snapshot.Scan.RescanIntervalSeconds == 0 {
		return
	}
	// 唯一入口：按扫描间隔发 workspace.tick，后续统一进入 kernel 执行 SyncOnce。
	interval := time.Duration(snapshot.Scan.RescanIntervalSeconds) * time.Second
	scheduler := ensureRuntimeTaskScheduler(o.rt, o.store)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "workspace_tick",
		Owner:     "orchestrator",
		Mode:      "static",
		Interval:  interval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			o.EmitSignal(orchestratorSignal{
				Source:       "workspace_worker",
				Type:         orchestratorSignalWorkspaceTick,
				AggregateKey: "workspace:default",
				Payload: map[string]any{
					"trigger": trigger,
				},
			})
			return map[string]any{
				"signal_type": orchestratorSignalWorkspaceTick,
				"trigger":     trigger,
			}, nil
		},
	}); err != nil {
		obs.Error(ServiceName, "workspace_signal_task_register_failed", map[string]any{"error": err.Error()})
	}
}

func (o *orchestrator) runChainTipSignalWorker(ctx context.Context) {
	// 区块高度由链维护进程统一拉取并发信号，这里保留空实现避免旧入口误用。
	<-ctx.Done()
}

func (o *orchestrator) handleSignal(sig orchestratorSignal) {
	now := time.Now()
	o.mu.Lock()
	o.status.SignalRecvCount++
	o.status.LastSignalAtUnix = sig.OccurredAt
	o.status.LastSignalType = strings.TrimSpace(sig.Type)
	dedupKey := strings.TrimSpace(sig.Type) + ":" + strings.TrimSpace(sig.AggregateKey)
	if last, ok := o.signalDedup[dedupKey]; ok && now.Sub(last) < o.dedupWindow {
		o.status.SignalDroppedCount++
		o.mu.Unlock()
		o.logEvent(orchestratorLogEntry{
			EventType:    orchestratorEventSignalDedupDrop,
			Source:       strings.TrimSpace(sig.Source),
			SignalType:   strings.TrimSpace(sig.Type),
			AggregateKey: strings.TrimSpace(sig.AggregateKey),
			TaskStatus:   "dropped_dedup",
			Payload: map[string]any{
				"dedup_key": dedupKey,
			},
		})
		return
	}
	o.signalDedup[dedupKey] = now
	o.mu.Unlock()

	o.logEvent(orchestratorLogEntry{
		EventType:    orchestratorEventSignalReceived,
		Source:       strings.TrimSpace(sig.Source),
		SignalType:   strings.TrimSpace(sig.Type),
		AggregateKey: strings.TrimSpace(sig.AggregateKey),
	})
	tasks := o.reconcileSignal(sig, now)
	o.logEvent(orchestratorLogEntry{
		EventType:    orchestratorEventSignalReconciled,
		Source:       strings.TrimSpace(sig.Source),
		SignalType:   strings.TrimSpace(sig.Type),
		AggregateKey: strings.TrimSpace(sig.AggregateKey),
		Payload: map[string]any{
			"task_count": len(tasks),
		},
	})
	for _, task := range tasks {
		o.enqueueTask(task)
	}
}

func (o *orchestrator) reconcileSignal(sig orchestratorSignal, now time.Time) []*orchestratorTask {
	out := make([]*orchestratorTask, 0, 8)
	switch strings.TrimSpace(sig.Type) {
	case orchestratorSignalWorkspaceTick:
		out = append(out, &orchestratorTask{
			TaskID:         newActionID(),
			TaskType:       "workspace.sync",
			RequestedBy:    "orchestrator",
			AggregateID:    "workspace:default",
			IdempotencyKey: fmt.Sprintf("workspace_sync:%d", now.Unix()/10),
			RequestedAt:    now.Unix(),
			Payload: map[string]any{
				"trigger": strings.TrimSpace(anyToString(sig.Payload["trigger"])),
			},
			NextRunAt: now,
		})
	case orchestratorSignalFeePoolTick:
		gwID := strings.TrimSpace(sig.AggregateKey)
		if gwID == "" {
			break
		}
		out = append(out, &orchestratorTask{
			TaskID:         newActionID(),
			TaskType:       "feepool.maintain",
			GatewayPeerID:  gwID,
			RequestedBy:    "orchestrator",
			AggregateID:    "gateway:" + gwID,
			IdempotencyKey: fmt.Sprintf("feepool_tick:%s:%d", gwID, now.Unix()/10),
			RequestedAt:    now.Unix(),
			Payload: map[string]any{
				"trigger": "billing_tick",
			},
			NextRunAt: now,
		})
	case orchestratorSignalChainTip:
		// 设计约束：链高度推进只服务 listen 资金池维护。
		// 非 listen 场景（例如 gateway 直付、普通 peer.call）不应该因为"配置了网关"就被动开池。
		if o == nil || o.rt == nil || !cfgBool(o.rt.ConfigSnapshot().Listen.Enabled, true) {
			break
		}
		for _, gw := range snapshotHealthyGateways(o.rt) {
			gwID := strings.TrimSpace(gw.ID.String())
			if gwID == "" {
				continue
			}
			out = append(out, &orchestratorTask{
				TaskID:         newActionID(),
				TaskType:       "feepool.maintain",
				GatewayPeerID:  gwID,
				RequestedBy:    "orchestrator",
				AggregateID:    "gateway:" + gwID,
				IdempotencyKey: fmt.Sprintf("chain_tip_tick:%s:%v", gwID, sig.Payload["tip_to"]),
				RequestedAt:    now.Unix(),
				Payload: map[string]any{
					"trigger":      "chain_tip_advanced",
					"observed_tip": anyToInt64(sig.Payload["tip_to"]),
				},
				NextRunAt: now,
			})
		}
	}
	return out
}

func (o *orchestrator) enqueueTask(task *orchestratorTask) {
	if o == nil || task == nil || strings.TrimSpace(task.IdempotencyKey) == "" {
		return
	}
	o.mu.Lock()
	if _, exists := o.pendingTask[task.IdempotencyKey]; exists {
		o.mu.Unlock()
		return
	}
	o.pendingTask[task.IdempotencyKey] = struct{}{}
	o.queue = append(o.queue, task)
	o.status.QueueLength = len(o.queue)
	queueLen := o.status.QueueLength
	o.mu.Unlock()
	o.logEvent(orchestratorLogEntry{
		EventType:      orchestratorEventTaskEnqueued,
		Source:         "orchestrator",
		AggregateKey:   strings.TrimSpace(task.AggregateID),
		IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
		CommandType:    strings.TrimSpace(task.TaskType),
		GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
		RetryCount:     task.RetryCount,
		QueueLength:    queueLen,
	})
	o.wakeupTaskRunner()
}

func (o *orchestrator) wakeupTaskRunner() {
	select {
	case o.taskCh <- struct{}{}:
	default:
	}
}

func (o *orchestrator) runOneTask(ctx context.Context) {
	o.mu.Lock()
	if len(o.queue) == 0 {
		o.mu.Unlock()
		return
	}
	now := time.Now()
	idx := -1
	for i, t := range o.queue {
		if t == nil {
			continue
		}
		if t.NextRunAt.IsZero() || !t.NextRunAt.After(now) {
			idx = i
			break
		}
	}
	if idx < 0 {
		o.mu.Unlock()
		return
	}
	task := o.queue[idx]
	o.queue = append(o.queue[:idx], o.queue[idx+1:]...)
	o.status.QueueLength = len(o.queue)
	o.status.InFlight = true
	o.status.LastRunAtUnix = now.Unix()
	queueLen := o.status.QueueLength
	o.mu.Unlock()

	defer func() {
		o.mu.Lock()
		o.status.InFlight = false
		o.status.QueueLength = len(o.queue)
		o.mu.Unlock()
	}()

	if task == nil {
		return
	}
	o.logEvent(orchestratorLogEntry{
		EventType:      orchestratorEventTaskDequeued,
		Source:         "orchestrator",
		AggregateKey:   strings.TrimSpace(task.AggregateID),
		IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
		CommandType:    strings.TrimSpace(task.TaskType),
		GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
		RetryCount:     task.RetryCount,
		QueueLength:    queueLen,
	})
	task.TriggerKey = strings.TrimSpace(task.IdempotencyKey)
	var res orchestratorTaskResult
	switch strings.TrimSpace(task.TaskType) {
	case "workspace.sync":
		res = o.runWorkspaceSync(ctx, task)
	case "feepool.maintain":
		res = o.runFeePoolMaintain(ctx, task)
	default:
		res = orchestratorTaskResult{Status: "failed", ErrorCode: "unsupported_task", ErrorMessage: fmt.Sprintf("unsupported task type: %s", task.TaskType)}
	}
	switch strings.TrimSpace(res.Status) {
	case "applied":
		o.mu.Lock()
		o.status.TaskAppliedCount++
		o.status.LastSuccessAtUnix = time.Now().Unix()
		delete(o.pendingTask, task.IdempotencyKey)
		o.mu.Unlock()
		o.logEvent(orchestratorLogEntry{
			EventType:      orchestratorEventTaskDispatchRes,
			Source:         "orchestrator",
			AggregateKey:   strings.TrimSpace(task.AggregateID),
			IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
			CommandType:    strings.TrimSpace(task.TaskType),
			GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
			TaskStatus:     "applied",
			RetryCount:     task.RetryCount,
		})
	case "rejected", "paused":
		o.mu.Lock()
		o.status.TaskRejectedCount++
		delete(o.pendingTask, task.IdempotencyKey)
		o.mu.Unlock()
		o.logEvent(orchestratorLogEntry{
			EventType:      orchestratorEventTaskDispatchRes,
			Source:         "orchestrator",
			AggregateKey:   strings.TrimSpace(task.AggregateID),
			IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
			CommandType:    strings.TrimSpace(task.TaskType),
			GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
			TaskStatus:     strings.TrimSpace(res.Status),
			RetryCount:     task.RetryCount,
			ErrorMessage:   strings.TrimSpace(res.ErrorMessage),
			Payload: map[string]any{
				"error_code": strings.TrimSpace(res.ErrorCode),
			},
		})
	default:
		err := strings.TrimSpace(res.ErrorMessage)
		if err == "" {
			err = strings.TrimSpace(res.ErrorCode)
		}
		if err == "" {
			err = "task failed"
		}
		o.logEvent(orchestratorLogEntry{
			EventType:      orchestratorEventTaskDispatchRes,
			Source:         "orchestrator",
			AggregateKey:   strings.TrimSpace(task.AggregateID),
			IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
			CommandType:    strings.TrimSpace(task.TaskType),
			GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
			TaskStatus:     "failed",
			RetryCount:     task.RetryCount,
			ErrorMessage:   err,
		})
		o.failTask(task, fmt.Errorf("%s", err), o.isRetryableFailure(res))
	}
}

// resolveFeePoolMaintainCommandType 决定 maintain 任务走 ensure_active 还是 cycle_tick。
// 有 active session 走 tick，没有走 ensure_active。
func resolveFeePoolMaintainCommandType(rt *Runtime, gatewayPeerID string) string {
	if rt == nil {
		return feePoolCommandEnsureActive
	}
	sess, ok := rt.getFeePool(strings.TrimSpace(gatewayPeerID))
	if ok && sess != nil && strings.TrimSpace(sess.SpendTxID) != "" && strings.TrimSpace(sess.Status) == "active" {
		return feePoolCommandCycleTick
	}
	return feePoolCommandEnsureActive
}

func (o *orchestrator) runWorkspaceSync(ctx context.Context, task *orchestratorTask) orchestratorTaskResult {
	return orchestratorTaskResult{
		Status:       "rejected",
		ErrorCode:    "workspace_sync_removed",
		ErrorMessage: "workspace sync is removed",
	}
}

func (o *orchestrator) runFeePoolMaintain(ctx context.Context, task *orchestratorTask) orchestratorTaskResult {
	if o.rt == nil || o.rt.feePool == nil {
		return orchestratorTaskResult{
			Status:       "rejected",
			ErrorCode:    "fee_pool_not_initialized",
			ErrorMessage: "fee pool not initialized",
		}
	}
	gwID := strings.TrimSpace(task.GatewayPeerID)
	if gwID == "" {
		return orchestratorTaskResult{
			Status:       "rejected",
			ErrorCode:    "gateway_required",
			ErrorMessage: "gateway peer id required",
		}
	}
	gw, err := o.rt.PickGatewayForBusiness(gwID)
	if err != nil || gw.ID == "" {
		return orchestratorTaskResult{
			Status:       "rejected",
			ErrorCode:    "gateway_not_found",
			ErrorMessage: fmt.Sprintf("gateway not found: %s", gwID),
		}
	}
	cmdType := resolveFeePoolMaintainCommandType(o.rt, gwID)
	cmd := feePoolKernelCommand{
		CommandID:       strings.TrimSpace(task.TaskID),
		CommandType:     cmdType,
		RequestedBy:     strings.TrimSpace(task.RequestedBy),
		AllowWhenPaused: false,
		TriggerKey:      strings.TrimSpace(task.TriggerKey),
		Payload:         task.Payload,
	}
	res := o.rt.feePool.dispatch(ctx, gw, cmd)
	return orchestratorTaskResult{
		Status:       res.Status,
		ErrorCode:    res.ErrorCode,
		ErrorMessage: res.ErrorMessage,
		Data: map[string]any{
			"state_before": res.StateBefore,
			"state_after":  res.StateAfter,
		},
	}
}

// isRetryableFailure 定义调度层重试语义：余额暂停/前置条件缺失这类状态型失败不做指数重试。
func (o *orchestrator) isRetryableFailure(res orchestratorTaskResult) bool {
	code := strings.TrimSpace(strings.ToLower(res.ErrorCode))
	switch code {
	case "session_missing", "wallet_insufficient", "wallet_insufficient_paused":
		return false
	default:
		return true
	}
}

func (o *orchestrator) failTask(task *orchestratorTask, err error, retryable bool) {
	if o == nil || task == nil {
		return
	}
	if err == nil {
		err = fmt.Errorf("unknown error")
	}
	o.mu.Lock()
	o.status.TaskFailedCount++
	o.status.LastError = err.Error()
	o.mu.Unlock()

	if !retryable || task.RetryCount >= o.retryMax {
		o.mu.Lock()
		delete(o.pendingTask, task.IdempotencyKey)
		queueLen := len(o.queue)
		o.mu.Unlock()
		o.logEvent(orchestratorLogEntry{
			EventType:      orchestratorEventTaskFinalDrop,
			Source:         "orchestrator",
			AggregateKey:   strings.TrimSpace(task.AggregateID),
			IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
			CommandType:    strings.TrimSpace(task.TaskType),
			GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
			TaskStatus:     "dropped",
			RetryCount:     task.RetryCount,
			QueueLength:    queueLen,
			ErrorMessage:   err.Error(),
		})
		return
	}
	task.RetryCount++
	backoff := time.Second << (task.RetryCount - 1)
	if backoff > o.backoffCap {
		backoff = o.backoffCap
	}
	task.NextRunAt = time.Now().Add(backoff)
	o.mu.Lock()
	o.status.TaskRetriedCount++
	o.queue = append(o.queue, task)
	o.status.QueueLength = len(o.queue)
	queueLen := o.status.QueueLength
	o.mu.Unlock()
	o.logEvent(orchestratorLogEntry{
		EventType:      orchestratorEventTaskRetry,
		Source:         "orchestrator",
		AggregateKey:   strings.TrimSpace(task.AggregateID),
		IdempotencyKey: strings.TrimSpace(task.IdempotencyKey),
		CommandType:    strings.TrimSpace(task.TaskType),
		GatewayPeerID:  strings.TrimSpace(task.GatewayPeerID),
		TaskStatus:     "retry_scheduled",
		RetryCount:     task.RetryCount,
		QueueLength:    queueLen,
		ErrorMessage:   err.Error(),
		Payload: map[string]any{
			"backoff_seconds": int64(backoff / time.Second),
		},
	})
	o.wakeupTaskRunner()
}

func (o *orchestrator) setStartedAt(unix int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status.StartedAtUnix = unix
}

func (o *orchestrator) logEvent(e orchestratorLogEntry) {
	if o == nil || o.store == nil || o.rt == nil || o.rt.ctx == nil {
		return
	}
	dbAppendOrchestratorLog(context.WithoutCancel(o.rt.ctx), o.store, e)
}
