package clientapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procschedulertasks"
)

// periodicTaskSpec 描述一个独立的周期任务。
// 设计说明：
// - 调度层只负责“按时触发 + 统一调度日志/错误口径”；
// - 任务内部业务逻辑保持原样，不在这里做二次串行治理。
type periodicTaskSpec struct {
	Name      string
	Owner     string
	Mode      string
	Interval  time.Duration
	Immediate bool
	Timeout   time.Duration
	Run       func(ctx context.Context, trigger string) (map[string]any, error)
}

type periodicTaskStatus struct {
	Name              string `json:"name"`
	Owner             string `json:"owner"`
	Mode              string `json:"mode"`
	IntervalSeconds   int64  `json:"interval_seconds"`
	StartedAtUnix     int64  `json:"started_at_unix"`
	LastTrigger       string `json:"last_trigger"`
	LastStartedAtUnix int64  `json:"last_started_at_unix"`
	LastEndedAtUnix   int64  `json:"last_ended_at_unix"`
	LastDurationMS    int64  `json:"last_duration_ms"`
	LastError         string `json:"last_error"`
	InFlight          bool   `json:"in_flight"`
	RunCount          uint64 `json:"run_count"`
	SuccessCount      uint64 `json:"success_count"`
	FailureCount      uint64 `json:"failure_count"`
}

type periodicTaskRuntime struct {
	spec   periodicTaskSpec
	status periodicTaskStatus
	cancel context.CancelFunc
}

type taskScheduler struct {
	service string
	store   *clientDB
	ctx     context.Context

	mu       sync.RWMutex
	tasks    map[string]*periodicTaskRuntime
	shutdown bool
	wg       sync.WaitGroup
}

func newTaskScheduler(store *clientDB, service string) *taskScheduler {
	service = strings.TrimSpace(service)
	if service == "" {
		service = "bitcast-client"
	}
	return &taskScheduler{
		service: service,
		store:   store,
		tasks:   map[string]*periodicTaskRuntime{},
	}
}

func ensureRuntimeTaskScheduler(rt *Runtime, store *clientDB) *taskScheduler {
	if rt == nil {
		return nil
	}
	rt.taskSchedMu.Lock()
	defer rt.taskSchedMu.Unlock()
	if rt.taskSched == nil {
		rt.taskSched = newTaskScheduler(store, "bitcast-client")
	}
	if rt.taskSched.ctx == nil {
		rt.taskSched.ctx = rt.ctx
	}
	return rt.taskSched
}

func (s *taskScheduler) RegisterPeriodicTask(ctx context.Context, spec periodicTaskSpec) error {
	return s.registerPeriodicTask(ctx, spec, false)
}

func (s *taskScheduler) RegisterOrReplacePeriodicTask(ctx context.Context, spec periodicTaskSpec) error {
	return s.registerPeriodicTask(ctx, spec, true)
}

func (s *taskScheduler) registerPeriodicTask(ctx context.Context, spec periodicTaskSpec, replace bool) error {
	if s == nil {
		return fmt.Errorf("task scheduler is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" {
		return fmt.Errorf("task name is empty")
	}
	if spec.Interval <= 0 {
		return fmt.Errorf("task interval must be > 0")
	}
	if spec.Run == nil {
		return fmt.Errorf("task run function is nil")
	}
	spec.Owner = strings.TrimSpace(spec.Owner)
	spec.Mode = strings.ToLower(strings.TrimSpace(spec.Mode))
	if spec.Mode == "" {
		spec.Mode = "static"
	}
	if spec.Mode != "static" && spec.Mode != "dynamic" {
		return fmt.Errorf("task mode must be static or dynamic")
	}

	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return fmt.Errorf("task scheduler is shutting down")
	}
	if old, exists := s.tasks[spec.Name]; exists {
		if !replace {
			s.mu.Unlock()
			err := fmt.Errorf("task already registered: %s", spec.Name)
			s.logSchedulerError(spec.Name, "register_failed", err)
			return err
		}
		delete(s.tasks, spec.Name)
		if old != nil && old.cancel != nil {
			old.cancel()
		}
	}
	taskCtx, taskCancel := context.WithCancel(ctx)
	rt := &periodicTaskRuntime{
		spec: spec,
		status: periodicTaskStatus{
			Name:            spec.Name,
			Owner:           spec.Owner,
			Mode:            spec.Mode,
			IntervalSeconds: int64(spec.Interval / time.Second),
			StartedAtUnix:   time.Now().Unix(),
		},
		cancel: taskCancel,
	}
	s.tasks[spec.Name] = rt
	s.mu.Unlock()
	if err := s.upsertTaskProfile(rt, "active", 0); err != nil {
		s.logSchedulerError(spec.Name, "task_profile_upsert_failed", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runPeriodicTask(taskCtx, rt)
	}()
	return nil
}

func (s *taskScheduler) CancelTask(name string) bool {
	if s == nil {
		return false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	s.mu.Lock()
	rt, exists := s.tasks[name]
	if exists {
		delete(s.tasks, name)
	}
	s.mu.Unlock()
	if !exists {
		return false
	}
	if rt != nil && rt.cancel != nil {
		rt.cancel()
	}
	if err := s.markTaskStopped(name); err != nil {
		s.logSchedulerError(name, "task_profile_stop_failed", err)
	}
	return true
}

func (s *taskScheduler) CancelTasksByPrefix(prefix string) int {
	if s == nil {
		return 0
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return 0
	}
	s.mu.Lock()
	names := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	runtimes := make([]*periodicTaskRuntime, 0, len(names))
	for _, name := range names {
		runtimes = append(runtimes, s.tasks[name])
		delete(s.tasks, name)
	}
	s.mu.Unlock()
	for _, rt := range runtimes {
		if rt != nil && rt.cancel != nil {
			rt.cancel()
		}
		if rt != nil {
			if err := s.markTaskStopped(rt.spec.Name); err != nil {
				s.logSchedulerError(rt.spec.Name, "task_profile_stop_failed", err)
			}
		}
	}
	return len(names)
}

func (s *taskScheduler) HasTask(name string) bool {
	if s == nil {
		return false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.tasks[name]
	return ok
}

// Shutdown 用于 runtime 关闭阶段：
// - 先统一取消所有后台周期任务；
// - 再把调度表状态标记为 stopped；
// - 关闭后不再接受新任务注册，避免“DB 已关闭但后台又起新任务”。
func (s *taskScheduler) Shutdown() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.shutdown = true
	runtimes := make([]*periodicTaskRuntime, 0, len(s.tasks))
	for name, rt := range s.tasks {
		runtimes = append(runtimes, rt)
		delete(s.tasks, name)
	}
	s.mu.Unlock()

	var firstErr error
	for _, rt := range runtimes {
		if rt != nil && rt.cancel != nil {
			rt.cancel()
		}
	}
	s.wg.Wait()
	// 关闭阶段需要把任务状态落为 stopped。
	// 这里使用 root ctx 的 without-cancel 派生上下文，避免主流程 cancel 后误报 context canceled。
	stopCtx := s.ctx
	stopCancel := func() {}
	if s.ctx != nil {
		stopCtx, stopCancel = context.WithTimeout(context.WithoutCancel(s.ctx), 3*time.Second)
	}
	defer stopCancel()
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		if err := s.markTaskStoppedWithCtx(stopCtx, rt.spec.Name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *taskScheduler) runPeriodicTask(ctx context.Context, rt *periodicTaskRuntime) {
	if s == nil || rt == nil {
		return
	}
	if rt.spec.Immediate {
		s.executeTask(ctx, rt, "startup")
	}
	ticker := time.NewTicker(rt.spec.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.executeTask(ctx, rt, "periodic_tick")
		}
	}
}

func (s *taskScheduler) executeTask(parent context.Context, rt *periodicTaskRuntime, trigger string) {
	if s == nil || rt == nil {
		return
	}
	trigger = strings.TrimSpace(trigger)
	if trigger == "" {
		trigger = "system"
	}

	startAt := time.Now()
	s.mu.Lock()
	rt.status.InFlight = true
	rt.status.LastTrigger = trigger
	rt.status.LastStartedAtUnix = startAt.Unix()
	rt.status.RunCount++
	s.mu.Unlock()

	defer func() {
		endAt := time.Now()
		s.mu.Lock()
		rt.status.InFlight = false
		rt.status.LastEndedAtUnix = endAt.Unix()
		rt.status.LastDurationMS = endAt.Sub(startAt).Milliseconds()
		s.mu.Unlock()
	}()

	runCtx := parent
	cancel := func() {}
	if rt.spec.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(parent, rt.spec.Timeout)
	}
	defer cancel()

	if err := s.markTaskStarted(rt.spec.Name, trigger, startAt.Unix()); err != nil {
		s.logSchedulerError(rt.spec.Name, "task_mark_started_failed", err)
	}
	summary, err := rt.spec.Run(runCtx, trigger)
	if summary == nil {
		summary = map[string]any{}
	}
	endAt := time.Now()
	durationMS := endAt.Sub(startAt).Milliseconds()
	runStatus := "success"
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
		if errors.Is(err, context.Canceled) {
			runStatus = "canceled"
		} else {
			runStatus = "failed"
		}
	}
	if appendErr := s.appendTaskRunLog(rt.spec, trigger, startAt.Unix(), endAt.Unix(), durationMS, runStatus, errMsg, summary); appendErr != nil {
		s.logSchedulerError(rt.spec.Name, "task_run_log_append_failed", appendErr)
	}
	if finishErr := s.markTaskFinished(rt.spec.Name, endAt.Unix(), durationMS, errMsg, summary, runStatus == "success"); finishErr != nil {
		s.logSchedulerError(rt.spec.Name, "task_mark_finished_failed", finishErr)
	}
	if err != nil {
		s.mu.Lock()
		rt.status.FailureCount++
		rt.status.LastError = err.Error()
		s.mu.Unlock()
		s.logTaskError(rt.spec.Name, trigger, time.Since(startAt), err)
		return
	}
	s.mu.Lock()
	rt.status.SuccessCount++
	rt.status.LastError = ""
	s.mu.Unlock()
}

func (s *taskScheduler) SnapshotStatus() []periodicTaskStatus {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]periodicTaskStatus, 0, len(s.tasks))
	for _, rt := range s.tasks {
		if rt == nil {
			continue
		}
		out = append(out, rt.status)
	}
	return out
}

func (s *taskScheduler) logSchedulerError(task string, code string, err error) {
	if s == nil || err == nil {
		return
	}
	obs.Error(s.service, "scheduler_error", map[string]any{
		"task":       strings.TrimSpace(task),
		"error_code": strings.TrimSpace(code),
		"error":      err.Error(),
	})
}

func (s *taskScheduler) upsertTaskProfile(rt *periodicTaskRuntime, status string, closedAt int64) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	now := time.Now().Unix()
	spec := rt.spec
	if strings.TrimSpace(status) == "" {
		status = "active"
	}
	return clientDBEntTx(s.ctx, s.store, func(tx *gen.Tx) error {
		intervalSeconds := int64(spec.Interval / time.Second)
		createdAtUnix := int64(now)
		closedAtUnix := int64(closedAt)
		existing, err := tx.ProcSchedulerTasks.Query().Where(procschedulertasks.TaskNameEQ(spec.Name)).Only(s.ctx)
		if err == nil {
			_, err = existing.Update().
				SetOwner(strings.TrimSpace(spec.Owner)).
				SetMode(strings.TrimSpace(spec.Mode)).
				SetStatus(strings.TrimSpace(status)).
				SetIntervalSeconds(intervalSeconds).
				SetUpdatedAtUnix(createdAtUnix).
				SetClosedAtUnix(closedAtUnix).
				SetMetaJSON(mustJSONTask(map[string]any{"immediate": spec.Immediate, "timeout_ms": spec.Timeout.Milliseconds()})).
				Save(s.ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcSchedulerTasks.Create().
			SetTaskName(strings.TrimSpace(spec.Name)).
			SetOwner(strings.TrimSpace(spec.Owner)).
			SetMode(strings.TrimSpace(spec.Mode)).
			SetStatus(strings.TrimSpace(status)).
			SetIntervalSeconds(intervalSeconds).
			SetCreatedAtUnix(createdAtUnix).
			SetUpdatedAtUnix(createdAtUnix).
			SetClosedAtUnix(closedAtUnix).
			SetLastTrigger("").
			SetLastStartedAtUnix(0).
			SetLastEndedAtUnix(0).
			SetLastDurationMs(0).
			SetLastError("").
			SetInFlight(0).
			SetRunCount(0).
			SetSuccessCount(0).
			SetFailureCount(0).
			SetLastSummaryJSON("{}").
			SetMetaJSON(mustJSONTask(map[string]any{"immediate": spec.Immediate, "timeout_ms": spec.Timeout.Milliseconds()})).
			Save(s.ctx)
		return err
	})
}

func (s *taskScheduler) markTaskStopped(name string) error {
	return s.markTaskStoppedWithCtx(s.ctx, name)
}

func (s *taskScheduler) markTaskStoppedWithCtx(ctx context.Context, name string) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, s.store, func(tx *gen.Tx) error {
		existing, err := tx.ProcSchedulerTasks.Query().Where(procschedulertasks.TaskNameEQ(strings.TrimSpace(name))).Only(ctx)
		if err != nil {
			return err
		}
		_, err = existing.Update().
			SetStatus("stopped").
			SetClosedAtUnix(now).
			SetUpdatedAtUnix(now).
			SetInFlight(0).
			Save(ctx)
		return err
	})
}

func (s *taskScheduler) markTaskStarted(name string, trigger string, startedAt int64) error {
	if s == nil {
		return nil
	}
	if s.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(s.ctx, s.store, func(tx *gen.Tx) error {
		existing, err := tx.ProcSchedulerTasks.Query().Where(procschedulertasks.TaskNameEQ(strings.TrimSpace(name))).Only(s.ctx)
		if err != nil {
			return err
		}
		status := existing.Status
		if !strings.EqualFold(strings.TrimSpace(status), "stopped") {
			status = "active"
		}
		_, err = existing.Update().
			SetLastTrigger(strings.TrimSpace(trigger)).
			SetLastStartedAtUnix(startedAt).
			SetInFlight(1).
			SetUpdatedAtUnix(time.Now().Unix()).
			SetStatus(status).
			Save(s.ctx)
		return err
	})
}

func (s *taskScheduler) markTaskFinished(name string, endedAt int64, durationMS int64, errMsg string, summary map[string]any, success bool) error {
	if s == nil {
		return nil
	}
	if s.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	incSuccess := 0
	incFailure := 0
	if success {
		incSuccess = 1
	} else {
		incFailure = 1
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(s.ctx, s.store, func(tx *gen.Tx) error {
		existing, err := tx.ProcSchedulerTasks.Query().Where(procschedulertasks.TaskNameEQ(strings.TrimSpace(name))).Only(s.ctx)
		if err != nil {
			return err
		}
		_, err = existing.Update().
			SetLastEndedAtUnix(endedAt).
			SetLastDurationMs(durationMS).
			SetLastError(strings.TrimSpace(errMsg)).
			SetLastSummaryJSON(mustJSONTask(summary)).
			SetInFlight(0).
			SetRunCount(existing.RunCount + 1).
			SetSuccessCount(existing.SuccessCount + int64(incSuccess)).
			SetFailureCount(existing.FailureCount + int64(incFailure)).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(s.ctx)
		return err
	})
}

func (s *taskScheduler) appendTaskRunLog(spec periodicTaskSpec, trigger string, startedAt int64, endedAt int64, durationMS int64, status string, errMsg string, summary map[string]any) error {
	if s == nil {
		return nil
	}
	if s.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(s.ctx, s.store, func(tx *gen.Tx) error {
		_, err := tx.ProcSchedulerTaskRuns.Create().
			SetTaskName(strings.TrimSpace(spec.Name)).
			SetOwner(strings.TrimSpace(spec.Owner)).
			SetMode(strings.TrimSpace(spec.Mode)).
			SetTrigger(strings.TrimSpace(trigger)).
			SetStartedAtUnix(startedAt).
			SetEndedAtUnix(endedAt).
			SetDurationMs(durationMS).
			SetStatus(strings.TrimSpace(status)).
			SetErrorMessage(strings.TrimSpace(errMsg)).
			SetSummaryJSON(mustJSONTask(summary)).
			SetCreatedAtUnix(time.Now().Unix()).
			Save(s.ctx)
		return err
	})
}

func mustJSONTask(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *taskScheduler) logTaskError(task string, trigger string, duration time.Duration, err error) {
	if s == nil || err == nil {
		return
	}
	obs.Error(s.service, "scheduler_task_error", map[string]any{
		"task":        strings.TrimSpace(task),
		"trigger":     strings.TrimSpace(trigger),
		"duration_ms": duration.Milliseconds(),
		"error":       err.Error(),
	})
}

func (s *taskScheduler) ResetTaskProfilesForStartup(names []string, startupUnix int64) error {
	if s == nil || len(names) == 0 {
		return nil
	}
	if s.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		filtered = append(filtered, name)
	}
	if len(filtered) == 0 {
		return nil
	}
	if startupUnix <= 0 {
		startupUnix = time.Now().Unix()
	}
	if s.store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(s.ctx, s.store, func(tx *gen.Tx) error {
		_, err := tx.ProcSchedulerTasks.Update().
			Where(procschedulertasks.TaskNameIn(filtered...)).
			SetStatus("stopped").
			SetUpdatedAtUnix(startupUnix).
			SetClosedAtUnix(startupUnix).
			SetLastTrigger("").
			SetLastStartedAtUnix(0).
			SetLastEndedAtUnix(0).
			SetLastDurationMs(0).
			SetLastError("").
			SetInFlight(0).
			SetLastSummaryJSON("{}").
			Save(s.ctx)
		return err
	})
}
