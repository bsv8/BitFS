package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"runtime"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
)

type sqlTraceContextKey string

const (
	sqlTraceContextRoundIDKey     sqlTraceContextKey = "sql_trace_round_id"
	sqlTraceContextTriggerKey     sqlTraceContextKey = "sql_trace_trigger"
	sqlTraceContextIntentKey      sqlTraceContextKey = "sql_trace_intent"
	sqlTraceContextStageKey       sqlTraceContextKey = "sql_trace_stage"
	sqlTraceContextCallerChainKey sqlTraceContextKey = "sql_trace_caller_chain"
)

var sqlTraceState struct {
	enabled atomic.Bool
	mgr     atomic.Pointer[sqlTraceManager]
}

type sqlTraceManager struct {
	mu sync.Mutex

	rootDir     string
	eventsPath  string
	summaryPath string
	file        *os.File
	ringSize    int
	ring        []sqliteactor.TraceEvent
	next        int
	count       int
}

type sqlTraceRoundSummary struct {
	RoundID               string                `json:"round_id"`
	TotalSQL              int                   `json:"total_sql"`
	TotalWrite            int                   `json:"total_write"`
	SlowTop               []sqlTraceTopItem     `json:"slow_top"`
	RepeatUpdateTop       []sqlTraceCounterItem `json:"repeat_update_top"`
	MostCommonCallerChain string                `json:"most_common_caller_chain"`
	UpdatedAtUnix         int64                 `json:"updated_at_unix"`
}

type sqlTraceTopItem struct {
	Operation      string   `json:"operation"`
	SQLFingerprint string   `json:"sql_fingerprint"`
	ElapsedMS      int64    `json:"elapsed_ms"`
	RowsAffected   int64    `json:"rows_affected"`
	Intent         string   `json:"intent,omitempty"`
	Stage          string   `json:"stage,omitempty"`
	CallerChain    []string `json:"caller_chain,omitempty"`
}

type sqlTraceCounterItem struct {
	SQLFingerprint string   `json:"sql_fingerprint"`
	Count          int      `json:"count"`
	Intent         string   `json:"intent,omitempty"`
	Stage          string   `json:"stage,omitempty"`
	CallerChain    []string `json:"caller_chain,omitempty"`
}

func sqlTraceEnabled() bool {
	return sqlTraceState.enabled.Load()
}

func initSQLTrace(logFile string, debug bool) (*sqlTraceManager, error) {
	if !debug {
		sqliteactor.SetTraceSink(nil)
		sqlTraceState.enabled.Store(false)
		sqlTraceState.mgr.Store(nil)
		return nil, nil
	}
	mgr, err := newSQLTraceManager(logFile, 4096)
	if err != nil {
		return nil, err
	}
	sqliteactor.SetTraceSink(mgr)
	sqlTraceState.mgr.Store(mgr)
	sqlTraceState.enabled.Store(true)
	return mgr, nil
}

func closeSQLTrace() error {
	mgr := sqlTraceState.mgr.Swap(nil)
	sqlTraceState.enabled.Store(false)
	sqliteactor.SetTraceSink(nil)
	if mgr == nil {
		return nil
	}
	return mgr.Close()
}

func currentSQLTraceManager() *sqlTraceManager {
	return sqlTraceState.mgr.Load()
}

func flushSQLTraceRoundSummary(roundID string) {
	mgr := currentSQLTraceManager()
	if mgr == nil {
		return
	}
	path, err := mgr.FlushRoundSummary(roundID)
	if err != nil {
		obs.Error("bitcast-client", "sql_trace_round_summary_flush_failed", map[string]any{
			"round_id": roundID,
			"error":    err.Error(),
		})
		return
	}
	if strings.TrimSpace(path) != "" {
		obs.Info("bitcast-client", "sql_trace_round_summary_flushed", map[string]any{
			"round_id": roundID,
			"path":     path,
		})
	}
}

func currentSQLTracePath() string {
	mgr := currentSQLTraceManager()
	if mgr == nil {
		return ""
	}
	return mgr.TracePath()
}

func currentSQLTraceSummaryPath() string {
	mgr := currentSQLTraceManager()
	if mgr == nil {
		return ""
	}
	return mgr.SummaryPath()
}

func newSQLTraceManager(logFile string, ringSize int) (*sqlTraceManager, error) {
	logFile = strings.TrimSpace(logFile)
	if logFile == "" {
		return nil, fmt.Errorf("sql trace log file is empty")
	}
	if ringSize <= 0 {
		ringSize = 1024
	}
	dir := filepath.Join(filepath.Dir(logFile), "sql_trace")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	eventsPath := filepath.Join(dir, "sql_trace.jsonl")
	f, err := os.OpenFile(eventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	mgr := &sqlTraceManager{
		rootDir:     dir,
		eventsPath:  eventsPath,
		summaryPath: filepath.Join(dir, "summary.json"),
		file:        f,
		ringSize:    ringSize,
		ring:        make([]sqliteactor.TraceEvent, 0, ringSize),
	}
	return mgr, nil
}

func (m *sqlTraceManager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file != nil {
		_ = m.file.Sync()
		_ = m.file.Close()
		m.file = nil
	}
	return nil
}

func (m *sqlTraceManager) Handle(ev sqliteactor.TraceEvent) {
	if m == nil {
		return
	}
	ev = normalizeSQLTraceEvent(ev)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file != nil {
		if b, err := json.Marshal(ev); err == nil {
			_, _ = m.file.Write(append(b, '\n'))
		}
	}
	if len(m.ring) < m.ringSize {
		m.ring = append(m.ring, ev)
	} else {
		m.ring[m.next] = ev
		m.next = (m.next + 1) % m.ringSize
	}
	if m.count < m.ringSize {
		m.count++
	}
	obs.SQL("bitcast-client", "sql_trace", map[string]any{
		"round_id":        ev.RoundID,
		"trigger":         ev.Trigger,
		"intent":          ev.Intent,
		"stage":           ev.Stage,
		"op":              ev.Operation,
		"sql_fingerprint": ev.SQLFingerprint,
		"elapsed_ms":      ev.ElapsedMS,
		"rows_affected":   ev.RowsAffected,
		"err":             ev.Err,
		"caller_chain":    ev.CallerChain,
		"sql":             ev.SQL,
		"args":            ev.Args,
	})
}

func (m *sqlTraceManager) TracePath() string {
	if m == nil {
		return ""
	}
	return m.rootDir
}

func (m *sqlTraceManager) SummaryPath() string {
	if m == nil {
		return ""
	}
	return m.summaryPath
}

func (m *sqlTraceManager) FlushRoundSummary(roundID string) (string, error) {
	if m == nil {
		return "", nil
	}
	roundID = strings.TrimSpace(roundID)
	if roundID == "" {
		return "", fmt.Errorf("round_id is empty")
	}
	summary := m.buildRoundSummary(roundID)
	b, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return "", err
	}
	b = append(b, '\n')
	path := filepath.Join(m.rootDir, "round_"+sanitizeTraceFilePart(roundID)+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	_ = os.WriteFile(m.summaryPath, b, 0o644)
	return path, nil
}

func (m *sqlTraceManager) buildRoundSummary(roundID string) sqlTraceRoundSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.snapshotLocked()
	filtered := make([]sqliteactor.TraceEvent, 0, len(items))
	for _, ev := range items {
		if strings.TrimSpace(ev.RoundID) == roundID {
			filtered = append(filtered, ev)
		}
	}
	summary := sqlTraceRoundSummary{
		RoundID:       roundID,
		UpdatedAtUnix: time.Now().Unix(),
	}
	if len(filtered) == 0 {
		return summary
	}
	totalWrite := 0
	callerCount := map[string]int{}
	updateCount := map[string]int{}
	updateMeta := map[string]sqliteactor.TraceEvent{}
	slowItems := make([]sqliteactor.TraceEvent, 0, len(filtered))
	for _, ev := range filtered {
		summary.TotalSQL++
		if strings.EqualFold(ev.Operation, "exec") {
			totalWrite++
		}
		if len(ev.CallerChain) > 0 {
			key := strings.Join(ev.CallerChain, " > ")
			callerCount[key]++
		}
		if strings.EqualFold(ev.Operation, "exec") && strings.HasPrefix(strings.TrimSpace(strings.ToLower(ev.SQL)), "update") {
			updateCount[ev.SQLFingerprint]++
			if _, ok := updateMeta[ev.SQLFingerprint]; !ok {
				updateMeta[ev.SQLFingerprint] = ev
			}
		}
		slowItems = append(slowItems, ev)
	}
	summary.TotalWrite = totalWrite
	sort.Slice(slowItems, func(i, j int) bool {
		if slowItems[i].ElapsedMS == slowItems[j].ElapsedMS {
			return slowItems[i].SQLFingerprint < slowItems[j].SQLFingerprint
		}
		return slowItems[i].ElapsedMS > slowItems[j].ElapsedMS
	})
	for i := 0; i < len(slowItems) && i < 5; i++ {
		ev := slowItems[i]
		summary.SlowTop = append(summary.SlowTop, sqlTraceTopItem{
			Operation:      ev.Operation,
			SQLFingerprint: ev.SQLFingerprint,
			ElapsedMS:      ev.ElapsedMS,
			RowsAffected:   ev.RowsAffected,
			Intent:         ev.Intent,
			Stage:          ev.Stage,
			CallerChain:    append([]string(nil), ev.CallerChain...),
		})
	}
	type pair struct {
		key   string
		count int
	}
	updates := make([]pair, 0, len(updateCount))
	for k, v := range updateCount {
		updates = append(updates, pair{key: k, count: v})
	}
	sort.Slice(updates, func(i, j int) bool {
		if updates[i].count == updates[j].count {
			return updates[i].key < updates[j].key
		}
		return updates[i].count > updates[j].count
	})
	for i := 0; i < len(updates) && i < 5; i++ {
		ev := updateMeta[updates[i].key]
		summary.RepeatUpdateTop = append(summary.RepeatUpdateTop, sqlTraceCounterItem{
			SQLFingerprint: updates[i].key,
			Count:          updates[i].count,
			Intent:         ev.Intent,
			Stage:          ev.Stage,
			CallerChain:    append([]string(nil), ev.CallerChain...),
		})
	}
	if len(callerCount) > 0 {
		type callerPair struct {
			key   string
			count int
		}
		list := make([]callerPair, 0, len(callerCount))
		for k, v := range callerCount {
			list = append(list, callerPair{key: k, count: v})
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].count == list[j].count {
				return list[i].key < list[j].key
			}
			return list[i].count > list[j].count
		})
		summary.MostCommonCallerChain = list[0].key
	}
	return summary
}

func (m *sqlTraceManager) snapshotLocked() []sqliteactor.TraceEvent {
	if m == nil || len(m.ring) == 0 {
		return nil
	}
	out := make([]sqliteactor.TraceEvent, 0, len(m.ring))
	if m.count < len(m.ring) {
		out = append(out, m.ring[:m.count]...)
		return out
	}
	out = append(out, m.ring[m.next:]...)
	out = append(out, m.ring[:m.next]...)
	return out
}

func normalizeSQLTraceEvent(ev sqliteactor.TraceEvent) sqliteactor.TraceEvent {
	ev.SQL = strings.TrimSpace(ev.SQL)
	ev.Operation = strings.ToLower(strings.TrimSpace(ev.Operation))
	ev.RoundID = strings.TrimSpace(ev.RoundID)
	ev.Trigger = strings.TrimSpace(ev.Trigger)
	ev.Intent = strings.TrimSpace(ev.Intent)
	ev.Stage = strings.TrimSpace(ev.Stage)
	if len(ev.CallerChain) > 0 {
		trimmed := make([]string, 0, len(ev.CallerChain))
		for _, item := range ev.CallerChain {
			item = strings.TrimSpace(item)
			if item != "" {
				trimmed = append(trimmed, item)
			}
		}
		ev.CallerChain = trimmed
	}
	return ev
}

func sanitizeTraceFilePart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_").Replace(s)
}

func sqlTraceContextWithMeta(ctx context.Context, roundID, trigger, intent, stage string) context.Context {
	if ctx == nil {
		return nil
	}
	if roundID != "" {
		ctx = context.WithValue(ctx, sqlTraceContextRoundIDKey, roundID)
	}
	if trigger != "" {
		ctx = context.WithValue(ctx, sqlTraceContextTriggerKey, trigger)
	}
	if intent != "" {
		ctx = context.WithValue(ctx, sqlTraceContextIntentKey, intent)
	}
	if stage != "" {
		ctx = context.WithValue(ctx, sqlTraceContextStageKey, stage)
	}
	return ctx
}

func sqlTraceScopeFromContext(ctx context.Context, callerChain []string) sqliteactor.TraceScope {
	if !sqlTraceEnabled() {
		return sqliteactor.TraceScope{}
	}
	if ctx == nil {
		return sqliteactor.TraceScope{}
	}
	scope := sqliteactor.TraceScope{
		RoundID: strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey))),
		Trigger: strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey))),
		Intent:  strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextIntentKey))),
		Stage:   strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextStageKey))),
	}
	if len(callerChain) > 0 {
		scope.CallerChain = append([]string(nil), callerChain...)
	}
	return scope
}

func sqlTraceScopeActive(scope sqliteactor.TraceScope) bool {
	return strings.TrimSpace(scope.RoundID) != "" ||
		strings.TrimSpace(scope.Trigger) != "" ||
		strings.TrimSpace(scope.Intent) != "" ||
		strings.TrimSpace(scope.Stage) != "" ||
		len(scope.CallerChain) > 0
}

func sqlTraceCaptureCallerChain() []string {
	if !sqlTraceEnabled() {
		return nil
	}
	pcs := make([]uintptr, 16)
	n := runtimeCallers(3, pcs)
	frames := runtimeCallersFrames(pcs[:n])
	out := make([]string, 0, 6)
	for {
		frame, more := frames.Next()
		if keepSQLTraceFrame(frame.File, frame.Function) {
			out = append(out, frame.Function)
		}
		if !more || len(out) >= 6 {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	if len(out) > 6 {
		out = out[:6]
	}
	return out
}

func keepSQLTraceFrame(file string, fn string) bool {
	file = strings.TrimSpace(file)
	if file == "" {
		return false
	}
	if strings.Contains(file, string(filepath.Separator)+"pkg"+string(filepath.Separator)+"mod"+string(filepath.Separator)) {
		return false
	}
	if strings.Contains(file, "/runtime/") || strings.Contains(file, "/src/runtime/") {
		return false
	}
	return strings.Contains(file, "/BitFS/") || strings.Contains(file, "/BFTP/") || strings.Contains(file, "/e2e/")
}

var runtimeCallers = runtime.Callers
var runtimeCallersFrames = runtime.CallersFrames
