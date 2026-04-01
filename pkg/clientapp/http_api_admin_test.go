package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandleAdminStrategyDebugLog(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = filepath.Join(vaultDir, "workspace")
	cfg.Storage.DataDir = filepath.Join(vaultDir, "data")
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(vaultDir, "data", "client-index.sqlite")
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.FSHTTP.StrategyDebugLogEnabled = false
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}

	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	rt.runIn.ConfigPath = configPath
	srv := &httpAPIServer{
		rt:  rt,
		cfg: &cfg,
		db:  db,
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/fs-http/strategy-debug-log", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminStrategyDebugLog(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("get status mismatch: got=%d want=%d", rec.Code, http.StatusOK)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode get response: %v", err)
		}
		if got, _ := body["strategy_debug_log_enabled"].(bool); got {
			t.Fatalf("expected disabled on get")
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/fs-http/strategy-debug-log", strings.NewReader(`{"enabled":true}`))
		rec := httptest.NewRecorder()
		srv.handleAdminStrategyDebugLog(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("post status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode post response: %v", err)
		}
		if got, _ := body["strategy_debug_log_enabled"].(bool); !got {
			t.Fatalf("expected enabled in post response")
		}
	}

	if !rt.runIn.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("cfg pointer should be updated")
	}
	if !rt.runIn.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("runtime config should be updated")
	}

	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if !loaded.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("config file should persist strategy_debug_log_enabled=true")
	}
}

func TestHandleAdminSchedulerTasks(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	store := newClientDB(db, nil)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	scheduler := ensureRuntimeTaskScheduler(rt, store)
	if scheduler == nil {
		t.Fatalf("scheduler not initialized")
	}
	taskCtx, taskCancel := context.WithCancel(context.Background())
	defer taskCancel()
	if err := scheduler.RegisterPeriodicTask(taskCtx, periodicTaskSpec{
		Name:      "workspace_tick",
		Owner:     "orchestrator",
		Mode:      "static",
		Interval:  time.Hour,
		Immediate: false,
		Run: func(ctx context.Context, trigger string) (map[string]any, error) {
			return map[string]any{"ok": true}, nil
		},
	}); err != nil {
		t.Fatalf("register workspace task: %v", err)
	}
	if err := scheduler.RegisterPeriodicTask(taskCtx, periodicTaskSpec{
		Name:      "listen_billing_tick:gw-1",
		Owner:     "listen_loop",
		Mode:      "dynamic",
		Interval:  time.Hour,
		Immediate: false,
		Run: func(ctx context.Context, trigger string) (map[string]any, error) {
			return map[string]any{"ok": true}, nil
		},
	}); err != nil {
		t.Fatalf("register listen task: %v", err)
	}
	if _, err := db.Exec(`UPDATE scheduler_tasks SET last_error='mock error', failure_count=2 WHERE task_name=?`, "listen_billing_tick:gw-1"); err != nil {
		t.Fatalf("update scheduler task mock error: %v", err)
	}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db, store: newClientDB(db, nil)}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/scheduler/tasks", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminSchedulerTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Summary map[string]any `json:"summary"`
		Items   []struct {
			Name      string `json:"name"`
			Owner     string `json:"owner"`
			Mode      string `json:"mode"`
			LastError string `json:"last_error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("item count mismatch: got=%d want=2", len(out.Items))
	}
	if got, ok := out.Summary["task_count"].(float64); !ok || int(got) != 2 {
		t.Fatalf("task_count mismatch: got=%v", out.Summary["task_count"])
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/scheduler/tasks?mode=dynamic&owner=listen_loop&name_prefix=listen_billing_tick:&has_error=true", nil)
	srv.handleAdminSchedulerTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("filtered status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	out = struct {
		Summary map[string]any `json:"summary"`
		Items   []struct {
			Name      string `json:"name"`
			Owner     string `json:"owner"`
			Mode      string `json:"mode"`
			LastError string `json:"last_error"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode filtered response: %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("filtered item count mismatch: got=%d want=1", len(out.Items))
	}
	if out.Items[0].Name != "listen_billing_tick:gw-1" {
		t.Fatalf("filtered task mismatch: got=%s", out.Items[0].Name)
	}
	if out.Items[0].Mode != "dynamic" || out.Items[0].Owner != "listen_loop" {
		t.Fatalf("filtered task fields mismatch: mode=%s owner=%s", out.Items[0].Mode, out.Items[0].Owner)
	}
	if strings.TrimSpace(out.Items[0].LastError) == "" {
		t.Fatalf("expected filtered task has error")
	}
}

func TestHandleAdminSchedulerTasksDefaultOrder(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	now := time.Now().Unix()
	_, err = db.Exec(
		`INSERT INTO scheduler_tasks(
			task_name,owner,mode,status,interval_seconds,created_at_unix,updated_at_unix,closed_at_unix,
			last_trigger,last_started_at_unix,last_ended_at_unix,last_duration_ms,last_error,in_flight,
			run_count,success_count,failure_count,last_summary_json,meta_json
		) VALUES
		('active_b','o','static','active',10,?,?,0,'',100,0,0,'',0,0,0,0,'{}','{}'),
		('active_a','o','static','active',10,?,?,0,'',200,0,0,'',0,0,0,0,'{}','{}'),
		('stopped_old','o','dynamic','stopped',10,?,?,150,'',0,0,0,'',0,0,0,0,'{}','{}'),
		('stopped_new','o','dynamic','stopped',10,?,?,300,'',0,0,0,'',0,0,0,0,'{}','{}')`,
		now, now, now, now, now, now, now, now,
	)
	if err != nil {
		t.Fatalf("seed scheduler tasks: %v", err)
	}
	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/scheduler/tasks", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminSchedulerTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out.Items) < 4 {
		t.Fatalf("item count mismatch: got=%d want>=4", len(out.Items))
	}
	got := []string{out.Items[0].Name, out.Items[1].Name, out.Items[2].Name, out.Items[3].Name}
	want := []string{"active_a", "active_b", "stopped_new", "stopped_old"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("default order mismatch: got=%v want=%v", got, want)
	}
}

func TestHandleAdminSchedulerRuns(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO scheduler_task_runs(task_name,owner,mode,trigger,started_at_unix,ended_at_unix,duration_ms,status,error_message,summary_json,created_at_unix)
		 VALUES
		 ('workspace_tick','orchestrator','static','periodic_tick',100,101,1000,'success','','{"trigger":"periodic_tick"}',101),
		 ('live_follow_tick:abc','live_follow','dynamic','periodic_tick',200,201,1000,'failed','mock err','{"stream_id":"abc"}',201)`,
	)
	if err != nil {
		t.Fatalf("seed scheduler runs: %v", err)
	}
	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/scheduler/runs?status=failed&mode=dynamic", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminSchedulerRuns(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			TaskName string `json:"task_name"`
			Status   string `json:"status"`
			Mode     string `json:"mode"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Total != 1 || len(out.Items) != 1 {
		t.Fatalf("runs count mismatch: total=%d items=%d", out.Total, len(out.Items))
	}
	if out.Items[0].TaskName != "live_follow_tick:abc" || out.Items[0].Status != "failed" || out.Items[0].Mode != "dynamic" {
		t.Fatalf("run item mismatch: %+v", out.Items[0])
	}
}

func TestHandleAdminClientKernelCommands(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	dbAppendCommandJournal(nil, newClientDB(db, nil), commandJournalEntry{
		CommandID:     "ck_1",
		CommandType:   clientKernelCommandWorkspaceSync,
		GatewayPeerID: "workspace",
		AggregateID:   "workspace:default",
		RequestedBy:   "test",
		RequestedAt:   time.Now().Unix(),
		Accepted:      true,
		Status:        "applied",
		StateBefore:   "workspace_sync",
		StateAfter:    "workspace_sync",
		Payload:       map[string]any{"trigger": "manual"},
		Result:        map[string]any{"seed_count": 1},
	})
	dbAppendCommandJournal(nil, newClientDB(db, nil), commandJournalEntry{
		CommandID:     "fp_1",
		CommandType:   "fee_pool_internal_only",
		GatewayPeerID: "gw",
		AggregateID:   "gateway:gw",
		RequestedBy:   "test",
		RequestedAt:   time.Now().Unix(),
		Accepted:      true,
		Status:        "applied",
		StateBefore:   "active",
		StateAfter:    "active",
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/client-kernel/commands?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminClientKernelCommands(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			ID          int64  `json:"id"`
			CommandID   string `json:"command_id"`
			CommandType string `json:"command_type"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if out.Total != 1 || len(out.Items) != 1 {
		t.Fatalf("client-kernel list mismatch: total=%d items=%d", out.Total, len(out.Items))
	}
	if out.Items[0].CommandID != "ck_1" || out.Items[0].CommandType != clientKernelCommandWorkspaceSync {
		t.Fatalf("list content mismatch: id=%s type=%s", out.Items[0].CommandID, out.Items[0].CommandType)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/client-kernel/commands?command_type=fee_pool_internal_only", nil)
	badRec := httptest.NewRecorder()
	srv.handleAdminClientKernelCommands(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("bad command_type status mismatch: got=%d want=%d body=%s", badRec.Code, http.StatusBadRequest, badRec.Body.String())
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/client-kernel/commands/detail?id="+itoa64(out.Items[0].ID), nil)
	detailRec := httptest.NewRecorder()
	srv.handleAdminClientKernelCommandDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}

	var feePoolID int64
	if err := db.QueryRow(`SELECT id FROM command_journal WHERE command_id='fp_1'`).Scan(&feePoolID); err != nil {
		t.Fatalf("query feepool row id failed: %v", err)
	}
	notFoundReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/client-kernel/commands/detail?id="+itoa64(feePoolID), nil)
	notFoundRec := httptest.NewRecorder()
	srv.handleAdminClientKernelCommandDetail(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("non-client-kernel detail status mismatch: got=%d want=%d body=%s", notFoundRec.Code, http.StatusNotFound, notFoundRec.Body.String())
	}
}

func TestHandleAdminOrchestratorLogs(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	dbAppendOrchestratorLog(nil, newClientDB(db, nil), orchestratorLogEntry{
		EventType:      "signal_received",
		Source:         "workspace_worker",
		SignalType:     "workspace.tick",
		AggregateKey:   "workspace:default",
		IdempotencyKey: "workspace_sync:123456",
		CommandType:    clientKernelCommandWorkspaceSync,
		GatewayPeerID:  "16Uiu2HAm9hV4Nj8k8rZcZqWqQPKw28Y61S7",
		TaskStatus:     "queued",
		RetryCount:     0,
		QueueLength:    1,
		Payload:        map[string]any{"trigger": "periodic_tick"},
	})
	dbAppendOrchestratorLog(nil, newClientDB(db, nil), orchestratorLogEntry{
		EventType:      "task_dispatch_result",
		Source:         "orchestrator",
		SignalType:     "workspace.tick",
		AggregateKey:   "workspace:default",
		IdempotencyKey: "workspace_sync:123456",
		CommandType:    clientKernelCommandWorkspaceSync,
		GatewayPeerID:  "16Uiu2HAm9hV4Nj8k8rZcZqWqQPKw28Y61S7",
		TaskStatus:     "applied",
		RetryCount:     0,
		QueueLength:    0,
		Payload:        map[string]any{"duration_ms": 12},
	})

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orchestrator/logs?event_type=signal_received&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminOrchestratorLogs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			EventID        string `json:"event_id"`
			StepsCount     int    `json:"steps_count"`
			LatestEvent    string `json:"latest_event_type"`
			IdempotencyKey string `json:"idempotency_key"`
			GatewayPeerID  string `json:"gateway_pubkey_hex"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if out.Total != 1 || len(out.Items) != 1 {
		t.Fatalf("list mismatch: total=%d items=%d", out.Total, len(out.Items))
	}
	if out.Items[0].EventID != "workspace_sync:123456" {
		t.Fatalf("event_id mismatch: %s", out.Items[0].EventID)
	}
	if out.Items[0].StepsCount != 1 {
		t.Fatalf("steps_count mismatch: %d", out.Items[0].StepsCount)
	}
	if out.Items[0].LatestEvent != "signal_received" {
		t.Fatalf("latest_event_type mismatch: %s", out.Items[0].LatestEvent)
	}
	if out.Items[0].IdempotencyKey != "workspace_sync:123456" {
		t.Fatalf("idempotency_key mismatch: %s", out.Items[0].IdempotencyKey)
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orchestrator/logs/detail?event_id="+url.QueryEscape(out.Items[0].EventID), nil)
	detailRec := httptest.NewRecorder()
	srv.handleAdminOrchestratorLogDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detail struct {
		EventID        string `json:"event_id"`
		LatestEvent    string `json:"latest_event_type"`
		IdempotencyKey string `json:"idempotency_key"`
		StepsCount     int    `json:"steps_count"`
		Steps          []struct {
			EventType string `json:"event_type"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail response: %v", err)
	}
	if detail.EventID != out.Items[0].EventID {
		t.Fatalf("detail event_id mismatch: %s", detail.EventID)
	}
	if detail.LatestEvent != "task_dispatch_result" {
		t.Fatalf("detail latest_event_type mismatch: %s", detail.LatestEvent)
	}
	if detail.StepsCount != 2 || len(detail.Steps) != 2 {
		t.Fatalf("detail steps mismatch: steps_count=%d len=%d", detail.StepsCount, len(detail.Steps))
	}
	if detail.Steps[0].EventType != "signal_received" || detail.Steps[1].EventType != "task_dispatch_result" {
		t.Fatalf("detail step order mismatch: first=%s second=%s", detail.Steps[0].EventType, detail.Steps[1].EventType)
	}
	if detail.IdempotencyKey != "workspace_sync:123456" {
		t.Fatalf("detail idempotency_key mismatch: %s", detail.IdempotencyKey)
	}
}

func TestHandleAdminConfigUpdateValidation(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = filepath.Join(vaultDir, "workspace")
	cfg.Storage.DataDir = filepath.Join(vaultDir, "data")
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(vaultDir, "data", "client-index.sqlite")
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, "")}
	rt.runIn.ConfigPath = configPath
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}

	schemaReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/config/schema", nil)
	schemaRec := httptest.NewRecorder()
	srv.handleAdminConfigSchema(schemaRec, schemaReq)
	if schemaRec.Code != http.StatusOK {
		t.Fatalf("schema status mismatch: got=%d want=%d body=%s", schemaRec.Code, http.StatusOK, schemaRec.Body.String())
	}
	var schemaBody struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(schemaRec.Body.Bytes(), &schemaBody); err != nil {
		t.Fatalf("decode schema response: %v", err)
	}
	hasKey := map[string]bool{}
	for _, it := range schemaBody.Items {
		hasKey[it.Key] = true
	}
	for _, key := range []string{
		"listen.enabled",
		"listen.renew_threshold_seconds",
		"listen.auto_renew_rounds",
		"listen.offer_payment_satoshi",
		"listen.tick_seconds",
		"payment.preferred_scheme",
		"reachability.auto_announce_enabled",
		"reachability.announce_ttl_seconds",
	} {
		if !hasKey[key] {
			t.Fatalf("schema missing key: %s", key)
		}
	}

	// validate_only: 不应落库。
	validateReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config", strings.NewReader(`{
		"validate_only": true,
		"items": [{"key":"scan.rescan_interval_seconds","value":30}]
	}`))
	validateRec := httptest.NewRecorder()
	srv.handleAdminConfig(validateRec, validateReq)
	if validateRec.Code != http.StatusOK {
		t.Fatalf("validate_only status mismatch: got=%d want=%d body=%s", validateRec.Code, http.StatusOK, validateRec.Body.String())
	}
	if rt.runIn.Scan.RescanIntervalSeconds == 30 {
		t.Fatalf("validate_only should not mutate runtime config")
	}

	updateReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config", strings.NewReader(`{
		"items": [
			{"key":"http.listen_addr","value":"127.0.0.1:19999"},
			{"key":"listen.enabled","value":false},
			{"key":"listen.renew_threshold_seconds","value":77},
			{"key":"listen.auto_renew_rounds","value":12345},
			{"key":"listen.offer_payment_satoshi","value":888},
			{"key":"listen.tick_seconds","value":9},
			{"key":"payment.preferred_scheme","value":"chain_tx_v1"},
			{"key":"reachability.auto_announce_enabled","value":false},
			{"key":"reachability.announce_ttl_seconds","value":7200},
			{"key":"scan.rescan_interval_seconds","value":120},
			{"key":"seller.pricing.resale_discount_ratio","value":0.75}
		]
	}`))
	updateRec := httptest.NewRecorder()
	srv.handleAdminConfig(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status mismatch: got=%d want=%d body=%s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	if rt.runIn.HTTP.ListenAddr != "127.0.0.1:19999" {
		t.Fatalf("http.listen_addr not updated: %s", rt.runIn.HTTP.ListenAddr)
	}
	if rt.runIn.Scan.RescanIntervalSeconds != 120 {
		t.Fatalf("scan interval not updated: %d", rt.runIn.Scan.RescanIntervalSeconds)
	}
	if cfgBool(rt.runIn.Listen.Enabled, true) {
		t.Fatalf("listen.enabled not updated: got=true want=false")
	}
	if rt.runIn.Listen.RenewThresholdSeconds != 77 {
		t.Fatalf("listen.renew_threshold_seconds not updated: %d", rt.runIn.Listen.RenewThresholdSeconds)
	}
	if rt.runIn.Listen.AutoRenewRounds != 12345 {
		t.Fatalf("listen.auto_renew_rounds not updated: %d", rt.runIn.Listen.AutoRenewRounds)
	}
	if rt.runIn.Listen.OfferPaymentSatoshi != 888 {
		t.Fatalf("listen.offer_payment_satoshi not updated: %d", rt.runIn.Listen.OfferPaymentSatoshi)
	}
	if rt.runIn.Listen.TickSeconds != 9 {
		t.Fatalf("listen.tick_seconds not updated: %d", rt.runIn.Listen.TickSeconds)
	}
	if rt.runIn.Payment.PreferredScheme != "chain_tx_v1" {
		t.Fatalf("payment.preferred_scheme not updated: %s", rt.runIn.Payment.PreferredScheme)
	}
	if cfgBool(rt.runIn.Reachability.AutoAnnounceEnabled, true) {
		t.Fatalf("reachability.auto_announce_enabled not updated: got=true want=false")
	}
	if rt.runIn.Reachability.AnnounceTTLSeconds != 7200 {
		t.Fatalf("reachability.announce_ttl_seconds not updated: %d", rt.runIn.Reachability.AnnounceTTLSeconds)
	}
	if rt.runIn.Seller.Pricing.ResaleDiscountBPS != 7500 {
		t.Fatalf("resale_discount_bps mismatch: got=%d want=7500", rt.runIn.Seller.Pricing.ResaleDiscountBPS)
	}

	badReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config", strings.NewReader(`{
		"items": [{"key":"scan.rescan_interval_seconds","value":1}]
	}`))
	badRec := httptest.NewRecorder()
	srv.handleAdminConfig(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("bad bound status mismatch: got=%d want=%d body=%s", badRec.Code, http.StatusBadRequest, badRec.Body.String())
	}
}

func TestHandleLiveAPIFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	pubHost, _ := newSecpHost(t)
	defer pubHost.Close()
	subHost, _ := newSecpHost(t)
	defer subHost.Close()

	pubCfg := Config{}
	pubCfg.Storage.WorkspaceDir = t.TempDir()
	pubCfg.Storage.DataDir = t.TempDir()
	pubCfg.Index.Backend = "sqlite"
	pubCfg.Index.SQLitePath = ":memory:"
	pubCfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	pubCfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	pubCfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&pubCfg); err != nil {
		t.Fatalf("apply defaults pub: %v", err)
	}
	subCfg := pubCfg
	subCfg.Storage.WorkspaceDir = t.TempDir()
	subCfg.Storage.DataDir = t.TempDir()

	pubRT := &Runtime{Host: pubHost, runIn: NewRunInputFromConfig(pubCfg, ""), live: newLiveRuntime()}
	subRT := &Runtime{Host: subHost, runIn: NewRunInputFromConfig(subCfg, ""), live: newLiveRuntime()}
	pubStore := newClientDB(db, nil)
	subStore := newClientDB(db, nil)
	pubRT.kernel = newClientKernel(pubRT, pubStore)
	subRT.kernel = newClientKernel(subRT, subStore)
	registerLiveHandlers(nil, pubRT)
	registerLiveHandlers(subStore, subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)

	pubSrv := &httpAPIServer{rt: pubRT, cfg: &pubCfg}
	subSrv := &httpAPIServer{rt: subRT, cfg: &subCfg, db: db, store: newClientDB(db, nil)}

	streamID := strings.Repeat("ab", 32)

	reqURI := httptest.NewRequest(http.MethodGet, "/api/v1/live/subscribe-uri?stream_id="+streamID, nil)
	recURI := httptest.NewRecorder()
	pubSrv.handleLiveSubscribeURI(recURI, reqURI)
	if recURI.Code != http.StatusOK {
		t.Fatalf("subscribe-uri status: got=%d body=%s", recURI.Code, recURI.Body.String())
	}
	var uriResp map[string]any
	if err := json.Unmarshal(recURI.Body.Bytes(), &uriResp); err != nil {
		t.Fatalf("decode subscribe-uri: %v", err)
	}
	subscribeURI, _ := uriResp["subscribe_uri"].(string)
	if strings.TrimSpace(subscribeURI) == "" {
		t.Fatalf("subscribe_uri missing")
	}

	reqSub := httptest.NewRequest(http.MethodPost, "/api/v1/live/subscribe", strings.NewReader(`{"stream_uri":"`+subscribeURI+`","window":5}`))
	recSub := httptest.NewRecorder()
	subSrv.handleLiveSubscribe(recSub, reqSub)
	if recSub.Code != http.StatusOK {
		t.Fatalf("subscribe status: got=%d body=%s", recSub.Code, recSub.Body.String())
	}

	reqPub := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/latest", strings.NewReader(`{"stream_id":"`+streamID+`","recent_segments":[{"segment_index":7,"seed_hash":"`+strings.Repeat("cd", 32)+`"},{"segment_index":8,"seed_hash":"`+strings.Repeat("ef", 32)+`"}]}`))
	recPub := httptest.NewRecorder()
	pubSrv.handleLivePublishLatest(recPub, reqPub.WithContext(context.Background()))
	if recPub.Code != http.StatusOK {
		t.Fatalf("publish latest status: got=%d body=%s", recPub.Code, recPub.Body.String())
	}

	reqLatest := httptest.NewRequest(http.MethodGet, "/api/v1/live/latest?stream_id="+streamID, nil)
	recLatest := httptest.NewRecorder()
	subSrv.handleLiveLatest(recLatest, reqLatest)
	if recLatest.Code != http.StatusOK {
		t.Fatalf("live latest status: got=%d body=%s", recLatest.Code, recLatest.Body.String())
	}
	var latest LiveSubscriberSnapshot
	if err := json.Unmarshal(recLatest.Body.Bytes(), &latest); err != nil {
		t.Fatalf("decode latest: %v", err)
	}
	if len(latest.RecentSegments) != 2 {
		t.Fatalf("unexpected recent segment count: %d", len(latest.RecentSegments))
	}
	if latest.RecentSegments[1].SegmentIndex != 8 {
		t.Fatalf("unexpected latest segment index: %d", latest.RecentSegments[1].SegmentIndex)
	}

	reqPlan := httptest.NewRequest(http.MethodPost, "/api/v1/live/plan", strings.NewReader(`{"stream_id":"`+streamID+`","have_segment_index":6}`))
	recPlan := httptest.NewRecorder()
	subSrv.handleLivePlan(recPlan, reqPlan)
	if recPlan.Code != http.StatusOK {
		t.Fatalf("live plan status: got=%d body=%s", recPlan.Code, recPlan.Body.String())
	}

	if _, err := db.Exec(`INSERT INTO live_quotes(demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix,created_at_unix)
		VALUES(?,?,?,?,?,?,?)`,
		"ldmd_http",
		"seller-live",
		streamID,
		8,
		`[{"segment_index":8,"seed_hash":"`+strings.Repeat("ef", 32)+`"}]`,
		time.Now().Add(time.Minute).Unix(),
		time.Now().Unix(),
	); err != nil {
		t.Fatalf("insert live quote: %v", err)
	}
	reqQuotes := httptest.NewRequest(http.MethodGet, "/api/v1/live/quotes?demand_id=ldmd_http", nil)
	recQuotes := httptest.NewRecorder()
	subSrv.handleLiveQuotes(recQuotes, reqQuotes)
	if recQuotes.Code != http.StatusOK {
		t.Fatalf("live quotes status: got=%d body=%s", recQuotes.Code, recQuotes.Body.String())
	}
}

func TestHandleLivePublishSegmentFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	h, _ := newSecpHost(t)
	defer h.Close()

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = dbPath
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	workspace := &workspaceManager{cfg: &cfg, db: db, catalog: &sellerCatalog{seeds: map[string]sellerSeed{}}}
	if err := workspace.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	rt := &Runtime{Host: h, runIn: NewRunInputFromConfig(cfg, ""), Workspace: workspace, live: newLiveRuntime()}
	registerLiveHandlers(newClientDB(db, nil), rt)
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db, workspace: workspace}

	req0 := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/segment", strings.NewReader(`{
		"duration_ms": 2000,
		"mime_type": "video/mp2t",
		"media_bytes": "c2VnLTA="
	}`))
	rec0 := httptest.NewRecorder()
	srv.handleLivePublishSegment(rec0, req0)
	if rec0.Code != http.StatusOK {
		t.Fatalf("publish first segment status: got=%d body=%s", rec0.Code, rec0.Body.String())
	}
	var resp0 map[string]any
	if err := json.Unmarshal(rec0.Body.Bytes(), &resp0); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	streamID, _ := resp0["stream_id"].(string)
	seed0, _ := resp0["seed_hash"].(string)
	if !isSeedHashHex(streamID) || !isSeedHashHex(seed0) {
		t.Fatalf("invalid stream_id/seed_hash: stream=%q seed=%q", streamID, seed0)
	}
	if streamID != seed0 {
		t.Fatalf("first stream_id should equal first seed_hash: stream=%s seed=%s", streamID, seed0)
	}

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/segment", strings.NewReader(`{
		"stream_id": "`+streamID+`",
		"duration_ms": 3500,
		"is_discontinuity": true,
		"is_end": true,
		"mime_type": "video/mp2t",
		"init_seed_hash": "`+strings.Repeat("d", 64)+`",
		"playlist_uri_hint": "/custom/live/1.ts",
		"media_bytes": "c2VnLTE="
	}`))
	rec1 := httptest.NewRecorder()
	srv.handleLivePublishSegment(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("publish second segment status: got=%d body=%s", rec1.Code, rec1.Body.String())
	}

	latestReq := httptest.NewRequest(http.MethodGet, "/api/v1/live/latest?stream_id="+streamID, nil)
	latestRec := httptest.NewRecorder()
	srv.handleLiveLatest(latestRec, latestReq)
	if latestRec.Code != http.StatusOK {
		t.Fatalf("live latest status: got=%d body=%s", latestRec.Code, latestRec.Body.String())
	}
	var latest LiveSubscriberSnapshot
	if err := json.Unmarshal(latestRec.Body.Bytes(), &latest); err != nil {
		t.Fatalf("decode latest: %v", err)
	}
	if len(latest.RecentSegments) != 2 {
		t.Fatalf("unexpected recent segment count: %d", len(latest.RecentSegments))
	}

	var outPath string
	if err := db.QueryRow(`SELECT path FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix DESC LIMIT 1`, "%"+streamID+"%").Scan(&outPath); err != nil {
		t.Fatalf("query live segment path: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("live segment output missing: %v", err)
	}
}

func TestHandleLiveFollowFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	pubHost, _ := newSecpHost(t)
	defer pubHost.Close()
	subHost, _ := newSecpHost(t)
	defer subHost.Close()

	pubCfg := Config{}
	pubCfg.Storage.WorkspaceDir = t.TempDir()
	pubCfg.Storage.DataDir = t.TempDir()
	pubCfg.Index.Backend = "sqlite"
	pubCfg.Index.SQLitePath = dbPath
	pubCfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	pubCfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	pubCfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&pubCfg); err != nil {
		t.Fatalf("apply defaults pub: %v", err)
	}
	pubCfg.Live.Publish.BroadcastIntervalSec = 1
	subCfg := pubCfg
	subCfg.Storage.WorkspaceDir = t.TempDir()
	subCfg.Storage.DataDir = t.TempDir()
	streamID := strings.Repeat("ab", 32)
	subWorkspace := &workspaceManager{cfg: &subCfg, db: db}
	if err := subWorkspace.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}

	pubStore := newClientDB(db, nil)
	subStore := newClientDB(db, nil)
	pubRT := &Runtime{Host: pubHost, runIn: NewRunInputFromConfig(pubCfg, ""), live: newLiveRuntime()}
	subRT := &Runtime{Host: subHost, runIn: NewRunInputFromConfig(subCfg, ""), Workspace: subWorkspace, live: newLiveRuntime()}
	subRT.kernel = newClientKernel(subRT, subStore)
	registerLiveHandlers(nil, pubRT)
	registerLiveHandlers(subStore, subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)
	subRT.live.autoBuyFn = func(_ context.Context, _ *clientDB, _ *Runtime, decision LivePurchaseDecision, _ LiveSubscriberSnapshot) (liveAutoBuyResult, error) {
		outPath, err := subWorkspace.SelectLiveSegmentOutputPath(streamID, decision.TargetSegmentIndex, 1)
		if err != nil {
			return liveAutoBuyResult{}, err
		}
		return liveAutoBuyResult{
			SeedHash:       decision.SeedHash,
			SegmentIndex:   decision.TargetSegmentIndex,
			OutputFilePath: outPath,
		}, nil
	}

	pubSrv := &httpAPIServer{rt: pubRT, cfg: &pubCfg, store: pubStore}
	subSrv := &httpAPIServer{rt: subRT, cfg: &subCfg, store: subStore}
	reqURI := httptest.NewRequest(http.MethodGet, "/api/v1/live/subscribe-uri?stream_id="+streamID, nil)
	recURI := httptest.NewRecorder()
	pubSrv.handleLiveSubscribeURI(recURI, reqURI)
	var uriResp map[string]any
	if err := json.Unmarshal(recURI.Body.Bytes(), &uriResp); err != nil {
		t.Fatalf("decode subscribe-uri: %v", err)
	}
	subscribeURI, _ := uriResp["subscribe_uri"].(string)

	reqStart := httptest.NewRequest(http.MethodPost, "/api/v1/live/follow/start", strings.NewReader(`{"stream_uri":"`+subscribeURI+`"}`))
	recStart := httptest.NewRecorder()
	subSrv.handleLiveFollowStart(recStart, reqStart)
	if recStart.Code != http.StatusOK {
		t.Fatalf("follow start status: got=%d body=%s", recStart.Code, recStart.Body.String())
	}

	reqPub := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/latest", strings.NewReader(`{"stream_id":"`+streamID+`","recent_segments":[{"segment_index":3,"seed_hash":"`+strings.Repeat("cd", 32)+`","published_at_unix":1700000000}]}`))
	recPub := httptest.NewRecorder()
	pubSrv.handleLivePublishLatest(recPub, reqPub)
	if recPub.Code != http.StatusOK {
		t.Fatalf("publish latest status: got=%d body=%s", recPub.Code, recPub.Body.String())
	}

	var st LiveFollowStatus
	deadline := time.Now().Add(3 * time.Second)
	for {
		reqStatus := httptest.NewRequest(http.MethodGet, "/api/v1/live/follow/status?stream_id="+streamID, nil)
		recStatus := httptest.NewRecorder()
		subSrv.handleLiveFollowStatus(recStatus, reqStatus)
		if recStatus.Code == http.StatusOK {
			if err := json.Unmarshal(recStatus.Body.Bytes(), &st); err != nil {
				t.Fatalf("decode status: %v", err)
			}
			if st.HaveSegmentIndex >= 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("follow status did not progress: %+v", st)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st.HaveSegmentIndex < 0 {
		t.Fatalf("expected progressed have_segment_index")
	}
	if st.LastBoughtSeedHash != strings.Repeat("cd", 32) {
		t.Fatalf("unexpected bought seed hash: %s", st.LastBoughtSeedHash)
	}

	reqStop := httptest.NewRequest(http.MethodPost, "/api/v1/live/follow/stop", strings.NewReader(`{"stream_id":"`+streamID+`"}`))
	recStop := httptest.NewRecorder()
	subSrv.handleLiveFollowStop(recStop, reqStop)
	if recStop.Code != http.StatusOK {
		t.Fatalf("follow stop status: got=%d body=%s", recStop.Code, recStop.Body.String())
	}

	subRT.live = newLiveRuntime()
	loaded, err := TriggerLiveFollowStatus(newClientDB(db, nil), subRT, streamID)
	if err != nil {
		t.Fatalf("load persisted follow status failed: %v", err)
	}
	if loaded.HaveSegmentIndex != st.HaveSegmentIndex {
		t.Fatalf("persisted have_segment_index mismatch: got=%d want=%d", loaded.HaveSegmentIndex, st.HaveSegmentIndex)
	}
	if !strings.Contains(loaded.LastOutputFilePath, string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)) {
		t.Fatalf("unexpected live segment output path: %s", loaded.LastOutputFilePath)
	}
}
