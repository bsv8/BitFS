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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	cfg := Config{}
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

	rt := newRuntimeForTest(t, cfg, "", withRuntimeConfigPath(configPath))
	srv := &httpAPIServer{
		rt:        rt,
		cfgSource: staticConfigSnapshot(cfg),
		db:        db,
		store:     newClientDB(db, nil),
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

	if !rt.ConfigSnapshot().FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("cfg pointer should be updated")
	}
	if !rt.ConfigSnapshot().FSHTTP.StrategyDebugLogEnabled {
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	store := newClientDB(db, nil)
	rt := newRuntimeForTest(t, cfg, "")
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
	if _, err := db.Exec(`UPDATE proc_scheduler_tasks SET last_error='mock error', failure_count=2 WHERE task_name=?`, "listen_billing_tick:gw-1"); err != nil {
		t.Fatalf("update scheduler task mock error: %v", err)
	}
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}

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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	now := time.Now().Unix()
	_, err = db.Exec(
		`INSERT INTO proc_scheduler_tasks(
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
	cfg.Storage.DataDir = t.TempDir()
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, "")
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO proc_scheduler_task_runs(task_name,owner,mode,trigger,started_at_unix,ended_at_unix,duration_ms,status,error_message,summary_json,created_at_unix)
		 VALUES
		 ('workspace_tick','orchestrator','static','periodic_tick',100,101,1000,'success','','{"trigger":"periodic_tick"}',101),
		 ('live_follow_tick:abc','live_follow','dynamic','periodic_tick',200,201,1000,'failed','mock err','{"stream_id":"abc"}',201)`,
	)
	if err != nil {
		t.Fatalf("seed scheduler runs: %v", err)
	}
	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, "")
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	dbAppendOrchestratorLog(context.Background(), newClientDB(db, nil), orchestratorLogEntry{
		EventType:      "signal_received",
		Source:         "workspace_worker",
		SignalType:     "workspace.tick",
		AggregateKey:   "workspace_sync:123456",
		IdempotencyKey: "workspace_sync:123456",
		CommandType:    "workspace_sync",
		GatewayPeerID:  "16Uiu2HAm9hV4Nj8k8rZcZqWqQPKw28Y61S7",
		TaskStatus:     "queued",
		RetryCount:     0,
		QueueLength:    1,
		Payload:        map[string]any{"trigger": "periodic_tick"},
	})
	dbAppendOrchestratorLog(context.Background(), newClientDB(db, nil), orchestratorLogEntry{
		EventType:      "task_dispatch_result",
		Source:         "orchestrator",
		SignalType:     "workspace.tick",
		AggregateKey:   "workspace_sync:123456",
		IdempotencyKey: "workspace_sync:123456",
		CommandType:    "workspace_sync",
		GatewayPeerID:  "16Uiu2HAm9hV4Nj8k8rZcZqWqQPKw28Y61S7",
		TaskStatus:     "applied",
		RetryCount:     0,
		QueueLength:    0,
		Payload:        map[string]any{"duration_ms": 12},
	})

	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, "")
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orchestrator/logs?event_type=signal_received&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminOrchestratorLogs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			EventID       string `json:"event_id"`
			StepsCount    int    `json:"steps_count"`
			LatestEvent   string `json:"latest_event_type"`
			GatewayPeerID string `json:"gateway_pubkey_hex"`
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
	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orchestrator/logs/detail?event_id="+url.QueryEscape(out.Items[0].EventID), nil)
	detailRec := httptest.NewRecorder()
	srv.handleAdminOrchestratorLogDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detail struct {
		EventID     string `json:"event_id"`
		LatestEvent string `json:"latest_event_type"`
		StepsCount  int    `json:"steps_count"`
		Steps       []struct {
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	cfg := Config{}
	cfg.Storage.DataDir = filepath.Join(vaultDir, "data")
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(vaultDir, "data", "client-index.sqlite")
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, "", withRuntimeConfigPath(configPath))
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}

	schemaReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/config/schema", nil)
	schemaRec := httptest.NewRecorder()
	srv.handleAdminConfigSchema(schemaRec, schemaReq)
	if schemaRec.Code != http.StatusOK {
		t.Fatalf("schema status mismatch: got=%d want=%d body=%s", schemaRec.Code, http.StatusOK, schemaRec.Body.String())
	}
	var schemaBody struct {
		Items []struct {
			Key             string `json:"key"`
			RestartRequired bool   `json:"restart_required"`
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
		"http.listen_addr",
		"fs_http.listen_addr",
		"listen.enabled",
		"listen.renew_threshold_seconds",
		"listen.auto_renew_rounds",
		"listen.offer_payment_satoshi",
		"listen.tick_seconds",
		"payment.preferred_scheme",
		"reachability.auto_announce_enabled",
		"reachability.announce_ttl_seconds",
		"storage.data_dir",
		"index.sqlite_path",
	} {
		if !hasKey[key] {
			t.Fatalf("schema missing key: %s", key)
		}
	}
	for _, it := range schemaBody.Items {
		if it.Key == "http.listen_addr" || it.Key == "fs_http.listen_addr" || it.Key == "storage.data_dir" || it.Key == "index.sqlite_path" {
			if !it.RestartRequired {
				t.Fatalf("%s should require restart", it.Key)
			}
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
	if rt.ConfigSnapshot().Scan.RescanIntervalSeconds == 30 {
		t.Fatalf("validate_only should not mutate runtime config")
	}

	restartReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config", strings.NewReader(`{
		"items": [{"key":"http.listen_addr","value":"127.0.0.1:19999"}]
	}`))
	restartRec := httptest.NewRecorder()
	srv.handleAdminConfig(restartRec, restartReq)
	if restartRec.Code != http.StatusBadRequest {
		t.Fatalf("restart-required status mismatch: got=%d want=%d body=%s", restartRec.Code, http.StatusBadRequest, restartRec.Body.String())
	}
	if !strings.Contains(restartRec.Body.String(), "restart required for this key") {
		t.Fatalf("restart-required error mismatch: %s", restartRec.Body.String())
	}
	if rt.ConfigSnapshot().HTTP.ListenAddr != cfg.HTTP.ListenAddr {
		t.Fatalf("http.listen_addr should stay unchanged: %s", rt.ConfigSnapshot().HTTP.ListenAddr)
	}

	updateReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config", strings.NewReader(`{
		"items": [
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
	if rt.ConfigSnapshot().Scan.RescanIntervalSeconds != 120 {
		t.Fatalf("scan interval not updated: %d", rt.ConfigSnapshot().Scan.RescanIntervalSeconds)
	}
	if cfgBool(rt.ConfigSnapshot().Listen.Enabled, true) {
		t.Fatalf("listen.enabled not updated: got=true want=false")
	}
	if rt.ConfigSnapshot().Listen.RenewThresholdSeconds != 77 {
		t.Fatalf("listen.renew_threshold_seconds not updated: %d", rt.ConfigSnapshot().Listen.RenewThresholdSeconds)
	}
	if rt.ConfigSnapshot().Listen.AutoRenewRounds != 12345 {
		t.Fatalf("listen.auto_renew_rounds not updated: %d", rt.ConfigSnapshot().Listen.AutoRenewRounds)
	}
	if rt.ConfigSnapshot().Listen.OfferPaymentSatoshi != 888 {
		t.Fatalf("listen.offer_payment_satoshi not updated: %d", rt.ConfigSnapshot().Listen.OfferPaymentSatoshi)
	}
	if rt.ConfigSnapshot().Listen.TickSeconds != 9 {
		t.Fatalf("listen.tick_seconds not updated: %d", rt.ConfigSnapshot().Listen.TickSeconds)
	}
	if rt.ConfigSnapshot().Payment.PreferredScheme != "chain_tx_v1" {
		t.Fatalf("payment.preferred_scheme not updated: %s", rt.ConfigSnapshot().Payment.PreferredScheme)
	}
	if cfgBool(rt.ConfigSnapshot().Reachability.AutoAnnounceEnabled, true) {
		t.Fatalf("reachability.auto_announce_enabled not updated: got=true want=false")
	}
	if rt.ConfigSnapshot().Reachability.AnnounceTTLSeconds != 7200 {
		t.Fatalf("reachability.announce_ttl_seconds not updated: %d", rt.ConfigSnapshot().Reachability.AnnounceTTLSeconds)
	}
	if rt.ConfigSnapshot().Seller.Pricing.ResaleDiscountBPS != 7500 {
		t.Fatalf("resale_discount_bps mismatch: got=%d want=7500", rt.ConfigSnapshot().Seller.Pricing.ResaleDiscountBPS)
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	pubHost, _ := newSecpHost(t)
	defer pubHost.Close()
	subHost, _ := newSecpHost(t)
	defer subHost.Close()

	pubCfg := Config{}
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
	subCfg.Storage.DataDir = t.TempDir()

	pubRT := newRuntimeForTest(t, pubCfg, "", withRuntimeHost(pubHost), withRuntimeLiveRuntime(newLiveRuntime()))
	subRT := newRuntimeForTest(t, subCfg, "", withRuntimeHost(subHost), withRuntimeLiveRuntime(newLiveRuntime()))
	registerLiveHandlers(nil, pubRT)
	registerLiveHandlers(nil, subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)

	pubSrv := &httpAPIServer{rt: pubRT, cfgSource: staticConfigSnapshot(pubCfg)}
	subSrv := &httpAPIServer{rt: subRT, cfgSource: staticConfigSnapshot(subCfg), db: db, store: newClientDB(db, nil)}

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

	if _, err := db.Exec(`INSERT INTO biz_live_quotes(demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix,created_at_unix)
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	h, _ := newSecpHost(t)
	defer h.Close()

	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = dbPath
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	mustInsertTestWorkspaceRoot(t, db, cfg.Storage.DataDir)
	workspace := newTestFileStorageRuntime(t, &cfg, db)
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(h), withRuntimeFileStorage(workspace), withRuntimeLiveRuntime(newLiveRuntime()))
	registerLiveHandlers(newClientDB(db, nil), rt)
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil), fileStorage: workspace}

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

	var workspacePath, filePath string
	if err := db.QueryRow(`SELECT workspace_path,file_path FROM biz_workspace_files WHERE file_path LIKE ? ORDER BY workspace_path ASC,file_path ASC LIMIT 1`, "%"+streamID+"%").Scan(&workspacePath, &filePath); err != nil {
		t.Fatalf("query live segment path: %v", err)
	}
	if _, err := os.Stat(workspacePathJoin(workspacePath, filePath)); err != nil {
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	pubHost, _ := newSecpHost(t)
	defer pubHost.Close()
	subHost, _ := newSecpHost(t)
	defer subHost.Close()

	pubCfg := Config{}
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
	subCfg.Storage.DataDir = t.TempDir()
	streamID := strings.Repeat("ab", 32)
	mustInsertTestWorkspaceRoot(t, db, subCfg.Storage.DataDir)
	subWorkspace := newTestFileStorageRuntime(t, &subCfg, db)

	pubStore := newClientDB(db, nil)
	subStore := newClientDB(db, nil)
	pubRT := newRuntimeForTest(t, pubCfg, "", withRuntimeHost(pubHost), withRuntimeLiveRuntime(newLiveRuntime()))
	subRT := newRuntimeForTest(t, subCfg, "", withRuntimeHost(subHost), withRuntimeFileStorage(subWorkspace), withRuntimeLiveRuntime(newLiveRuntime()))
	registerLiveHandlers(nil, pubRT)
	registerLiveHandlers(subStore, subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)
	subRT.live.autoBuyFn = func(_ context.Context, _ *clientDB, _ *Runtime, decision LivePurchaseDecision, _ LiveSubscriberSnapshot) (liveAutoBuyResult, error) {
		outPath, err := subWorkspace.SelectLiveSegmentOutputPath(context.Background(), streamID, decision.TargetSegmentIndex, 1)
		if err != nil {
			return liveAutoBuyResult{}, err
		}
		return liveAutoBuyResult{
			SeedHash:       decision.SeedHash,
			SegmentIndex:   decision.TargetSegmentIndex,
			OutputFilePath: outPath,
		}, nil
	}

	pubSrv := &httpAPIServer{rt: pubRT, cfgSource: staticConfigSnapshot(pubCfg), store: pubStore}
	subSrv := &httpAPIServer{rt: subRT, cfgSource: staticConfigSnapshot(subCfg), store: subStore}
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
	var loaded LiveFollowStatus
	deadline = time.Now().Add(3 * time.Second)
	for {
		loaded, err = TriggerLiveFollowStatus(context.Background(), newClientDB(db, nil), subRT, streamID)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "database is locked") || time.Now().After(deadline) {
			t.Fatalf("load persisted follow status failed: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if loaded.HaveSegmentIndex != st.HaveSegmentIndex {
		t.Fatalf("persisted have_segment_index mismatch: got=%d want=%d", loaded.HaveSegmentIndex, st.HaveSegmentIndex)
	}
	if !strings.Contains(loaded.LastOutputFilePath, string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)) {
		t.Fatalf("unexpected live segment output path: %s", loaded.LastOutputFilePath)
	}
}

// TestHandleAdminCommandJournalTriggerKeyFilter 验证 trigger_key 过滤功能
// - 列表接口按 trigger_key 过滤能查到正确记录
// - 非匹配 trigger_key 返回空
// - detail 返回体里要带 trigger_key
// - 直接命令的 trigger_key 必须是空字符串
func TestHandleAdminCommandJournalTriggerKeyFilter(t *testing.T) {
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	store := newClientDB(db, nil)
	now := time.Now().Unix()

	// 插入一条 orchestrator 发起的命令（带 trigger_key）
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "orch_cmd_1",
		CommandType:   commandTypeFeePoolMaintain,
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "orchestrator",
		RequestedAt:   now,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "workspace_sync:12345", // 模拟 orchestrator 的 idempotency_key
		StateBefore:   "active",
		StateAfter:    "active",
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	// 插入一条直接命令（trigger_key 为空）
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "direct_cmd_1",
		CommandType:   commandTypeDownloadByHash,
		GatewayPeerID: "direct",
		AggregateID:   "seed:abc123",
		RequestedBy:   "client_kernel",
		RequestedAt:   now,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "", // 直接命令，无 trigger_key
		StateBefore:   "direct_download",
		StateAfter:    "direct_download",
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, "")
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db, store: newClientDB(db, nil)}

	// 测试 1：按匹配的 trigger_key 过滤，应该查到 orchestrator 命令
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?trigger_key=workspace_sync:12345", nil)
	rec1 := httptest.NewRecorder()
	srv.handleAdminFeePoolCommands(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("list by trigger_key status mismatch: got=%d want=%d body=%s", rec1.Code, http.StatusOK, rec1.Body.String())
	}
	var out1 struct {
		Total int `json:"total"`
		Items []struct {
			ID         int64  `json:"id"`
			CommandID  string `json:"command_id"`
			TriggerKey string `json:"trigger_key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &out1); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if out1.Total != 1 || len(out1.Items) != 1 {
		t.Fatalf("expected 1 item with trigger_key, got total=%d items=%d", out1.Total, len(out1.Items))
	}
	if out1.Items[0].CommandID != "orch_cmd_1" {
		t.Fatalf("expected command_id=orch_cmd_1, got=%s", out1.Items[0].CommandID)
	}
	if out1.Items[0].TriggerKey != "workspace_sync:12345" {
		t.Fatalf("expected trigger_key=workspace_sync:12345, got=%s", out1.Items[0].TriggerKey)
	}

	// 测试 2：按不匹配的 trigger_key 过滤，应该返回空
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?trigger_key=nonexistent_key", nil)
	rec2 := httptest.NewRecorder()
	srv.handleAdminFeePoolCommands(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("list by nonexistent trigger_key status mismatch: got=%d want=%d body=%s", rec2.Code, http.StatusOK, rec2.Body.String())
	}
	var out2 struct {
		Total int   `json:"total"`
		Items []any `json:"items"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &out2); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if out2.Total != 0 || len(out2.Items) != 0 {
		t.Fatalf("expected 0 items for nonexistent trigger_key, got total=%d items=%d", out2.Total, len(out2.Items))
	}

	// 测试 3：detail 接口返回体里要带 trigger_key
	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands/detail?id="+itoa64(out1.Items[0].ID), nil)
	detailRec := httptest.NewRecorder()
	srv.handleAdminFeePoolCommandDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detailOut struct {
		ID         int64  `json:"id"`
		CommandID  string `json:"command_id"`
		TriggerKey string `json:"trigger_key"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detailOut); err != nil {
		t.Fatalf("decode detail response: %v", err)
	}
	if detailOut.TriggerKey != "workspace_sync:12345" {
		t.Fatalf("detail expected trigger_key=workspace_sync:12345, got=%s", detailOut.TriggerKey)
	}
}
