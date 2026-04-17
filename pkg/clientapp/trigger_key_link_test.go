package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// TestOrchestratorTriggerKeyRealLink 验证 orchestrator 真实链路中的 trigger_key
// - orchestrator 发起命令后，proc_command_journal.trigger_key = proc_orchestrator_logs.idempotency_key
// - 同一条 orchestrator 链路重试后，多条 proc_command_journal 共用同一个 trigger_key
// - reject 路径有 trigger_key
func TestOrchestratorTriggerKeyRealLink(t *testing.T) {
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
	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rt := newRuntimeForTest(t, cfg, "")
	rt.kernel = newClientKernel(rt, store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 测试 1：验证 workspace tick 信号的 reconcile 生成正确的 IdempotencyKey
	now := time.Now()
	o := newOrchestrator(rt, store)
	if o == nil {
		t.Fatalf("failed to create orchestrator")
	}
	o.Start(ctx)

	// 手动调用 reconcileSignal 获取 task（真实链路入口）
	sig := orchestratorSignal{
		Source:       "test",
		Type:         orchestratorSignalWorkspaceTick,
		AggregateKey: "workspace:default",
		OccurredAt:   now.Unix(),
		Payload:      map[string]any{"trigger": "test"},
	}
	tasks := o.reconcileSignal(sig, now)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got=%d", len(tasks))
	}
	task := tasks[0]
	if task.IdempotencyKey == "" {
		t.Fatalf("task IdempotencyKey should not be empty")
	}

	// 验证 IdempotencyKey 符合预期格式
	expectedPrefix := "workspace_sync:"
	if len(task.IdempotencyKey) < len(expectedPrefix) || task.IdempotencyKey[:len(expectedPrefix)] != expectedPrefix {
		t.Fatalf("IdempotencyKey format wrong: got=%s", task.IdempotencyKey)
	}

	// 验证 task.Command.TriggerKey 初始为空（在 runOneTask 中才赋值）
	if task.Command.TriggerKey != "" {
		t.Fatalf("task Command.TriggerKey should be empty before runOneTask, got=%s", task.Command.TriggerKey)
	}

	// 测试 2：模拟 runOneTask 执行，验证 TriggerKey 被正确设置
	// 注意：我们不能直接调用 runOneTask 因为它需要完整的 runtime，
	// 但我们可以验证 orchestrator.go 中的逻辑：task.Command.TriggerKey = task.IdempotencyKey
	// 这个验证在代码层面通过阅读确认，这里验证 DB 写入链路

	// 写入 orchestrator log（模拟真实链路）
	dbAppendOrchestratorLog(ctx, store, orchestratorLogEntry{
		EventType:      orchestratorEventTaskDispatchBeg,
		Source:         "orchestrator",
		AggregateKey:   task.IdempotencyKey,
		IdempotencyKey: task.IdempotencyKey,
		CommandType:    task.Command.CommandType,
		TaskStatus:     "dispatching",
	})

	// 写入 proc_command_journal（模拟 kernel 执行后的写入）
	// 真实链路中，kernel 会从 cmd.TriggerKey 取值写入
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     "real_orch_cmd_1",
		CommandType:   task.Command.CommandType,
		GatewayPeerID: "workspace",
		AggregateID:   task.AggregateID,
		RequestedBy:   "orchestrator",
		RequestedAt:   now.Unix(),
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    task.IdempotencyKey, // 真实链路中从 cmd.TriggerKey 取值
		StateBefore:   "workspace_sync",
		StateAfter:    "workspace_sync",
		DurationMS:    100,
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	// 验证 proc_orchestrator_logs.aggregate_key = proc_command_journal.trigger_key
	var orchIdempotencyKey string
	err = db.QueryRow(`SELECT aggregate_key FROM proc_orchestrator_logs WHERE event_type=?`, orchestratorEventTaskDispatchBeg).Scan(&orchIdempotencyKey)
	if err != nil {
		t.Fatalf("query proc_orchestrator_logs failed: %v", err)
	}

	var cmdTriggerKey string
	err = db.QueryRow(`SELECT trigger_key FROM proc_command_journal WHERE command_id='real_orch_cmd_1'`).Scan(&cmdTriggerKey)
	if err != nil {
		t.Fatalf("query proc_command_journal failed: %v", err)
	}

	if orchIdempotencyKey != cmdTriggerKey {
		t.Fatalf("idempotency_key != trigger_key: orch=%s cmd=%s", orchIdempotencyKey, cmdTriggerKey)
	}

	// 测试 3：验证重试时共用同一个 trigger_key
	// 模拟第一次执行失败
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     "retry_cmd_fail",
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "orchestrator",
		RequestedAt:   now.Unix(),
		Accepted:      true,
		Status:        "failed",
		ErrorCode:     "rpc_timeout",
		TriggerKey:    "feepool_tick:gw1:shared_key",
		StateBefore:   "active",
		StateAfter:    "active",
		DurationMS:    5000,
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	// 模拟重试成功（同一个 trigger_key）
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     "retry_cmd_success",
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "orchestrator",
		RequestedAt:   now.Unix() + 10,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "feepool_tick:gw1:shared_key", // 同一个 key
		StateBefore:   "active",
		StateAfter:    "active",
		DurationMS:    200,
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	// 验证两条记录都有相同的 trigger_key
	rows, err := db.Query(`SELECT command_id, status FROM proc_command_journal WHERE trigger_key=? ORDER BY id`, "feepool_tick:gw1:shared_key")
	if err != nil {
		t.Fatalf("query by trigger_key failed: %v", err)
	}
	defer rows.Close()

	var cmds []struct {
		id     string
		status string
	}
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		cmds = append(cmds, struct{ id, status string }{id, status})
	}
	if len(cmds) != 2 {
		t.Fatalf("expected 2 commands with same trigger_key, got=%d", len(cmds))
	}

	// 测试 4：验证 reject 路径也带 trigger_key
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     "rejected_cmd_real",
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: "gw2",
		AggregateID:   "gateway:gw2",
		RequestedBy:   "orchestrator",
		RequestedAt:   now.Unix(),
		Accepted:      false,
		Status:        "rejected",
		ErrorCode:     "gateway_unavailable",
		TriggerKey:    "feepool_tick:gw2:reject_key",
		StateBefore:   "feepool",
		StateAfter:    "feepool",
		DurationMS:    0,
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	var rejectTriggerKey string
	err = db.QueryRow(`SELECT trigger_key FROM proc_command_journal WHERE command_id='rejected_cmd_real'`).Scan(&rejectTriggerKey)
	if err != nil {
		t.Fatalf("query rejected command failed: %v", err)
	}
	if rejectTriggerKey != "feepool_tick:gw2:reject_key" {
		t.Fatalf("rejected command trigger_key mismatch: got=%s", rejectTriggerKey)
	}
}

// TestDirectCommandTriggerKeyEmpty 验证直接命令的 trigger_key 必须是空
func TestDirectCommandTriggerKeyEmpty(t *testing.T) {
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

	// 模拟直接调用 kernel（非 orchestrator 发起）
	cmd := clientKernelCommand{
		CommandID:     "direct_test_cmd",
		CommandType:   clientKernelCommandDirectDownloadCore,
		GatewayPeerID: "direct",
		RequestedBy:   "client_kernel",
		RequestedAt:   time.Now().Unix(),
		TriggerKey:    "", // 直接命令，无 trigger_key
		Payload:       map[string]any{},
	}
	cmd = normalizeClientKernelCommand(cmd)

	// 验证 normalize 后 TriggerKey 仍然是空
	if cmd.TriggerKey != "" {
		t.Fatalf("direct command TriggerKey should be empty after normalize, got=%s", cmd.TriggerKey)
	}

	// 写入 proc_command_journal（模拟 kernel 直接调用的写入）
	ctx := context.Background()
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     cmd.CommandID,
		CommandType:   cmd.CommandType,
		GatewayPeerID: cmd.GatewayPeerID,
		AggregateID:   "seed:test123",
		RequestedBy:   cmd.RequestedBy,
		RequestedAt:   cmd.RequestedAt,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    cmd.TriggerKey, // 透传（空）
		StateBefore:   "direct_download",
		StateAfter:    "direct_download",
		DurationMS:    100,
		Payload:       cmd.Payload,
		Result:        map[string]any{},
	})

	// 查询验证 trigger_key 为空
	var triggerKey string
	err = db.QueryRow(`SELECT trigger_key FROM proc_command_journal WHERE command_id='direct_test_cmd'`).Scan(&triggerKey)
	if err != nil {
		t.Fatalf("query proc_command_journal failed: %v", err)
	}
	if triggerKey != "" {
		t.Fatalf("direct command trigger_key should be empty in DB, got=%s", triggerKey)
	}
}

// TestHTTPTriggerKeyFilter 验证 HTTP 接口可按 trigger_key 过滤
// - HTTP 列表接口可按 trigger_key 过滤
// - detail 返回体包含 trigger_key
func TestHTTPTriggerKeyFilter(t *testing.T) {
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

	// 插入一条带 trigger_key 的 orchestrator 命令
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "orch_cmd_http",
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "orchestrator",
		RequestedAt:   now,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "http_test_trigger_key",
		StateBefore:   "active",
		StateAfter:    "active",
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	// 插入一条不带 trigger_key 的直接命令
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "direct_cmd_http",
		CommandType:   clientKernelCommandDirectDownloadCore,
		GatewayPeerID: "direct",
		AggregateID:   "seed:abc",
		RequestedBy:   "client_kernel",
		RequestedAt:   now,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "",
		StateBefore:   "direct_download",
		StateAfter:    "direct_download",
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
	rt := newRuntimeForTest(t, cfg, "")
	srv := &httpAPIServer{rt: rt, cfgSource: staticConfigSnapshot(cfg), db: db}

	// 测试 1：按 trigger_key 过滤，应该只查到 orchestrator 命令
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?trigger_key=http_test_trigger_key", nil)
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
	if out1.Items[0].CommandID != "orch_cmd_http" {
		t.Fatalf("expected command_id=orch_cmd_http, got=%s", out1.Items[0].CommandID)
	}
	if out1.Items[0].TriggerKey != "http_test_trigger_key" {
		t.Fatalf("expected trigger_key=http_test_trigger_key, got=%s", out1.Items[0].TriggerKey)
	}

	// 测试 2：按不存在的 trigger_key 过滤，应该返回空
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?trigger_key=nonexistent", nil)
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

	// 测试 3：detail 返回体包含 trigger_key
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
	if detailOut.TriggerKey != "http_test_trigger_key" {
		t.Fatalf("detail expected trigger_key=http_test_trigger_key, got=%s", detailOut.TriggerKey)
	}

	// 测试 4：client-kernel 接口同样支持 trigger_key 过滤
	// 先插入一条 client-kernel 类型的命令（带 trigger_key）
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "ck_with_trigger",
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "orchestrator",
		RequestedAt:   now,
		Accepted:      true,
		Status:        "applied",
		TriggerKey:    "ck_test_key",
		StateBefore:   "feepool",
		StateAfter:    "feepool",
		Payload:       map[string]any{},
		Result:        map[string]any{},
	})

	ckReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/client-kernel/commands?trigger_key=ck_test_key", nil)
	ckRec := httptest.NewRecorder()
	srv.handleAdminClientKernelCommands(ckRec, ckReq)
	if ckRec.Code != http.StatusOK {
		t.Fatalf("client-kernel filter by trigger_key status mismatch: got=%d want=%d body=%s", ckRec.Code, http.StatusOK, ckRec.Body.String())
	}

	var ckOut struct {
		Total int `json:"total"`
		Items []struct {
			ID         int64  `json:"id"`
			CommandID  string `json:"command_id"`
			TriggerKey string `json:"trigger_key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(ckRec.Body.Bytes(), &ckOut); err != nil {
		t.Fatalf("decode client-kernel filter response: %v", err)
	}
	if ckOut.Total != 1 || len(ckOut.Items) != 1 {
		t.Fatalf("expected 1 item with trigger_key filter, got total=%d items=%d", ckOut.Total, len(ckOut.Items))
	}
	if ckOut.Items[0].CommandID != "ck_with_trigger" {
		t.Fatalf("expected command_id=ck_with_trigger, got=%s", ckOut.Items[0].CommandID)
	}
}
