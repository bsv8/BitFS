package clientapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandleAdminWorkspacesPut(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws1 := filepath.Join(base, "ws1")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(ws1, 0o755); err != nil {
		t.Fatalf("mkdir ws1: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "biz_seeds"), 0o755); err != nil {
		t.Fatalf("mkdir biz_seeds: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws1, "a.txt"), []byte("abc"), 0o644); err != nil {
		t.Fatalf("write ws file: %v", err)
	}

	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws1
	cfg.Storage.DataDir = dataDir
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	mgr := newTestWorkspaceManager(context.Background(), &cfg, db)
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	if _, err := mgr.SyncOnce(t.Context()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	items, err := mgr.List()
	if err != nil || len(items) == 0 {
		t.Fatalf("list biz_workspaces failed: err=%v len=%d", err, len(items))
	}
	workspacePath := items[0].WorkspacePath

	rt := newRuntimeForTest(t, cfg, "", withRuntimeWorkspace(mgr))
	srv := &httpAPIServer{rt: rt, workspace: mgr, kernel: rt.ClientKernel()}
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/biz_workspaces?workspace_path="+url.QueryEscape(workspacePath), strings.NewReader(`{"max_bytes":2048,"enabled":false}`))
	rec := httptest.NewRecorder()
	srv.handleAdminWorkspaces(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put workspace status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var maxBytes uint64
	var enabled int64
	if err := db.QueryRow(`SELECT max_bytes,enabled FROM biz_workspaces WHERE workspace_path=?`, workspacePath).Scan(&maxBytes, &enabled); err != nil {
		t.Fatalf("query workspace after put: %v", err)
	}
	if maxBytes != 2048 {
		t.Fatalf("max_bytes mismatch: got=%d want=2048", maxBytes)
	}
	if enabled != 0 {
		t.Fatalf("enabled mismatch: got=%d want=0", enabled)
	}
}

func TestHandleGetFileCancel(t *testing.T) {
	t.Parallel()
	canceled := false
	srv := &httpAPIServer{
		getJobs: map[string]*fileGetJob{
			"job_running": {
				ID:     "job_running",
				Status: "running",
				cancel: func() { canceled = true },
			},
			"job_done": {
				ID:     "job_done",
				Status: "done",
			},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/get-file/cancel", strings.NewReader(`{"id":"job_running"}`))
	srv.handleGetFileCancel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel running status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !canceled {
		t.Fatalf("cancel func should be called")
	}
	if !srv.getJobs["job_running"].CancelRequested {
		t.Fatalf("cancel_requested should be true")
	}

	recDone := httptest.NewRecorder()
	reqDone := httptest.NewRequest(http.MethodPost, "/api/v1/files/get-file/cancel", strings.NewReader(`{"id":"job_done"}`))
	srv.handleGetFileCancel(recDone, reqDone)
	if recDone.Code != http.StatusBadRequest {
		t.Fatalf("cancel done status mismatch: got=%d want=%d", recDone.Code, http.StatusBadRequest)
	}
}

func TestHandleAdminLiveStreamsListAndDelete(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	streamID := strings.Repeat("ab", 32)
	liveFile := filepath.Join(ws, "live", streamID, "000001.seg")
	if err := os.MkdirAll(filepath.Dir(liveFile), 0o755); err != nil {
		t.Fatalf("mkdir live dir: %v", err)
	}
	if err := os.WriteFile(liveFile, []byte("seg"), 0o644); err != nil {
		t.Fatalf("write live file: %v", err)
	}

	db := newWalletAPITestDB(t)
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, ws, 1, 0, time.Now().Unix()); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	seedHash := strings.Repeat("cd", 32)
	seedPath := filepath.Join(base, "data", "biz_seeds", seedHash+".seed")
	if err := os.MkdirAll(filepath.Dir(seedPath), 0o755); err != nil {
		t.Fatalf("mkdir seed dir: %v", err)
	}
	if err := os.WriteFile(seedPath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, 1, 4, seedPath, "", ""); err != nil {
		t.Fatalf("insert biz_seeds: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		ws, filepath.Join("live", streamID, "000001.seg"), seedHash, 0); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}
	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	srv := &httpAPIServer{ctx: context.Background(), db: db, cfgSource: staticConfigSnapshot(cfg)}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/live/streams", nil)
	listRec := httptest.NewRecorder()
	srv.handleAdminLiveStreams(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list live streams status mismatch: got=%d want=%d body=%s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	var listBody struct {
		Total int `json:"total"`
		Items []struct {
			StreamID string `json:"stream_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode live list: %v", err)
	}
	if listBody.Total != 1 || len(listBody.Items) != 1 || listBody.Items[0].StreamID != streamID {
		t.Fatalf("unexpected live list body: %+v", listBody)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/live/streams?stream_id="+streamID, nil)
	delRec := httptest.NewRecorder()
	srv.handleAdminLiveStreams(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete live stream status mismatch: got=%d want=%d body=%s", delRec.Code, http.StatusOK, delRec.Body.String())
	}
	if _, err := os.Stat(liveFile); !os.IsNotExist(err) {
		t.Fatalf("live file should be deleted, err=%v", err)
	}
}

func TestHandleAdminStaticTreeAndPrice(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "biz_seeds"), 0o755); err != nil {
		t.Fatalf("mkdir biz_seeds: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "biz_seeds"), 0o755); err != nil {
		t.Fatalf("mkdir biz_seeds: %v", err)
	}
	filePath := filepath.Join(ws, "docs", "a.txt")
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write static file: %v", err)
	}

	db := newWalletAPITestDB(t)
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, ws, 1, 0, time.Now().Unix()); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	seedHash := strings.Repeat("ef", 32)
	seedPath := filepath.Join(dataDir, "biz_seeds", seedHash+".seed")
	if err := os.WriteFile(seedPath, make([]byte, 64), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, 1, 64, seedPath, "", ""); err != nil {
		t.Fatalf("insert biz_seeds: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		ws, filepath.Join("docs", "a.txt"), seedHash, 0); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = dataDir
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	srv := &httpAPIServer{db: db, cfgSource: staticConfigSnapshot(cfg)}

	treeReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/tree?path=/docs", nil)
	treeRec := httptest.NewRecorder()
	srv.handleAdminStaticTree(treeRec, treeReq)
	if treeRec.Code != http.StatusOK {
		t.Fatalf("static tree status mismatch: got=%d want=%d body=%s", treeRec.Code, http.StatusOK, treeRec.Body.String())
	}

	priceReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/price/set", strings.NewReader(`{"path":"/docs/a.txt","floor_price_sat_per_64k":15,"resale_discount_bps":8000}`))
	priceRec := httptest.NewRecorder()
	srv.handleAdminStaticPriceSet(priceRec, priceReq)
	if priceRec.Code != http.StatusOK {
		t.Fatalf("set static price status mismatch: got=%d want=%d body=%s", priceRec.Code, http.StatusOK, priceRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/price?path=/docs/a.txt", nil)
	getRec := httptest.NewRecorder()
	srv.handleAdminStaticPriceGet(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get static price status mismatch: got=%d want=%d body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getBody map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getBody); err != nil {
		t.Fatalf("decode price get body: %v", err)
	}
	if int64(getBody["floor_unit_price_sat_per_64k"].(float64)) != 15 {
		t.Fatalf("floor price mismatch: got=%v want=15", getBody["floor_unit_price_sat_per_64k"])
	}
}

func TestHandleAdminStaticTreeRecursive(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	if err := os.MkdirAll(filepath.Join(ws, "a", "b"), 0o755); err != nil {
		t.Fatalf("mkdir tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws, "a", "b", "c.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	srv := &httpAPIServer{db: db, cfgSource: staticConfigSnapshot(cfg)}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/tree?path=/&recursive=true&max_depth=4", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminStaticTree(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("recursive tree status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode recursive tree: %v", err)
	}
	items, _ := body["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("recursive tree should have items")
	}
	first, _ := items[0].(map[string]any)
	if first["type"] != "dir" {
		t.Fatalf("first node should be dir: %+v", first)
	}
	children, _ := first["children"].([]any)
	if len(children) == 0 {
		t.Fatalf("recursive children should exist")
	}
}

func TestHandleAdminStaticUploadAndMoveByTargetDir(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = dataDir
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("init data dirs: %v", err)
	}
	mgr := newTestWorkspaceManager(context.Background(), &cfg, db)
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	srv := &httpAPIServer{db: db, cfgSource: staticConfigSnapshot(cfg), workspace: mgr, kernel: newTestWorkspaceKernel(t, cfg, mgr)}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("target_dir", "/docs"); err != nil {
		t.Fatalf("write field: %v", err)
	}
	part, err := writer.CreateFormFile("file", "a.txt")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write([]byte("hello upload")); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	uploadReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/upload", &body)
	uploadReq.Header.Set("Content-Type", writer.FormDataContentType())
	uploadRec := httptest.NewRecorder()
	srv.handleAdminStaticUpload(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusOK {
		t.Fatalf("upload status mismatch: got=%d want=%d body=%s", uploadRec.Code, http.StatusOK, uploadRec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "docs", "a.txt")); err != nil {
		t.Fatalf("uploaded file missing: %v", err)
	}

	moveReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/move", strings.NewReader(`{
		"source_path":"/docs/a.txt",
		"target_dir":"/archive/2026",
		"new_name":"moved.txt"
	}`))
	moveRec := httptest.NewRecorder()
	srv.handleAdminStaticMove(moveRec, moveReq)
	if moveRec.Code != http.StatusOK {
		t.Fatalf("move status mismatch: got=%d want=%d body=%s", moveRec.Code, http.StatusOK, moveRec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "archive", "2026", "moved.txt")); err != nil {
		t.Fatalf("moved file missing: %v", err)
	}
}

func TestHandleAdminRegisterDownloadedFileAcceptsAbsolutePath(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = dataDir
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("init data dirs: %v", err)
	}
	mgr := newTestWorkspaceManager(context.Background(), &cfg, db)
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	srv := &httpAPIServer{db: db, cfgSource: staticConfigSnapshot(cfg), workspace: mgr, kernel: newTestWorkspaceKernel(t, cfg, mgr)}

	downloadedPath := filepath.Join(ws, "downloads", "abs-register.bin")
	if err := os.MkdirAll(filepath.Dir(downloadedPath), 0o755); err != nil {
		t.Fatalf("mkdir downloads: %v", err)
	}
	if err := os.WriteFile(downloadedPath, []byte("downloaded-content"), 0o644); err != nil {
		t.Fatalf("write downloaded file: %v", err)
	}
	_, expectedSeedHash, _, err := buildSeedV1(downloadedPath)
	if err != nil {
		t.Fatalf("build seed: %v", err)
	}

	callRegister := func(path string) *httptest.ResponseRecorder {
		raw, err := json.Marshal(map[string]any{"file_path": path})
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/workspace/register-downloaded-file", bytes.NewReader(raw))
		rec := httptest.NewRecorder()
		srv.handleAdminRegisterDownloadedFile(rec, req)
		return rec
	}

	recAbs := callRegister(downloadedPath)
	if recAbs.Code != http.StatusOK {
		t.Fatalf("register absolute path status mismatch: got=%d want=%d body=%s", recAbs.Code, http.StatusOK, recAbs.Body.String())
	}
	var bodyAbs map[string]any
	if err := json.Unmarshal(recAbs.Body.Bytes(), &bodyAbs); err != nil {
		t.Fatalf("decode absolute response: %v", err)
	}
	gotSeedHash := func(body map[string]any) string {
		return strings.ToLower(strings.TrimSpace(strings.TrimSpace(fmt.Sprint(body["seed_hash"]))))
	}
	if got := gotSeedHash(bodyAbs); got != expectedSeedHash {
		t.Fatalf("absolute register seed mismatch: got=%s want=%s", got, expectedSeedHash)
	}

	recRel := callRegister("/downloads/abs-register.bin")
	if recRel.Code != http.StatusOK {
		t.Fatalf("register workspace path status mismatch: got=%d want=%d body=%s", recRel.Code, http.StatusOK, recRel.Body.String())
	}
	var bodyRel map[string]any
	if err := json.Unmarshal(recRel.Body.Bytes(), &bodyRel); err != nil {
		t.Fatalf("decode workspace response: %v", err)
	}
	if got := gotSeedHash(bodyRel); got != expectedSeedHash {
		t.Fatalf("workspace register seed mismatch: got=%s want=%s", got, expectedSeedHash)
	}
}
