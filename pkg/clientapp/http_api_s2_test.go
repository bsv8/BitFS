package clientapp

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	filestoragemod "github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage"
)

func TestHandleAdminLiveStreamsListAndDelete(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws := filepath.Join(base, "ws")
	pubkeyHex := strings.Repeat("11", 32)
	seedDir := filepath.Join(base, "config", pubkeyHex, "seeds")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		t.Fatalf("mkdir seed dir: %v", err)
	}
	streamID := strings.Repeat("ab", 32)
	liveFile := filepath.Join(ws, "live", streamID, "000001.seg")
	if err := os.MkdirAll(filepath.Dir(liveFile), 0o755); err != nil {
		t.Fatalf("mkdir live dir: %v", err)
	}
	if err := os.WriteFile(liveFile, []byte("seg"), 0o644); err != nil {
		t.Fatalf("write live file: %v", err)
	}
	seedBytes, liveSeedHash, _, err := buildSeedV1(liveFile)
	if err != nil {
		t.Fatalf("build live seed: %v", err)
	}
	seedFile := filepath.Join(seedDir, liveSeedHash+".bse")
	if err := os.MkdirAll(filepath.Dir(seedFile), 0o755); err != nil {
		t.Fatalf("mkdir live seed dir: %v", err)
	}
	if err := os.WriteFile(seedFile, seedBytes, 0o644); err != nil {
		t.Fatalf("write live seed: %v", err)
	}

	db := newWalletAPITestDB(t)
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, ws, 1, 0, time.Now().Unix()); err != nil {
		t.Fatalf("insert fileStorage: %v", err)
	}
	rawSeedPath := filepath.Join(base, "content", "source.bin")
	if err := os.MkdirAll(filepath.Dir(rawSeedPath), 0o755); err != nil {
		t.Fatalf("mkdir seed dir: %v", err)
	}
	if err := os.WriteFile(rawSeedPath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed source file: %v", err)
	}
	seedBytesStatic, seedHash, _, err := buildSeedV1(rawSeedPath)
	if err != nil {
		t.Fatalf("build static seed: %v", err)
	}
	seedPath := filepath.Join(seedDir, seedHash+".bse")
	if err := os.WriteFile(seedPath, seedBytesStatic, 0o644); err != nil {
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
	mustInsertTestWorkspaceRoot(t, db, ws)
	mgr := newTestFileStorageRuntime(t, &cfg, db)
	rt := newRuntimeForTest(t, cfg, strings.Repeat("11", 32), withRuntimeFileStorage(mgr), withRuntimeStore(newClientDB(db, nil)))
	srv := &httpAPIServer{
		ctx:         context.Background(),
		rt:          rt,
		db:          db,
		store:       newClientDB(db, nil),
		cfgSource:   staticConfigSnapshot(cfg),
		fileStorage: mgr,
	}

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
	pubkeyHex := strings.Repeat("22", 32)
	seedDir := filepath.Join(base, "config", pubkeyHex, "seeds")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		t.Fatalf("mkdir seed dir: %v", err)
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
		t.Fatalf("insert fileStorage: %v", err)
	}
	seedHash := strings.Repeat("ef", 32)
	seedPath := filepath.Join(seedDir, seedHash+".bse")
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
	mustInsertTestWorkspaceRoot(t, db, ws)

	cfg := Config{}
	cfg.Storage.DataDir = dataDir
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rt := newRuntimeForTest(t, cfg, strings.Repeat("11", 32), withRuntimeStore(newClientDB(db, nil)))
	cleanup, err := filestoragemod.Install(t.Context(), newModuleHost(rt, newClientDB(db, nil)))
	if err != nil {
		t.Fatalf("install filestorage module: %v", err)
	}
	t.Cleanup(cleanup)
	srv := newHTTPAPIServer(rt, staticConfigSnapshot(cfg), db, newClientDB(db, nil), nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler: %v", err)
	}

	treeReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/tree?workspace_path="+url.QueryEscape(ws)+"&path=/docs", nil)
	treeRec := httptest.NewRecorder()
	handler.ServeHTTP(treeRec, treeReq)
	if treeRec.Code != http.StatusOK {
		t.Fatalf("static tree status mismatch: got=%d want=%d body=%s", treeRec.Code, http.StatusOK, treeRec.Body.String())
	}
	var treeResp struct {
		Status string         `json:"status"`
		Data   map[string]any `json:"data"`
	}
	if err := json.Unmarshal(treeRec.Body.Bytes(), &treeResp); err != nil {
		t.Fatalf("decode tree body: %v", err)
	}
	items, _ := treeResp.Data["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("tree should contain items: %+v", treeResp)
	}

	priceReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/price/set", strings.NewReader(`{"workspace_path":"`+ws+`","path":"/docs/a.txt","floor_price_sat_per_64k":15,"resale_discount_bps":8000}`))
	priceRec := httptest.NewRecorder()
	handler.ServeHTTP(priceRec, priceReq)
	if priceRec.Code != http.StatusOK {
		t.Fatalf("set static price status mismatch: got=%d want=%d body=%s", priceRec.Code, http.StatusOK, priceRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/price?workspace_path="+url.QueryEscape(ws)+"&path=/docs/a.txt", nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get static price status mismatch: got=%d want=%d body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getBody map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getBody); err != nil {
		t.Fatalf("decode price get body: %v", err)
	}
	data, _ := getBody["data"].(map[string]any)
	if data == nil {
		t.Fatalf("price get missing data: %+v", getBody)
	}
	if int64(data["floor_unit_price_sat_per_64k"].(float64)) != 15 {
		t.Fatalf("floor price mismatch: got=%v want=15", data["floor_unit_price_sat_per_64k"])
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
	mustInsertTestWorkspaceRoot(t, db, ws)
	cfg := Config{}
	rt := newRuntimeForTest(t, cfg, strings.Repeat("11", 32), withRuntimeStore(newClientDB(db, nil)))
	cleanup, err := filestoragemod.Install(t.Context(), newModuleHost(rt, newClientDB(db, nil)))
	if err != nil {
		t.Fatalf("install filestorage module: %v", err)
	}
	t.Cleanup(cleanup)
	srv := newHTTPAPIServer(rt, staticConfigSnapshot(cfg), db, newClientDB(db, nil), nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/static/tree?workspace_path="+url.QueryEscape(ws)+"&path=/&recursive=true&max_depth=4", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("recursive tree status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode recursive tree: %v", err)
	}
	data, _ := body["data"].(map[string]any)
	items, _ := data["items"].([]any)
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
	cfg.Storage.DataDir = dataDir
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("init data dirs: %v", err)
	}
	mustInsertTestWorkspaceRoot(t, db, ws)
	rt := newRuntimeForTest(t, cfg, strings.Repeat("11", 32), withRuntimeStore(newClientDB(db, nil)))
	cleanup, err := filestoragemod.Install(t.Context(), newModuleHost(rt, newClientDB(db, nil)))
	if err != nil {
		t.Fatalf("install filestorage module: %v", err)
	}
	t.Cleanup(cleanup)
	srv := newHTTPAPIServer(rt, staticConfigSnapshot(cfg), db, newClientDB(db, nil), nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler: %v", err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("workspace_path", ws); err != nil {
		t.Fatalf("write workspace_path: %v", err)
	}
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
	handler.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusOK {
		t.Fatalf("upload status mismatch: got=%d want=%d body=%s", uploadRec.Code, http.StatusOK, uploadRec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "docs", "a.txt")); err != nil {
		t.Fatalf("uploaded file missing: %v", err)
	}

	moveReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/move", strings.NewReader(`{
		"workspace_path":"`+ws+`",
		"source_path":"/docs/a.txt",
		"target_dir":"/archive/2026",
		"new_name":"moved.txt"
	}`))
	moveRec := httptest.NewRecorder()
	handler.ServeHTTP(moveRec, moveReq)
	if moveRec.Code != http.StatusOK {
		t.Fatalf("move status mismatch: got=%d want=%d body=%s", moveRec.Code, http.StatusOK, moveRec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "archive", "2026", "moved.txt")); err != nil {
		t.Fatalf("moved file missing: %v", err)
	}
	var movedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws, filepath.ToSlash(filepath.Join("docs", "a.txt"))).Scan(&movedCount); err != nil {
		t.Fatalf("query old workspace row: %v", err)
	}
	if movedCount != 0 {
		t.Fatalf("old workspace row should be removed, got=%d", movedCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws, filepath.ToSlash(filepath.Join("archive", "2026", "moved.txt"))).Scan(&movedCount); err != nil {
		t.Fatalf("query new workspace row: %v", err)
	}
	if movedCount != 1 {
		t.Fatalf("new workspace row should exist, got=%d", movedCount)
	}
}
