package clientapp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleAdminWorkspacesPut(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	ws1 := filepath.Join(base, "ws1")
	dataDir := filepath.Join(base, "data")
	if err := os.MkdirAll(ws1, 0o755); err != nil {
		t.Fatalf("mkdir ws1: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "seeds"), 0o755); err != nil {
		t.Fatalf("mkdir seeds: %v", err)
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
	mgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		catalog: &sellerCatalog{seeds: map[string]sellerSeed{}},
	}
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	if _, err := mgr.SyncOnce(t.Context()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	items, err := mgr.List()
	if err != nil || len(items) == 0 {
		t.Fatalf("list workspaces failed: err=%v len=%d", err, len(items))
	}
	id := items[0].ID

	srv := &httpAPIServer{workspace: mgr}
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/workspaces?id="+itoa64(id), strings.NewReader(`{"max_bytes":2048,"enabled":false}`))
	rec := httptest.NewRecorder()
	srv.handleAdminWorkspaces(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put workspace status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var maxBytes uint64
	var enabled int64
	if err := db.QueryRow(`SELECT max_bytes,enabled FROM workspaces WHERE id=?`, id).Scan(&maxBytes, &enabled); err != nil {
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
