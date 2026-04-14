package clientapp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewRuntimeAPIHandler_DoesNotRequireRuntimeHTTPServer(t *testing.T) {
	t.Parallel()

	h, _ := newSecpHost(t)
	t.Cleanup(func() { _ = h.Close() })

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(h), withRuntimeWorkspace(&workspaceManager{}))
	rt.HTTP = nil

	handler, err := NewRuntimeAPIHandler(rt)
	if err != nil {
		t.Fatalf("new runtime api handler: %v", err)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/config/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("schema status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
