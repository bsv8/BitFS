package clientapp

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestWithAuth_PassThrough(t *testing.T) {
	t.Parallel()

	srv := &httpAPIServer{}
	called := false
	h := srv.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !called {
		t.Fatalf("next handler should be called")
	}
}

func TestWithAuth_InjectVisitMeta(t *testing.T) {
	t.Parallel()

	srv := &httpAPIServer{}
	h := srv.withAuth(func(w http.ResponseWriter, r *http.Request) {
		meta := requestVisitMetaFromContext(r.Context())
		if meta.VisitID != "visit-123" {
			t.Fatalf("visit id mismatch: got=%q want=%q", meta.VisitID, "visit-123")
		}
		if meta.VisitLocator != "node:abcdef/index" {
			t.Fatalf("visit locator mismatch: got=%q want=%q", meta.VisitLocator, "node:abcdef/index")
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	req.Header.Set(headerVisitID, "visit-123")
	req.Header.Set(headerVisitLocator, "node:abcdef/index")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestHTTPAPIServer_StartAndShutdown(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "client-index.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rt := newRuntimeForTest(t, cfg, "")
	rt.ctx = context.Background()
	srv := newHTTPAPIServer(rt, staticConfigSnapshot(cfg), db, newClientDB(db, nil), nil, nil, nil, nil)

	done := make(chan error, 1)
	go func() {
		done <- srv.Start()
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		srv.srvMu.RLock()
		started := srv.srv != nil
		srv.srvMu.RUnlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("http api server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown http api server: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("http api server start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("http api server did not stop")
	}
}
