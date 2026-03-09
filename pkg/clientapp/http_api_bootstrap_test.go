package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithAuth_RejectWhenTokenNotInitialized(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	srv := &httpAPIServer{cfg: &cfg}
	called := false
	h := srv.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if called {
		t.Fatalf("next handler should not be called")
	}
	if !strings.Contains(rec.Body.String(), "token is not initialized") {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
}

func TestBootstrapToken_SetAndUseImmediately(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigInDB(db, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}
	runCfg := NewRunInputFromConfig(cfg, "")
	rt := &Runtime{DB: db, runIn: runCfg}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap/status", nil)
	statusRec := httptest.NewRecorder()
	srv.handleBootstrapStatus(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status api code mismatch: got=%d want=%d", statusRec.Code, http.StatusOK)
	}
	var statusBody map[string]any
	if err := json.Unmarshal(statusRec.Body.Bytes(), &statusBody); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if got, _ := statusBody["needs_bootstrap"].(bool); !got {
		t.Fatalf("needs_bootstrap should be true before init")
	}

	bootstrapReq := httptest.NewRequest(http.MethodPost, "/api/v1/bootstrap/token", strings.NewReader(`{"auth_token":"m1n2r3y4"}`))
	bootstrapReq.RemoteAddr = "127.0.0.1:18080"
	bootstrapRec := httptest.NewRecorder()
	srv.handleBootstrapToken(bootstrapRec, bootstrapReq)
	if bootstrapRec.Code != http.StatusOK {
		t.Fatalf("bootstrap code mismatch: got=%d want=%d body=%s", bootstrapRec.Code, http.StatusOK, bootstrapRec.Body.String())
	}
	if strings.TrimSpace(srv.cfg.HTTP.AuthToken) != "m1n2r3y4" {
		t.Fatalf("server cfg token not updated")
	}
	if strings.TrimSpace(rt.runIn.HTTP.AuthToken) != "m1n2r3y4" {
		t.Fatalf("runtime cfg token not updated")
	}

	authorized := false
	authHandler := srv.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		authorized = true
		w.WriteHeader(http.StatusOK)
	})
	authReq := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	authReq.Header.Set("Authorization", "Bearer m1n2r3y4")
	authRec := httptest.NewRecorder()
	authHandler(authRec, authReq)
	if authRec.Code != http.StatusOK {
		t.Fatalf("auth code mismatch: got=%d want=%d body=%s", authRec.Code, http.StatusOK, authRec.Body.String())
	}
	if !authorized {
		t.Fatalf("authorized handler should be called")
	}
}

func TestBootstrapToken_UpdateRequiresExistingAuth(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigInDB(db, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}
	runCfg := NewRunInputFromConfig(cfg, "")
	rt := &Runtime{DB: db, runIn: runCfg}
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db}

	// 首次初始化：非 loopback 来源应拒绝。
	initBadReq := httptest.NewRequest(http.MethodPost, "/api/v1/bootstrap/token", strings.NewReader(`{"auth_token":"init-token"}`))
	initBadReq.RemoteAddr = "10.0.0.8:19090"
	initBadRec := httptest.NewRecorder()
	srv.handleBootstrapToken(initBadRec, initBadReq)
	if initBadRec.Code != http.StatusForbidden {
		t.Fatalf("init from non-loopback should be forbidden: got=%d want=%d body=%s", initBadRec.Code, http.StatusForbidden, initBadRec.Body.String())
	}

	// 首次初始化：loopback 允许。
	initReq := httptest.NewRequest(http.MethodPost, "/api/v1/bootstrap/token", strings.NewReader(`{"auth_token":"init-token"}`))
	initReq.RemoteAddr = "127.0.0.1:19090"
	initRec := httptest.NewRecorder()
	srv.handleBootstrapToken(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init code mismatch: got=%d want=%d body=%s", initRec.Code, http.StatusOK, initRec.Body.String())
	}

	// 已初始化后：未携带旧 token 认证，不允许改 token。
	updateNoAuthReq := httptest.NewRequest(http.MethodPost, "/api/v1/bootstrap/token", strings.NewReader(`{"auth_token":"new-token"}`))
	updateNoAuthReq.RemoteAddr = "10.0.0.8:19090"
	updateNoAuthRec := httptest.NewRecorder()
	srv.handleBootstrapToken(updateNoAuthRec, updateNoAuthReq)
	if updateNoAuthRec.Code != http.StatusUnauthorized {
		t.Fatalf("update without auth should be unauthorized: got=%d want=%d body=%s", updateNoAuthRec.Code, http.StatusUnauthorized, updateNoAuthRec.Body.String())
	}

	// 已初始化后：携带旧 token 认证，允许改 token。
	updateReq := httptest.NewRequest(http.MethodPost, "/api/v1/bootstrap/token", strings.NewReader(`{"auth_token":"new-token"}`))
	updateReq.RemoteAddr = "10.0.0.8:19090"
	updateReq.Header.Set("Authorization", "Bearer init-token")
	updateRec := httptest.NewRecorder()
	srv.handleBootstrapToken(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update code mismatch: got=%d want=%d body=%s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	if strings.TrimSpace(srv.cfg.HTTP.AuthToken) != "new-token" {
		t.Fatalf("server cfg token not updated to new token")
	}
	if strings.TrimSpace(rt.runIn.HTTP.AuthToken) != "new-token" {
		t.Fatalf("runtime cfg token not updated to new token")
	}
}
