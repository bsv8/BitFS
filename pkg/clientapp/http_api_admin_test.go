package clientapp

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAdminStrategyDebugLog(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
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
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.FSHTTP.StrategyDebugLogEnabled = false
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigInDB(db, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}

	rt := &Runtime{DB: db, Config: cfg}
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

	if !cfg.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("cfg pointer should be updated")
	}
	if !rt.Config.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("runtime config should be updated")
	}

	var raw string
	if err := db.QueryRow(`SELECT config_toml FROM app_config WHERE id=1`).Scan(&raw); err != nil {
		t.Fatalf("query app config: %v", err)
	}
	loaded, err := ParseConfigTOML([]byte(raw))
	if err != nil {
		t.Fatalf("parse app config: %v", err)
	}
	if !loaded.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("db persisted config should be enabled")
	}
}
