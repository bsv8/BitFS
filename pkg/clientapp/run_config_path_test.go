package clientapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

func TestRun_RejectsEmptyConfigPathWhenHTTPManagementIsEnabled(t *testing.T) {
	dir := t.TempDir()
	wsDir := filepath.Join(dir, "workspace")
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.DataDir = dataDir
	cfg.Storage.MinFreeBytes = 1
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.Enabled = false
	cfg.Debug = true
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"

	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeTest); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = false
	cfg.HTTP.ListenAddr = "127.0.0.1:0"

	dbPath := filepath.Join(dataDir, "client-index.sqlite")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	openedDB, err := sqliteactor.Open(dbPath, true)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = openedDB.Actor.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ensureClientDBSchemaOnDB(ctx, openedDB.DB); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	_, err = Run(ctx, cfg, RunDeps{
		Store:   NewClientStore(openedDB.DB, openedDB.Actor),
		RawDB:   openedDB.DB,
		DBActor: openedDB.Actor,
		OwnsDB:  true,
	}, RunOptions{
		StartupMode:         StartupModeTest,
		EffectivePrivKeyHex: cfg.Keys.PrivkeyHex,
	})
	if err == nil {
		t.Fatal("expected Run to reject empty config path")
	}
	if !strings.Contains(err.Error(), "config path is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}
