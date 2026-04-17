package clientapp

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkspaceKernelListAddUpdateDeleteSync(t *testing.T) {
	root := t.TempDir()
	db := openWorkspaceKernelTestDB(t, root)

	cfg := workspaceKernelTestConfig(root)
	mgr := newTestWorkspaceManager(t.Context(), &cfg, db)
	rt := newRuntimeForTest(t, cfg, "", withRuntimeWorkspace(mgr))
	ws := rt.ClientKernel().Workspace()
	if ws == nil {
		t.Fatal("workspace manager should not be nil")
	}

	list, err := ws.ListWithContext(t.Context())
	if err != nil {
		t.Fatalf("list workspace: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("list should be empty: %+v", list)
	}

	wsDir := filepath.Join(root, "workspace-a")
	if err := writeWorkspaceTestFile(wsDir, "seed-a.bin", []byte("workspace-seed-a")); err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}

	added, err := ws.AddWithContext(t.Context(), wsDir, 0)
	if err != nil {
		t.Fatalf("add workspace: %v", err)
	}
	if strings.TrimSpace(added.WorkspacePath) == "" {
		t.Fatal("added workspace path is empty")
	}
	seedsAfterAdd, err := ws.SyncOnce(t.Context())
	if err != nil {
		t.Fatalf("sync after add: %v", err)
	}
	if len(seedsAfterAdd) == 0 {
		t.Fatal("sync result should not be empty after add")
	}

	maxBytes := uint64(4096)
	enabled := false
	updated, err := ws.UpdateByPathWithContext(t.Context(), wsDir, &maxBytes, &enabled)
	if err != nil {
		t.Fatalf("update workspace: %v", err)
	}
	if updated.WorkspacePath == "" || updated.Enabled {
		t.Fatalf("workspace should be disabled after update: %+v", updated)
	}

	listAfterUpdate, err := ws.ListWithContext(t.Context())
	if err != nil {
		t.Fatalf("list after update: %v", err)
	}
	if len(listAfterUpdate) != 1 {
		t.Fatalf("list after update total=%d, want 1", len(listAfterUpdate))
	}

	if err := ws.DeleteByPathWithContext(t.Context(), wsDir); err != nil {
		t.Fatalf("delete workspace: %v", err)
	}
	listAfterDelete, err := ws.ListWithContext(t.Context())
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listAfterDelete) != 0 {
		t.Fatalf("list after delete total=%d, want 0", len(listAfterDelete))
	}

	syncRes, err := ws.SyncOnce(t.Context())
	if err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if len(syncRes) != 0 {
		t.Fatalf("sync once after delete seed_count=%d, want 0", len(syncRes))
	}
}

func TestWorkspaceKernelWalletModeAndValidation(t *testing.T) {
	root := t.TempDir()
	db := openWorkspaceKernelTestDB(t, root)
	cfg := workspaceKernelTestConfig(root)
	rt := newRuntimeForTest(t, cfg, "", withRuntimeWorkspace(newTestWorkspaceManager(t.Context(), &cfg, db)))
	mustUpdateRuntimeConfigMemoryOnly(t, rt, func(next *Config) {
		next.Storage.WorkspaceDir = ""
	})
	ws := rt.ClientKernel().Workspace()

	list, err := ws.ListWithContext(t.Context())
	if err != nil {
		t.Fatalf("wallet list: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("wallet list should be empty: %+v", list)
	}

	syncRes, err := ws.SyncOnce(t.Context())
	if err != nil {
		t.Fatalf("wallet sync once: %v", err)
	}
	if len(syncRes) != 0 {
		t.Fatalf("wallet sync should be empty: %+v", syncRes)
	}

	if _, err := ws.AddWithContext(t.Context(), "", 0); err == nil {
		t.Fatal("missing path should fail")
	}
	if _, err := ws.AddWithContext(t.Context(), filepath.Join(root, "workspace-a"), 0); err == nil {
		t.Fatal("wallet mode add should fail")
	}
	if _, err := ws.UpdateByPathWithContext(t.Context(), filepath.Join(root, "workspace-a"), nil, nil); err == nil {
		t.Fatal("missing update fields should fail")
	}
	if err := ws.DeleteByPathWithContext(t.Context(), ""); err == nil {
		t.Fatal("missing delete path should fail")
	}
}

func TestWorkspaceKernelAddThenSyncFailure(t *testing.T) {
	root := t.TempDir()
	db := openWorkspaceKernelTestDB(t, root)

	cfg := workspaceKernelTestConfig(root)
	mgr := newTestWorkspaceManager(t.Context(), &cfg, db)
	rt := newRuntimeForTest(t, cfg, "", withRuntimeWorkspace(mgr))
	ws := rt.ClientKernel().Workspace()

	wsDir := filepath.Join(root, "workspace-fail")
	if err := writeWorkspaceTestFile(wsDir, "seed-fail.bin", []byte("workspace-seed-fail")); err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}
	blocked := filepath.Join(wsDir, "blocked")
	if err := os.MkdirAll(blocked, 0o755); err != nil {
		t.Fatalf("prepare blocked dir: %v", err)
	}
	if err := os.Chmod(blocked, 0o000); err != nil {
		t.Fatalf("chmod blocked dir: %v", err)
	}

	if _, err := ws.AddWithContext(t.Context(), wsDir, 0); err != nil {
		t.Fatalf("add should still apply: %v", err)
	}
	if _, err := ws.SyncOnce(t.Context()); err == nil {
		t.Fatal("sync failure should be returned")
	}

	list, listErr := ws.ListWithContext(t.Context())
	if listErr != nil {
		t.Fatalf("list after failed sync: %v", listErr)
	}
	if len(list) != 1 {
		t.Fatalf("workspace should stay applied after sync failure, total=%d", len(list))
	}
}

func openWorkspaceKernelTestDB(t *testing.T, root string) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(root, "workspace.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := os.MkdirAll(filepath.Join(root, "data", "biz_seeds"), 0o755); err != nil {
		t.Fatalf("mkdir biz_seeds: %v", err)
	}
	if err := EnsureClientStoreSchema(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return db
}

func workspaceKernelTestConfig(root string) Config {
	var cfg Config
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeTest); err != nil {
		panic(err)
	}
	cfg.Storage.WorkspaceDir = filepath.Join(root, "workspace")
	cfg.Storage.DataDir = filepath.Join(root, "data")
	cfg.Index.SQLitePath = filepath.Join(root, "data", "client-index.sqlite")
	if err := os.MkdirAll(cfg.Storage.WorkspaceDir, 0o755); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		panic(err)
	}
	return cfg
}

func writeWorkspaceTestFile(rootDir, name string, body []byte) error {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(rootDir, name), body, 0o644); err != nil {
		return err
	}
	return nil
}
