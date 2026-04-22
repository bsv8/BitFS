package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newWalletAPITestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func mustInsertTestWorkspaceRoot(t *testing.T, db *sql.DB, workspacePath string) {
	t.Helper()
	if db == nil {
		t.Fatal("db is nil")
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, workspacePath, 1, 0, time.Now().Unix()); err != nil {
		t.Fatalf("insert workspace root: %v", err)
	}
}

func newTestFileStorageRuntime(t *testing.T, cfg *Config, db *sql.DB) fileStorageRuntime {
	t.Helper()
	store := newClientDB(db, nil)
	rt := newRuntimeForTest(t, *cfg, strings.Repeat("11", 32), withRuntimeStore(store))
	seedStore := newModuleHost(rt, store).SeedStorage()
	return newFileStorageRuntimeAdapter(store, seedStore)
}

func newTestTaskScheduler(ctx context.Context, store *clientDB) *taskScheduler {
	s := newTaskScheduler(store, ServiceName)
	s.ctx = ctx
	return s
}
