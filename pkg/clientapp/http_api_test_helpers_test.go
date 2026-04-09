package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func newTestWorkspaceManager(ctx context.Context, cfg *Config, db *sql.DB) *workspaceManager {
	return &workspaceManager{
		ctx:     ctx,
		cfg:     cfg,
		db:      db,
		catalog: &sellerCatalog{biz_seeds: map[string]sellerSeed{}},
	}
}

func newTestTaskScheduler(ctx context.Context, store *clientDB) *taskScheduler {
	s := newTaskScheduler(store, "bitcast-client")
	s.ctx = ctx
	return s
}
