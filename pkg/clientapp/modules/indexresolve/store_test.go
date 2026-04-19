package indexresolve

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

type testDBCapability struct {
	db     *sql.DB
	serial bool
}

func (t testDBCapability) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.db.ExecContext(ctx, query, args...)
}

func (t testDBCapability) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.db.QueryContext(ctx, query, args...)
}

func (t testDBCapability) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.db.QueryRowContext(ctx, query, args...)
}

func (t testDBCapability) SerialAccess() bool {
	return t.serial
}

func TestBootstrapStoreAndLifecycle(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "indexresolve.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("pragma foreign_keys: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE biz_seeds(
		seed_hash TEXT PRIMARY KEY,
		recommended_file_name TEXT NOT NULL,
		mime_hint TEXT NOT NULL,
		file_size INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create biz_seeds: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,recommended_file_name,mime_hint,file_size) VALUES(?,?,?,?)`,
		strings.Repeat("aa", 32), "movie.mp4", "video/mp4", 4096); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	capability := testDBCapability{db: db}
	store, err := BootstrapStore(context.Background(), capability, nil)
	if err != nil {
		t.Fatalf("bootstrap store: %v", err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='proc_index_resolve_routes'`).Scan(&name); err != nil {
		t.Fatalf("module table missing: %v", err)
	}

	svc := NewService(store, nil)
	item, err := svc.Upsert(context.Background(), "movie", strings.Repeat("aa", 32), 123)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if item.Route != "/movie" {
		t.Fatalf("unexpected route item: %+v", item)
	}
	manifest, err := svc.Resolve(context.Background(), "/movie")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if manifest.SeedHash != strings.Repeat("aa", 32) || manifest.RecommendedFileName != "movie.mp4" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	if err := svc.Delete(context.Background(), "movie"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := svc.Resolve(context.Background(), "/movie"); err == nil || CodeOf(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected route not found after delete, got %v", err)
	}
}
