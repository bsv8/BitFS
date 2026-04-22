package indexresolve

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve/storedb/gen"
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

type testStore struct {
	db *sql.DB
}

func (t testStore) Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error {
	return fn(t.db)
}

func (t testStore) WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	wtx := testWriteTx{tx: tx}
	if err := fn(wtx); err != nil {
		return err
	}
	return tx.Commit()
}

type testWriteTx struct {
	tx *sql.Tx
}

func (t testWriteTx) Exec(query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(query, args...)
}
func (t testWriteTx) QueryRow(query string, args ...any) *sql.Row {
	return t.tx.QueryRow(query, args...)
}
func (t testWriteTx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.tx.Query(query, args...)
}
func (t testWriteTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}
func (t testWriteTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}
func (t testWriteTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}
func (t testWriteTx) Tx(context.Context, func(moduleapi.WriteTx) error) error { return sql.ErrTxDone }

func prepareIndexResolveSchema(ctx context.Context, db *sql.DB) error {
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
	return client.Schema.Create(ctx)
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
	if err := coredb.EnsureSchema(context.Background(), db); err != nil {
		t.Fatalf("create core schema: %v", err)
	}
	if err := prepareIndexResolveSchema(context.Background(), db); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("aa", 32), 1, 4096, filepath.Join(t.TempDir(), "movie.bse"), "movie.mp4", "video/mp4"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	store, err := BootstrapStore(context.Background(), testStore{db: db})
	if err != nil {
		t.Fatalf("bootstrap store: %v", err)
	}

	item, err := BizSettingsUpsert(context.Background(), store, "movie", strings.Repeat("aa", 32))
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if item.Route != "/movie" {
		t.Fatalf("unexpected route item: %+v", item)
	}
	manifest, err := BizResolve(context.Background(), store, "/movie")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if manifest.SeedHash != strings.Repeat("aa", 32) || manifest.RecommendedFileName != "movie.mp4" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	if err := BizSettingsDelete(context.Background(), store, "movie"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := BizResolve(context.Background(), store, "/movie"); err == nil || CodeOf(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected route not found after delete, got %v", err)
	}
}
