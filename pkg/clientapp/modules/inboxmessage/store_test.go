package inboxmessage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/inboxmessage/storedb/gen"
	_ "modernc.org/sqlite"
)

type testDBConn struct {
	db *sql.DB
}

func (t testDBConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.db.ExecContext(ctx, query, args...)
}

func (t testDBConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.db.QueryContext(ctx, query, args...)
}

type testSerialExec struct {
	db *sql.DB
}

func (t testSerialExec) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.db.ExecContext(ctx, query, args...)
}

func (t testSerialExec) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.db.QueryContext(ctx, query, args...)
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

func prepareInboxMessageSchema(ctx context.Context, db *sql.DB) error {
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
	return client.Schema.Create(ctx)
}

func TestBootstrapStoreAndWriteRead(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "inboxmessage.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("pragma foreign_keys: %v", err)
	}
	if err := prepareInboxMessageSchema(context.Background(), db); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	store, err := BootstrapStore(context.Background(), testStore{db: db})
	if err != nil {
		t.Fatalf("bootstrap store: %v", err)
	}

	receipt, err := store.WriteInboxMessage(context.Background(), "msg1", "sender1", "target1", "route1", "application/json", []byte(`{"hello":"world"}`), 1234567890)
	if err != nil {
		t.Fatalf("write inbox message: %v", err)
	}
	if receipt.InboxMessageID <= 0 {
		t.Fatalf("expected positive inbox_id, got: %d", receipt.InboxMessageID)
	}

	receipt2, err := store.WriteInboxMessage(context.Background(), "msg1", "sender1", "target1", "route1", "application/json", []byte(`{"hello":"world2"}`), 1234567891)
	if err != nil {
		t.Fatalf("idempotent write: %v", err)
	}
	if receipt.InboxMessageID != receipt2.InboxMessageID {
		t.Fatalf("idempotent write should return same id: %d vs %d", receipt.InboxMessageID, receipt2.InboxMessageID)
	}

	items, err := store.ListInboxMessages(context.Background())
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 message, got: %d", len(items))
	}

	detail, err := store.GetInboxMessageDetail(context.Background(), receipt.InboxMessageID)
	if err != nil {
		t.Fatalf("get detail: %v", err)
	}
	if detail.MessageID != "msg1" {
		t.Fatalf("unexpected message_id: %s", detail.MessageID)
	}
	if detail.BodySizeBytes <= 0 {
		t.Fatalf("expected positive body_size_bytes, got: %d", detail.BodySizeBytes)
	}
}

func TestStoreListNotFound(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "inboxmessage2.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := BootstrapStore(context.Background(), testStore{db: db})
	if err != nil {
		t.Fatalf("bootstrap store: %v", err)
	}

	_, err = store.GetInboxMessageDetail(context.Background(), 999)
	if err == nil {
		t.Fatalf("expected NOT_FOUND for non-existent message")
	}
}
