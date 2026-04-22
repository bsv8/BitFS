package clientapp

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestEnsureClientDBSchemaTurnsOnForeignKeys(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	var enabled int
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil {
		t.Fatalf("read foreign_keys pragma failed: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("foreign_keys pragma mismatch: got=%d want=1", enabled)
	}
}
