package clientapp

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestEnsureClientDBSchemaTurnsOnForeignKeys(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_fk=1")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas failed: %v", err)
	}

	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
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
