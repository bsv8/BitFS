package storedb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve/storedb/gen"
)

// EnsureSchema 只负责 indexresolve owner 的建表。
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := ensureSchema(ctx, db); err == nil {
		return nil
	} else if !strings.Contains(err.Error(), `missing "_fk=1"`) {
		return err
	}

	fkDB, cleanup, err := openSQLiteForeignKeyDB(ctx, db)
	if err != nil {
		return fmt.Errorf("indexresolve schema ensure failed: %w", err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	return ensureSchema(ctx, fkDB)
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
	return client.Schema.Create(ctx)
}

func openSQLiteForeignKeyDB(ctx context.Context, db *sql.DB) (*sql.DB, func(), error) {
	filePath, err := sqliteMainDBFilePath(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	if filePath == "" {
		return nil, nil, fmt.Errorf("sqlite database path is empty")
	}
	uri := (&url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(filePath),
		RawQuery: "_fk=1",
	}).String()
	fkDB, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = fkDB.Close()
	}
	if err := applySQLiteForeignKeyPragmas(fkDB); err != nil {
		cleanup()
		return nil, nil, err
	}
	return fkDB, cleanup, nil
}

func sqliteMainDBFilePath(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA database_list`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, filePath string
		if err := rows.Scan(&seq, &name, &filePath); err != nil {
			return "", err
		}
		if strings.EqualFold(name, "main") {
			return strings.TrimSpace(filePath), nil
		}
	}
	return "", rows.Err()
}

func applySQLiteForeignKeyPragmas(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("sqlite pragma foreign_keys: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("sqlite pragma journal_mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=15000`); err != nil {
		return fmt.Errorf("sqlite pragma busy_timeout: %w", err)
	}
	return nil
}
