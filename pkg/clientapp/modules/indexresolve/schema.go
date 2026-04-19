package indexresolve

import (
	"context"
	"fmt"
)

const (
	indexResolveRoutesTableSQL = `CREATE TABLE IF NOT EXISTS proc_index_resolve_routes(
		route TEXT PRIMARY KEY,
		seed_hash TEXT NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`
	indexResolveRoutesSeedHashIndexSQL = `CREATE INDEX IF NOT EXISTS idx_proc_index_resolve_routes_seed_hash
		ON proc_index_resolve_routes(seed_hash)`
	indexResolveRoutesUpdatedAtIndexSQL = `CREATE INDEX IF NOT EXISTS idx_proc_index_resolve_routes_updated_at
		ON proc_index_resolve_routes(updated_at_unix DESC, route ASC)`
)

func bootstrapSchema(ctx context.Context, db dbCapability) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	stmts := []string{
		indexResolveRoutesTableSQL,
		indexResolveRoutesSeedHashIndexSQL,
		indexResolveRoutesUpdatedAtIndexSQL,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap index_resolve schema: %w", err)
		}
	}
	return nil
}
