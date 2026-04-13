package clientapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadedFileUpsertDoesNotTouchSeedPricingPolicy(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "index.sqlite")
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

	wsRoot := filepath.Join(base, "workspace")
	if err := os.MkdirAll(wsRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsRoot, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}

	abs := filepath.Join(wsRoot, "downloads", "sample.bin")
	seedHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, int64(1), int64(256), filepath.Join(base, "seed", "sample.bse"), "", ""); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if err := dbUpsertDownloadedFile(context.Background(), newClientDB(db, nil), abs, seedHash, filepath.Join(base, "seed", "sample.bse"), 4, 256, "sample.bin", "application/octet-stream", true); err != nil {
		t.Fatalf("upsert downloaded file: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, wsRoot, "downloads/sample.bin").Scan(&count); err != nil {
		t.Fatalf("count workspace file: %v", err)
	}
	if count != 1 {
		t.Fatalf("workspace file should be upserted, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count seed: %v", err)
	}
	if count != 1 {
		t.Fatalf("seed should be upserted, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count pricing policy: %v", err)
	}
	if count != 0 {
		t.Fatalf("download registration should not create pricing policy, got=%d", count)
	}
}

func TestOrphanCleanupKeepsSharedSeedState(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "index.sqlite")
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

	wsA := filepath.Join(base, "ws-a")
	wsB := filepath.Join(base, "ws-b")
	for _, root := range []string{wsA, wsB} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("mkdir workspace: %v", err)
		}
	}
	seedHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsA, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert wsA: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsB, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert wsB: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, int64(2), int64(128), filepath.Join(base, "biz_seeds", "seed.bse"), "", ""); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seed_chunk_supply(seed_hash,chunk_index) VALUES(?,?),(?,?)`, seedHash, int64(0), seedHash, int64(1)); err != nil {
		t.Fatalf("insert supply: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seed_pricing_policy(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix) VALUES(?,?,?,?,?)`, seedHash, uint64(10), uint64(1000), "user", int64(9)); err != nil {
		t.Fatalf("insert policy: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, wsA, "alpha.bin", seedHash, 0); err != nil {
		t.Fatalf("insert file a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, wsB, "beta.bin", seedHash, 0); err != nil {
		t.Fatalf("insert file b: %v", err)
	}

	if _, err := db.Exec(`DELETE FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, wsA, "alpha.bin"); err != nil {
		t.Fatalf("delete one file instance: %v", err)
	}
	if err := dbCleanupOrphanSeedState(context.Background(), newClientDB(db, nil)); err != nil {
		t.Fatalf("cleanup with shared seed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count seed after shared cleanup: %v", err)
	}
	if count != 1 {
		t.Fatalf("seed should stay while shared, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count policy after shared cleanup: %v", err)
	}
	if count != 1 {
		t.Fatalf("policy should stay while shared, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seed_chunk_supply WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count supply after shared cleanup: %v", err)
	}
	if count != 2 {
		t.Fatalf("supply should stay while shared, got=%d", count)
	}

	if _, err := db.Exec(`DELETE FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, wsB, "beta.bin"); err != nil {
		t.Fatalf("delete second file instance: %v", err)
	}
	if err := dbCleanupOrphanSeedState(context.Background(), newClientDB(db, nil)); err != nil {
		t.Fatalf("cleanup after final delete: %v", err)
	}
	for _, query := range []string{
		`SELECT COUNT(1) FROM biz_seeds WHERE seed_hash=?`,
		`SELECT COUNT(1) FROM biz_seed_pricing_policy WHERE seed_hash=?`,
		`SELECT COUNT(1) FROM biz_seed_chunk_supply WHERE seed_hash=?`,
	} {
		if err := db.QueryRow(query, seedHash).Scan(&count); err != nil {
			t.Fatalf("count after final cleanup: %v", err)
		}
		if count != 0 {
			t.Fatalf("orphan cleanup should remove seed state, query=%s got=%d", query, count)
		}
	}
}

func createLegacyInventorySchema(db *sql.DB, withSupplyTable bool) error {
	stmts := []string{
		`CREATE TABLE biz_workspaces(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			workspace_path TEXT NOT NULL,
			path TEXT NOT NULL,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE biz_workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			file_size INTEGER NOT NULL DEFAULT 0,
			mtime_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE biz_seeds(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE seed_price_state(
			seed_hash TEXT PRIMARY KEY,
			last_buy_unit_price_sat_per_64k INTEGER NOT NULL,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL
		)`,
	}
	if withSupplyTable {
		stmts = append(stmts, `CREATE TABLE seed_available_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL
		)`)
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
