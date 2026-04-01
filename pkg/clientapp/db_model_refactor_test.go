package clientapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestClientDBLegacyMigrationAndSystemPricingFill(t *testing.T) {
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
	if err := createLegacyInventorySchema(db, true); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}

	seed1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	seed2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	wsRoot := filepath.Join(base, "workspace")
	if err := os.MkdirAll(wsRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(workspace_path,path,max_bytes,enabled,created_at_unix,updated_at_unix) VALUES(?,?,?,?,?,?)`, wsRoot, wsRoot, int64(4096), int64(1), int64(11), int64(12)); err != nil {
		t.Fatalf("insert legacy workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,path,seed_hash,seed_locked,file_size,mtime_unix,updated_at_unix) VALUES(?,?,?,?,?,?,?,?)`, wsRoot, "file-a.bin", filepath.Join(wsRoot, "file-a.bin"), seed1, int64(0), int64(123), int64(22), int64(23)); err != nil {
		t.Fatalf("insert legacy workspace file a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,path,seed_hash,seed_locked,file_size,mtime_unix,updated_at_unix) VALUES(?,?,?,?,?,?,?,?)`, wsRoot, "file-b.bin", filepath.Join(wsRoot, "file-b.bin"), seed2, int64(1), int64(456), int64(33), int64(34)); err != nil {
		t.Fatalf("insert legacy workspace file b: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`, seed1, int64(3), int64(300), filepath.Join(base, "seeds", "seed1.bse"), "file-a.bin", "text/plain", int64(44)); err != nil {
		t.Fatalf("insert legacy seed 1: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`, seed2, int64(2), int64(200), filepath.Join(base, "seeds", "seed2.bse"), "file-b.bin", "text/plain", int64(55)); err != nil {
		t.Fatalf("insert legacy seed 2: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seed_price_state(seed_hash,last_buy_unit_price_sat_per_64k,floor_unit_price_sat_per_64k,resale_discount_bps) VALUES(?,?,?,?)`, seed1, int64(9), int64(7), int64(1250)); err != nil {
		t.Fatalf("insert legacy price state: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seed_available_chunks(seed_hash,chunk_index) VALUES(?,?),(?,?)`, seed1, int64(0), seed1, int64(1)); err != nil {
		t.Fatalf("insert legacy supply marker: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cols, err := tableColumns(db, "seed_chunk_supply")
	if err != nil {
		t.Fatalf("read chunk supply columns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("seed_chunk_supply column count mismatch: got=%d want=2", len(cols))
	}
	if _, ok := cols["seed_hash"]; !ok {
		t.Fatalf("seed_chunk_supply missing seed_hash")
	}
	if _, ok := cols["chunk_index"]; !ok {
		t.Fatalf("seed_chunk_supply missing chunk_index")
	}
	if _, ok := cols["chunk_size"]; ok {
		t.Fatalf("seed_chunk_supply should not keep chunk_size")
	}
	if _, ok := cols["chunk_hash"]; ok {
		t.Fatalf("seed_chunk_supply should not keep chunk_hash")
	}

	var filePath string
	var locked int64
	if err := db.QueryRow(`SELECT file_path,seed_locked FROM workspace_files WHERE workspace_path=? AND seed_hash=?`, wsRoot, seed1).Scan(&filePath, &locked); err != nil {
		t.Fatalf("query migrated workspace file: %v", err)
	}
	if filePath != "file-a.bin" {
		t.Fatalf("migrated file path mismatch: got=%s want=%s", filePath, "file-a.bin")
	}
	if locked != 0 {
		t.Fatalf("migrated locked mismatch: got=%d want=0", locked)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seed1).Scan(&count); err != nil {
		t.Fatalf("count migrated supply: %v", err)
	}
	if count != 2 {
		t.Fatalf("migrated supply count mismatch: got=%d want=2", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seed2).Scan(&count); err != nil {
		t.Fatalf("count migrated supply seed2: %v", err)
	}
	if count != 0 {
		t.Fatalf("migrated supply count mismatch for seed2: got=%d want=0", count)
	}

	var floor, resale uint64
	var source string
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source FROM seed_pricing_policy WHERE seed_hash=?`, seed1).Scan(&floor, &resale, &source); err != nil {
		t.Fatalf("query migrated user policy: %v", err)
	}
	if floor != 7 || resale != 1250 || source != "user" {
		t.Fatalf("migrated policy mismatch: floor=%d resale=%d source=%s", floor, resale, source)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_pricing_policy WHERE seed_hash=?`, seed2).Scan(&count); err != nil {
		t.Fatalf("count policy seed2: %v", err)
	}
	if count != 0 {
		t.Fatalf("seed2 should not have legacy policy yet, got=%d", count)
	}

	if err := dbSyncSystemSeedPricingPolicies(db, 11, 2222); err != nil {
		t.Fatalf("sync system policies: %v", err)
	}
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source FROM seed_pricing_policy WHERE seed_hash=?`, seed1).Scan(&floor, &resale, &source); err != nil {
		t.Fatalf("query preserved user policy: %v", err)
	}
	if floor != 7 || resale != 1250 || source != "user" {
		t.Fatalf("user policy should be preserved: floor=%d resale=%d source=%s", floor, resale, source)
	}
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source FROM seed_pricing_policy WHERE seed_hash=?`, seed2).Scan(&floor, &resale, &source); err != nil {
		t.Fatalf("query synced system policy: %v", err)
	}
	if floor != 11 || resale != 2222 || source != "system" {
		t.Fatalf("system policy mismatch: floor=%d resale=%d source=%s", floor, resale, source)
	}

	for _, table := range []string{"seed_available_chunks", "seed_price_state"} {
		if exists, err := hasTable(db, table); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		} else if exists {
			t.Fatalf("legacy table should be dropped: %s", table)
		}
	}
}

func TestClientDBLegacyMigrationWithoutLegacySupplyTableGeneratesFullChunks(t *testing.T) {
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
	if err := createLegacyInventorySchema(db, false); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}

	seed1 := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	seed2 := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	wsRoot := filepath.Join(base, "workspace")
	if err := os.MkdirAll(wsRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(workspace_path,path,max_bytes,enabled,created_at_unix,updated_at_unix) VALUES(?,?,?,?,?,?)`, wsRoot, wsRoot, int64(4096), int64(1), int64(11), int64(12)); err != nil {
		t.Fatalf("insert legacy workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`, seed1, int64(3), int64(300), filepath.Join(base, "seeds", "seed1.bse"), "file-a.bin", "text/plain", int64(44)); err != nil {
		t.Fatalf("insert legacy seed 1: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`, seed2, int64(2), int64(200), filepath.Join(base, "seeds", "seed2.bse"), "file-b.bin", "text/plain", int64(55)); err != nil {
		t.Fatalf("insert legacy seed 2: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seed1).Scan(&count); err != nil {
		t.Fatalf("count migrated supply seed1: %v", err)
	}
	if count != 3 {
		t.Fatalf("migrated supply count mismatch for seed1: got=%d want=3", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seed2).Scan(&count); err != nil {
		t.Fatalf("count migrated supply seed2: %v", err)
	}
	if count != 2 {
		t.Fatalf("migrated supply count mismatch for seed2: got=%d want=2", count)
	}
}

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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	wsRoot := filepath.Join(base, "workspace")
	if err := os.MkdirAll(wsRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsRoot, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert workspace: %v", err)
	}

	abs := filepath.Join(wsRoot, "downloads", "sample.bin")
	seedHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := dbUpsertDownloadedFile(context.Background(), newClientDB(db, nil), abs, seedHash, filepath.Join(base, "seed", "sample.bse"), 4, 256, "sample.bin", "application/octet-stream", true); err != nil {
		t.Fatalf("upsert downloaded file: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspace_files WHERE workspace_path=? AND file_path=?`, wsRoot, "downloads/sample.bin").Scan(&count); err != nil {
		t.Fatalf("count workspace file: %v", err)
	}
	if count != 1 {
		t.Fatalf("workspace file should be upserted, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seeds WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count seed: %v", err)
	}
	if count != 1 {
		t.Fatalf("seed should be upserted, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	wsA := filepath.Join(base, "ws-a")
	wsB := filepath.Join(base, "ws-b")
	for _, root := range []string{wsA, wsB} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("mkdir workspace: %v", err)
		}
	}
	seedHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, err := db.Exec(`INSERT INTO workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsA, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert wsA: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, wsB, int64(1), int64(0), int64(1)); err != nil {
		t.Fatalf("insert wsB: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, int64(2), int64(128), filepath.Join(base, "seeds", "seed.bse"), "", ""); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seed_chunk_supply(seed_hash,chunk_index) VALUES(?,?),(?,?)`, seedHash, int64(0), seedHash, int64(1)); err != nil {
		t.Fatalf("insert supply: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seed_pricing_policy(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix) VALUES(?,?,?,?,?)`, seedHash, uint64(10), uint64(1000), "user", int64(9)); err != nil {
		t.Fatalf("insert policy: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, wsA, "alpha.bin", seedHash, 0); err != nil {
		t.Fatalf("insert file a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, wsB, "beta.bin", seedHash, 0); err != nil {
		t.Fatalf("insert file b: %v", err)
	}

	if _, err := db.Exec(`DELETE FROM workspace_files WHERE workspace_path=? AND file_path=?`, wsA, "alpha.bin"); err != nil {
		t.Fatalf("delete one file instance: %v", err)
	}
	if err := dbCleanupOrphanSeedState(context.Background(), newClientDB(db, nil)); err != nil {
		t.Fatalf("cleanup with shared seed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM seeds WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count seed after shared cleanup: %v", err)
	}
	if count != 1 {
		t.Fatalf("seed should stay while shared, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count policy after shared cleanup: %v", err)
	}
	if count != 1 {
		t.Fatalf("policy should stay while shared, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seedHash).Scan(&count); err != nil {
		t.Fatalf("count supply after shared cleanup: %v", err)
	}
	if count != 2 {
		t.Fatalf("supply should stay while shared, got=%d", count)
	}

	if _, err := db.Exec(`DELETE FROM workspace_files WHERE workspace_path=? AND file_path=?`, wsB, "beta.bin"); err != nil {
		t.Fatalf("delete second file instance: %v", err)
	}
	if err := dbCleanupOrphanSeedState(context.Background(), newClientDB(db, nil)); err != nil {
		t.Fatalf("cleanup after final delete: %v", err)
	}
	for _, query := range []string{
		`SELECT COUNT(1) FROM seeds WHERE seed_hash=?`,
		`SELECT COUNT(1) FROM seed_pricing_policy WHERE seed_hash=?`,
		`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`,
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
		`CREATE TABLE workspaces(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			workspace_path TEXT NOT NULL,
			path TEXT NOT NULL,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			file_size INTEGER NOT NULL DEFAULT 0,
			mtime_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE seeds(
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
