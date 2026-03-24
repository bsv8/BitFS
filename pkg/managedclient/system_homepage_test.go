package managedclient

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSystemHomepageStateAndInstall(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	bundleDir := filepath.Join(root, "bundle")
	workspaceDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatalf("mkdir bundle: %v", err)
	}
	rootHash := "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd"
	childHash := "052b8d352bbab7129389974c9734b6bf0b6661a55b25cadb2a0b947dfb063ce0"
	manifest := systemHomepageManifest{
		RootEntryHash: rootHash,
		Files: map[string]json.RawMessage{
			rootHash:  json.RawMessage(`{"kind":"asset"}`),
			childHash: json.RawMessage(`{"kind":"asset"}`),
		},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, rootHash), []byte("root-home"), 0o644); err != nil {
		t.Fatalf("write root seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, childHash), []byte("child-home"), 0o644); err != nil {
		t.Fatalf("write child seed: %v", err)
	}

	state, err := loadSystemHomepageState(bundleDir, workspaceDir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil {
		t.Fatalf("expected state")
	}
	if got, want := state.DefaultSeedHash, rootHash; got != want {
		t.Fatalf("default seed hash=%q, want %q", got, want)
	}
	if err := state.InstallIntoWorkspace(); err != nil {
		t.Fatalf("install workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, ".bitfs-system", "homepage", rootHash)); err != nil {
		t.Fatalf("stat installed root seed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceDir, ".bitfs-system", "homepage", childHash)); err != nil {
		t.Fatalf("stat installed child seed: %v", err)
	}
}

func TestSystemHomepageEnsureSeedPricesOnlyFillsMissingFloor(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "homepage.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE seeds(seed_hash TEXT PRIMARY KEY, seed_file_path TEXT NOT NULL, chunk_count INTEGER, file_size INTEGER, created_at_unix INTEGER)`); err != nil {
		t.Fatalf("create seeds: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE seed_price_state(seed_hash TEXT PRIMARY KEY, last_buy_unit_price_sat_per_64k INTEGER, floor_unit_price_sat_per_64k INTEGER, resale_discount_bps INTEGER, unit_price_sat_per_64k INTEGER, updated_at_unix INTEGER)`); err != nil {
		t.Fatalf("create seed_price_state: %v", err)
	}

	rootHash := "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd"
	childHash := "052b8d352bbab7129389974c9734b6bf0b6661a55b25cadb2a0b947dfb063ce0"
	rootSeedPath := filepath.Join(root, rootHash+".bse")
	childSeedPath := filepath.Join(root, childHash+".bse")
	if err := os.WriteFile(rootSeedPath, []byte("root-seed"), 0o644); err != nil {
		t.Fatalf("write root seed: %v", err)
	}
	if err := os.WriteFile(childSeedPath, []byte("child-seed"), 0o644); err != nil {
		t.Fatalf("write child seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,created_at_unix) VALUES(?,?,?,?,?)`, rootHash, rootSeedPath, 1, 9, 1); err != nil {
		t.Fatalf("insert root seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,created_at_unix) VALUES(?,?,?,?,?)`, childHash, childSeedPath, 1, 10, 1); err != nil {
		t.Fatalf("insert child seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seed_price_state(seed_hash,last_buy_unit_price_sat_per_64k,floor_unit_price_sat_per_64k,resale_discount_bps,unit_price_sat_per_64k,updated_at_unix) VALUES(?,?,?,?,?,?)`, childHash, nil, 9, 5000, 9, 1); err != nil {
		t.Fatalf("insert existing price: %v", err)
	}

	state := &systemHomepageState{
		DefaultSeedHash: rootHash,
		SeedHashes:      []string{rootHash, childHash},
	}
	if err := state.EnsureSeedPrices(db, 5000); err != nil {
		t.Fatalf("ensure prices: %v", err)
	}

	var rootFloor uint64
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k FROM seed_price_state WHERE seed_hash=?`, rootHash).Scan(&rootFloor); err != nil {
		t.Fatalf("query root floor: %v", err)
	}
	if got, want := rootFloor, systemHomepageFloorPriceSatPer64K; got != want {
		t.Fatalf("root floor=%d, want %d", got, want)
	}
	var childFloor uint64
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k FROM seed_price_state WHERE seed_hash=?`, childHash).Scan(&childFloor); err != nil {
		t.Fatalf("query child floor: %v", err)
	}
	if got, want := childFloor, uint64(9); got != want {
		t.Fatalf("child floor=%d, want %d", got, want)
	}
}

func TestSystemHomepageApplySeedMetadataOnlyFillsMissingOrLowConfidenceFields(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "homepage-meta.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE seeds(
		seed_hash TEXT PRIMARY KEY,
		seed_file_path TEXT NOT NULL,
		chunk_count INTEGER,
		file_size INTEGER,
		recommended_file_name TEXT NOT NULL DEFAULT '',
		mime_hint TEXT NOT NULL DEFAULT '',
		created_at_unix INTEGER
	)`); err != nil {
		t.Fatalf("create seeds: %v", err)
	}

	rootHash := "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd"
	childHash := "052b8d352bbab7129389974c9734b6bf0b6661a55b25cadb2a0b947dfb063ce0"
	if _, err := db.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?),(?,?,?,?,?,?,?)`,
		rootHash, filepath.Join(root, rootHash), 1, 10, rootHash, "application/octet-stream", 1,
		childHash, filepath.Join(root, childHash), 1, 11, "keep.css", "text/css", 1,
	); err != nil {
		t.Fatalf("insert seeds: %v", err)
	}

	state := &systemHomepageState{
		DefaultSeedHash: rootHash,
		SeedHashes:      []string{rootHash, childHash},
		FileMetaBySeed: map[string]systemHomepageFileMeta{
			rootHash: {
				OriginalName: "index.html",
				MIME:         "text/html",
			},
			childHash: {
				OriginalName: "override.js",
				MIME:         "application/javascript",
			},
		},
	}
	if err := state.ApplySeedMetadata(db); err != nil {
		t.Fatalf("apply metadata: %v", err)
	}

	var rootName, rootMIME string
	if err := db.QueryRow(`SELECT recommended_file_name,mime_hint FROM seeds WHERE seed_hash=?`, rootHash).Scan(&rootName, &rootMIME); err != nil {
		t.Fatalf("query root seed: %v", err)
	}
	if rootName != "index.html" || rootMIME != "text/html" {
		t.Fatalf("root seed metadata mismatch: name=%q mime=%q", rootName, rootMIME)
	}

	var childName, childMIME string
	if err := db.QueryRow(`SELECT recommended_file_name,mime_hint FROM seeds WHERE seed_hash=?`, childHash).Scan(&childName, &childMIME); err != nil {
		t.Fatalf("query child seed: %v", err)
	}
	if childName != "keep.css" || childMIME != "text/css" {
		t.Fatalf("child seed metadata should not be overwritten: name=%q mime=%q", childName, childMIME)
	}
}
