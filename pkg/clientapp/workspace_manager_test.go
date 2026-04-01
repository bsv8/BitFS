package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkspaceAddSyncAndDeleteCleanup(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	ws1 := filepath.Join(base, "ws1")
	ws2 := filepath.Join(base, "ws2")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "seeds"), 0o755); err != nil {
		t.Fatalf("mkdir seeds dir: %v", err)
	}
	if err := os.MkdirAll(ws1, 0o755); err != nil {
		t.Fatalf("mkdir ws1: %v", err)
	}
	if err := os.MkdirAll(ws2, 0o755); err != nil {
		t.Fatalf("mkdir ws2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws1, "a.txt"), []byte("hello-workspace-1"), 0o644); err != nil {
		t.Fatalf("write ws1 file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws2, "b.txt"), []byte("hello-workspace-2"), 0o644); err != nil {
		t.Fatalf("write ws2 file: %v", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(base, "index.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws1
	cfg.Storage.DataDir = dataDir
	cfg.Seller.Pricing.FloorPriceSatPer64K = 10
	cfg.Seller.Pricing.ResaleDiscountBPS = 8000
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	mgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		catalog: &sellerCatalog{seeds: map[string]sellerSeed{}},
	}
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	if _, err := mgr.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once (ws1): %v", err)
	}

	assertCount := func(query string, want int) {
		t.Helper()
		var got int
		if err := db.QueryRow(query).Scan(&got); err != nil {
			t.Fatalf("query count failed: %v", err)
		}
		if got != want {
			t.Fatalf("count mismatch query=%s got=%d want=%d", query, got, want)
		}
	}

	assertCount(`SELECT COUNT(1) FROM workspace_files`, 1)
	assertCount(`SELECT COUNT(1) FROM seeds`, 1)

	ws2Item, err := mgr.Add(ws2, 0)
	if err != nil {
		t.Fatalf("add ws2: %v", err)
	}
	if _, err := mgr.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once (ws1+ws2): %v", err)
	}
	assertCount(`SELECT COUNT(1) FROM workspace_files`, 2)
	assertCount(`SELECT COUNT(1) FROM seeds`, 2)

	// 删除 ws2 后，相关索引数据应被清理；再扫描会清理孤儿 seed 文件。
	if err := mgr.DeleteByPath(ws2Item.WorkspacePath); err != nil {
		t.Fatalf("delete ws2: %v", err)
	}
	if _, err := mgr.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once after delete: %v", err)
	}

	var ws2Files int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspace_files WHERE workspace_path=?`, ws2).Scan(&ws2Files); err != nil {
		t.Fatalf("query ws2 files: %v", err)
	}
	if ws2Files != 0 {
		t.Fatalf("ws2 files should be cleaned, got=%d", ws2Files)
	}
	assertCount(`SELECT COUNT(1) FROM seeds`, 1)
}

func TestRegisterPartialFileKeepSeedOnRescan(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	ws := filepath.Join(base, "ws")
	if err := os.MkdirAll(filepath.Join(dataDir, "seeds"), 0o755); err != nil {
		t.Fatalf("mkdir seeds dir: %v", err)
	}
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	fullPath := filepath.Join(base, "full.bin")
	full := make([]byte, int(seedBlockSize*2+123))
	for i := range full {
		full[i] = byte(i % 251)
	}
	if err := os.WriteFile(fullPath, full, 0o644); err != nil {
		t.Fatalf("write full file: %v", err)
	}
	seedBytes, seedHash, chunkCount, err := buildSeedV1(fullPath)
	if err != nil {
		t.Fatalf("build seed: %v", err)
	}
	if chunkCount < 2 {
		t.Fatalf("invalid test seed chunk count=%d", chunkCount)
	}

	partialPath := filepath.Join(ws, "partial.bin")
	if err := os.WriteFile(partialPath, full[:seedBlockSize], 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(base, "index.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = dataDir
	cfg.Seller.Pricing.FloorPriceSatPer64K = 10
	cfg.Seller.Pricing.ResaleDiscountBPS = 8000
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	mgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		catalog: &sellerCatalog{seeds: map[string]sellerSeed{}},
	}
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	if _, err := mgr.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              partialPath,
		Seed:                  seedBytes,
		AvailableChunkIndexes: []uint32{0},
	}); err != nil {
		t.Fatalf("register downloaded file: %v", err)
	}

	if _, err := mgr.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	var gotSeedHash string
	var gotLocked int64
	err = db.QueryRow(`SELECT seed_hash,seed_locked FROM workspace_files WHERE workspace_path=? AND file_path=?`, ws, filepath.Base(partialPath)).Scan(&gotSeedHash, &gotLocked)
	if err != nil {
		t.Fatalf("query workspace file: %v", err)
	}
	if gotSeedHash != seedHash {
		t.Fatalf("workspace seed hash changed after rescan: got=%s want=%s", gotSeedHash, seedHash)
	}
	if gotLocked != 1 {
		t.Fatalf("workspace file should stay locked, got=%d", gotLocked)
	}

	var gotSeedPath string
	var gotChunkCount uint32
	if err := db.QueryRow(`SELECT seed_file_path,chunk_count FROM seeds WHERE seed_hash=?`, seedHash).Scan(&gotSeedPath, &gotChunkCount); err != nil {
		t.Fatalf("query seeds: %v", err)
	}
	if gotChunkCount != chunkCount {
		t.Fatalf("seed chunk count mismatch: got=%d want=%d", gotChunkCount, chunkCount)
	}
	onDiskSeed, err := os.ReadFile(gotSeedPath)
	if err != nil {
		t.Fatalf("read seed file: %v", err)
	}
	if hex.EncodeToString(onDiskSeed) != hex.EncodeToString(seedBytes) {
		t.Fatalf("seed bytes mismatch after register")
	}

	var availCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM seed_chunk_supply WHERE seed_hash=?`, seedHash).Scan(&availCount); err != nil {
		t.Fatalf("count available chunks: %v", err)
	}
	if availCount != 1 {
		t.Fatalf("available chunk count mismatch: got=%d want=1", availCount)
	}
	var idx uint32
	if err := db.QueryRow(`SELECT chunk_index FROM seed_chunk_supply WHERE seed_hash=?`, seedHash).Scan(&idx); err != nil {
		t.Fatalf("query available chunk index: %v", err)
	}
	if idx != 0 {
		t.Fatalf("available chunk index mismatch: got=%d want=0", idx)
	}
}

func TestEnforceLiveCacheLimit_DeleteWholeOldStream(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	ws := filepath.Join(base, "ws")
	if err := os.MkdirAll(filepath.Join(dataDir, "seeds"), 0o755); err != nil {
		t.Fatalf("mkdir seeds dir: %v", err)
	}
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(base, "index.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = dataDir
	cfg.Seller.Pricing.FloorPriceSatPer64K = 10
	cfg.Seller.Pricing.ResaleDiscountBPS = 8000
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	mgr := &workspaceManager{
		cfg:     &cfg,
		db:      db,
		catalog: &sellerCatalog{seeds: map[string]sellerSeed{}},
	}
	wsItem, err := mgr.Add(ws, 100)
	if err != nil {
		t.Fatalf("add workspace: %v", err)
	}
	if wsItem.MaxBytes != 100 {
		t.Fatalf("workspace max bytes mismatch: got=%d want=100", wsItem.MaxBytes)
	}
	if err := mgr.ValidateLiveCacheCapacity(101); err == nil {
		t.Fatalf("expected live cache validation failure")
	}
	if err := mgr.ValidateLiveCacheCapacity(100); err != nil {
		t.Fatalf("validate live cache: %v", err)
	}

	streamOld := strings.Repeat("a", 64)
	streamNew := strings.Repeat("b", 64)
	oldPath := filepath.Join(ws, "live", streamOld, "000000.seg")
	newPath := filepath.Join(ws, "live", streamNew, "000000.seg")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o755); err != nil {
		t.Fatalf("mkdir old stream: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		t.Fatalf("mkdir new stream: %v", err)
	}
	if err := os.WriteFile(oldPath, []byte(strings.Repeat("o", 60)), 0o644); err != nil {
		t.Fatalf("write old seg: %v", err)
	}
	if err := os.WriteFile(newPath, []byte(strings.Repeat("n", 30)), 0o644); err != nil {
		t.Fatalf("write new seg: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, ws, filepath.Join("live", streamOld, "000000.seg"), strings.Repeat("c", 64), 0); err != nil {
		t.Fatalf("insert old workspace file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, ws, filepath.Join("live", streamNew, "000000.seg"), strings.Repeat("d", 64), 0); err != nil {
		t.Fatalf("insert new workspace file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, strings.Repeat("c", 64), 1, 60, filepath.Join(dataDir, "seeds", "c.bse"), "", ""); err != nil {
		t.Fatalf("insert old seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, strings.Repeat("d", 64), 1, 30, filepath.Join(dataDir, "seeds", "d.bse"), "", ""); err != nil {
		t.Fatalf("insert new seed: %v", err)
	}

	if err := mgr.EnforceLiveCacheLimit(40); err != nil {
		t.Fatalf("enforce live cache: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old stream file should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("new stream file should remain: %v", err)
	}
	var remain int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspace_files WHERE file_path LIKE ?`, "%"+streamOld+"%").Scan(&remain); err != nil {
		t.Fatalf("count old stream files: %v", err)
	}
	if remain != 0 {
		t.Fatalf("old stream rows should be removed, got=%d", remain)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM seeds WHERE seed_hash=?`, strings.Repeat("c", 64)).Scan(&remain); err != nil {
		t.Fatalf("count old seed: %v", err)
	}
	if remain != 0 {
		t.Fatalf("old seed should be cleaned, got=%d", remain)
	}
}
