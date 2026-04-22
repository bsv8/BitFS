package clientapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

func TestWorkspaceSyncOnceWithActorEmptyWorkspace(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "workspace-actor.sqlite")
	opened, err := sqliteactor.Open(dbPath, false)
	if err != nil {
		t.Fatalf("open sqliteactor: %v", err)
	}
	t.Cleanup(func() {
		if opened.Actor != nil {
			_ = opened.Actor.Close()
		}
		if opened.DB != nil {
			_ = opened.DB.Close()
		}
	})
	if err := ensureClientDBSchemaOnDB(t.Context(), opened.DB); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	workspaceDir := filepath.Join(root, "workspace")
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "biz_seeds"), 0o755); err != nil {
		t.Fatalf("mkdir biz_seeds: %v", err)
	}

	var cfg Config
	cfg.Storage.WorkspaceDir = workspaceDir
	cfg.Storage.DataDir = dataDir
	cfg.Seller.Pricing.FloorPriceSatPer64K = 10
	cfg.Seller.Pricing.ResaleDiscountBPS = 8000
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	mgr := &workspaceManager{
		ctx:     context.Background(),
		cfg:     &cfg,
		db:      opened.DB,
		store:   NewClientStore(opened.DB, opened.Actor),
		catalog: newSellerCatalog(),
	}
	if err := mgr.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	seeds, err := mgr.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if len(seeds) != 0 {
		t.Fatalf("seed count mismatch: got=%d want=0", len(seeds))
	}
}
