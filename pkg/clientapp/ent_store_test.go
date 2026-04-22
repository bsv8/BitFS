package clientapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	bizseeds "github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
)

func TestReadEnt_OnlyProvidesQueryRoot(t *testing.T) {
	db := openSchemaTestDB(t)
	store := NewClientStore(db, nil)
	if err := EnsureClientStoreSchema(t.Context(), store); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	seedHash := "read-ent-seed"
	if err := store.WriteEntTx(t.Context(), func(root EntWriteRoot) error {
		_, err := root.BizSeeds.Create().
			SetSeedHash(seedHash).
			SetChunkCount(1).
			SetFileSize(64).
			SetSeedFilePath("/tmp/read-ent-seed").
			SetRecommendedFileName("read-ent.bin").
			SetMimeHint("application/octet-stream").
			Save(t.Context())
		return err
	}); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	err := store.ReadEnt(t.Context(), func(root EntReadRoot) error {
		if _, ok := any(root).(interface {
			Tx(context.Context) (*gen.Tx, error)
		}); ok {
			t.Fatal("read root must not expose Tx")
		}
		count, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Count(t.Context())
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("expected 1 row, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read ent failed: %v", err)
	}
}

func TestWriteEntTx_AllowsWriteAndQueryInSameTx(t *testing.T) {
	db := openSchemaTestDB(t)
	store := NewClientStore(db, nil)
	if err := EnsureClientStoreSchema(t.Context(), store); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	seedHash := "write-ent-seed"
	if err := store.WriteEntTx(t.Context(), func(root EntWriteRoot) error {
		_, err := root.BizSeeds.Create().
			SetSeedHash(seedHash).
			SetChunkCount(2).
			SetFileSize(128).
			SetSeedFilePath("/tmp/write-ent-seed").
			SetRecommendedFileName("write-ent.bin").
			SetMimeHint("application/octet-stream").
			Save(t.Context())
		if err != nil {
			return err
		}
		count, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Count(t.Context())
		if err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("expected 1 row inside tx, got %d", count)
		}
		return nil
	}); err != nil {
		t.Fatalf("write ent tx failed: %v", err)
	}
}

func TestWriteEntTx_UsesWriterQueue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	opened, err := sqliteactor.Open(dbPath, false)
	if err != nil {
		t.Fatalf("open sqlite actor failed: %v", err)
	}
	if err := applySQLitePragmas(opened.DB); err != nil {
		t.Fatalf("apply pragmas failed: %v", err)
	}
	t.Cleanup(func() {
		_ = opened.Actor.Close()
		_ = opened.DB.Close()
	})

	store := NewClientStore(opened.DB, opened.Actor)
	if err := EnsureClientStoreSchema(t.Context(), store); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	secondEntered := make(chan struct{})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- store.WriteEntTx(t.Context(), func(root EntWriteRoot) error {
			_, err := root.BizSeeds.Create().
				SetSeedHash("queue-seed-1").
				SetChunkCount(1).
				SetFileSize(64).
				SetSeedFilePath("/tmp/queue-seed-1").
				SetRecommendedFileName("queue-1.bin").
				SetMimeHint("application/octet-stream").
				Save(t.Context())
			if err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first write did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- store.WriteEntTx(t.Context(), func(root EntWriteRoot) error {
			close(secondEntered)
			_, err := root.BizSeeds.Create().
				SetSeedHash("queue-seed-2").
				SetChunkCount(1).
				SetFileSize(64).
				SetSeedFilePath("/tmp/queue-seed-2").
				SetRecommendedFileName("queue-2.bin").
				SetMimeHint("application/octet-stream").
				Save(t.Context())
			return err
		})
	}()

	select {
	case <-secondEntered:
		t.Fatal("second write entered before first released")
	case <-time.After(250 * time.Millisecond):
	}

	close(release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("second write did not enter after release")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second write failed: %v", err)
	}
}
