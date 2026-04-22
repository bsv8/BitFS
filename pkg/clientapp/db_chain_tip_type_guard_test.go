package clientapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

func TestDBUpsertChainTipStateDoesNotTriggerDriverTypeError(t *testing.T) {
	dir := t.TempDir()
	opened, err := sqliteactor.Open(filepath.Join(dir, "chain-tip.sqlite"), false)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer func() { _ = opened.Actor.Close() }()

	if err := ensureClientSchemaOnDB(context.Background(), opened.DB); err != nil {
		t.Fatalf("ensure schema failed: %v", err)
	}

	store := newClientDB(opened.DB, opened.Actor)
	tip := uint32(^uint32(0))
	if err := dbUpsertChainTipState(context.Background(), store, tip, "", "test", 123, 456); err != nil {
		t.Fatalf("dbUpsertChainTipState failed: %v", err)
	}

	got, err := dbLoadChainTipState(context.Background(), store)
	if err != nil {
		t.Fatalf("dbLoadChainTipState failed: %v", err)
	}
	if got.TipHeight != tip {
		t.Fatalf("unexpected tip height: got %d want %d", got.TipHeight, tip)
	}
}
