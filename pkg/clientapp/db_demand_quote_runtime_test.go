package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTriggerGatewayPublishDemandRecordsDemand(t *testing.T) {
	t.Skip("broadcast demand publish 尚未迁移到独立 protocol，gateway 仍使用 pool-based base_tx 分发")
}

func TestTriggerGatewayPublishDemandBeginObsOnlySeedHash(t *testing.T) {
	t.Skip("broadcast demand publish 尚未迁移到独立 protocol，gateway 仍使用 pool-based base_tx 分发")
}

func TestDefaultArbiterPubHexUsesPubHexFallback(t *testing.T) {
	buyerHost, _ := newSecpHost(t)
	defer buyerHost.Close()
	arbHost, arbPubHex := newSecpHost(t)
	defer arbHost.Close()
	if err := buyerHost.Connect(context.Background(), peer.AddrInfo{ID: arbHost.ID(), Addrs: arbHost.Addrs()}); err != nil {
		t.Fatalf("connect arbiter: %v", err)
	}

	rt := newRuntimeForTest(t, Config{}, "")
	rt.Host = buyerHost
	rt.HealthyArbiters = []peer.AddrInfo{
		{ID: arbHost.ID(), Addrs: arbHost.Addrs()},
	}

	got := defaultArbiterPubHex(rt)
	if got != arbPubHex {
		t.Fatalf("default arbiter mismatch: got=%s want=%s", got, arbPubHex)
	}
	if got == arbHost.ID().String() {
		t.Fatalf("default arbiter still uses peer id string: %s", got)
	}
}

func TestDemandQuoteSchemaRebuildAddsForeignKeys(t *testing.T) {
	t.Skip("migration test removed")
}

func TestDemandQuoteSchemaRebuildRejectsOrphans(t *testing.T) {
	t.Skip("migration test removed")
}

func newDemandQuoteFKTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}

func enableForeignKeys(db *sql.DB) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(`PRAGMA foreign_keys=ON`)
	return err
}
