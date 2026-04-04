package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTriggerGatewayPublishDemandRecordsDemand(t *testing.T) {
	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	buyerHost, buyerPubHex := newSecpHost(t)
	defer buyerHost.Close()
	gwHost, gwPubHex := newSecpHost(t)
	defer gwHost.Close()
	buyerHost.Peerstore().AddAddrs(gwHost.ID(), gwHost.Addrs(), time.Minute)
	gwHost.Peerstore().AddAddrs(buyerHost.ID(), buyerHost.Addrs(), time.Minute)

	rt := &Runtime{
		Host: buyerHost,
		HealthyGWs: []peer.AddrInfo{
			{ID: gwHost.ID(), Addrs: gwHost.Addrs()},
		},
	}
	rt.runIn.Network.Gateways = []PeerNode{
		{Enabled: true, Addr: gwHost.Addrs()[0].String() + "/p2p/" + gwHost.ID().String(), Pubkey: gwPubHex},
	}
	ncall.Register(gwHost, nodeSecForRuntime(rt), func(_ context.Context, _ ncall.CallContext, req ncall.CallReq) (ncall.CallResp, error) {
		if req.Route != broadcastmodule.RouteBroadcastV1DemandPublish {
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
		body, err := oldproto.Marshal(&broadcastmodule.DemandPublishPaidResp{
			Success:   true,
			Status:    "ok",
			DemandID:  "dmd_publish_records",
			Published: true,
		})
		if err != nil {
			return ncall.CallResp{}, err
		}
		return ncall.CallResp{
			Ok:          true,
			Code:        "OK",
			ContentType: ncall.ContentTypeProto,
			Body:        body,
		}, nil
	}, nil)

	resp, err := TriggerGatewayPublishDemand(context.Background(), store, rt, PublishDemandParams{
		SeedHash:   "seed_publish_records",
		ChunkCount: 1,
	})
	if err != nil {
		t.Fatalf("publish demand: %v", err)
	}
	if !resp.Success || resp.DemandID != "dmd_publish_records" {
		t.Fatalf("unexpected publish resp: %+v", resp)
	}

	var gotSeedHash string
	if err := db.QueryRow(`SELECT seed_hash FROM biz_demands WHERE demand_id=?`, resp.DemandID).Scan(&gotSeedHash); err != nil {
		t.Fatalf("load demand: %v", err)
	}
	if gotSeedHash != "seed_publish_records" {
		t.Fatalf("seed hash mismatch: got=%s want=%s", gotSeedHash, "seed_publish_records")
	}
	if buyerPubHex == "" {
		t.Fatalf("buyer pub hex missing")
	}
}

func TestDefaultArbiterPubHexUsesPubHexFallback(t *testing.T) {
	buyerHost, _ := newSecpHost(t)
	defer buyerHost.Close()
	arbHost, arbPubHex := newSecpHost(t)
	defer arbHost.Close()
	if err := buyerHost.Connect(context.Background(), peer.AddrInfo{ID: arbHost.ID(), Addrs: arbHost.Addrs()}); err != nil {
		t.Fatalf("connect arbiter: %v", err)
	}

	rt := &Runtime{
		Host: buyerHost,
		HealthyArbiters: []peer.AddrInfo{
			{ID: arbHost.ID(), Addrs: arbHost.Addrs()},
		},
	}

	got := defaultArbiterPubHex(rt)
	if got != arbPubHex {
		t.Fatalf("default arbiter mismatch: got=%s want=%s", got, arbPubHex)
	}
	if got == arbHost.ID().String() {
		t.Fatalf("default arbiter still uses peer id string: %s", got)
	}
}

func TestDemandQuoteForeignKeysRejectOrphanRows(t *testing.T) {
	db := newDemandQuoteFKTestDB(t)

	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"missing_demand", "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, 10, 1, 1024, "x.bin", "text/plain", "ff", 1893427200, 1700000001,
	); err == nil {
		t.Fatalf("expected demand quote foreign key failure")
	}

	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_fk", "seed_fk", 1700000002); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"dmd_fk", "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, 10, 1, 1024, "x.bin", "text/plain", "ff", 1893427200, 1700000001,
	); err != nil {
		t.Fatalf("insert valid quote: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?)`, 999, "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"); err == nil {
		t.Fatalf("expected arbiter foreign key failure")
	}
}

func TestDemandQuoteSchemaRebuildAddsForeignKeys(t *testing.T) {
	db := newDemandQuoteNoFKSchemaDB(t)

	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_fk_rebuild", "seed_fk_rebuild", 1700000001); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1, "dmd_fk_rebuild", "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, 10, 1, 1024, "keep.bin", "text/plain", "ff", 1893427200, 1700000002,
	); err != nil {
		t.Fatalf("insert quote: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quote_arbiters(id,quote_id,arbiter_pub_hex) VALUES(?,?,?)`, 1, 1, "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"); err != nil {
		t.Fatalf("insert arbiter: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	if err := enableForeignKeys(db); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	hasFK, err := demandQuoteTableMissingFK(db, "biz_demand_quotes", "demand_id", "biz_demands", "demand_id")
	if err != nil {
		t.Fatalf("check biz_demand_quotes fk: %v", err)
	}
	if hasFK {
		t.Fatalf("biz_demand_quotes should have foreign key after rebuild")
	}
	hasFK, err = demandQuoteTableMissingFK(db, "biz_demand_quote_arbiters", "quote_id", "biz_demand_quotes", "id")
	if err != nil {
		t.Fatalf("check biz_demand_quote_arbiters fk: %v", err)
	}
	if hasFK {
		t.Fatalf("biz_demand_quote_arbiters should have foreign key after rebuild")
	}

	var seedHash string
	if err := db.QueryRow(`SELECT seed_hash FROM biz_demands WHERE demand_id=?`, "dmd_fk_rebuild").Scan(&seedHash); err != nil {
		t.Fatalf("load demand: %v", err)
	}
	if seedHash != "seed_fk_rebuild" {
		t.Fatalf("seed hash mismatch: %s", seedHash)
	}

	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"missing_demand", "02dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 100, 10, 1, 1024, "x.bin", "text/plain", "ff", 1893427200, 1700000003,
	); err == nil {
		t.Fatalf("expected demand quote foreign key failure after rebuild")
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?)`, 999, "02eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"); err == nil {
		t.Fatalf("expected arbiter foreign key failure after rebuild")
	}
}

func TestDemandQuoteSchemaRebuildRejectsOrphans(t *testing.T) {
	db := newDemandQuoteNoFKSchemaDB(t)

	if _, err := db.Exec(`INSERT INTO biz_demand_quotes(
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1, "missing_demand", "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 100, 10, 1, 1024, "keep.bin", "text/plain", "ff", 1893427200, 1700000002,
	); err != nil {
		t.Fatalf("insert orphan quote: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_demand_quote_arbiters(id,quote_id,arbiter_pub_hex) VALUES(?,?,?)`, 1, 1, "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"); err != nil {
		t.Fatalf("insert orphan arbiter: %v", err)
	}

	err := initIndexDB(db)
	if err == nil {
		t.Fatalf("expected rebuild failure for orphan rows")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "orphan") {
		t.Fatalf("expected orphan error, got: %v", err)
	}
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

func newDemandQuoteNoFKSchemaDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "client-index-no-fk.sqlite")
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
	stmts := []string{
		`CREATE TABLE biz_demands(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id)
		)`,
		`CREATE TABLE biz_demand_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pub_hex TEXT NOT NULL,
			seed_price_satoshi INTEGER NOT NULL,
			chunk_price_satoshi INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL,
			file_size_bytes INTEGER NOT NULL,
			recommended_file_name TEXT NOT NULL,
			mime_type TEXT NOT NULL,
			available_chunk_bitmap_hex TEXT NOT NULL,
			expires_at_unix INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id, seller_pub_hex)
		)`,
		`CREATE TABLE biz_demand_quote_arbiters(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			quote_id INTEGER NOT NULL,
			arbiter_pub_hex TEXT NOT NULL,
			UNIQUE(quote_id, arbiter_pub_hex)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create legacy new schema: %v", err)
		}
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
