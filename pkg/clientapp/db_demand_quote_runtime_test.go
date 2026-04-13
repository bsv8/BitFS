package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
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
		if req.Route != string(contractroute.RouteBroadcastV1DemandPublish) {
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
		body, err := oldproto.Marshal(&contractmessage.DemandPublishPaidResp{
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
			ContentType: contractmessage.ContentTypeProto,
			Body:        body,
		}, nil
	}, nil)

	resp, err := TriggerGatewayPublishDemand(context.Background(), store, rt, PublishDemandParams{
		SeedHash:      "seed_publish_records",
		ChunkCount:    1,
		GatewayPeerID: gwPubHex,
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

func TestDemandQuoteSchemaRebuildAddsForeignKeys(t *testing.T) {
	t.Skip("legacy migration test removed")
}

func TestDemandQuoteSchemaRebuildRejectsOrphans(t *testing.T) {
	t.Skip("legacy migration test removed")
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

func enableForeignKeys(db *sql.DB) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(`PRAGMA foreign_keys=ON`)
	return err
}
