package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/domainsvc"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/host"
)

func TestResolverResolveRoundTripOverP2P(t *testing.T) {
	t.Parallel()

	callerDB := openResolveCallTestDB(t)
	defer callerDB.Close()
	targetDB := openResolveCallTestDB(t)
	defer targetDB.Close()

	callerHost, _ := newSecpHost(t)
	defer callerHost.Close()
	resolverHost, resolverPubkeyHex := newSecpHost(t)
	defer resolverHost.Close()
	targetHost, targetPubkeyHex := newSecpHost(t)
	defer targetHost.Close()

	callerRT := &Runtime{Host: callerHost, DB: callerDB}
	targetRT := &Runtime{Host: targetHost, DB: targetDB}
	registerFakeDomainResolveHandler(resolverHost, "mp3.david", targetPubkeyHex, time.Now().Add(time.Hour).Unix())
	registerResolveCallHandlers(targetRT)

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)
	callerHost.Peerstore().AddAddrs(targetHost.ID(), targetHost.Addrs(), time.Minute)

	if _, err := targetDB.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("ef", 32),
		"/tmp/song.mp3",
		2048,
		"song.mp3",
		"audio/mpeg",
		time.Now().Unix(),
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := upsertPublishedRouteIndex(targetDB, "album", strings.Repeat("ef", 32)); err != nil {
		t.Fatalf("upsert route index: %v", err)
	}

	resolveResp, err := TriggerResolverResolve(context.Background(), callerRT, TriggerResolverResolveParams{
		ResolverPubkeyHex: resolverPubkeyHex,
		Name:              "MP3.DAVID",
	})
	if err != nil {
		t.Fatalf("trigger resolver resolve failed: %v", err)
	}
	if !resolveResp.Ok || resolveResp.TargetPubkeyHex != targetPubkeyHex || resolveResp.Name != "mp3.david" {
		t.Fatalf("unexpected resolver response: %+v", resolveResp)
	}

	resolveRespFromTarget, err := TriggerClientResolve(context.Background(), callerRT, TriggerClientResolveParams{
		To:    resolveResp.TargetPubkeyHex,
		Route: "album",
	})
	if err != nil {
		t.Fatalf("trigger client resolve failed: %v", err)
	}
	if !resolveRespFromTarget.Ok {
		t.Fatalf("resolve response not ok: %+v", resolveRespFromTarget)
	}
	var manifest routeIndexManifest
	if err := json.Unmarshal(resolveRespFromTarget.Body, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.SeedHash != strings.Repeat("ef", 32) {
		t.Fatalf("unexpected manifest seed hash: %s", manifest.SeedHash)
	}
}

func TestHTTPAPIResolverResolve(t *testing.T) {
	t.Parallel()

	callerDB := openResolveCallTestDB(t)
	defer callerDB.Close()

	callerHost, _ := newSecpHost(t)
	defer callerHost.Close()
	resolverHost, resolverPubkeyHex := newSecpHost(t)
	defer resolverHost.Close()
	targetHost, targetPubkeyHex := newSecpHost(t)
	defer targetHost.Close()

	callerRT := &Runtime{Host: callerHost, DB: callerDB}
	registerFakeDomainResolveHandler(resolverHost, "movie.david", targetPubkeyHex, time.Now().Add(time.Hour).Unix())

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)

	callerSrv := &httpAPIServer{rt: callerRT, db: callerDB}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/resolvers/resolve", strings.NewReader(`{"resolver_pubkey_hex":"`+resolverPubkeyHex+`","name":"movie.david"}`))
	rec := httptest.NewRecorder()
	callerSrv.handleResolverResolve(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("resolver resolve status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), targetPubkeyHex) {
		t.Fatalf("expected target pubkey hex in body: %s", rec.Body.String())
	}
}

func registerFakeDomainResolveHandler(h host.Host, expectedName string, targetPubkeyHex string, expireAtUnix int64) {
	p2prpc.HandleProto[domainsvc.ResolveNamePaidReq, domainsvc.ResolveNamePaidResp](h, domainsvc.ProtoResolveNamePaid, p2prpc.SecurityConfig{
		Domain:  "bitcast-domain",
		Network: "test",
		TTL:     30 * time.Second,
	}, func(_ context.Context, req domainsvc.ResolveNamePaidReq) (domainsvc.ResolveNamePaidResp, error) {
		name, err := normalizeResolverNameCanonical(req.Name)
		if err != nil {
			return domainsvc.ResolveNamePaidResp{Success: false, Status: "bad_request", Error: err.Error()}, nil
		}
		if name != expectedName {
			return domainsvc.ResolveNamePaidResp{Success: false, Status: "not_found", Name: name, Error: "domain name not found"}, nil
		}
		return domainsvc.ResolveNamePaidResp{
			Success:         true,
			Status:          "ok",
			Name:            name,
			TargetPubkeyHex: targetPubkeyHex,
			ExpireAtUnix:    expireAtUnix,
		}, nil
	})
}
