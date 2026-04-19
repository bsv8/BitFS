//go:build with_indexresolve

package clientapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	oldproto "github.com/golang/protobuf/proto"
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

	callerRT := &Runtime{Host: callerHost}
	targetRT := &Runtime{Host: targetHost, ctx: t.Context(), modules: newModuleRegistry()}
	callerStore := newClientDB(callerDB, nil)
	targetStore := newClientDB(targetDB, nil)
	closeModule, err := registerOptionalModules(t.Context(), targetRT, targetStore)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	if closeModule != nil {
		t.Cleanup(closeModule)
	}
	registerFakeDomainResolveHandler(resolverHost, "mp3.david", targetPubkeyHex, time.Now().Add(time.Hour).Unix())
	registerNodeRouteHandlers(targetRT, targetStore)
	targetSrv := &httpAPIServer{rt: targetRT, db: targetDB, store: targetStore}
	targetHandler, err := targetSrv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)
	callerHost.Peerstore().AddAddrs(targetHost.ID(), targetHost.Addrs(), time.Minute)

	if _, err := targetDB.Exec(
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("ef", 32),
		1,
		2048,
		"/tmp/song.mp3",
		"song.mp3",
		"audio/mpeg",
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"album","seed_hash":"`+strings.Repeat("ef", 32)+`"}`))
		rec := httptest.NewRecorder()
		targetHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("upsert route index status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	resolveResp, err := TriggerResolverResolve(context.Background(), callerStore, callerRT, TriggerResolverResolveParams{
		ResolverPubkeyHex: resolverPubkeyHex,
		Name:              "MP3.DAVID",
	})
	if err != nil {
		t.Fatalf("trigger resolver resolve failed: %v", err)
	}
	if !resolveResp.Ok || resolveResp.TargetPubkeyHex != targetPubkeyHex || resolveResp.Name != "mp3.david" {
		t.Fatalf("unexpected resolver response: %+v", resolveResp)
	}

	resolveRespFromTarget, err := TriggerPeerResolve(context.Background(), callerRT, TriggerPeerResolveParams{
		To:    resolveResp.TargetPubkeyHex,
		Route: "album",
		Store: callerStore,
	})
	if err != nil {
		t.Fatalf("trigger client resolve failed: %v", err)
	}
	if !resolveRespFromTarget.Ok {
		t.Fatalf("resolve response not ok: %+v", resolveRespFromTarget)
	}
	var manifest routeIndexManifest
	if err := oldproto.Unmarshal(resolveRespFromTarget.Body, &manifest); err != nil {
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

	callerRT := &Runtime{Host: callerHost}
	callerStore := newClientDB(callerDB, nil)
	registerFakeDomainResolveHandler(resolverHost, "movie.david", targetPubkeyHex, time.Now().Add(time.Hour).Unix())

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)

	callerSrv := &httpAPIServer{rt: callerRT, db: callerDB, store: callerStore}
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
	ncall.Register(h, pproto.SecurityConfig{
		Domain:  "bitfs-node",
		Network: "test",
		TTL:     30 * time.Second,
	}, func(_ context.Context, _ ncall.CallContext, req ncall.CallReq) (ncall.CallResp, error) {
		if strings.TrimSpace(req.Route) != string(contractroute.RouteDomainV1Resolve) {
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
		var body contractmessage.NameRouteReq
		if err := oldproto.Unmarshal(req.Body, &body); err != nil {
			return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: "invalid protobuf body"}, nil
		}
		name, err := normalizeResolverNameCanonical(body.Name)
		if err != nil {
			raw, _ := oldproto.Marshal(&contractmessage.ResolveNamePaidResp{Success: false, Status: "bad_request", Error: err.Error()})
			return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: err.Error(), ContentType: contractmessage.ContentTypeProto, Body: raw}, nil
		}
		var routeResp contractmessage.ResolveNamePaidResp
		if name != expectedName {
			routeResp = contractmessage.ResolveNamePaidResp{Success: false, Status: "not_found", Name: name, Error: "domain name not found"}
		} else {
			routeResp = contractmessage.ResolveNamePaidResp{
				Success:         true,
				Status:          "ok",
				Name:            name,
				TargetPubkeyHex: targetPubkeyHex,
				ExpireAtUnix:    expireAtUnix,
			}
		}
		raw, err := oldproto.Marshal(&routeResp)
		if err != nil {
			return ncall.CallResp{}, err
		}
		return ncall.CallResp{Ok: true, Code: "OK", ContentType: contractmessage.ContentTypeProto, Body: raw}, nil
	}, nil)
}
