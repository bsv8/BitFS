package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResolverResolveRoundTripOverP2P(t *testing.T) {
	t.Parallel()

	callerDB := openResolveCallTestDB(t)
	defer callerDB.Close()
	resolverDB := openResolveCallTestDB(t)
	defer resolverDB.Close()
	targetDB := openResolveCallTestDB(t)
	defer targetDB.Close()

	callerHost, _ := newSecpHost(t)
	defer callerHost.Close()
	resolverHost, resolverPubkeyHex := newSecpHost(t)
	defer resolverHost.Close()
	targetHost, targetPubkeyHex := newSecpHost(t)
	defer targetHost.Close()

	callerRT := &Runtime{Host: callerHost, DB: callerDB}
	resolverRT := &Runtime{Host: resolverHost, DB: resolverDB}
	targetRT := &Runtime{Host: targetHost, DB: targetDB}
	registerResolverHandlers(resolverRT)
	registerResolveCallHandlers(targetRT)

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)
	callerHost.Peerstore().AddAddrs(targetHost.ID(), targetHost.Addrs(), time.Minute)

	if _, err := upsertResolverRecord(resolverDB, "mp3.david", targetPubkeyHex); err != nil {
		t.Fatalf("upsert resolver record: %v", err)
	}
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

func TestHTTPAPIResolverResolveAndAdminRecords(t *testing.T) {
	t.Parallel()

	callerDB := openResolveCallTestDB(t)
	defer callerDB.Close()
	resolverDB := openResolveCallTestDB(t)
	defer resolverDB.Close()

	callerHost, _ := newSecpHost(t)
	defer callerHost.Close()
	resolverHost, resolverPubkeyHex := newSecpHost(t)
	defer resolverHost.Close()
	targetHost, targetPubkeyHex := newSecpHost(t)
	defer targetHost.Close()

	callerRT := &Runtime{Host: callerHost, DB: callerDB}
	resolverRT := &Runtime{Host: resolverHost, DB: resolverDB}
	registerResolverHandlers(resolverRT)

	callerHost.Peerstore().AddAddrs(resolverHost.ID(), resolverHost.Addrs(), time.Minute)

	callerSrv := &httpAPIServer{rt: callerRT, db: callerDB}
	resolverSrv := &httpAPIServer{rt: resolverRT, db: resolverDB}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/resolvers/records", strings.NewReader(`{"name":"Movie.David","target_pubkey_hex":"`+targetPubkeyHex+`"}`))
		rec := httptest.NewRecorder()
		resolverSrv.handleAdminResolverRecords(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("admin resolver records status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	{
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

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/resolvers/records", nil)
		rec := httptest.NewRecorder()
		resolverSrv.handleAdminResolverRecords(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list resolver records status mismatch: got=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"movie.david"`) {
			t.Fatalf("expected normalized resolver name in body: %s", rec.Body.String())
		}
	}
}
