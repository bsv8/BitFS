package indexresolve

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type fakeStore struct {
	routes map[string]Manifest
	seeds  map[string]SeedItem
}

func (f *fakeStore) ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error) {
	out := make([]RouteItem, 0, len(f.routes))
	for route, manifest := range f.routes {
		out = append(out, RouteItem{
			Route:         route,
			SeedHash:      manifest.SeedHash,
			UpdatedAtUnix: manifest.UpdatedAtUnix,
		})
	}
	return out, nil
}

func (f *fakeStore) ResolveIndexRoute(ctx context.Context, route string) (Manifest, error) {
	if manifest, ok := f.routes[route]; ok {
		return manifest, nil
	}
	return Manifest{}, sql.ErrNoRows
}

func (f *fakeStore) UpsertIndexResolveRoute(ctx context.Context, route string, seedHash string, updatedAtUnix int64) (RouteItem, error) {
	seed, ok := f.seeds[seedHash]
	if !ok {
		return RouteItem{}, sql.ErrNoRows
	}
	manifest := Manifest{
		Route:               route,
		SeedHash:            seedHash,
		RecommendedFileName: seed.RecommendedFileName,
		MIMEHint:            seed.MIMEHint,
		FileSize:            seed.FileSize,
		UpdatedAtUnix:       updatedAtUnix,
	}
	f.routes[route] = manifest
	return RouteItem{
		Route:         route,
		SeedHash:      seedHash,
		UpdatedAtUnix: updatedAtUnix,
	}, nil
}

func (f *fakeStore) DeleteIndexResolveRoute(ctx context.Context, route string) error {
	delete(f.routes, route)
	return nil
}

func (f *fakeStore) GetIndexResolveSeed(ctx context.Context, seedHash string) (SeedItem, error) {
	seed, ok := f.seeds[seedHash]
	if !ok {
		return SeedItem{}, sql.ErrNoRows
	}
	return seed, nil
}

func TestNormalizeRoute(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":         DefaultRouteKey,
		"/":        DefaultRouteKey,
		"/index":   DefaultRouteKey,
		"index":    DefaultRouteKey,
		"music":    "/music",
		"/movie":   "/movie",
		"  movie ": "/movie",
	}
	for input, expect := range cases {
		got, err := NormalizeRoute(input)
		if err != nil {
			t.Fatalf("normalize route %q failed: %v", input, err)
		}
		if got != expect {
			t.Fatalf("normalize route %q = %q, want %q", input, got, expect)
		}
	}
	if _, err := NormalizeRoute("bad?route"); err == nil || CodeOf(err) != "ROUTE_INVALID" {
		t.Fatalf("expected route invalid, got %v", err)
	}
}

func TestServiceSettingsAndResolve(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		routes: map[string]Manifest{},
	}
	store.seeds = map[string]SeedItem{
		"abababababababababababababababababababababababababababababababab": {
			SeedHash:            "abababababababababababababababababababababababababababababababab",
			RecommendedFileName: "movie.mp4",
			MIMEHint:            "video/mp4",
			FileSize:            1234,
		},
	}
	svc := NewService(store, nil)
	if !svc.Enabled() {
		t.Fatal("service should be enabled")
	}
	if cap := svc.Capability(); cap == nil || strings.TrimSpace(cap.ID) == "" {
		t.Fatal("capability should exist")
	}

	routeItem, err := svc.Upsert(context.Background(), "movie", "abababababababababababababababababababababababababababababababab", 100)
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if routeItem.Route != "/movie" {
		t.Fatalf("unexpected route item: %+v", routeItem)
	}
	manifest, err := svc.Resolve(context.Background(), "/movie")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if manifest.SeedHash != "abababababababababababababababababababababababababababababababab" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}

	if err := svc.Delete(context.Background(), "movie"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := svc.Resolve(context.Background(), "/movie"); err == nil || CodeOf(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected route not found, got %v", err)
	}
}

func TestServiceRejectsBadSeedHash(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeStore{}, nil)
	_, err := svc.Upsert(context.Background(), "movie", "123", 0)
	if err == nil || CodeOf(err) != "SEED_HASH_INVALID" {
		t.Fatalf("expected seed hash invalid, got %v", err)
	}
}

func TestServiceRejectsMissingSeed(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeStore{seeds: map[string]SeedItem{}}, nil)
	_, err := svc.Upsert(context.Background(), "movie", "abababababababababababababababababababababababababababababababab", 0)
	if err == nil || CodeOf(err) != "SEED_NOT_FOUND" {
		t.Fatalf("expected seed not found, got %v", err)
	}
}

func TestErrorHelpers(t *testing.T) {
	t.Parallel()
	err := NewError("ROUTE_INVALID", "route is invalid")
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatal("expected typed error")
	}
	if CodeOf(err) != "ROUTE_INVALID" {
		t.Fatalf("unexpected code: %s", CodeOf(err))
	}
	if MessageOf(err) != "route is invalid" {
		t.Fatalf("unexpected message: %s", MessageOf(err))
	}
}
