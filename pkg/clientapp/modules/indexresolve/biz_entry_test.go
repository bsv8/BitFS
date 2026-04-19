package indexresolve

import (
	"context"
	"database/sql"
	"errors"
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

func TestBizSettingsAndResolve(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		routes: map[string]Manifest{},
		seeds: map[string]SeedItem{
			"abababababababababababababababababababababababababababababababab": {
				SeedHash:            "abababababababababababababababababababababababababababababababab",
				RecommendedFileName: "movie.mp4",
				MIMEHint:            "video/mp4",
				FileSize:            1234,
			},
		},
	}

	routeItem, err := BizSettingsUpsert(context.Background(), store, "movie", "abababababababababababababababababababababababababababababababab")
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if routeItem.Route != "/movie" {
		t.Fatalf("unexpected route item: %+v", routeItem)
	}

	manifest, err := BizResolve(context.Background(), store, "/movie")
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if manifest.SeedHash != "abababababababababababababababababababababababababababababababab" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}

	items, err := BizSettingsList(context.Background(), store)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 1 || items[0].Route != "/movie" {
		t.Fatalf("unexpected list: %+v", items)
	}

	if err := BizSettingsDelete(context.Background(), store, "movie"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := BizResolve(context.Background(), store, "/movie"); err == nil || CodeOf(err) != "ROUTE_NOT_FOUND" {
		t.Fatalf("expected route not found, got %v", err)
	}
}

func TestBizSettingsRejectsBadSeedHash(t *testing.T) {
	t.Parallel()
	_, err := BizSettingsUpsert(context.Background(), &fakeStore{}, "movie", "123")
	if err == nil || CodeOf(err) != "SEED_HASH_INVALID" {
		t.Fatalf("expected seed hash invalid, got %v", err)
	}
}

func TestBizSettingsRejectsMissingSeed(t *testing.T) {
	t.Parallel()
	_, err := BizSettingsUpsert(context.Background(), &fakeStore{seeds: map[string]SeedItem{}}, "movie", "abababababababababababababababababababababababababababababababab")
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
