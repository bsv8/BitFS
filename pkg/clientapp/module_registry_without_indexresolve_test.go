//go:build !with_indexresolve

package clientapp

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

func TestModuleRegistryResolveDoesNotTriggerSettingsHook(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	var resolveCount int
	var settingsCount int
	closeFn, err := reg.registerIndexResolve(
		nil,
		func(context.Context, string) (routeIndexManifest, error) {
			resolveCount++
			return routeIndexManifest{Route: "/movie", SeedHash: "aa"}, nil
		},
		func(*httpAPIServer, *http.ServeMux, string) {
			settingsCount++
		},
		nil,
	)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	if closeFn == nil {
		t.Fatal("expected cleanup handle")
	}

	if _, err := reg.resolveIndex(context.Background(), "movie"); err != nil {
		t.Fatalf("resolve index failed: %v", err)
	}
	if resolveCount != 1 {
		t.Fatalf("resolve hook count=%d, want 1", resolveCount)
	}
	if settingsCount != 0 {
		t.Fatalf("settings hook count=%d, want 0", settingsCount)
	}
}

func TestModuleRegistryKeepsGenericModuleLocks(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	cleanupLocks, err := reg.registerModuleLockProvider("stub", func() []modulelock.LockedFunction {
		return []modulelock.LockedFunction{
			{ID: "stub.one", Module: "stub", Package: "./stub", Symbol: "One", Signature: "func One()", Note: "n"},
		}
	})
	if err != nil {
		t.Fatalf("register module locks failed: %v", err)
	}
	defer cleanupLocks()

	if caps := reg.capabilityItems(); len(caps) != 0 {
		t.Fatalf("capability items should stay empty before capability hook, got %d", len(caps))
	}

	items, missing := reg.moduleLockItems("stub")
	if len(missing) != 0 {
		t.Fatalf("unexpected missing modules: %#v", missing)
	}
	if len(items) != 1 || items[0].ID != "stub.one" {
		t.Fatalf("unexpected module lock items: %#v", items)
	}
}
