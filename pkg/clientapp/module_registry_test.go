package clientapp

import (
	"context"
	"net/http"
	"testing"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelocks"
)

func TestModuleRegistryResolveDoesNotTriggerSettingsHook(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	var resolveCount int
	var settingsCount int
	closeFn, err := reg.registerIndexResolve(
		func() *contractmessage.CapabilityItem {
			return &contractmessage.CapabilityItem{ID: "index_resolve", Version: 1}
		},
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

func TestModuleRegistrySeparatesCapabilityAndModuleLocks(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	cleanupLocks, err := reg.registerModuleLockProvider(modulelocks.ModuleIdentity, modulelocks.FunctionLocks)
	if err != nil {
		t.Fatalf("register module locks failed: %v", err)
	}
	defer cleanupLocks()

	if caps := reg.capabilityItems(); len(caps) != 0 {
		t.Fatalf("capability items should stay empty before capability hook, got %d", len(caps))
	}

	items, missing := reg.moduleLockItems(modulelocks.ModuleIdentity)
	if len(missing) != 0 {
		t.Fatalf("unexpected missing modules: %#v", missing)
	}
	if len(items) == 0 {
		t.Fatal("expected module lock items")
	}

	closeFn, err := reg.registerIndexResolve(
		func() *contractmessage.CapabilityItem {
			return &contractmessage.CapabilityItem{ID: "index_resolve", Version: 1}
		},
		func(context.Context, string) (routeIndexManifest, error) {
			return routeIndexManifest{Route: "/movie", SeedHash: "aa"}, nil
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("register capability hook failed: %v", err)
	}
	if closeFn == nil {
		t.Fatal("expected capability cleanup")
	}

	caps := reg.capabilityItems()
	if len(caps) != 1 || caps[0].ID != "index_resolve" {
		t.Fatalf("unexpected capability items: %#v", caps)
	}

	closeFn()
	items, missing = reg.moduleLockItems(modulelocks.ModuleIdentity)
	if len(items) == 0 || len(missing) != 0 {
		t.Fatalf("module locks should stay until lock cleanup, got items=%#v missing=%#v", items, missing)
	}

	cleanupLocks()
	if items, missing := reg.moduleLockItems(modulelocks.ModuleIdentity); len(items) != 0 || len(missing) != 1 {
		t.Fatalf("module locks should be removed with lock cleanup, got items=%#v missing=%#v", items, missing)
	}
}
