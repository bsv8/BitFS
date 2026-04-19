package clientapp

import (
	"testing"

	indexresolve "github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

func TestInstallBuiltinModulesRejectsDuplicateRegistration(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}

	firstClose, err := installBuiltinModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	if firstClose == nil {
		t.Fatalf("expected cleanup handle")
	}

	if _, err := installBuiltinModules(t.Context(), rt, store); err == nil {
		t.Fatalf("expected duplicate register error")
	}

	firstClose()
	secondClose, err := installBuiltinModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("reinstall after cleanup failed: %v", err)
	}
	if secondClose == nil {
		t.Fatalf("expected cleanup handle on reinstall")
	}
	secondClose()
	firstClose()
}

func TestIndexResolveModuleLockRegistered(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}

	cleanup, err := installBuiltinModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("install builtin modules failed: %v", err)
	}
	defer cleanup()

	items, missing := rt.modules.moduleLockItems(indexresolve.ModuleIdentity)
	if len(missing) > 0 {
		t.Fatalf("indexresolve module lock items missing: %v", missing)
	}
	if len(items) != 4 {
		t.Fatalf("expected 4 indexresolve locked functions, got %d", len(items))
	}
}
