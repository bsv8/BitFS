package clientapp

import "testing"

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
