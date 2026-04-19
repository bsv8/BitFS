//go:build with_indexresolve

package clientapp

import "testing"

func TestRegisterOptionalModulesRejectsDuplicateRegistration(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}

	firstClose, err := registerOptionalModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	if firstClose == nil {
		t.Fatalf("expected cleanup handle")
	}

	if _, err := registerOptionalModules(t.Context(), rt, store); err == nil {
		t.Fatalf("expected duplicate register error")
	}

	firstClose()
	firstClose()
}
