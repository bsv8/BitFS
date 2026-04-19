package clientapp

import "testing"

func TestBuiltinModulesOrderAndCapabilityRegistration(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}

	closeModule, err := installBuiltinModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("install builtin modules failed: %v", err)
	}
	if closeModule != nil {
		t.Cleanup(closeModule)
	}

	modules := builtinModuleCatalog()
	if len(modules) != 3 {
		t.Fatalf("unexpected builtin module count: %d", len(modules))
	}

	caps := clientCapabilitiesShowBody(rt).Capabilities
	if len(caps) < 5 {
		t.Fatalf("unexpected capability count: %d", len(caps))
	}
	got := []string{caps[2].ID, caps[3].ID, caps[4].ID}
	want := []string{"domain", "index_resolve", "inbox_message"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected builtin module order: got=%v want=%v", got, want)
		}
	}
}
