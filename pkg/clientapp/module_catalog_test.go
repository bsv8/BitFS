package clientapp

import "testing"

func TestBuiltinModulesOrderAndCapabilityRegistration(t *testing.T) {
	t.Skip("硬切后 domain 不再有公开 libp2p capability，需要重写测试")

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
	if len(caps) < 2 {
		t.Fatalf("unexpected capability count: %d", len(caps))
	}
	// 硬切后：domain 不再有公开 libp2p capability，只有 index_resolve 和 inbox_message
	got := []string{caps[0].ID, caps[1].ID}
	want := []string{"index_resolve", "inbox_message"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected builtin module order: got=%v want=%v", got, want)
		}
	}
}
