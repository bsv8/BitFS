package clientapp

import "testing"

func TestBuiltinModuleCatalogIncludesFileStorage(t *testing.T) {
	t.Parallel()

	found := false
	for _, entry := range builtinModuleCatalog() {
		if entry.Name == "filestorage" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("builtin module catalog should include filestorage")
	}
}
