package clientapp

import (
	"path/filepath"
	"testing"
)

func TestDefaultConfigSeed_DoesNotSetWorkspaceDir(t *testing.T) {
	t.Parallel()

	cfg := defaultConfigSeed()
	if got := cfg.Storage.WorkspaceDir; got != "" {
		t.Fatalf("default workspace_dir should be empty, got=%q", got)
	}
	if got, want := cfg.Storage.DataDir, "data"; got != want {
		t.Fatalf("default data_dir mismatch: got=%q want=%q", got, want)
	}
}

func TestResolveAndSaveConfigPathValue_AllowEmptyWorkspace(t *testing.T) {
	t.Parallel()

	baseDir := filepath.Join(t.TempDir(), "vault")
	if got := resolveConfigPathValue(baseDir, "", ""); got != "" {
		t.Fatalf("resolve config path with empty fallback should stay empty, got=%q", got)
	}
	if got := saveConfigPathValue(baseDir, "", ""); got != "" {
		t.Fatalf("save config path with empty fallback should stay empty, got=%q", got)
	}
}
