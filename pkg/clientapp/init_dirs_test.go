package clientapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitDataDirsCreatesWorkspaceDir(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ws := filepath.Join(root, "workspace")
	data := filepath.Join(root, "data")

	cfg := Config{}
	cfg.Storage.WorkspaceDir = ws
	cfg.Storage.DataDir = data
	cfg.Storage.MinFreeBytes = 1
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("ApplyConfigDefaults failed: %v", err)
	}

	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("initDataDirs failed: %v", err)
	}
	st, err := os.Stat(ws)
	if err != nil {
		t.Fatalf("workspace dir not created: %v", err)
	}
	if !st.IsDir() {
		t.Fatalf("workspace path is not directory")
	}
}
