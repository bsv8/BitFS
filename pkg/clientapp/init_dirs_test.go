package clientapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitDataDirsCreatesDataDirOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data := filepath.Join(root, "data")

	cfg := Config{}
	cfg.Storage.DataDir = data
	cfg.Storage.MinFreeBytes = 1
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("ApplyConfigDefaults failed: %v", err)
	}

	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("initDataDirs failed: %v", err)
	}
	if st, err := os.Stat(data); err != nil || !st.IsDir() {
		t.Fatalf("data dir not created: st=%v err=%v", st, err)
	}
}

func TestInitDataDirs_AllowsEmptyDataDir(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data := filepath.Join(root, "data")

	cfg := Config{}
	cfg.Storage.DataDir = data
	cfg.Storage.MinFreeBytes = 1
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("ApplyConfigDefaults failed: %v", err)
	}

	if err := initDataDirs(&cfg); err != nil {
		t.Fatalf("initDataDirs failed: %v", err)
	}
	if _, err := os.Stat(data); err != nil {
		t.Fatalf("data dir not created: %v", err)
	}
}
