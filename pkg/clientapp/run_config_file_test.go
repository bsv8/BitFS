package clientapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigFile_PersistsRelativePaths(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.WorkspaceDir = filepath.Join(vaultDir, "workspace")
	cfg.Storage.DataDir = filepath.Join(vaultDir, "data")
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(vaultDir, "data", "client-index.sqlite")
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.Log.File = filepath.Join(vaultDir, "logs", "bitfs.log")
	cfg.ClientID = "should-not-be-saved"
	cfg.Keys.PrivkeyHex = "should-not-be-saved"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config file: %v", err)
	}

	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read saved config: %v", err)
	}
	saved := string(raw)
	if strings.Contains(saved, "client_pubkey_hex") {
		t.Fatalf("config file should not persist client_pubkey_hex")
	}
	if strings.Contains(saved, "privkey_hex") {
		t.Fatalf("config file should not persist privkey_hex")
	}
	if !strings.Contains(saved, "workspace: workspace") && !strings.Contains(saved, "workspace_dir: workspace") {
		t.Fatalf("workspace path should be saved as relative, got:\n%s", saved)
	}

	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.Storage.WorkspaceDir != filepath.Join(vaultDir, "workspace") {
		t.Fatalf("workspace dir mismatch: got=%q", loaded.Storage.WorkspaceDir)
	}
	if loaded.Storage.DataDir != filepath.Join(vaultDir, "data") {
		t.Fatalf("data dir mismatch: got=%q", loaded.Storage.DataDir)
	}
	if loaded.Index.SQLitePath != filepath.Join(vaultDir, "data", "client-index.sqlite") {
		t.Fatalf("sqlite path mismatch: got=%q", loaded.Index.SQLitePath)
	}
	if loaded.Log.File != filepath.Join(vaultDir, "logs", "bitfs.log") {
		t.Fatalf("log file mismatch: got=%q", loaded.Log.File)
	}
}

func TestLoadOrInitConfigFile_CreatesDefaultConfig(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")

	seed := Config{}
	seed.BSV.Network = "test"
	seed.HTTP.Enabled = true
	seed.FSHTTP.Enabled = true
	seed.Index.Backend = "sqlite"
	seed.Storage.WorkspaceDir = "workspace"
	seed.Storage.DataDir = "data"
	seed.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	seed.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))
	if err := ApplyConfigDefaults(&seed); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	res, err := LoadOrInitConfigFile(configPath, seed)
	if err != nil {
		t.Fatalf("load or init config file: %v", err)
	}
	if !res.Created {
		t.Fatalf("expected created config file")
	}
	if res.Config.Index.SQLitePath != filepath.Join(vaultDir, "data", "client-index.sqlite") {
		t.Fatalf("sqlite path mismatch: got=%q", res.Config.Index.SQLitePath)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("config file should exist: %v", err)
	}
}
