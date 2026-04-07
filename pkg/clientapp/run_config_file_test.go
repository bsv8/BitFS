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

func TestLoadConfigWithSeed_PreservesEmptyPeers(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")
	raw := []byte("bsv:\n  network: test\nnetwork:\n  gateways: []\n  arbiters: []\nstorage:\n  workspace_dir: workspace\n  data_dir: data\nindex:\n  backend: sqlite\n  sqlite_path: data/client-index.sqlite\nhttp:\n  enabled: true\n  listen_addr: 127.0.0.1:18080\nfs_http:\n  enabled: true\n  listen_addr: 127.0.0.1:18090\n")
	if err := os.WriteFile(configPath, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	seed := Config{}
	seed.BSV.Network = "test"
	seed.Storage.WorkspaceDir = "workspace"
	seed.Storage.DataDir = "data"
	seed.Index.Backend = "sqlite"
	seed.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	seed.HTTP.Enabled = true
	seed.HTTP.ListenAddr = "127.0.0.1:18080"
	seed.FSHTTP.Enabled = true
	seed.FSHTTP.ListenAddr = "127.0.0.1:18090"
	seed.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))

	loaded, _, err := LoadConfigWithSeed(configPath, seed)
	if err != nil {
		t.Fatalf("load config with seed: %v", err)
	}
	if len(loaded.Network.Gateways) != 0 {
		t.Fatalf("expected empty gateways to be preserved, got=%d", len(loaded.Network.Gateways))
	}
	if len(loaded.Network.Arbiters) != 0 {
		t.Fatalf("expected empty arbiters to be preserved, got=%d", len(loaded.Network.Arbiters))
	}
}

func TestSaveConfigFile_PreservesEmptyPeers(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.WorkspaceDir = "workspace"
	cfg.Storage.DataDir = "data"
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))

	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config file: %v", err)
	}
	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(loaded.Network.Gateways) != 0 {
		t.Fatalf("saved config should keep gateways empty, got=%d", len(loaded.Network.Gateways))
	}
	if len(loaded.Network.Arbiters) != 0 {
		t.Fatalf("saved config should keep arbiters empty, got=%d", len(loaded.Network.Arbiters))
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

func TestLoadOrInitConfigFileForMode_TestModeKeepsEmptyPeersOnCreate(t *testing.T) {
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

	res, err := LoadOrInitConfigFileForMode(configPath, seed, StartupModeTest)
	if err != nil {
		t.Fatalf("load or init config file in test mode: %v", err)
	}
	if !res.Created {
		t.Fatalf("expected created config file")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	saved := string(raw)
	if strings.Contains(saved, "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e") {
		t.Fatalf("test mode should not inject default gateway, got:\n%s", saved)
	}
	if strings.Contains(saved, "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4") {
		t.Fatalf("test mode should keep config file free of injected peers, got:\n%s", saved)
	}
}

func TestLoadOrInitConfigFileForMode_ProductModeBackfillsPeersOnCreate(t *testing.T) {
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

	res, err := LoadOrInitConfigFileForMode(configPath, seed, StartupModeProduct)
	if err != nil {
		t.Fatalf("load or init config file in product mode: %v", err)
	}
	if !res.Created {
		t.Fatalf("expected created config file")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	saved := string(raw)
	if !strings.Contains(saved, "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e") {
		t.Fatalf("product mode should persist gateways, got:\n%s", saved)
	}
	if !strings.Contains(saved, "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4") {
		t.Fatalf("product mode should persist arbiters, got:\n%s", saved)
	}
}
