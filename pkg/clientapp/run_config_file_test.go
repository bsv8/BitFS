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
	cfg.Storage.WorkspaceDir = filepath.Join(vaultDir, "workspace-custom")
	cfg.Storage.DataDir = filepath.Join(vaultDir, "data-custom")
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(vaultDir, "data-custom", "client-index.sqlite")
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.Log.File = filepath.Join(vaultDir, "logs-custom", "bitfs.log")
	cfg.Listen.AutoRenewRounds = 9
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
	if !strings.Contains(saved, "workspace-custom") {
		t.Fatalf("workspace path should be saved as relative, got:\n%s", saved)
	}

	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.Storage.WorkspaceDir != filepath.Join(vaultDir, "workspace-custom") {
		t.Fatalf("workspace dir mismatch: got=%q", loaded.Storage.WorkspaceDir)
	}
	if loaded.Storage.DataDir != filepath.Join(vaultDir, "data-custom") {
		t.Fatalf("data dir mismatch: got=%q", loaded.Storage.DataDir)
	}
	if loaded.Index.SQLitePath != filepath.Join(vaultDir, "data-custom", "client-index.sqlite") {
		t.Fatalf("sqlite path mismatch: got=%q", loaded.Index.SQLitePath)
	}
	if loaded.Log.File != filepath.Join(vaultDir, "logs-custom", "bitfs.log") {
		t.Fatalf("log file mismatch: got=%q", loaded.Log.File)
	}
	if loaded.Listen.AutoRenewRounds != 9 {
		t.Fatalf("listen.auto_renew_rounds mismatch: got=%d want=9", loaded.Listen.AutoRenewRounds)
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
	cfg.Storage.WorkspaceDir = ""
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
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config file should be deleted when there is no diff")
	}
}

func TestLoadOrInitConfigFile_ReturnsDefaultConfigWithoutCreatingFile(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")

	seed := Config{}
	seed.BSV.Network = "test"
	seed.HTTP.Enabled = true
	seed.FSHTTP.Enabled = true
	seed.Index.Backend = "sqlite"
	seed.Storage.WorkspaceDir = ""
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
	if res.Created {
		t.Fatalf("created should be false when no config file is written")
	}
	if res.Config.Index.SQLitePath != filepath.Join(vaultDir, "data", "client-index.sqlite") {
		t.Fatalf("sqlite path mismatch: got=%q", res.Config.Index.SQLitePath)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config file should not exist when no diff")
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
	seed.Storage.WorkspaceDir = ""
	seed.Storage.DataDir = "data"
	seed.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	seed.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))

	res, err := LoadOrInitConfigFileForMode(configPath, seed, StartupModeTest)
	if err != nil {
		t.Fatalf("load or init config file in test mode: %v", err)
	}
	if res.Created {
		t.Fatalf("created should be false when no config file is written")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config file should not exist in test mode when no diff")
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
	seed.Storage.WorkspaceDir = ""
	seed.Storage.DataDir = "data"
	seed.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	seed.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))

	res, err := LoadOrInitConfigFileForMode(configPath, seed, StartupModeProduct)
	if err != nil {
		t.Fatalf("load or init config file in product mode: %v", err)
	}
	if res.Created {
		t.Fatalf("created should be false when no config file is written")
	}
	if len(res.Config.Network.Gateways) == 0 {
		t.Fatalf("product mode should backfill gateways in runtime config")
	}
	if len(res.Config.Network.Arbiters) == 0 {
		t.Fatalf("product mode should backfill arbiters in runtime config")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config file should not exist when no diff")
	}
}

func TestSaveConfigFile_DiffAndDeleteWhenResetToDefault(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.Listen.AutoRenewRounds = 9
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config file with diff: %v", err)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	if !strings.Contains(string(raw), "auto_renew_rounds: 9") {
		t.Fatalf("diff key should be persisted, got:\n%s", string(raw))
	}

	cfg.Listen.AutoRenewRounds = 5
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config file reset default: %v", err)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		raw, readErr := os.ReadFile(configPath)
		if readErr != nil {
			t.Fatalf("config file should be deleted after reset to default: %v", readErr)
		}
		t.Fatalf("config file should be deleted after reset to default, got:\n%s", string(raw))
	}
}

func TestSaveConfigFile_ProductModeStripsMandatoryPeersFromDiff(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")
	gwHost, _ := newSecpHost(t)
	defer gwHost.Close()
	arbHost, _ := newSecpHost(t)
	defer arbHost.Close()
	gwPub, err := localPubKeyHex(gwHost)
	if err != nil {
		t.Fatalf("gateway pubkey: %v", err)
	}
	arbPub, err := localPubKeyHex(arbHost)
	if err != nil {
		t.Fatalf("arbiter pubkey: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.Network.Gateways = append(cfg.Network.Gateways, PeerNode{
		Enabled: false,
		Addr:    gwHost.Addrs()[0].String() + "/p2p/" + gwHost.ID().String(),
		Pubkey:  strings.ToLower(gwPub),
	})
	cfg.Network.Arbiters = append(cfg.Network.Arbiters, PeerNode{
		Enabled: false,
		Addr:    arbHost.Addrs()[0].String() + "/p2p/" + arbHost.ID().String(),
		Pubkey:  strings.ToLower(arbPub),
	})
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config file: %v", err)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	saved := string(raw)
	defaults, err := networkInitDefaults("test")
	if err != nil {
		t.Fatalf("network defaults: %v", err)
	}
	for _, mandatory := range append(defaults.DefaultGateways, defaults.DefaultArbiters...) {
		if strings.Contains(saved, mandatory.Pubkey) {
			t.Fatalf("mandatory peer should not be written: %s", mandatory.Pubkey)
		}
	}
	if !strings.Contains(saved, gwPub) || !strings.Contains(saved, arbPub) {
		t.Fatalf("custom peers should be written, got:\n%s", saved)
	}
}

func TestSaveConfigFile_ProductModeDeletesFileAfterReset(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := filepath.Join(vaultDir, "config.yaml")
	gwHost, _ := newSecpHost(t)
	defer gwHost.Close()
	gwPub, err := localPubKeyHex(gwHost)
	if err != nil {
		t.Fatalf("gateway pubkey: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.Network.Gateways = append(cfg.Network.Gateways, PeerNode{
		Enabled: false,
		Addr:    gwHost.Addrs()[0].String() + "/p2p/" + gwHost.ID().String(),
		Pubkey:  strings.ToLower(gwPub),
	})
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save diff config: %v", err)
	}
	cfg.Network.Gateways = nil
	cfg.Network.Arbiters = nil
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("reapply defaults: %v", err)
	}
	if err := SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save reset config: %v", err)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		raw, readErr := os.ReadFile(configPath)
		if readErr != nil {
			t.Fatalf("config file should be deleted after reset: %v", readErr)
		}
		t.Fatalf("config file should be deleted after reset, got:\n%s", string(raw))
	}
}
