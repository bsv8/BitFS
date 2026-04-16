package managedclient

import (
	"os"
	"strings"
	"testing"
)

func TestLoadRuntimeConfigOrInit_UsesInitNetworkOnFirstCreate(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	cfg, created, err := LoadRuntimeConfigOrInit(vaultDir+"/config.yaml", "main")
	if err != nil {
		t.Fatalf("loadRuntimeConfigOrInit create failed: %v", err)
	}
	if created {
		t.Fatalf("created should be false when config file has no diff")
	}
	if got, want := cfg.BSV.Network, "main"; got != want {
		t.Fatalf("bsv.network=%q, want %q", got, want)
	}
	if got, want := cfg.Listen.RenewThresholdSeconds, uint32(1800); got != want {
		t.Fatalf("listen.renew_threshold_seconds=%d, want %d", got, want)
	}
	if got, want := cfg.Listen.TickSeconds, uint32(30); got != want {
		t.Fatalf("listen.tick_seconds=%d, want %d", got, want)
	}
	if _, err := os.Stat(vaultDir + "/config.yaml"); !os.IsNotExist(err) {
		t.Fatalf("config file should be deleted when no diff")
	}
}

func TestLoadRuntimeConfigOrInit_ExistingConfigNotOverriddenByInitNetwork(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := vaultDir + "/config.yaml"
	raw := []byte("bsv:\n  network: main\nlisten:\n  auto_renew_rounds: 7\n")
	if err := os.WriteFile(configPath, raw, 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg2, created2, err := LoadRuntimeConfigOrInit(configPath, "test")
	if err != nil {
		t.Fatalf("second loadRuntimeConfigOrInit failed: %v", err)
	}
	if created2 {
		t.Fatalf("expected second created=false")
	}
	if got, want := cfg2.BSV.Network, "main"; got != want {
		t.Fatalf("second bsv.network=%q, want %q", cfg2.BSV.Network, want)
	}
	if got, want := cfg2.Listen.AutoRenewRounds, uint64(7); got != want {
		t.Fatalf("listen.auto_renew_rounds=%d, want %d", got, want)
	}
	savedRaw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	saved := string(savedRaw)
	if !strings.Contains(saved, "auto_renew_rounds: 7") {
		t.Fatalf("config diff should keep explicit override, got:\n%s", saved)
	}
}

func TestLoadRuntimeConfigOrInit_ProductModeBackfillsExistingEmptyPeers(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := vaultDir + "/config.yaml"
	raw := []byte("bsv:\n  network: main\nnetwork:\n  gateways: []\n  arbiters: []\nstorage:\n  data_dir: data\nindex:\n  backend: sqlite\n  sqlite_path: data/client-index.sqlite\nhttp:\n  enabled: true\n  listen_addr: 127.0.0.1:18080\nfs_http:\n  enabled: true\n  listen_addr: 127.0.0.1:18090\n")
	if err := os.WriteFile(configPath, raw, 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, created, err := LoadRuntimeConfigOrInit(configPath, "test")
	if err != nil {
		t.Fatalf("loadRuntimeConfigOrInit failed: %v", err)
	}
	if created {
		t.Fatalf("expected created=false for existing config")
	}
	if cfg.BSV.Network != "main" {
		t.Fatalf("network=%q, want main", cfg.BSV.Network)
	}
	if len(cfg.Network.Gateways) == 0 {
		t.Fatalf("runtime config should backfill gateways")
	}
	if len(cfg.Network.Arbiters) == 0 {
		t.Fatalf("runtime config should backfill arbiters")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config file should be deleted when there is no diff")
	}
}

func TestNewDefaultConfig_WorkspaceDirStartsEmpty(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig("test")
	if cfg.Storage.WorkspaceDir != "" {
		t.Fatalf("workspace_dir should be empty by default, got=%q", cfg.Storage.WorkspaceDir)
	}
	if cfg.Storage.DataDir == "" {
		t.Fatalf("data_dir should not be empty")
	}
}
