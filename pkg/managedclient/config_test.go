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
	if !created {
		t.Fatalf("expected created=true on first init")
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
	raw, err := os.ReadFile(vaultDir + "/config.yaml")
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	saved := string(raw)
	if !strings.Contains(saved, "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e") {
		t.Fatalf("product init should persist gateways, got:\n%s", saved)
	}
	if !strings.Contains(saved, "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4") {
		t.Fatalf("product init should persist arbiters, got:\n%s", saved)
	}
}

func TestLoadRuntimeConfigOrInit_ExistingConfigNotOverriddenByInitNetwork(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := vaultDir + "/config.yaml"
	cfg1, created1, err := LoadRuntimeConfigOrInit(configPath, "main")
	if err != nil {
		t.Fatalf("first loadRuntimeConfigOrInit failed: %v", err)
	}
	if !created1 {
		t.Fatalf("expected first created=true")
	}
	if cfg1.BSV.Network != "main" {
		t.Fatalf("first network=%q, want main", cfg1.BSV.Network)
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
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	saved := string(raw)
	if !strings.Contains(saved, "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e") {
		t.Fatalf("product init should keep gateways persisted, got:\n%s", saved)
	}
	if !strings.Contains(saved, "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4") {
		t.Fatalf("product init should keep arbiters persisted, got:\n%s", saved)
	}
}

func TestLoadRuntimeConfigOrInit_ProductModeBackfillsExistingEmptyPeers(t *testing.T) {
	t.Parallel()

	vaultDir := t.TempDir()
	configPath := vaultDir + "/config.yaml"
	raw := []byte("bsv:\n  network: main\nnetwork:\n  gateways: []\n  arbiters: []\nstorage:\n  workspace_dir: workspace\n  data_dir: data\nindex:\n  backend: sqlite\n  sqlite_path: data/client-index.sqlite\nhttp:\n  enabled: true\n  listen_addr: 127.0.0.1:18080\nfs_http:\n  enabled: true\n  listen_addr: 127.0.0.1:18090\n")
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
	saved, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	text := string(saved)
	if !strings.Contains(text, "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e") {
		t.Fatalf("existing config should be backfilled with gateways, got:\n%s", text)
	}
	if !strings.Contains(text, "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4") {
		t.Fatalf("existing config should be backfilled with arbiters, got:\n%s", text)
	}
}
