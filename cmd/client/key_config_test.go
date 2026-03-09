package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadOrInitKeyConfig_CreateWhenMissing(t *testing.T) {
	t.Parallel()
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	res, err := loadOrInitKeyConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadOrInitKeyConfig create failed: %v", err)
	}
	if !res.Created {
		t.Fatalf("expected Created=true")
	}
	if res.Upgraded {
		t.Fatalf("expected Upgraded=false")
	}
	if strings.TrimSpace(res.Config.Keys.PrivkeyHex) == "" {
		t.Fatalf("expected generated privkey_hex")
	}
	if _, err := parsePrivHex(res.Config.Keys.PrivkeyHex); err != nil {
		t.Fatalf("generated privkey_hex parse failed: %v", err)
	}
}

func TestLoadOrInitKeyConfig_UpgradeMissingPrivkey(t *testing.T) {
	t.Parallel()
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgPath, []byte("[keys]\n"), 0o644); err != nil {
		t.Fatalf("write partial config failed: %v", err)
	}
	res, err := loadOrInitKeyConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadOrInitKeyConfig upgrade failed: %v", err)
	}
	if res.Created {
		t.Fatalf("expected Created=false")
	}
	if !res.Upgraded {
		t.Fatalf("expected Upgraded=true")
	}
	if strings.TrimSpace(res.Config.Keys.PrivkeyHex) == "" {
		t.Fatalf("expected upgraded privkey_hex")
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read upgraded config failed: %v", err)
	}
	if !strings.Contains(string(raw), "privkey_hex") {
		t.Fatalf("upgraded config missing privkey_hex: %s", string(raw))
	}
}
