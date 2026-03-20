package main

import "testing"

func TestLoadRuntimeConfigOrInit_UsesInitNetworkOnFirstCreate(t *testing.T) {
	t.Parallel()

	dbPath := t.TempDir() + "/runtime-config.sqlite"
	cfg, created, err := loadRuntimeConfigOrInit("bitfs-it-main", dbPath, "main")
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
}

func TestLoadRuntimeConfigOrInit_ExistingConfigNotOverriddenByInitNetwork(t *testing.T) {
	t.Parallel()

	dbPath := t.TempDir() + "/runtime-config.sqlite"
	cfg1, created1, err := loadRuntimeConfigOrInit("bitfs-it-main", dbPath, "main")
	if err != nil {
		t.Fatalf("first loadRuntimeConfigOrInit failed: %v", err)
	}
	if !created1 {
		t.Fatalf("expected first created=true")
	}
	if cfg1.BSV.Network != "main" {
		t.Fatalf("first network=%q, want main", cfg1.BSV.Network)
	}

	cfg2, created2, err := loadRuntimeConfigOrInit("bitfs-it-main", dbPath, "test")
	if err != nil {
		t.Fatalf("second loadRuntimeConfigOrInit failed: %v", err)
	}
	if created2 {
		t.Fatalf("expected second created=false")
	}
	if got, want := cfg2.BSV.Network, "main"; got != want {
		t.Fatalf("second bsv.network=%q, want %q", got, want)
	}
}
