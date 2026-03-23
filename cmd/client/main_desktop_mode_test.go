package main

import (
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp"
)

func TestRuntimeListenOverridesApply(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	overrides := runtimeListenOverrides{
		httpListenAddr:   "127.0.0.1:19181",
		fsHTTPListenAddr: "127.0.0.1:19182",
	}
	overrides.apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:19181"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:19182"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestRuntimeListenOverridesApply_EmptyKeepsConfig(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	var overrides runtimeListenOverrides
	overrides.apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:18080"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:18090"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestApplyDesktopRuntimeBootstrap_EnablesStartupFullScanWhenSystemHomepageExists(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd",
		},
	}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false

	d.applyDesktopRuntimeBootstrap(&cfg)

	if !cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should be enabled for desktop system homepage bootstrap")
	}
}

func TestApplyDesktopRuntimeBootstrap_NoHomepageKeepsScanConfig(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false

	d.applyDesktopRuntimeBootstrap(&cfg)

	if cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should stay unchanged when no system homepage bundle is active")
	}
}
