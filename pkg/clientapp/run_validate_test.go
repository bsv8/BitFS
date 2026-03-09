package clientapp

import (
	"strings"
	"testing"
)

func TestValidateConfig_HTTPTokenRequiredOnNonLoopback(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.HTTP.ListenAddr = "0.0.0.0:18080"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.HTTP.AuthToken = ""

	err := ValidateConfig(&cfg)
	if err == nil {
		t.Fatalf("expected error when non-loopback http listen addr has empty token")
	}
	if !strings.Contains(err.Error(), "http.auth_token is required when http.listen_addr is not loopback") {
		t.Fatalf("unexpected error: %v", err)
	}
}
