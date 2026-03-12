package clientapp

import "testing"

func TestValidateConfig_HTTPListenAddrRequired(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.HTTP.ListenAddr = ""
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.HTTP.ListenAddr = ""

	err := ValidateConfig(&cfg)
	if err == nil {
		t.Fatalf("expected error when http.listen_addr is empty")
	}
	if err.Error() != "http.listen_addr is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}
