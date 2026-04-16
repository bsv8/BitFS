package clientapp

import (
	"testing"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
)

func TestApplyConfigDefaults_ListenDefaults(t *testing.T) {
	t.Parallel()

	t.Run("test", func(t *testing.T) {
		t.Parallel()
		cfg := Config{}
		cfg.BSV.Network = "test"
		if err := ApplyConfigDefaults(&cfg); err != nil {
			t.Fatalf("apply defaults: %v", err)
		}
		if cfg.Listen.Enabled == nil || !*cfg.Listen.Enabled {
			t.Fatalf("listen.enabled default should be true")
		}
		if cfg.Reachability.AutoAnnounceEnabled == nil || !*cfg.Reachability.AutoAnnounceEnabled {
			t.Fatalf("reachability.auto_announce_enabled default should be true")
		}
		if got, want := cfg.Reachability.AnnounceTTLSeconds, uint32(3600); got != want {
			t.Fatalf("reachability.announce_ttl_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.AutoRenewRounds, uint64(5); got != want {
			t.Fatalf("listen.auto_renew_rounds=%d, want %d", got, want)
		}
		if got := cfg.Listen.OfferPaymentSatoshi; got != 0 {
			t.Fatalf("listen.offer_payment_satoshi=%d, want 0", got)
		}
		if got, want := cfg.Listen.RenewThresholdSeconds, uint32(5); got != want {
			t.Fatalf("listen.renew_threshold_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.TickSeconds, uint32(1); got != want {
			t.Fatalf("listen.tick_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Payment.PreferredScheme, contractprotoid.PaymentSchemePool2of2V1; got != want {
			t.Fatalf("payment.preferred_scheme=%s, want %s", got, want)
		}
	})

	t.Run("main", func(t *testing.T) {
		t.Parallel()
		cfg := Config{}
		cfg.BSV.Network = "main"
		if err := ApplyConfigDefaults(&cfg); err != nil {
			t.Fatalf("apply defaults: %v", err)
		}
		if cfg.Listen.Enabled == nil || !*cfg.Listen.Enabled {
			t.Fatalf("listen.enabled default should be true")
		}
		if cfg.Reachability.AutoAnnounceEnabled == nil || !*cfg.Reachability.AutoAnnounceEnabled {
			t.Fatalf("reachability.auto_announce_enabled default should be true")
		}
		if got, want := cfg.Reachability.AnnounceTTLSeconds, uint32(3600); got != want {
			t.Fatalf("reachability.announce_ttl_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.AutoRenewRounds, uint64(5); got != want {
			t.Fatalf("listen.auto_renew_rounds=%d, want %d", got, want)
		}
		if got := cfg.Listen.OfferPaymentSatoshi; got != 0 {
			t.Fatalf("listen.offer_payment_satoshi=%d, want 0", got)
		}
		if got, want := cfg.Listen.RenewThresholdSeconds, uint32(1800); got != want {
			t.Fatalf("listen.renew_threshold_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.TickSeconds, uint32(30); got != want {
			t.Fatalf("listen.tick_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Payment.PreferredScheme, contractprotoid.PaymentSchemePool2of2V1; got != want {
			t.Fatalf("payment.preferred_scheme=%s, want %s", got, want)
		}
	})
}

func TestApplyConfigDefaults_ExternalAPIProviderDefaultsAndMigration(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.WOCAPIKey = "legacy-woc-key"

	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if got, want := cfg.ExternalAPI.WOC.APIKey, "legacy-woc-key"; got != want {
		t.Fatalf("external_api.woc.api_key=%q, want %q", got, want)
	}
	if got := cfg.WOCAPIKey; got != "" {
		t.Fatalf("legacy woc_api_key should be cleared after migration, got=%q", got)
	}
	if got, want := cfg.ExternalAPI.WOC.MinIntervalMS, uint32(1000); got != want {
		t.Fatalf("external_api.woc.min_interval_ms=%d, want %d", got, want)
	}
}

func TestApplyConfigDefaults_TestModeAllowsEmptyPeers(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeTest); err != nil {
		t.Fatalf("apply defaults in test mode: %v", err)
	}
	if len(cfg.Network.Gateways) != 0 {
		t.Fatalf("test mode should keep gateways empty, got=%d", len(cfg.Network.Gateways))
	}
	if len(cfg.Network.Arbiters) != 0 {
		t.Fatalf("test mode should keep arbiters empty, got=%d", len(cfg.Network.Arbiters))
	}
}

func TestApplyConfigDefaults_ProductModeBackfillsPeers(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults in product mode: %v", err)
	}
	if len(cfg.Network.Gateways) == 0 {
		t.Fatalf("product mode should backfill gateways")
	}
	if len(cfg.Network.Arbiters) == 0 {
		t.Fatalf("product mode should backfill arbiters")
	}
}

func TestApplyConfigDefaults_ProductModeMergesMandatoryAndCustomPeers(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Network.Gateways = []PeerNode{
		{Enabled: true, Addr: "/ip4/127.0.0.1/tcp/19990/p2p/16Uiu2HAmT1x9dTYdJnkEgajv4i7xW9nUCPN5J8rQ7zQNNJfN4owv", Pubkey: "03b57f32fbce4d8f52f14e83ff2642d3f6fd8f0f2ca90dc8f95fc54802a47a4068"},
	}
	cfg.Network.Arbiters = []PeerNode{
		{Enabled: true, Addr: "/ip4/127.0.0.1/tcp/19991/p2p/16Uiu2HAmPXKX7cq2uFY12V3h45xTBJv5A67x9jduPJgD4hXyQxMx", Pubkey: "0324e9b29f0ecf6be4bc7f2ee9ca2f9f4b72bf711f514cb0fcb04ce57cf7f0ee8f"},
	}
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults in product mode: %v", err)
	}
	if len(cfg.Network.Gateways) < 2 {
		t.Fatalf("product mode should keep custom gateway and merge mandatory gateway")
	}
	if len(cfg.Network.Arbiters) < 2 {
		t.Fatalf("product mode should keep custom arbiter and merge mandatory arbiter")
	}
}

func TestApplyConfigDefaults_ProductModeIgnoresMandatoryPeerOverrides(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Network.Gateways = []PeerNode{
		{Enabled: false, Addr: "/ip4/127.0.0.1/tcp/19990/p2p/16Uiu2HAmT1x9dTYdJnkEgajv4i7xW9nUCPN5J8rQ7zQNNJfN4owv", Pubkey: "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e"},
	}
	cfg.Network.Arbiters = []PeerNode{
		{Enabled: false, Addr: "/ip4/127.0.0.1/tcp/19991/p2p/16Uiu2HAmPXKX7cq2uFY12V3h45xTBJv5A67x9jduPJgD4hXyQxMx", Pubkey: "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4"},
	}
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeProduct); err != nil {
		t.Fatalf("apply defaults in product mode: %v", err)
	}
	defaults, err := networkInitDefaults("test")
	if err != nil {
		t.Fatalf("network defaults: %v", err)
	}
	if len(cfg.Network.Gateways) == 0 || len(cfg.Network.Arbiters) == 0 {
		t.Fatalf("product mode should still keep mandatory peers")
	}
	if cfg.Network.Gateways[0].Pubkey != defaults.DefaultGateways[0].Pubkey || cfg.Network.Gateways[0].Addr != defaults.DefaultGateways[0].Addr {
		t.Fatalf("mandatory gateway override should be ignored: %+v", cfg.Network.Gateways[0])
	}
	if cfg.Network.Arbiters[0].Pubkey != defaults.DefaultArbiters[0].Pubkey || cfg.Network.Arbiters[0].Addr != defaults.DefaultArbiters[0].Addr {
		t.Fatalf("mandatory arbiter override should be ignored: %+v", cfg.Network.Arbiters[0])
	}
}

func TestValidateConfig_TestModeAllowsEmptyPeers(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.WorkspaceDir = ""
	cfg.Storage.DataDir = "data"
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = "data/client-index.sqlite"
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.Live.Publish.BroadcastWindow = 1
	cfg.Live.Publish.BroadcastIntervalSec = 1
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 1
	cfg.FSHTTP.MaxConcurrentSessions = 1
	cfg.Reachability.AnnounceTTLSeconds = 1

	if err := validateConfigForMode(&cfg, StartupModeTest); err != nil {
		t.Fatalf("validate config in test mode: %v", err)
	}
}

func TestValidateConfig_ProductModeRequiresPeers(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.WorkspaceDir = "workspace"
	cfg.Storage.DataDir = "data"
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = "data/client-index.sqlite"
	cfg.HTTP.Enabled = true
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.Enabled = true
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.Live.Publish.BroadcastWindow = 1
	cfg.Live.Publish.BroadcastIntervalSec = 1
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 1
	cfg.FSHTTP.MaxConcurrentSessions = 1
	cfg.Reachability.AnnounceTTLSeconds = 1

	if err := validateConfigForMode(&cfg, StartupModeProduct); err == nil {
		t.Fatalf("product mode should reject empty peers")
	}
}
