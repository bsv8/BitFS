package clientapp

import (
	"testing"

	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
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
		if got, want := cfg.Payment.PreferredScheme, ncall.PaymentSchemePool2of2V1; got != want {
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
		if got, want := cfg.Payment.PreferredScheme, ncall.PaymentSchemePool2of2V1; got != want {
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
