package clientapp

import "testing"

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
		if got, want := cfg.Listen.AutoRenewRounds, uint64(5); got != want {
			t.Fatalf("listen.auto_renew_rounds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.RenewThresholdSeconds, uint32(5); got != want {
			t.Fatalf("listen.renew_threshold_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.TickSeconds, uint32(1); got != want {
			t.Fatalf("listen.tick_seconds=%d, want %d", got, want)
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
		if got, want := cfg.Listen.AutoRenewRounds, uint64(5); got != want {
			t.Fatalf("listen.auto_renew_rounds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.RenewThresholdSeconds, uint32(1800); got != want {
			t.Fatalf("listen.renew_threshold_seconds=%d, want %d", got, want)
		}
		if got, want := cfg.Listen.TickSeconds, uint32(30); got != want {
			t.Fatalf("listen.tick_seconds=%d, want %d", got, want)
		}
	})
}
