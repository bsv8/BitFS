package clientapp

import (
	"strings"
	"testing"
	"time"
)

func TestComputeLiveUnitPriceSatPer64K(t *testing.T) {
	pricing := LiveSellerPricing{
		BasePriceSatPer64K:  100,
		FloorPriceSatPer64K: 20,
		DecayPerMinuteBPS:   1000,
	}
	if got := ComputeLiveUnitPriceSatPer64K(pricing, 0); got != 100 {
		t.Fatalf("age=0 got=%d want=100", got)
	}
	if got := ComputeLiveUnitPriceSatPer64K(pricing, 3*time.Minute); got != 70 {
		t.Fatalf("age=3m got=%d want=70", got)
	}
	if got := ComputeLiveUnitPriceSatPer64K(pricing, 20*time.Minute); got != 20 {
		t.Fatalf("age=20m got=%d want=20", got)
	}
}

func TestApplyConfigDefaults_LivePricing(t *testing.T) {
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = "db.sqlite"
	cfg.Storage.WorkspaceDir = "/tmp/ws"
	cfg.Storage.DataDir = "/tmp/data"
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults failed: %v", err)
	}
	if cfg.Live.Publish.BroadcastWindow != 10 {
		t.Fatalf("unexpected broadcast_window: %d", cfg.Live.Publish.BroadcastWindow)
	}
	if cfg.Seller.Pricing.LiveDecayPerMinuteBPS == 0 {
		t.Fatalf("live decay should be defaulted")
	}
	if cfg.Seller.Pricing.LiveFloorPriceSatPer64K == 0 {
		t.Fatalf("live floor should be defaulted")
	}
}

func TestPlanLivePurchase_PreferOlderWhenBudgetTight(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	snap := LiveSubscriberSnapshot{
		StreamID: "stream",
		RecentSegments: []LiveSegmentRef{
			{SegmentIndex: 10, SeedHash: strings.Repeat("aa", 32), PublishedAtUnix: now.Add(-5 * time.Minute).Unix()},
			{SegmentIndex: 11, SeedHash: strings.Repeat("bb", 32), PublishedAtUnix: now.Add(-2 * time.Minute).Unix()},
			{SegmentIndex: 12, SeedHash: strings.Repeat("cc", 32), PublishedAtUnix: now.Unix()},
		},
	}
	decision, err := PlanLivePurchase(snap, 9, LiveBuyerStrategy{
		TargetLagSegments:   1,
		MaxBudgetPerMinute:  75,
		PreferOlderSegments: true,
	}, LiveSellerPricing{
		BasePriceSatPer64K:  100,
		FloorPriceSatPer64K: 20,
		DecayPerMinuteBPS:   1000,
	}, now)
	if err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if decision.SeedHash != strings.Repeat("aa", 32) {
		t.Fatalf("unexpected chosen seed: %s", decision.SeedHash)
	}
	if decision.Mode != "delayed_backfill" {
		t.Fatalf("unexpected mode: %s", decision.Mode)
	}
}

func TestComputeLiveQuotePrices(t *testing.T) {
	seed := sellerSeed{SeedHash: "x", SeedPrice: 300, ChunkPrice: 100}
	got := ComputeLiveQuotePrices(seed, liveSegmentMeta{PublishedAtUnix: time.Now().Add(-2 * time.Minute).Unix()}, LiveSellerPricing{
		BasePriceSatPer64K:  100,
		FloorPriceSatPer64K: 20,
		DecayPerMinuteBPS:   1000,
	}, time.Now())
	if got.ChunkPrice != 80 {
		t.Fatalf("unexpected chunk price: %d", got.ChunkPrice)
	}
	if got.SeedPrice != 240 {
		t.Fatalf("unexpected seed price: %d", got.SeedPrice)
	}
}

func TestBestLiveQuoteSnapshot(t *testing.T) {
	snap, sellerPeerID, ok := bestLiveQuoteSnapshot(strings.Repeat("a", 64), []LiveQuoteItem{
		{
			SellerPeerID:       "seller-old",
			StreamID:           strings.Repeat("a", 64),
			LatestSegmentIndex: 11,
			RecentSegments: []LiveQuoteSegment{
				{SegmentIndex: 10, SeedHash: strings.Repeat("b", 64)},
				{SegmentIndex: 11, SeedHash: strings.Repeat("c", 64)},
			},
		},
		{
			SellerPeerID:       "seller-new",
			StreamID:           strings.Repeat("a", 64),
			LatestSegmentIndex: 12,
			RecentSegments: []LiveQuoteSegment{
				{SegmentIndex: 12, SeedHash: strings.Repeat("d", 64)},
			},
		},
	})
	if !ok {
		t.Fatalf("expected snapshot")
	}
	if sellerPeerID != "seller-new" {
		t.Fatalf("unexpected seller peer id: %s", sellerPeerID)
	}
	if len(snap.RecentSegments) != 1 || snap.RecentSegments[0].SeedHash != strings.Repeat("d", 64) {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}
