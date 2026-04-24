package dealprod

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPublishLiveDemandAndListLiveQuotes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "dealprod.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := InitDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	svc := &Service{DB: db}

	pub, err := svc.PublishLiveDemand(PublishLiveDemandReq{
		StreamID:         strings.Repeat("a", 64),
		BuyerPeerID:      "buyer-1",
		BuyerAddrs:       []string{"/ip4/127.0.0.1/tcp/1"},
		HaveSegmentIndex: 8,
		Window:           10,
	})
	if err != nil {
		t.Fatalf("publish live demand: %v", err)
	}
	if strings.TrimSpace(pub.DemandID) == "" {
		t.Fatalf("demand id missing")
	}

	quote, err := svc.SubmitLiveQuote(LiveQuoteSubmitReq{
		DemandID:           pub.DemandID,
		SellerPeerID:       "seller-1",
		StreamID:           strings.Repeat("a", 64),
		LatestSegmentIndex: 12,
		RecentSegments: []*LiveQuoteSegment{
			{SegmentIndex: 11, SeedHash: strings.Repeat("b", 64)},
			{SegmentIndex: 12, SeedHash: strings.Repeat("c", 64)},
		},
	})
	if err != nil {
		t.Fatalf("submit live quote: %v", err)
	}
	if strings.TrimSpace(quote.QuoteID) == "" {
		t.Fatalf("quote id missing")
	}

	list, err := svc.ListLiveQuotes(LiveQuoteListReq{DemandID: pub.DemandID})
	if err != nil {
		t.Fatalf("list live quotes: %v", err)
	}
	if len(list.Quotes) != 1 {
		t.Fatalf("unexpected quote count: %d", len(list.Quotes))
	}
	if list.Quotes[0].LatestSegmentIndex != 12 {
		t.Fatalf("unexpected latest segment index: %d", list.Quotes[0].LatestSegmentIndex)
	}
	if len(list.Quotes[0].RecentSegments) != 2 {
		t.Fatalf("unexpected recent segment count: %d", len(list.Quotes[0].RecentSegments))
	}
}

func TestPublishDemandBatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "dealprod-batch.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := InitDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	svc := &Service{DB: db}

	resp, err := svc.PublishDemandBatch(PublishDemandBatchReq{
		BuyerPeerID: "buyer-1",
		BuyerAddrs:  []string{"/ip4/127.0.0.1/tcp/1"},
		Items: []PublishDemandBatchItemReq{
			{SeedHash: strings.Repeat("a", 64), ChunkCount: 1},
			{SeedHash: strings.Repeat("b", 64), ChunkCount: 2},
		},
	})
	if err != nil {
		t.Fatalf("publish demand batch: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("unexpected published item count: %d", len(resp.Items))
	}
	for _, item := range resp.Items {
		if strings.TrimSpace(item.DemandID) == "" {
			t.Fatalf("demand id missing: %+v", item)
		}
		if item.Status != "open" {
			t.Fatalf("unexpected item status: %+v", item)
		}
	}
}
