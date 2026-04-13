package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiveSubscribeURI_RoundTrip(t *testing.T) {
	raw, err := BuildLiveSubscribeURI("02"+strings.Repeat("ab", 32), strings.Repeat("cd", 32))
	if err != nil {
		t.Fatalf("build uri failed: %v", err)
	}
	parsed, err := ParseLiveSubscribeURI(raw)
	if err != nil {
		t.Fatalf("parse uri failed: %v", err)
	}
	if parsed.PublisherPubKey != "02"+strings.Repeat("ab", 32) {
		t.Fatalf("unexpected publisher_pubkey: %s", parsed.PublisherPubKey)
	}
	if parsed.StreamID != strings.Repeat("cd", 32) {
		t.Fatalf("unexpected stream_id: %s", parsed.StreamID)
	}
}

func TestBuildAndVerifyLiveSegment(t *testing.T) {
	h, _ := newSecpHost(t)
	defer h.Close()

	rt := &Runtime{Host: h}
	segBytes, seedHash, err := BuildLiveSegment(context.Background(), rt, liveSegmentDataPB{
		Version:           1,
		SegmentIndex:      0,
		DurationMs:        2345,
		PublishedAtUnixMs: 1_700_000_123_000,
		IsDiscontinuity:   true,
		MimeType:          "video/mp2t",
		InitSeedHash:      strings.Repeat("ab", 32),
		PlaylistUriHint:   "/custom/seg0.ts",
		IsEnd:             true,
	}, []byte("hello-live"))
	if err != nil {
		t.Fatalf("build segment failed: %v", err)
	}
	data, media, gotSeedHash, err := VerifyLiveSegment(segBytes)
	if err != nil {
		t.Fatalf("verify segment failed: %v", err)
	}
	if gotSeedHash != seedHash {
		t.Fatalf("seed hash mismatch: got=%s want=%s", gotSeedHash, seedHash)
	}
	if string(media) != "hello-live" {
		t.Fatalf("unexpected media: %q", string(media))
	}
	if data.SegmentIndex != 0 {
		t.Fatalf("unexpected segment index: %d", data.SegmentIndex)
	}
	if data.DurationMs != 2345 {
		t.Fatalf("unexpected duration_ms: %d", data.DurationMs)
	}
	if data.PublishedAtUnixMs != 1_700_000_123_000 {
		t.Fatalf("unexpected published_at_unix_ms: %d", data.PublishedAtUnixMs)
	}
	if !data.IsDiscontinuity || !data.IsEnd {
		t.Fatalf("expected discontinuity and end flags")
	}
	if data.MimeType != "video/mp2t" {
		t.Fatalf("unexpected mime_type: %s", data.MimeType)
	}
	if data.InitSeedHash != strings.Repeat("ab", 32) {
		t.Fatalf("unexpected init_seed_hash: %s", data.InitSeedHash)
	}
	if data.PlaylistUriHint != "/custom/seg0.ts" {
		t.Fatalf("unexpected playlist_uri_hint: %s", data.PlaylistUriHint)
	}
	if strings.TrimSpace(data.PublisherPubkey) == "" {
		t.Fatalf("publisher_pubkey missing")
	}
}

func TestLiveSubscribeAndPublishLatest(t *testing.T) {
	pubHost, _ := newSecpHost(t)
	defer pubHost.Close()
	subHost, _ := newSecpHost(t)
	defer subHost.Close()

	subDBPath := filepath.Join(t.TempDir(), "sub.sqlite")
	subDB, err := sql.Open("sqlite", subDBPath)
	if err != nil {
		t.Fatalf("open sub db: %v", err)
	}
	defer subDB.Close()
	if err := applySQLitePragmas(subDB); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(subDB); err != nil {
		t.Fatalf("init db: %v", err)
	}

	pubRT := &Runtime{Host: pubHost, live: newLiveRuntime()}
	subRT := &Runtime{Host: subHost, live: newLiveRuntime()}
	registerLiveHandlers(nil, pubRT)
	registerLiveHandlers(newClientDB(subDB, nil), subRT)

	pubHex, err := localPubKeyHex(pubHost)
	if err != nil {
		t.Fatalf("publisher pubkey failed: %v", err)
	}
	streamID := strings.Repeat("ab", 32)
	uri, err := BuildLiveSubscribeURI(pubHex, streamID)
	if err != nil {
		t.Fatalf("build uri failed: %v", err)
	}

	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)
	res, err := TriggerLiveSubscribe(context.Background(), subRT, uri, 5)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if res.StreamID != streamID {
		t.Fatalf("unexpected stream id: %s", res.StreamID)
	}

	err = TriggerLivePublishLatest(context.Background(), pubRT, streamID, []LiveSegmentRef{
		{SegmentIndex: 11, SeedHash: strings.Repeat("cd", 32)},
		{SegmentIndex: 12, SeedHash: strings.Repeat("ef", 32)},
	})
	if err != nil {
		t.Fatalf("publish latest failed: %v", err)
	}
	got, err := TriggerLiveGetLatest(subRT, streamID)
	if err != nil {
		t.Fatalf("get latest failed: %v", err)
	}
	if len(got.RecentSegments) != 2 {
		t.Fatalf("unexpected segment count: %d", len(got.RecentSegments))
	}
	if got.RecentSegments[1].SegmentIndex != 12 {
		t.Fatalf("unexpected latest segment index: %d", got.RecentSegments[1].SegmentIndex)
	}
}

func TestLiveQuoteSubmitAndList(t *testing.T) {
	buyerDBPath := filepath.Join(t.TempDir(), "buyer.sqlite")
	buyerDB, err := sql.Open("sqlite", buyerDBPath)
	if err != nil {
		t.Fatalf("open buyer db: %v", err)
	}
	defer buyerDB.Close()
	if err := applySQLitePragmas(buyerDB); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(buyerDB); err != nil {
		t.Fatalf("init db: %v", err)
	}
	buyerHost, _ := newSecpHost(t)
	defer buyerHost.Close()
	sellerHost, _ := newSecpHost(t)
	defer sellerHost.Close()

	buyerRT := &Runtime{Host: buyerHost, live: newLiveRuntime()}
	sellerRT := &Runtime{Host: sellerHost, live: newLiveRuntime()}
	registerLiveHandlers(newClientDB(buyerDB, nil), buyerRT)
	sellerHost.Peerstore().AddAddrs(buyerHost.ID(), buyerHost.Addrs(), time.Minute)
	buyerPub, err := localPubKeyHex(buyerHost)
	if err != nil {
		t.Fatalf("buyer pubkey failed: %v", err)
	}
	err = TriggerClientSubmitLiveQuote(context.Background(), sellerRT, LiveQuoteParams{
		DemandID:           "ldmd_test",
		BuyerPeerID:        buyerPub,
		BuyerAddrs:         localAdvertiseAddrs(buyerRT),
		StreamID:           strings.Repeat("a", 64),
		LatestSegmentIndex: 12,
		RecentSegments: []LiveQuoteSegment{
			{SegmentIndex: 11, SeedHash: strings.Repeat("b", 64)},
			{SegmentIndex: 12, SeedHash: strings.Repeat("c", 64)},
		},
	})
	if err != nil {
		t.Fatalf("submit live quote failed: %v", err)
	}
	quotes, err := TriggerClientListLiveQuotes(context.Background(), newClientDB(buyerDB, nil), "ldmd_test")
	if err != nil {
		t.Fatalf("list live quotes failed: %v", err)
	}
	if len(quotes) != 1 {
		t.Fatalf("unexpected quote count: %d", len(quotes))
	}
	if quotes[0].LatestSegmentIndex != 12 || len(quotes[0].RecentSegments) != 2 {
		t.Fatalf("unexpected live quote payload: %+v", quotes[0])
	}
}
