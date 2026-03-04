package clientapp

import (
	"testing"
	"time"
)

func TestBuildTransferStrategyDefault(t *testing.T) {
	s := buildTransferStrategy("")
	if _, ok := s.(*smartDispatchStrategy); !ok {
		t.Fatalf("expected smartDispatchStrategy, got %T", s)
	}
}

func TestBuildTransferStrategyIgnoreUnknown(t *testing.T) {
	s := buildTransferStrategy("unknown-strategy")
	if _, ok := s.(*smartDispatchStrategy); !ok {
		t.Fatalf("expected smartDispatchStrategy, got %T", s)
	}
}

func TestSmartStrategyPruneWorstOnSaturation(t *testing.T) {
	s := newSmartDispatchStrategy()
	workers := []*transferSellerWorker{
		{quote: DirectQuoteItem{SellerPeerID: "a", ChunkPrice: 10}, emaBPS: 1000},
		{quote: DirectQuoteItem{SellerPeerID: "b", ChunkPrice: 10}, emaBPS: 200},
		{quote: DirectQuoteItem{SellerPeerID: "c", ChunkPrice: 20}, emaBPS: 500},
	}

	now := time.Now()
	s.lastWindowAt = now.Add(-3 * time.Second)
	s.lastWindowBytes = 0
	s.lastWindowBPS = 100
	s.OnChunkDone(now, 300, workers)

	now2 := now.Add(3 * time.Second)
	s.lastWindowAt = now2.Add(-3 * time.Second)
	s.lastWindowBytes = 300
	s.lastWindowBPS = 100
	s.OnChunkDone(now2, 600, workers)

	if !workers[1].pruned {
		t.Fatalf("expected worker b to be pruned")
	}
	if s.lastPrunedSellerID != "b" {
		t.Fatalf("expected last pruned seller b, got=%s", s.lastPrunedSellerID)
	}
}
