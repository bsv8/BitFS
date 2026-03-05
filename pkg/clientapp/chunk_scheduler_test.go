package clientapp

import "testing"

func TestPickDispatchChunkRespectAvailableChunks(t *testing.T) {
	t.Parallel()

	workers := []*transferSellerWorker{
		{quote: DirectQuoteItem{SellerPeerID: "s1"}, availableChunks: chunkIndexSet([]uint32{0})},
		{quote: DirectQuoteItem{SellerPeerID: "s2"}, availableChunks: chunkIndexSet([]uint32{1})},
	}
	ready := []int{0, 1}
	pending := map[uint32]bool{0: true, 1: true}
	inflight := map[uint32]bool{}

	// 首选块 1，应该选中只提供块 1 的卖家。
	chunk, candidates, ok := pickDispatchChunk(ready, workers, pending, inflight, func(pending map[uint32]bool, inflight map[uint32]bool) (uint32, bool) {
		return 1, true
	})
	if !ok {
		t.Fatalf("dispatch chunk should be selectable")
	}
	if chunk != 1 {
		t.Fatalf("selected chunk mismatch: got=%d want=1", chunk)
	}
	if len(candidates) != 1 || candidates[0] != 1 {
		t.Fatalf("candidate workers mismatch: got=%v want=[1]", candidates)
	}
}

func TestHasAnyProviderForPending(t *testing.T) {
	t.Parallel()

	workers := []*transferSellerWorker{
		{quote: DirectQuoteItem{SellerPeerID: "s1"}, availableChunks: chunkIndexSet([]uint32{0})},
	}
	if !hasAnyProviderForPending(map[uint32]bool{0: true}, workers) {
		t.Fatalf("chunk 0 should have provider")
	}
	if hasAnyProviderForPending(map[uint32]bool{1: true}, workers) {
		t.Fatalf("chunk 1 should not have provider")
	}
}
