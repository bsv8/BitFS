package clientapp

import (
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestRuntimeMasterGatewayAccessIsSerialized(t *testing.T) {
	rt := newRuntimeForTest(t, Config{}, "1111111111111111111111111111111111111111111111111111111111111111")

	const workers = 8
	const rounds = 1000

	ids := []peer.ID{
		"peer-a",
		"peer-b",
		"peer-c",
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				next := ids[(worker+round)%len(ids)]
				_ = rt.SetMasterGateway(next)
				_ = rt.MasterGateway()
			}
		}(i)
	}
	wg.Wait()
}
