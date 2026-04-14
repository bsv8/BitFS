package clientapp

import (
	"fmt"
	"testing"
	"time"

	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
)

func TestNodeReachabilityCacheAndSelfState(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	hNode, nodePubkeyHex := newSecpHost(t)
	defer hNode.Close()
	hGateway, gatewayPubkeyHex := newSecpHost(t)
	defer hGateway.Close()
	ann := broadcastmodule.NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      []string{fmt.Sprintf("%s/p2p/%s", hNode.Addrs()[0].String(), hNode.ID().String())},
		HeadHeight:      321,
		Seq:             7,
		PublishedAtUnix: 1000,
		ExpiresAtUnix:   2000,
		Signature:       []byte{0xaa, 0xbb},
	}
	if err := storeNodeReachabilityCache(newClientDB(db, nil), gatewayPubkeyHex, ann); err != nil {
		t.Fatalf("save cache: %v", err)
	}
	got, found, err := loadNodeReachabilityCache(newClientDB(db, nil), ann.NodePubkeyHex, 1500)
	if err != nil {
		t.Fatalf("load cache: %v", err)
	}
	if !found {
		t.Fatalf("expected cache found")
	}
	if got.HeadHeight != ann.HeadHeight || got.Seq != ann.Seq || len(got.Multiaddrs) != 1 {
		t.Fatalf("unexpected cache announcement: %+v", got)
	}
	if err := storeSelfNodeReachabilityState(newClientDB(db, nil), selfNodeReachabilityState{
		NodePubkeyHex: ann.NodePubkeyHex,
		HeadHeight:    ann.HeadHeight,
		Seq:           ann.Seq,
	}); err != nil {
		t.Fatalf("save self state: %v", err)
	}
	state, found, err := loadSelfNodeReachabilityState(newClientDB(db, nil), ann.NodePubkeyHex)
	if err != nil {
		t.Fatalf("load self state: %v", err)
	}
	if !found {
		t.Fatalf("expected self state found")
	}
	if state.HeadHeight != ann.HeadHeight || state.Seq != ann.Seq {
		t.Fatalf("unexpected self state: %+v", state)
	}
}

func TestShouldAutoNodeReachabilityAnnounce(t *testing.T) {
	t.Parallel()

	nowUnix := int64(10_000)
	t.Run("first announce", func(t *testing.T) {
		t.Parallel()
		if !shouldAutoNodeReachabilityAnnounce(nowUnix, autoNodeReachabilityAnnounceState{}, "[\"a\"]", "gw-a") {
			t.Fatalf("first announce should be required")
		}
	})

	t.Run("same fingerprint before renew window", func(t *testing.T) {
		t.Parallel()
		state := autoNodeReachabilityAnnounceState{
			LastFingerprint:      "[\"a\"]",
			LastGatewayPubkeyHex: "gw-a",
			LastExpiresAtUnix:    nowUnix + 3600,
		}
		if shouldAutoNodeReachabilityAnnounce(nowUnix, state, "[\"a\"]", "gw-a") {
			t.Fatalf("announce should not be required")
		}
	})

	t.Run("fingerprint changed", func(t *testing.T) {
		t.Parallel()
		state := autoNodeReachabilityAnnounceState{
			LastFingerprint:      "[\"a\"]",
			LastGatewayPubkeyHex: "gw-a",
			LastExpiresAtUnix:    nowUnix + 3600,
		}
		if !shouldAutoNodeReachabilityAnnounce(nowUnix, state, "[\"b\"]", "gw-a") {
			t.Fatalf("announce should be required when fingerprint changes")
		}
	})

	t.Run("gateway changed", func(t *testing.T) {
		t.Parallel()
		state := autoNodeReachabilityAnnounceState{
			LastFingerprint:      "[\"a\"]",
			LastGatewayPubkeyHex: "gw-a",
			LastExpiresAtUnix:    nowUnix + 3600,
		}
		if !shouldAutoNodeReachabilityAnnounce(nowUnix, state, "[\"a\"]", "gw-b") {
			t.Fatalf("announce should be required when gateway changes")
		}
	})

	t.Run("renew window reached", func(t *testing.T) {
		t.Parallel()
		state := autoNodeReachabilityAnnounceState{
			LastFingerprint:      "[\"a\"]",
			LastGatewayPubkeyHex: "gw-a",
			LastExpiresAtUnix:    nowUnix + int64(autoNodeReachabilityAnnounceRenewLead/time.Second),
		}
		if !shouldAutoNodeReachabilityAnnounce(nowUnix, state, "[\"a\"]", "gw-a") {
			t.Fatalf("announce should be required when renew window reached")
		}
	})
}

func TestAutoNodeReachabilityNotifieeEnqueue(t *testing.T) {
	t.Parallel()

	ch := make(chan string, 2)
	n := &autoNodeReachabilityNotifiee{
		enqueue: func(reason string) {
			ch <- reason
		},
	}
	n.Listen(nil, nil)
	n.ListenClose(nil, nil)

	first := <-ch
	second := <-ch
	if first != "listen_changed" || second != "listen_changed" {
		t.Fatalf("unexpected notifiee reasons: first=%s second=%s", first, second)
	}
}
