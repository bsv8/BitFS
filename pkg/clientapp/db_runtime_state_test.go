package clientapp

import (
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
)

func TestReachabilityCacheRoundTrip(t *testing.T) {
	db := openClientappTestDB(t)
	store := NewClientStore(db, nil)

	ann := gatewayclient.NodeReachabilityAnnouncement{
		NodePubkeyHex:   strings.Repeat("ab", 32),
		Multiaddrs:      []string{"/ip4/127.0.0.1/tcp/10001/p2p/" + strings.Repeat("ab", 32)},
		HeadHeight:      1234,
		Seq:             7,
		PublishedAtUnix: time.Now().Unix(),
		ExpiresAtUnix:   time.Now().Add(5 * time.Minute).Unix(),
		Signature:       []byte("sig"),
	}
	if err := storeNodeReachabilityCache(store, strings.Repeat("cd", 32), ann); err != nil {
		t.Fatalf("store cache failed: %v", err)
	}

	got, found, err := loadNodeReachabilityCache(store, ann.NodePubkeyHex, time.Now().Unix())
	if err != nil {
		t.Fatalf("load cache failed: %v", err)
	}
	if !found {
		t.Fatal("expected cache to be found")
	}
	if got.NodePubkeyHex != ann.NodePubkeyHex {
		t.Fatalf("node pubkey mismatch: got %s want %s", got.NodePubkeyHex, ann.NodePubkeyHex)
	}
	if got.HeadHeight != ann.HeadHeight || got.Seq != ann.Seq {
		t.Fatalf("cache fields mismatch: got %+v want %+v", got, ann)
	}
	if len(got.Multiaddrs) != len(ann.Multiaddrs) || got.Multiaddrs[0] != ann.Multiaddrs[0] {
		t.Fatalf("multiaddrs mismatch: got %+v want %+v", got.Multiaddrs, ann.Multiaddrs)
	}

	_, found, err = loadNodeReachabilityCache(store, ann.NodePubkeyHex, time.Now().Add(10*time.Minute).Unix())
	if err != nil {
		t.Fatalf("load expired cache failed: %v", err)
	}
	if found {
		t.Fatal("expected expired cache to be hidden")
	}
}

func TestSelfReachabilityStateRoundTrip(t *testing.T) {
	db := openClientappTestDB(t)
	store := NewClientStore(db, nil)

	want := selfNodeReachabilityState{
		NodePubkeyHex: strings.Repeat("ef", 32),
		HeadHeight:    88,
		Seq:           9,
	}
	if err := storeSelfNodeReachabilityState(store, want); err != nil {
		t.Fatalf("store self state failed: %v", err)
	}

	got, found, err := loadSelfNodeReachabilityState(store, want.NodePubkeyHex)
	if err != nil {
		t.Fatalf("load self state failed: %v", err)
	}
	if !found {
		t.Fatal("expected self state to be found")
	}
	if got.NodePubkeyHex != want.NodePubkeyHex || got.HeadHeight != want.HeadHeight || got.Seq != want.Seq {
		t.Fatalf("self state mismatch: got %+v want %+v", got, want)
	}
}
