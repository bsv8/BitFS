package broadcast

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNodeReachabilityAnnouncementVerify(t *testing.T) {
	t.Parallel()

	priv, pub, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	pubRaw, err := pub.Raw()
	if err != nil {
		t.Fatalf("pub raw: %v", err)
	}
	nodePubkeyHex := hex.EncodeToString(pubRaw)
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}
	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/19090/p2p/%s", pid.String())
	payload, err := BuildNodeReachabilitySignPayload(nodePubkeyHex, []string{addr, addr}, 123, 1, 1000, 1060)
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}
	sig, err := priv.Sign(payload)
	if err != nil {
		t.Fatalf("sign payload: %v", err)
	}
	ann := NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      []string{addr, addr},
		HeadHeight:      123,
		Seq:             1,
		PublishedAtUnix: 1000,
		ExpiresAtUnix:   1060,
		Signature:       sig,
	}
	if err := VerifyNodeReachabilityAnnouncement(ann); err != nil {
		t.Fatalf("verify announcement: %v", err)
	}
}

func TestNormalizeNodeReachabilityAddrsRejectPeerMismatch(t *testing.T) {
	t.Parallel()

	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	pubRaw, err := priv.GetPublic().Raw()
	if err != nil {
		t.Fatalf("pub raw: %v", err)
	}
	nodePubkeyHex := hex.EncodeToString(pubRaw)

	otherPriv, otherPub, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		t.Fatalf("generate other key pair: %v", err)
	}
	_ = otherPriv
	otherPID, err := peer.IDFromPublicKey(otherPub)
	if err != nil {
		t.Fatalf("other peer id: %v", err)
	}
	_, err = NormalizeNodeReachabilityAddrs(nodePubkeyHex, []string{
		fmt.Sprintf("/ip4/127.0.0.1/tcp/19091/p2p/%s", otherPID.String()),
	})
	if err == nil {
		t.Fatalf("expected peer mismatch error")
	}
}
