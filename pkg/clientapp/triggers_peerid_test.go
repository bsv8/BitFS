package clientapp

import (
	"encoding/hex"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPeerIDFromClientID(t *testing.T) {
	// 使用固定私钥，保证测试可重复。
	privRaw := make([]byte, 32)
	for i := range privRaw {
		privRaw[i] = byte(i + 1)
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(privRaw)
	if err != nil {
		t.Fatalf("unmarshal secp256k1 private key failed: %v", err)
	}
	pub := priv.GetPublic()
	wantPID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("derive peer id failed: %v", err)
	}

	rawCompressed, err := pub.Raw()
	if err != nil {
		t.Fatalf("read raw compressed pubkey failed: %v", err)
	}
	t.Run("accept compressed secp256k1 hex", func(t *testing.T) {
		got, err := peerIDFromClientID(hex.EncodeToString(rawCompressed))
		if err != nil {
			t.Fatalf("peerIDFromClientID failed: %v", err)
		}
		if got != wantPID {
			t.Fatalf("peer id mismatch: got=%s want=%s", got, wantPID)
		}
	})

	t.Run("reject libp2p marshal hex", func(t *testing.T) {
		marshalBytes, err := crypto.MarshalPublicKey(pub)
		if err != nil {
			t.Fatalf("marshal libp2p public key failed: %v", err)
		}
		if _, err := peerIDFromClientID(hex.EncodeToString(marshalBytes)); err == nil {
			t.Fatalf("expected error for legacy marshal format")
		}
	})

	t.Run("reject invalid hex", func(t *testing.T) {
		if _, err := peerIDFromClientID("zzzz"); err == nil {
			t.Fatalf("expected error for invalid hex")
		}
	})
}
