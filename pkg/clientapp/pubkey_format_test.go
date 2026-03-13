package clientapp

import (
	"encoding/hex"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestNormalizeCompressedPubKeyHex(t *testing.T) {
	privRaw := make([]byte, 32)
	for i := range privRaw {
		privRaw[i] = byte(i + 1)
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(privRaw)
	if err != nil {
		t.Fatalf("unmarshal secp256k1 private key failed: %v", err)
	}
	raw, err := priv.GetPublic().Raw()
	if err != nil {
		t.Fatalf("read raw pubkey failed: %v", err)
	}
	pubHex := hex.EncodeToString(raw)

	if got, err := normalizeCompressedPubKeyHex(pubHex); err != nil || got != pubHex {
		t.Fatalf("normalizeCompressedPubKeyHex failed: got=%q err=%v", got, err)
	}

	marshalBytes, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatalf("marshal public key failed: %v", err)
	}
	if _, err := normalizeCompressedPubKeyHex(hex.EncodeToString(marshalBytes)); err == nil {
		t.Fatalf("expected strict normalize reject legacy marshal format")
	}

	got, err := normalizeCompressedPubKeyHexLegacyAware(hex.EncodeToString(marshalBytes))
	if err != nil {
		t.Fatalf("legacy-aware normalize failed: %v", err)
	}
	if got != pubHex {
		t.Fatalf("legacy-aware normalize mismatch: got=%s want=%s", got, pubHex)
	}
}
