package pproto

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	cryptorand "crypto/rand"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ReplayStore interface {
	Get(scope, msgID string) (entry ReplayEntry, found bool, err error)
	Put(scope, msgID, payloadHash string, response []byte, expireAt int64) error
}

type ReplayEntry struct {
	PayloadHash string
	Response    []byte
	ExpireAt    int64
}

type SecurityConfig struct {
	Domain  string
	Network string
	TTL     time.Duration
	Replay  ReplayStore
	Trace   TraceSink
}

type contextKey string

const senderPubkeyHexContextKey contextKey = "p2prpc_sender_pubkey_hex"
const messageIDContextKey contextKey = "p2prpc_message_id"

func SenderPubkeyHexFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(senderPubkeyHexContextKey)
	s, ok := v.(string)
	return s, ok && s != ""
}

func MessageIDFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(messageIDContextKey)
	s, ok := v.(string)
	return s, ok && s != ""
}

const secp256k1PubKeyType = 0x09

func secp256k1CompressedToLibP2PPubkey(compressed []byte) ([]byte, error) {
	if len(compressed) != 33 || (compressed[0] != 0x02 && compressed[0] != 0x03) {
		return nil, fmt.Errorf("expected secp256k1 compressed pubkey (33 bytes with 0x02/0x03 prefix), got %d bytes with 0x%02x", len(compressed), func() byte { if len(compressed) > 0 { return compressed[0] }; return 0 }())
	}
	stretched := make([]byte, 1+len(compressed))
	stretched[0] = secp256k1PubKeyType
	copy(stretched[1:], compressed)
	return stretched, nil
}

func verifyRemotePeerBytes(remote peer.ID, senderPub []byte) error {
	expected, err := peerIDFromSenderPubkey(senderPub)
	if err != nil {
		return fmt.Errorf("invalid sender pubkey: %w", err)
	}
	if expected != remote {
		return fmt.Errorf("remote peer mismatch")
	}
	return nil
}

func peerIDFromSenderPubkey(senderPub []byte) (peer.ID, error) {
	if len(senderPub) == 32 {
		priv := secp256k1.PrivKeyFromBytes(senderPub)
		pub := priv.PubKey()
		var libp2pPub crypto.PubKey = (*crypto.Secp256k1PublicKey)(pub)
		return peer.IDFromPublicKey(libp2pPub)
	}
	if len(senderPub) == 33 && (senderPub[0] == 0x02 || senderPub[0] == 0x03) {
		pk, err := secp256k1.ParsePubKey(senderPub)
		if err == nil {
			var libp2pPub crypto.PubKey = (*crypto.Secp256k1PublicKey)(pk)
			return peer.IDFromPublicKey(libp2pPub)
		}
	}
	pub, err := crypto.UnmarshalPublicKey(senderPub)
	if err != nil {
		if len(senderPub) == 33 && senderPub[0] == secp256k1PubKeyType {
			return peer.ID(""), fmt.Errorf("unmarshal sender pubkey: %w", err)
		}
		if len(senderPub) == 65 && senderPub[0] == 0x04 {
			if pk, parseErr := secp256k1.ParsePubKey(senderPub); parseErr == nil {
				var libp2pPub crypto.PubKey = (*crypto.Secp256k1PublicKey)(pk)
				return peer.IDFromPublicKey(libp2pPub)
			}
		}
		return peer.ID(""), fmt.Errorf("unmarshal sender pubkey: %w", err)
	}
	return peer.IDFromPublicKey(pub)
}

func senderPubkeyHex(senderPub []byte) string {
	if len(senderPub) == 32 {
		priv := secp256k1.PrivKeyFromBytes(senderPub)
		return strings.ToLower(hex.EncodeToString(priv.PubKey().SerializeCompressed()))
	}
	if len(senderPub) == 33 && (senderPub[0] == 0x02 || senderPub[0] == 0x03) {
		return strings.ToLower(hex.EncodeToString(senderPub))
	}
	pub, err := crypto.UnmarshalPublicKey(senderPub)
	if err != nil {
		return strings.ToLower(hex.EncodeToString(senderPub))
	}
	raw, err := pub.Raw()
	if err != nil {
		return strings.ToLower(hex.EncodeToString(senderPub))
	}
	if len(raw) == 32 {
		priv := secp256k1.PrivKeyFromBytes(raw)
		return strings.ToLower(hex.EncodeToString(priv.PubKey().SerializeCompressed()))
	}
	return strings.ToLower(hex.EncodeToString(raw))
}

func randomMsgID() (string, error) {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
