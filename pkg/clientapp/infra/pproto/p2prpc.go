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
	if len(senderPub) == 33 && (senderPub[0] == 0x02 || senderPub[0] == 0x03) {
		pk, err := secp256k1.ParsePubKey(senderPub)
		if err != nil {
			return fmt.Errorf("parse secp256k1 pubkey: %w", err)
		}

		var libp2pPub crypto.PubKey = (*crypto.Secp256k1PublicKey)(pk)
		expected, err := peer.IDFromPublicKey(libp2pPub)
		if err != nil {
			return fmt.Errorf("id from pubkey: %w", err)
		}
		if expected != remote {
			return fmt.Errorf("remote peer mismatch")
		}
		return nil
	}

	pub, err := crypto.UnmarshalPublicKey(senderPub)
	if err != nil {
		return fmt.Errorf("invalid sender pubkey: %w", err)
	}
	expected, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return err
	}
	if expected != remote {
		return fmt.Errorf("remote peer mismatch")
	}
	return nil
}

func senderPubkeyHex(senderPub []byte) string {
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
	return strings.ToLower(hex.EncodeToString(raw))
}

func randomMsgID() (string, error) {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
