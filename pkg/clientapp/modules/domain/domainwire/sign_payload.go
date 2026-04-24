package domainwire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	quotePayloadVersion      = "bsv8-domain-register-quote-v1"
	receiptPayloadVersion    = "bsv8-domain-register-receipt-v1"
	resolutionPayloadVersion = "bsv8-domain-resolution-v1"
)

type SignFunc func([]byte) ([]byte, error)
type VerifyFunc func([]byte, []byte) (bool, error)

// SignedEnvelope 是链上 OP_RETURN 与链下 RPC 共用的完整签名原文。
// JSON 线格式固定为：[[字段...], "signature_hex"]。
type SignedEnvelope struct {
	Fields       []any
	SignatureHex string
}

func (e SignedEnvelope) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{e.Fields, e.SignatureHex})
}

func (e *SignedEnvelope) UnmarshalJSON(raw []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return err
	}
	if len(parts) != 2 {
		return fmt.Errorf("signed envelope must have 2 items")
	}
	var fields []any
	if err := json.Unmarshal(parts[0], &fields); err != nil {
		return err
	}
	var sigHex string
	if err := json.Unmarshal(parts[1], &sigHex); err != nil {
		return err
	}
	e.Fields = fields
	e.SignatureHex = sigHex
	return nil
}

type RegisterQuoteFields struct {
	QuoteID                  string
	Name                     string
	OwnerPubkeyHex           string
	TargetPubkeyHex          string
	PayToAddress             string
	RegisterPriceSatoshi     uint64
	RegisterSubmitFeeSatoshi uint64
	TotalPaySatoshi          uint64
	LockExpiresAtUnix        int64
	TermSeconds              uint64
}

func (f RegisterQuoteFields) Array() []any {
	return []any{
		quotePayloadVersion,
		f.QuoteID,
		f.Name,
		f.OwnerPubkeyHex,
		f.TargetPubkeyHex,
		f.PayToAddress,
		f.RegisterPriceSatoshi,
		f.RegisterSubmitFeeSatoshi,
		f.TotalPaySatoshi,
		f.LockExpiresAtUnix,
		f.TermSeconds,
	}
}

type RegisterReceiptFields struct {
	Name            string
	OwnerPubkeyHex  string
	TargetPubkeyHex string
	ExpireAtUnix    int64
	RegisterTxID    string
	IssuedAtUnix    int64
}

func (f RegisterReceiptFields) Array() []any {
	return []any{
		receiptPayloadVersion,
		f.Name,
		f.OwnerPubkeyHex,
		f.TargetPubkeyHex,
		f.ExpireAtUnix,
		f.RegisterTxID,
		f.IssuedAtUnix,
	}
}

type ResolutionFields struct {
	Name            string
	OwnerPubkeyHex  string
	TargetPubkeyHex string
	ExpireAtUnix    int64
	IssuedAtUnix    int64
}

func (f ResolutionFields) Array() []any {
	return []any{
		resolutionPayloadVersion,
		f.Name,
		f.OwnerPubkeyHex,
		f.TargetPubkeyHex,
		f.ExpireAtUnix,
		f.IssuedAtUnix,
	}
}

func SignEnvelope(fields []any, sign SignFunc) ([]byte, error) {
	if sign == nil {
		return nil, fmt.Errorf("sign func is nil")
	}
	digest, err := EnvelopeDigest(fields)
	if err != nil {
		return nil, err
	}
	sig, err := sign(digest)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(SignedEnvelope{
		Fields:       append([]any(nil), fields...),
		SignatureHex: hex.EncodeToString(sig),
	})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func VerifyEnvelope(raw []byte, verify VerifyFunc) (SignedEnvelope, error) {
	if verify == nil {
		return SignedEnvelope{}, fmt.Errorf("verify func is nil")
	}
	var env SignedEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return SignedEnvelope{}, fmt.Errorf("decode signed envelope: %w", err)
	}
	sig, err := hex.DecodeString(stringsTrim(env.SignatureHex))
	if err != nil || len(sig) == 0 {
		return SignedEnvelope{}, fmt.Errorf("signature hex invalid")
	}
	digest, err := EnvelopeDigest(env.Fields)
	if err != nil {
		return SignedEnvelope{}, err
	}
	ok, err := verify(digest, sig)
	if err != nil {
		return SignedEnvelope{}, err
	}
	if !ok {
		return SignedEnvelope{}, fmt.Errorf("signed envelope invalid")
	}
	return env, nil
}

func EnvelopeDigest(fields []any) ([]byte, error) {
	raw, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("marshal signed fields: %w", err)
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

func stringsTrim(v string) string {
	return strings.TrimSpace(v)
}
