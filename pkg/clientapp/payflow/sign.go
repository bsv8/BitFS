package payflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	ecdsa "github.com/bsv-blockchain/go-sdk/primitives/ecdsa"
)

func ClientCommitDigest(v ClientCommit) ([]byte, error) {
	raw, err := MarshalClientCommit(v)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

func serviceQuoteDigest(v ServiceQuote) ([]byte, error) {
	if err := v.ValidateUnsigned(); err != nil {
		return nil, err
	}
	raw, err := marshalArray(v.UnsignedArray())
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

func SignServiceQuote(v ServiceQuote, priv *ec.PrivateKey) (ServiceQuote, error) {
	if priv == nil {
		return ServiceQuote{}, fmt.Errorf("service node private key required")
	}
	v = v.Normalize()
	v.SignatureHex = ""
	digest, err := serviceQuoteDigest(v)
	if err != nil {
		return ServiceQuote{}, err
	}
	sig, err := priv.Sign(digest)
	if err != nil {
		return ServiceQuote{}, fmt.Errorf("sign service quote: %w", err)
	}
	v.SignatureHex = hex.EncodeToString(sig.Serialize())
	return v.Normalize(), nil
}

func VerifyServiceQuoteSignature(v ServiceQuote, pub *ec.PublicKey) error {
	if pub == nil {
		return fmt.Errorf("service node public key required")
	}
	v = v.Normalize()
	if err := v.Validate(); err != nil {
		return err
	}
	sigRaw, err := hex.DecodeString(v.SignatureHex)
	if err != nil {
		return fmt.Errorf("decode service quote signature: %w", err)
	}
	parsed, err := ec.ParseDERSignature(sigRaw)
	if err != nil {
		return fmt.Errorf("parse service quote signature: %w", err)
	}
	unsigned := v
	unsigned.SignatureHex = ""
	digest, err := serviceQuoteDigest(unsigned)
	if err != nil {
		return err
	}
	if !ecdsa.Verify(digest, parsed, pub.ToECDSA()) {
		return fmt.Errorf("service quote signature invalid")
	}
	return nil
}

func SignClientCommit(v ClientCommit, priv *ec.PrivateKey) ([]byte, error) {
	if priv == nil {
		return nil, fmt.Errorf("client private key required")
	}
	digest, err := ClientCommitDigest(v)
	if err != nil {
		return nil, err
	}
	sig, err := priv.Sign(digest)
	if err != nil {
		return nil, fmt.Errorf("sign client commit: %w", err)
	}
	return sig.Serialize(), nil
}

// SignClientCommitEnvelope 输出统一线格式 [[字段...], "signature_hex"]。
// 设计说明：
// - ClientCommit 业务体继续保持“无签名原文”语义，便于 hash 和校验复用；
// - 真正在线上传播时，一律使用 envelope，避免 commit 和 signature 被多层协议重复拆装。
func SignClientCommitEnvelope(v ClientCommit, priv *ec.PrivateKey) ([]byte, error) {
	sig, err := SignClientCommit(v, priv)
	if err != nil {
		return nil, err
	}
	return MarshalSignedClientCommit(v, sig)
}

func VerifyClientCommitSignature(v ClientCommit, sig []byte, pub *ec.PublicKey) error {
	if pub == nil {
		return fmt.Errorf("client public key required")
	}
	if len(sig) == 0 {
		return fmt.Errorf("client commit signature required")
	}
	digest, err := ClientCommitDigest(v)
	if err != nil {
		return err
	}
	parsed, err := ec.ParseDERSignature(sig)
	if err != nil {
		return fmt.Errorf("parse client commit signature: %w", err)
	}
	if !ecdsa.Verify(digest, parsed, pub.ToECDSA()) {
		return fmt.Errorf("client commit signature invalid")
	}
	return nil
}

func VerifySignedClientCommit(raw []byte, pub *ec.PublicKey) (ClientCommit, error) {
	commit, sig, err := UnmarshalSignedClientCommit(raw)
	if err != nil {
		return ClientCommit{}, err
	}
	if err := VerifyClientCommitSignature(commit, sig, pub); err != nil {
		return ClientCommit{}, err
	}
	return commit, nil
}

func UpdateTemplateHashFromTxHex(txHex string) (string, error) {
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	if txHex == "" {
		return "", fmt.Errorf("update tx hex required")
	}
	raw, err := hex.DecodeString(txHex)
	if err != nil {
		return "", fmt.Errorf("decode update tx hex: %w", err)
	}
	return hashRawHex(raw), nil
}

func BuildNextProofState(prev ProofState, accepted AcceptedCharge) (ProofState, error) {
	if err := accepted.Validate(); err != nil {
		return ProofState{}, err
	}
	prev = prev.Normalize()
	if prev.AcceptedTipHash != "" && accepted.PrevAcceptedHash != prev.AcceptedTipHash {
		return ProofState{}, fmt.Errorf("accepted charge prev hash mismatch")
	}
	hash, err := HashAcceptedCharge(accepted)
	if err != nil {
		return ProofState{}, err
	}
	return ProofState{
		Version:                proofStateVersion,
		SpendTxID:              accepted.SpendTxID,
		SequenceNumber:         accepted.SequenceNumber,
		ServerAmountSatoshi:    accepted.ServerAmountAfter,
		AcceptedTipHash:        hash,
		LastAcceptedChargeHash: hash,
		ServiceDeadlineUnix:    accepted.ServiceDeadlineUnix,
	}.Normalize(), nil
}

func serviceReceiptDigest(v ServiceReceipt) ([]byte, error) {
	if err := v.ValidateUnsigned(); err != nil {
		return nil, err
	}
	raw, err := marshalArray(v.UnsignedArray())
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

func SignServiceReceipt(v ServiceReceipt, priv *ec.PrivateKey) (ServiceReceipt, error) {
	if priv == nil {
		return ServiceReceipt{}, fmt.Errorf("service node private key required")
	}
	v = v.Normalize()
	v.SignatureHex = ""
	digest, err := serviceReceiptDigest(v)
	if err != nil {
		return ServiceReceipt{}, err
	}
	sig, err := priv.Sign(digest)
	if err != nil {
		return ServiceReceipt{}, fmt.Errorf("sign service receipt: %w", err)
	}
	v.SignatureHex = hex.EncodeToString(sig.Serialize())
	return v.Normalize(), nil
}

func VerifyServiceReceiptSignature(v ServiceReceipt, pub *ec.PublicKey) error {
	if pub == nil {
		return fmt.Errorf("service node public key required")
	}
	v = v.Normalize()
	if err := v.Validate(); err != nil {
		return err
	}
	sigRaw, err := hex.DecodeString(v.SignatureHex)
	if err != nil {
		return fmt.Errorf("decode service receipt signature: %w", err)
	}
	parsed, err := ec.ParseDERSignature(sigRaw)
	if err != nil {
		return fmt.Errorf("parse service receipt signature: %w", err)
	}
	unsigned := v
	unsigned.SignatureHex = ""
	digest, err := serviceReceiptDigest(unsigned)
	if err != nil {
		return err
	}
	if !ecdsa.Verify(digest, parsed, pub.ToECDSA()) {
		return fmt.Errorf("service receipt signature invalid")
	}
	return nil
}
