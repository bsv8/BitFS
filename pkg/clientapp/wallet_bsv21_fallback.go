package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	bip32 "github.com/bsv-blockchain/go-sdk/compat/bip32"
	chaincfg "github.com/bsv-blockchain/go-sdk/transaction/chaincfg"
)

const (
	walletBSV21FallbackFeePerOutputSat = 1000
	walletBSV21FeeXPUB                 = "xpub661MyMwAqRbcF221R74MPqdipLsgUevAAX4hZP2rywyEeShpbe3v2r9ciAvSGT6FB22TEmFLdUyeEDJL4ekG8s9H5WXbzDQPr6eW1zEYYy9"
)

type walletBSV21SendRules struct {
	Symbol       string
	FeePerOutput uint64
	FeeAddress   string
	Source       string
}

func loadWalletBSV21SendRules(ctx context.Context, rt *Runtime, tokenID string) (walletBSV21SendRules, error) {
	_ = ctx
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return walletBSV21SendRules{}, fmt.Errorf("bsv21 token id is required")
	}
	// 设计说明：
	// - 1Sat overlay 侧的 authority/provider 已整体下线，不再向外部索取 protocol 规则；
	// - 当前 send 只保留“按已知公开规则构造链上交易”的最小闭环；
	// - 最终是否成立，统一以后续 WoC 观察结果为准。
	feeAddress, err := deriveWalletBSV21FeeAddress(tokenID, walletRuntimeBSVNetwork(rt))
	if err != nil {
		return walletBSV21SendRules{}, err
	}
	return walletBSV21SendRules{
		FeePerOutput: walletBSV21FallbackFeePerOutputSat,
		FeeAddress:   feeAddress,
		Source:       "local-fallback",
	}, nil
}

func deriveWalletBSV21FeeAddress(tokenID string, network string) (string, error) {
	key, err := bip32.NewKeyFromString(walletBSV21FeeXPUB)
	if err != nil {
		return "", fmt.Errorf("parse bsv21 fee xpub failed: %w", err)
	}
	derivationSeed, err := walletBSV21FeeDerivationSeed(tokenID)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(derivationSeed)
	path := fmt.Sprintf("21/%d/%d", binary.BigEndian.Uint32(hash[0:4])>>1, binary.BigEndian.Uint32(hash[24:28])>>1)
	child, err := key.DeriveChildFromPath(path)
	if err != nil {
		return "", fmt.Errorf("derive bsv21 fee address failed: %w", err)
	}
	return child.Address(walletBSV21ChainParams(network)), nil
}

func walletBSV21FeeDerivationSeed(tokenID string) ([]byte, error) {
	parts := strings.Split(strings.TrimSpace(tokenID), "_")
	if len(parts) != 2 {
		return nil, fmt.Errorf("bsv21 token id invalid")
	}
	txid := strings.ToLower(strings.TrimSpace(parts[0]))
	if len(txid) != 64 {
		return nil, fmt.Errorf("bsv21 token id invalid")
	}
	txidBytes, err := hex.DecodeString(txid)
	if err != nil {
		return nil, fmt.Errorf("bsv21 token id invalid: %w", err)
	}
	vout, ok := parseUint32(strings.TrimSpace(parts[1]))
	if !ok {
		return nil, fmt.Errorf("bsv21 token id invalid")
	}
	seed := make([]byte, 36)
	copy(seed, txidBytes)
	binary.BigEndian.PutUint32(seed[32:], vout)
	return seed, nil
}

func walletBSV21ChainParams(network string) *chaincfg.Params {
	if strings.TrimSpace(network) == "main" {
		return &chaincfg.MainNet
	}
	return &chaincfg.TestNet
}

func walletRuntimeBSVNetwork(rt *Runtime) string {
	if rt == nil {
		return ""
	}
	return strings.TrimSpace(rt.runIn.BSV.Network)
}

func parseUint32(raw string) (uint32, bool) {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(value), true
}
