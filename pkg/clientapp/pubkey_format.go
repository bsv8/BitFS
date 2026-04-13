package clientapp

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// normalizeCompressedPubKeyHex 统一系统内公钥格式为 secp256k1 压缩公钥 hex（33 字节，前缀 02/03）。
func normalizeCompressedPubKeyHex(in string) (string, error) {
	pubHex := strings.ToLower(strings.TrimSpace(in))
	if pubHex == "" {
		return "", fmt.Errorf("pubkey hex required")
	}
	raw, err := hex.DecodeString(pubHex)
	if err != nil {
		return "", fmt.Errorf("invalid pubkey hex")
	}
	if len(raw) != 33 {
		return "", fmt.Errorf("invalid compressed pubkey length: got=%d want=33", len(raw))
	}
	if raw[0] != 0x02 && raw[0] != 0x03 {
		return "", fmt.Errorf("invalid compressed pubkey prefix")
	}
	return pubHex, nil
}

// normalizeCompressedPubKeyHexLegacyAware 仅用于历史数据迁移：
// 允许把旧 libp2p MarshalPublicKey hex（080212...）转换成压缩公钥 hex。
func normalizeCompressedPubKeyHexLegacyAware(in string) (string, error) {
	pubHex, err := normalizeCompressedPubKeyHex(in)
	if err == nil {
		return pubHex, nil
	}
	converted, convErr := poolcore.Libp2pMarshalPubHexToSecpCompressedHex(strings.TrimSpace(in))
	if convErr != nil {
		return "", err
	}
	return normalizeCompressedPubKeyHex(converted)
}

// normalizePubHexList 统一整理一组公钥 hex。
// 设计说明：
// - 只接受系统内的压缩公钥 hex；
// - 自动去重、去空白；
// - 遇到任意非法项直接返回错误，避免把脏值继续往下传。
func normalizePubHexList(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		pubHex, err := normalizeCompressedPubKeyHex(raw)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[pubHex]; ok {
			continue
		}
		seen[pubHex] = struct{}{}
		out = append(out, pubHex)
	}
	return out, nil
}
