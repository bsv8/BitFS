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
	converted, convErr := dual2of2.Libp2pMarshalPubHexToSecpCompressedHex(strings.TrimSpace(in))
	if convErr != nil {
		return "", err
	}
	return normalizeCompressedPubKeyHex(converted)
}
