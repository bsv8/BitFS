package poolcore

import (
	contractid "github.com/bsv8/BFTP-contract/pkg/v1/id"
)

// Libp2pMarshalPubHexToSecpCompressedHex 兼容两种输入并统一输出 secp256k1 压缩公钥(hex，33字节)：
// - libp2p MarshalPublicKey(hex)
// - secp256k1 压缩公钥(hex)
// 这是把 pproto 的 sender_pubkey 语义，桥接到 KeymasterMultisigPool 的 BSV 公钥语义上。
func Libp2pMarshalPubHexToSecpCompressedHex(marshalPubHex string) (string, error) {
	return contractid.Libp2pMarshalPubHexToSecpCompressedHex(marshalPubHex)
}

// NormalizeClientIDStrict 统一 client_pubkey_hex 到 secp256k1 压缩公钥 hex（小写）。
// 说明：输入支持历史 marshal 格式与标准 compressed 格式；非法输入返回错误。
func NormalizeClientIDStrict(clientID string) (string, error) {
	return contractid.NormalizeClientIDStrict(clientID)
}

// NormalizeClientIDLoose 尝试规范化 client_pubkey_hex；无法解析时仅做 lower+trim。
// 说明：该函数用于历史数据迁移与兼容查询，避免旧测试夹具（如 client_a）被硬失败。
func NormalizeClientIDLoose(clientID string) string {
	return contractid.NormalizeClientIDLoose(clientID)
}

// ClientIDAliasesForQuery 返回查询别名集合（canonical + legacy marshal + 输入本身）。
// 说明：用于过渡期兼容旧数据，迁移完成后理论上只命中 canonical。
func ClientIDAliasesForQuery(clientID string) []string {
	return contractid.ClientIDAliasesForQuery(clientID)
}

// MarshalClientIDHexFromCanonical 将 canonical compressed pubkey 还原为历史 marshal hex。
func MarshalClientIDHexFromCanonical(canonicalHex string) (string, error) {
	return contractid.MarshalClientIDHexFromCanonical(canonicalHex)
}
