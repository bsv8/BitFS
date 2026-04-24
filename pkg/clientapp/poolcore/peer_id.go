package poolcore

import (
	"encoding/hex"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerIDFromClientID 把系统内唯一 ID（压缩公钥 hex）映射到 libp2p transport 语境里的 peer.ID。
// 设计约束：peer.ID 只用于 libp2p 内部连接，不进入系统业务层主语义。
func PeerIDFromClientID(clientID string) (peer.ID, error) {
	pubHex, err := NormalizeClientIDStrict(clientID)
	if err != nil {
		return "", err
	}
	b, err := hex.DecodeString(pubHex)
	if err != nil {
		return "", fmt.Errorf("decode client_pubkey_hex: %w", err)
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(b)
	if err != nil {
		return "", fmt.Errorf("unmarshal client pubkey: %w", err)
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("derive peer id: %w", err)
	}
	return pid, nil
}
