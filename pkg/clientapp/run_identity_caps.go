package clientapp

import (
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// clientIdentityCaps 是运行时启动后一次性装配好的身份能力。
// 设计约束：
// - 只读缓存，不允许在运行中改写；
// - 业务层只拿这个能力，不再直接读 runIn；
// - 这里同时保留公钥、是否主网和签名 actor，避免业务侧自己重复拼装。
type clientIdentityCaps struct {
	ClientID      string
	ClientIDLower string
	IsMainnet     bool
	Actor         *poolcore.Actor
}

// buildClientIdentityCaps 从启动输入里构建客户端身份能力。
// 说明：
// - 私钥必须合法；
// - client_pubkey_hex 与私钥不一致时直接失败；
// - network 只在启动时读取一次，后续不可变。
func buildClientIdentityCaps(in RunInput) (*clientIdentityCaps, error) {
	privHex, err := normalizeRawSecp256k1PrivKeyHex(in.EffectivePrivKeyHex)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(privHex) == "" {
		return nil, fmt.Errorf("effective private key is required")
	}
	isMainnet := strings.EqualFold(strings.TrimSpace(in.BSV.Network), "main")
	actor, err := poolcore.BuildActor("client", privHex, isMainnet)
	if err != nil {
		return nil, err
	}
	clientID, err := normalizeCompressedPubKeyHex(strings.TrimSpace(actor.PubHex))
	if err != nil {
		return nil, err
	}
	configuredClientID := strings.ToLower(strings.TrimSpace(in.ClientID))
	if configuredClientID != "" && !strings.EqualFold(configuredClientID, clientID) {
		return nil, fmt.Errorf("client_pubkey_hex and signing key mismatch")
	}
	return &clientIdentityCaps{
		ClientID:      clientID,
		ClientIDLower: clientID,
		IsMainnet:     isMainnet,
		Actor:         actor,
	}, nil
}
