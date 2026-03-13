package clientapp

import (
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/obs"
)

// NetworkInitDefaults 按网络定义“默认补齐阶段使用”的初始化值。
// 设计约束：
// - 为了让运维/调参有单一入口，这里对 test/main 都完整列字段（即使当前值相同）；
// - 业务入口（首启初始化、配置补齐）统一读取此处，避免多处散落常量。
type NetworkInitDefaults struct {
	IndexBackend                string
	IndexSQLitePath             string
	SellerFloorPriceSatPer64K   uint64
	SellerResaleDiscountBPS     uint64
	SellerLiveBaseMultiplier    uint64
	SellerLiveDecayPerMinuteBPS uint64
	LiveBuyerTargetLagSegments  uint32
	LivePublishBroadcastWindow  uint32
	LivePublishIntervalSeconds  uint32
	ListenEnabled               bool
	ListenRenewThresholdSeconds uint32
	ListenAutoRenewRounds       uint64
	ListenTickSeconds           uint32
	ScanRescanIntervalSeconds   uint32
	StorageMinFreeBytes         uint64
	HTTPListenAddr              string
	FSHTTPListenAddr            string
	FSHTTPDownloadWaitSeconds   uint32
	FSHTTPMaxConcurrentSessions uint32
	FSHTTPQuoteWaitSeconds      uint32
	FSHTTPQuotePollSeconds      uint32
	FSHTTPPrefetchDistanceChunks uint32
	LogConsoleMinLevel          string
	DefaultGateways            []InitPeerNode
	DefaultArbiters            []InitPeerNode
}

// InitPeerNode 表示“初始化默认表”的节点模板。
// 说明：
// - 不包含 enabled 字段；写在这里的节点默认都会启用；
// - note 仅用于人类阅读，不参与业务逻辑与校验。
type InitPeerNode struct {
	Addr   string
	Pubkey string
	Note   string
}

var networkInitDefaultsByNetwork = map[string]NetworkInitDefaults{
	"test": {
		IndexBackend:                "sqlite",
		IndexSQLitePath:             defaultIndexRelPath,
		SellerFloorPriceSatPer64K:   10,
		SellerResaleDiscountBPS:     8000,
		SellerLiveBaseMultiplier:    4,
		SellerLiveDecayPerMinuteBPS: 1000,
		LiveBuyerTargetLagSegments:  3,
		LivePublishBroadcastWindow:  10,
		LivePublishIntervalSeconds:  3,
		ListenEnabled:               true,
		ListenRenewThresholdSeconds: 5,
		ListenAutoRenewRounds:       5,
		ListenTickSeconds:           1,
		ScanRescanIntervalSeconds:   300,
		StorageMinFreeBytes:         128 * 1024 * 1024,
		HTTPListenAddr:              "127.0.0.1:18080",
		FSHTTPListenAddr:            "127.0.0.1:18090",
		FSHTTPDownloadWaitSeconds:   60,
		FSHTTPMaxConcurrentSessions: 4,
		FSHTTPQuoteWaitSeconds:      60,
		FSHTTPQuotePollSeconds:      2,
		FSHTTPPrefetchDistanceChunks: 8,
		LogConsoleMinLevel:          obs.LevelNone,
		// default_gateways:
		// - addr:   libp2p multiaddr，必须包含 peer id（示例：/ip4/127.0.0.1/tcp/7001/p2p/12D3...）
		// - pubkey: 节点 secp256k1 公钥 hex，必须与 addr 中 peer id 一一对应
		// - note:   备注（仅人类可读）
		DefaultGateways: []InitPeerNode{
			{
				Addr:   "/ip4/127.0.0.1/tcp/19090/p2p/16Uiu2HAkvGTqivBc48yDA8fBLywxETzhfcb6b533nVcUK1wkSiAR",
				Pubkey: "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e",
				Note:   "test gateway",
			},
		},
		// default_arbiters:
		// - addr:   libp2p multiaddr，必须包含 peer id
		// - pubkey: 节点 secp256k1 公钥 hex，必须与 addr 中 peer id 一一对应
		// - note:   备注（仅人类可读）
		DefaultArbiters: []InitPeerNode{
			{
				Addr:   "/ip4/127.0.0.1/tcp/19091/p2p/16Uiu2HAmRJafBEXEL5JjpstQ2LACZv7ki9EC3hv24Dmasfh4Ro1D",
				Pubkey: "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4",
				Note:   "test arbiter",
			},
		},
	},
	"main": {
		IndexBackend:                "sqlite",
		IndexSQLitePath:             defaultIndexRelPath,
		SellerFloorPriceSatPer64K:   10,
		SellerResaleDiscountBPS:     8000,
		SellerLiveBaseMultiplier:    4,
		SellerLiveDecayPerMinuteBPS: 1000,
		LiveBuyerTargetLagSegments:  3,
		LivePublishBroadcastWindow:  10,
		LivePublishIntervalSeconds:  3,
		ListenEnabled:               true,
		ListenRenewThresholdSeconds: 1800,
		ListenAutoRenewRounds:       5,
		ListenTickSeconds:           30,
		ScanRescanIntervalSeconds:   300,
		StorageMinFreeBytes:         128 * 1024 * 1024,
		HTTPListenAddr:              "127.0.0.1:18080",
		FSHTTPListenAddr:            "127.0.0.1:18090",
		FSHTTPDownloadWaitSeconds:   60,
		FSHTTPMaxConcurrentSessions: 4,
		FSHTTPQuoteWaitSeconds:      60,
		FSHTTPQuotePollSeconds:      2,
		FSHTTPPrefetchDistanceChunks: 8,
		LogConsoleMinLevel:          obs.LevelNone,
		// default_gateways:
		// - addr:   libp2p multiaddr，必须包含 peer id（示例：/ip4/127.0.0.1/tcp/7001/p2p/12D3...）
		// - pubkey: 节点 secp256k1 公钥 hex，必须与 addr 中 peer id 一一对应
		// - note:   备注（仅人类可读）
		DefaultGateways: []InitPeerNode{
			{
				Addr:   "/ip4/127.0.0.1/tcp/19090/p2p/16Uiu2HAkvGTqivBc48yDA8fBLywxETzhfcb6b533nVcUK1wkSiAR",
				Pubkey: "020c7fbbdf69c2bce8431a4fbc8e89ded25fa6bc524eb5988aa7da05923dcaea3e",
				Note:   "main gateway（暂时复用 test）",
			},
		},
		// default_arbiters:
		// - addr:   libp2p multiaddr，必须包含 peer id
		// - pubkey: 节点 secp256k1 公钥 hex，必须与 addr 中 peer id 一一对应
		// - note:   备注（仅人类可读）
		DefaultArbiters: []InitPeerNode{
			{
				Addr:   "/ip4/127.0.0.1/tcp/19091/p2p/16Uiu2HAmRJafBEXEL5JjpstQ2LACZv7ki9EC3hv24Dmasfh4Ro1D",
				Pubkey: "03bbed86936b5b8157dcc5ce9d1cef2be7e0a1185b6e17e3b020a4e413110143f4",
				Note:   "main arbiter（暂时复用 test）",
			},
		},
	},
}

// NormalizeBSVNetwork 归一化 bsv.network 输入，仅支持 test/main。
func NormalizeBSVNetwork(raw string) (string, error) {
	n := strings.ToLower(strings.TrimSpace(raw))
	switch n {
	case "", "testnet":
		n = "test"
	case "mainnet":
		n = "main"
	}
	if n != "test" && n != "main" {
		return "", fmt.Errorf("bsv.network must be test or main")
	}
	return n, nil
}

func networkInitDefaults(network string) (NetworkInitDefaults, error) {
	n, err := NormalizeBSVNetwork(network)
	if err != nil {
		return NetworkInitDefaults{}, err
	}
	v, ok := networkInitDefaultsByNetwork[n]
	if !ok {
		return NetworkInitDefaults{}, fmt.Errorf("unsupported network defaults: %s", n)
	}
	return v, nil
}

func initPeerNodesToPeerNodes(in []InitPeerNode) []PeerNode {
	if len(in) == 0 {
		return []PeerNode{}
	}
	out := make([]PeerNode, 0, len(in))
	for _, it := range in {
		out = append(out, PeerNode{
			Enabled: true,
			Addr:    strings.TrimSpace(it.Addr),
			Pubkey:  strings.TrimSpace(it.Pubkey),
		})
	}
	return out
}
