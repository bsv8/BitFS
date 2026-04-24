package clientapp

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/obs"
)

// NetworkInitDefaults 按网络定义“产品启动补齐阶段使用”的初始化值。
// 设计约束：
// - 为了让运维/调参有单一入口，这里对 test/main 都完整列字段（即使当前值相同）；
// - 业务入口在 product 模式下读取此处，test 模式不走这份默认表。
type NetworkInitDefaults struct {
	IndexBackend                 string
	IndexSQLitePath              string
	SellerFloorPriceSatPer64K    uint64
	SellerResaleDiscountBPS      uint64
	SellerLiveBaseMultiplier     uint64
	SellerLiveDecayPerMinuteBPS  uint64
	LiveBuyerTargetLagSegments   uint32
	LivePublishBroadcastWindow   uint32
	LivePublishIntervalSeconds   uint32
	ListenEnabled                bool
	ListenRenewThresholdSeconds  uint32
	ListenAutoRenewRounds        uint64
	ListenTickSeconds            uint32
	ScanRescanIntervalSeconds    uint32
	StorageMinFreeBytes          uint64
	HTTPListenAddr               string
	FSHTTPListenAddr             string
	FSHTTPDownloadWaitSeconds    uint32
	FSHTTPMaxConcurrentSessions  uint32
	FSHTTPQuoteWaitSeconds       uint32
	FSHTTPQuotePollSeconds       uint32
	FSHTTPPrefetchDistanceChunks uint32
	LogConsoleMinLevel           string
	DefaultGateways              []InitPeerNode
	DefaultArbiters              []InitPeerNode
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
		IndexBackend:                 "sqlite",
		IndexSQLitePath:              defaultIndexRelPath,
		SellerFloorPriceSatPer64K:    10,
		SellerResaleDiscountBPS:      8000,
		SellerLiveBaseMultiplier:     4,
		SellerLiveDecayPerMinuteBPS:  1000,
		LiveBuyerTargetLagSegments:   3,
		LivePublishBroadcastWindow:   10,
		LivePublishIntervalSeconds:   3,
		ListenEnabled:                true,
		ListenRenewThresholdSeconds:  5,
		ListenAutoRenewRounds:        5,
		ListenTickSeconds:            1,
		ScanRescanIntervalSeconds:    300,
		StorageMinFreeBytes:          128 * 1024 * 1024,
		HTTPListenAddr:               "127.0.0.1:18080",
		FSHTTPListenAddr:             "127.0.0.1:18090",
		FSHTTPDownloadWaitSeconds:    60,
		FSHTTPMaxConcurrentSessions:  4,
		FSHTTPQuoteWaitSeconds:       60,
		FSHTTPQuotePollSeconds:       2,
		FSHTTPPrefetchDistanceChunks: 8,
		LogConsoleMinLevel:           obs.LevelNone,
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
		IndexBackend:                 "sqlite",
		IndexSQLitePath:              defaultIndexRelPath,
		SellerFloorPriceSatPer64K:    10,
		SellerResaleDiscountBPS:      8000,
		SellerLiveBaseMultiplier:     4,
		SellerLiveDecayPerMinuteBPS:  1000,
		LiveBuyerTargetLagSegments:   3,
		LivePublishBroadcastWindow:   10,
		LivePublishIntervalSeconds:   3,
		ListenEnabled:                true,
		ListenRenewThresholdSeconds:  1800,
		ListenAutoRenewRounds:        5,
		ListenTickSeconds:            30,
		ScanRescanIntervalSeconds:    300,
		StorageMinFreeBytes:          128 * 1024 * 1024,
		HTTPListenAddr:               "127.0.0.1:18080",
		FSHTTPListenAddr:             "127.0.0.1:18090",
		FSHTTPDownloadWaitSeconds:    60,
		FSHTTPMaxConcurrentSessions:  4,
		FSHTTPQuoteWaitSeconds:       60,
		FSHTTPQuotePollSeconds:       2,
		FSHTTPPrefetchDistanceChunks: 8,
		LogConsoleMinLevel:           obs.LevelNone,
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

var (
	errCannotModifyBuiltInGateway  = errors.New("cannot modify built-in gateway")
	errCannotDeleteBuiltInGateway  = errors.New("cannot delete built-in gateway")
	errCannotDisableBuiltInGateway = errors.New("cannot disable built-in gateway")
	errCannotModifyBuiltInArbiter  = errors.New("cannot modify built-in arbiter")
	errCannotDeleteBuiltInArbiter  = errors.New("cannot delete built-in arbiter")
	errCannotDisableBuiltInArbiter = errors.New("cannot disable built-in arbiter")
)

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

func mandatoryPeerPubkeySet(defaults []InitPeerNode) map[string]struct{} {
	out := make(map[string]struct{}, len(defaults))
	for _, it := range defaults {
		pub := strings.ToLower(strings.TrimSpace(it.Pubkey))
		if pub == "" {
			continue
		}
		out[pub] = struct{}{}
	}
	return out
}

func isMandatoryPeer(pubkey string, mandatorySet map[string]struct{}) bool {
	pub := strings.ToLower(strings.TrimSpace(pubkey))
	if pub == "" || len(mandatorySet) == 0 {
		return false
	}
	_, ok := mandatorySet[pub]
	return ok
}

func mandatoryGatewayPubkeySet(network string) (map[string]struct{}, error) {
	defaults, err := networkInitDefaults(network)
	if err != nil {
		return nil, err
	}
	return mandatoryPeerPubkeySet(defaults.DefaultGateways), nil
}

func mandatoryArbiterPubkeySet(network string) (map[string]struct{}, error) {
	defaults, err := networkInitDefaults(network)
	if err != nil {
		return nil, err
	}
	return mandatoryPeerPubkeySet(defaults.DefaultArbiters), nil
}

// mergeMandatoryPeerNodes 保证代码内置节点始终在前、始终存在、始终按内置值生效。
// 设计说明：
// - 内置节点不可删改禁，因此这里直接以代码内置值为准；
// - 文件里若出现同 pubkey 条目，视为“用户尝试覆盖”，会被忽略；
// - 仅把非内置节点作为用户新增项追加到后面。
func mergeMandatoryPeerNodes(existing []PeerNode, defaults []InitPeerNode) []PeerNode {
	mandatorySet := mandatoryPeerPubkeySet(defaults)
	out := initPeerNodesToPeerNodes(defaults)
	for _, it := range existing {
		if isMandatoryPeer(it.Pubkey, mandatorySet) {
			continue
		}
		out = append(out, it)
	}
	return out
}

// stripMandatoryPeerNodesForFile 只保留“用户新增节点”，用于差异配置文件落盘。
func stripMandatoryPeerNodesForFile(existing []PeerNode, defaults []InitPeerNode) []PeerNode {
	mandatorySet := mandatoryPeerPubkeySet(defaults)
	out := make([]PeerNode, 0, len(existing))
	for _, it := range existing {
		if isMandatoryPeer(it.Pubkey, mandatorySet) {
			continue
		}
		out = append(out, it)
	}
	return out
}

func warnMandatoryPeerOverrides(kind string, existing []PeerNode, defaults []InitPeerNode) {
	mandatorySet := mandatoryPeerPubkeySet(defaults)
	defaultByPub := make(map[string]InitPeerNode, len(defaults))
	for _, def := range defaults {
		pub := strings.ToLower(strings.TrimSpace(def.Pubkey))
		if pub == "" {
			continue
		}
		defaultByPub[pub] = def
	}
	seen := make(map[string]struct{}, len(existing))
	overridden := make([]string, 0, len(existing))
	for _, it := range existing {
		pub := strings.ToLower(strings.TrimSpace(it.Pubkey))
		if pub == "" {
			continue
		}
		if !isMandatoryPeer(pub, mandatorySet) {
			continue
		}
		if _, ok := seen[pub]; ok {
			overridden = append(overridden, pub)
			continue
		}
		seen[pub] = struct{}{}
		def, ok := defaultByPub[pub]
		if !ok {
			overridden = append(overridden, pub)
			continue
		}
		if strings.TrimSpace(it.Addr) != strings.TrimSpace(def.Addr) || !it.Enabled {
			overridden = append(overridden, pub)
		}
	}
	if len(overridden) == 0 {
		return
	}
	obs.Important(ServiceName, "mandatory_peer_override_ignored", map[string]any{
		"kind":    kind,
		"count":   len(overridden),
		"pubkeys": overridden,
	})
}
