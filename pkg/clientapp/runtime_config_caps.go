package clientapp

import "context"

// 运行期配置能力按最小读写面拆开，避免业务层再拿全量配置对象到处传。
// 设计原则：
// - 读取只给快照，不给可变引用；
// - 写入只给单点入口，不允许散落保存文件；
// - 业务模块按自己需要的字段拿能力，不要顺手把整份 Config 传来传去。

type GatewayConfigReader interface {
	GatewaySnapshot() []PeerNode
}

type GatewayConfigWriter interface {
	AddGateway(ctx context.Context, node PeerNode) (int, error)
	UpdateGateway(ctx context.Context, index int, node PeerNode) error
	DeleteGateway(ctx context.Context, index int) error
	RefreshGatewayConnections(ctx context.Context, h hostConnector)
}

type ArbiterConfigWriter interface {
	AddArbiter(ctx context.Context, node PeerNode) (int, error)
	UpdateArbiter(ctx context.Context, index int, node PeerNode) error
	DeleteArbiter(ctx context.Context, index int) error
}

type ListenPolicyReader interface {
	ListenEnabled() bool
	ListenRenewThresholdSeconds() uint32
	ListenAutoRenewRounds() uint64
	ListenOfferPaymentSatoshi() uint64
	ListenTickSeconds() uint32
}

type LivePolicyReader interface {
	LiveCacheMaxBytes() uint64
	LiveBuyerTargetLagSegments() uint32
	LiveBuyerMaxBudgetPerMinute() uint64
	LiveBuyerPreferOlderSegments() bool
	LivePublishBroadcastWindow() uint32
	LivePublishBroadcastIntervalSec() uint32
	LiveBuyerPricing() LiveBuyerStrategy
	LiveSellerPricing() LiveSellerPricing
}

type WOCPolicyReader interface {
	WOCAPIKey() string
	WOCMinIntervalMS() uint32
}

type PaymentPolicyReader interface {
	PreferredPaymentScheme() string
}

type ReachabilityPolicyReader interface {
	ReachabilityAutoAnnounceEnabled() bool
	ReachabilityAnnounceTTLSeconds() uint32
}

// configSnapshotter 只提供运行期快照读取，不暴露可变配置对象。
type configSnapshotter interface {
	ConfigSnapshot() Config
}

// Config 也实现快照接口，方便测试夹具直接把临时配置作为只读源传入。
// 运行时主路径仍然优先传运行期配置服务，不把可变配置指针挂到运行对象里。
func (c Config) ConfigSnapshot() Config {
	return cloneConfig(c)
}

// staticConfigSnapshot 是测试和启动装配时用的只读快照源。
type staticConfigSnapshot Config

func (s staticConfigSnapshot) ConfigSnapshot() Config {
	return cloneConfig(Config(s))
}

// hostConnector 只暴露 gateway 连接所需的最小面，避免把完整 host 句柄塞进配置服务。
type hostConnector interface {
	Connect(context.Context, PeerNode) error
}
