package moduleapi

import (
	"context"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/libp2p/go-libp2p/core/peer"

	poolcore "github.com/bsv8/BitFS/pkg/clientapp/poolcore"
)

// PaymentHost is the payment capability extension for Host interface.
// 设计说明：payment 模块需要访问 Actor（私钥）来签署交易，需要通过这个接口获取。
// SendProto 用于模块直接发送 proto 消息给 peer，不经过 TriggerPeerCall 的 payment 逻辑。
type PaymentHost interface {
	PeerCallRaw(ctx context.Context, peer string, protocolID string, request PeerCallRequest) (PeerCallResponse, error)
	QuoteService(ctx context.Context, peer string, protocolID string, request ServiceQuoteRequest) (ServiceQuoteResponse, error)
	PaymentFacade() PaymentFacade

	// Actor 返回客户端签名身份（包含私钥）。
	// 注意：只有 payment 模块才需要这个，其他模块不应使用。
	Actor() (*poolcore.Actor, error)

	// SendProto 直接发送 proto 消息给 peer，不经过 TriggerPeerCall 的 payment 逻辑。
	// 用于 PayQuotedService 内部发送已构建的交易给 gateway。
	SendProto(ctx context.Context, peer string, protocolID string, in, out any) error

	// WalletUTXOs 返回钱包中可用的 UTXO 列表。
	// 用于 chain_tx_v1 等需要构建链上交易的 payment 模块。
	WalletUTXOs(ctx context.Context) ([]poolcore.UTXO, error)

	// PoolInfo 获取指定 peer 的费用池信息。
	PoolInfo(ctx context.Context, peer string) (poolcore.InfoResp, error)

	// GetGatewayPubkey 获取指定 peer 的网关公钥。
	GetGatewayPubkey(peerID peer.ID) (*ec.PublicKey, error)
}

type ServiceQuoteRequest struct {
	ServiceType       string
	Target            string
	Route             string
	Body              []byte
	ContentType       string
	PaymentSatoshi    uint64
	PaymentScheme     string
	RequestedDuration uint64
}

type ServiceQuoteResponse struct {
	OK            bool
	Code          string
	Message       string
	Quote         []byte
	QuoteHash     string
	ExpiresAt     int64
	PaymentScheme string
}
