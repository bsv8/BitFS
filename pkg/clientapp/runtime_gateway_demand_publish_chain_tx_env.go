package clientapp

import (
	"context"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// 这组方法只给 gateway demand publish 这条触发链用。
// 设计说明：
// - 触发入口不再拿整颗 Runtime；
// - Runtime 只负责把自己已有的能力薄薄转出去，业务链拿显式能力。
func (r *Runtime) HealthyGatewayInfos() []peer.AddrInfo {
	if r == nil || len(r.HealthyGWs) == 0 {
		return nil
	}
	out := make([]peer.AddrInfo, len(r.HealthyGWs))
	copy(out, r.HealthyGWs)
	return out
}

func (r *Runtime) PickGatewayForBusiness(gatewayPeerID string) (peer.AddrInfo, error) {
	return pickGatewayForBusiness(r, gatewayPeerID)
}

func (r *Runtime) GatewayBusinessID(pid peer.ID) string {
	return gatewayBusinessID(r, pid)
}

func (r *Runtime) LocalAdvertiseAddrs() []string {
	return localAdvertiseAddrs(r)
}

func (r *Runtime) CallNodeRoute(ctx context.Context, peerID peer.ID, req ncall.CallReq, protoID protocol.ID) (ncall.CallResp, error) {
	return callNodeRoute(ctx, r, peerID, protoID, req)
}

func (r *Runtime) CallGatewayRoute(ctx context.Context, peerID peer.ID, req ncall.CallReq, protoID protocol.ID) (ncall.CallResp, error) {
	return callGatewayRoute(ctx, r, peerID, protoID, req)
}

func (r *Runtime) RequestPeerCallChainTxQuote(ctx context.Context, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, protoID protocol.ID) (peerCallChainTxQuoteBuilt, error) {
	return requestPeerCallChainTxQuote(ctx, r, store, peerID, req, option, protoID)
}

func (r *Runtime) PayPeerCallWithChainTxQuote(ctx context.Context, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted peerCallChainTxQuoteBuilt, protoID protocol.ID) (ncall.CallResp, error) {
	return payPeerCallWithChainTxQuote(ctx, r, store, peerID, req, option, quoted, protoID)
}
