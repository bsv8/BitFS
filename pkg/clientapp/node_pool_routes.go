package clientapp

import (
	"context"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// callNodePoolProto 直连费用池独立协议，不再套 ncall 外壳。
// 说明：网关侧 ProtoPoolV1* handler 现在直接收发 poolcore.* 消息；
// 如果这里继续用 CallReq/CallResp 包一层，客户端会把真实响应误解成空 body。
func callNodePoolProto[TReq any, TResp any](ctx context.Context, rt *Runtime, peerID peer.ID, protoID protocol.ID, req TReq) (TResp, error) {
	var out TResp
	if err := pproto.CallProto(ctx, rt.Host, peerID, protoID, nodeSecForRuntime(rt), req, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolInfo(ctx context.Context, rt *Runtime, peerID peer.ID) (poolcore.InfoResp, error) {
	return callNodePoolProto[poolcore.InfoReq, poolcore.InfoResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1Info, poolcore.InfoReq{})
}

func callNodePoolCreate(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CreateReq) (poolcore.CreateResp, error) {
	req.ClientID = ""
	return callNodePoolProto[poolcore.CreateReq, poolcore.CreateResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1Create, req)
}

func callNodePoolBaseTx(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.BaseTxReq) (poolcore.BaseTxResp, error) {
	req.ClientID = ""
	return callNodePoolProto[poolcore.BaseTxReq, poolcore.BaseTxResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1BaseTx, req)
}

func callNodePoolPayConfirm(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.PayConfirmReq) (poolcore.PayConfirmResp, error) {
	req.ClientID = ""
	return callNodePoolProto[poolcore.PayConfirmReq, poolcore.PayConfirmResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1PayConfirm, req)
}

func callNodePoolClose(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CloseReq) (poolcore.CloseResp, error) {
	req.ClientID = ""
	return callNodePoolProto[poolcore.CloseReq, poolcore.CloseResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1Close, req)
}

func callNodePoolSessionState(ctx context.Context, rt *Runtime, peerID peer.ID, spendTxID string) (poolcore.StateResp, error) {
	req := poolcore.StateReq{
		SpendTxID: spendTxID,
	}
	return callNodePoolProto[poolcore.StateReq, poolcore.StateResp](ctx, rt, peerID, contractprotoid.ProtoPoolV1SessionState, req)
}
