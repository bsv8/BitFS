package ncall

import (
	"context"

	"github.com/bsv8/BitFS/pkg/clientapp/infra/pproto"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/host"
)

type CallContext struct {
	SenderPubkeyHex string
	MessageID       string
}

type CallHandler func(context.Context, CallContext, CallReq) (CallResp, error)

// RegisterCapabilitiesShow 在任意节点上挂载能力发现协议。
// 设计说明：
// - 硬切后不再使用 node.call + route 分发模型；
// - 能力发现改用独立协议 /bsv-transfer/capabilities/show/1.0.0；
// - 各模块不再通过 route 分发，直接注册独立 protocol.ID handler。
func RegisterCapabilitiesShow(h host.Host, sec pproto.SecurityConfig, showHandler func() CapabilitiesShowBody) {
	if h == nil {
		return
	}
	pproto.HandleProto[CallReq, CallResp](h, protocol.ID(ProtoCapabilitiesShow), sec, func(ctx context.Context, req CallReq) (CallResp, error) {
		senderPubkeyHex, ok := pproto.SenderPubkeyHexFromContext(ctx)
		if !ok {
			return CallResp{Ok: false, Code: "UNAUTHORIZED", Message: "sender identity missing"}, nil
		}
		messageID, ok := pproto.MessageIDFromContext(ctx)
		if !ok {
			return CallResp{Ok: false, Code: "BAD_REQUEST", Message: "message id missing"}, nil
		}
		_ = senderPubkeyHex
		_ = messageID
		if showHandler == nil {
			return CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
		body := showHandler()
		return MarshalProto(&body)
	})
}

// RegisterCapabilitiesShow 在任意节点上挂载能力发现协议。


