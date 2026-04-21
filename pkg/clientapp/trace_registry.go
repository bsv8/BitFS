package clientapp

import (
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"

	oldproto "github.com/golang/protobuf/proto"
)

// NewTraceProtoMessage 为调试工具提供 client 私有 protobuf 类型实例。
// 设计说明：
// - client 很多消息类型是包内私有结构，业务运行时不需要导出；
// - 逐层剥壳工具需要在包外解码这些 payload，因此通过受控工厂暴露。
func NewTraceProtoMessage(protoID string, kind string) (oldproto.Message, bool) {
	switch protoID {
	case string(ProtoSeedGet):
		if kind == "req" {
			return &seedGetReq{}, true
		}
		if kind == "resp" {
			return &seedGetResp{}, true
		}
	case string(ProtoQuoteDirectSubmit):
		if kind == "req" {
			return &directQuoteSubmitReq{}, true
		}
		if kind == "resp" {
			return &directQuoteSubmitResp{}, true
		}
	case string(ProtoLiveQuoteSubmit):
		if kind == "req" {
			return &liveQuoteSubmitReq{}, true
		}
		if kind == "resp" {
			return &liveQuoteSubmitResp{}, true
		}
	case string(ProtoDirectDealAccept):
		if kind == "req" {
			return &directDealAcceptReq{}, true
		}
		if kind == "resp" {
			return &directDealAcceptResp{}, true
		}
	case string(ProtoTransferPoolOpen):
		if kind == "req" {
			return &directTransferPoolOpenReq{}, true
		}
		if kind == "resp" {
			return &directTransferPoolOpenResp{}, true
		}
	case string(ProtoTransferChunkGet):
		if kind == "req" {
			return &directTransferChunkGetReq{}, true
		}
		if kind == "resp" {
			return &directTransferChunkGetResp{}, true
		}
	case string(ProtoTransferPoolPay):
		if kind == "req" {
			return &directTransferPoolPayReq{}, true
		}
		if kind == "resp" {
			return &directTransferPoolPayResp{}, true
		}
	case string(ProtoTransferArbitrate):
		if kind == "req" {
			return &directTransferArbitrateReq{}, true
		}
		if kind == "resp" {
			return &directTransferArbitrateResp{}, true
		}
	case string(ProtoTransferPoolClose):
		if kind == "req" {
			return &directTransferPoolCloseReq{}, true
		}
		if kind == "resp" {
			return &directTransferPoolCloseResp{}, true
		}
	case string(ProtoLiveSubscribe):
		if kind == "req" {
			return &liveSubscribeReq{}, true
		}
		if kind == "resp" {
			return &liveSubscribeResp{}, true
		}
	case string(ProtoLiveHeadPush):
		if kind == "req" {
			return &liveHeadPushReq{}, true
		}
		if kind == "resp" {
			return &liveHeadPushResp{}, true
		}
	case string(ncall.ProtoBroadcastV1NodeReachabilityQuery):
		if kind == "req" {
			return &contractmessage.NodeReachabilityQueryReq{}, true
		}
		if kind == "resp" {
			return &contractmessage.NodeReachabilityQueryPaidResp{}, true
		}
	}
	return nil, false
}
