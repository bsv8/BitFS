package gatewayclient

import (
	"context"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
)

type DemandPublishBatchParams struct {
	Items        []*DemandPublishBatchPaidItem
	GatewayPeerID string
}

// DemandPublishBatchResult 是 batch demand publish 的结果。
type DemandPublishBatchResult struct {
	GatewayPubkeyHex string
	QuoteChargeSatoshi uint64
	PaymentTxID      string
	DemandIDs        []string
	Status           string
	Error            string
}

// PublishDemandBatch 批量发布需求到 gateway。
func (s *service) PublishDemandBatch(ctx context.Context, p DemandPublishBatchParams) (DemandPublishBatchResult, error) {
	if s == nil || s.host == nil {
		return DemandPublishBatchResult{}, nil
	}
	out := DemandPublishBatchResult{}

	gateway := s.host.PreferredGatewayPubkeyHex()
	if gateway == "" {
		out.Error = "no gateway available"
		return out, nil
	}
	out.GatewayPubkeyHex = gateway

	buyerAddrs := s.host.LocalAdvertiseAddrs()
	// TODO: 修复类型转换 - 需要将 []*DemandPublishBatchPaidItem 转换为 []*message.DemandPublishBatchPaidItem
	body := &contractmessage.DemandPublishBatchReq{
		BuyerAddrs: buyerAddrs,
	}
	bodyRaw, err := marshalProto(body)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	resp, err := s.peerCall(ctx, gateway, ProtoBroadcastV1DemandPublishBatch, bodyRaw)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	result, err := callRespToResult(resp)
	if err != nil || result == nil {
		out.Error = "gateway call failed"
		return out, nil
	}

	return out, nil
}