package gatewayclient

import (
	"context"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
)

type LiveDemandPublishParams struct {
	StreamID         string
	HaveSegmentIndex int64
	Window           uint32
	GatewayPeerID    string
}

type LiveDemandPublishResult struct {
	GatewayPubkeyHex string
	Status           string
	Error            string
}

func (s *service) PublishLiveDemand(ctx context.Context, p LiveDemandPublishParams) (LiveDemandPublishResult, error) {
	if s == nil || s.host == nil {
		return LiveDemandPublishResult{}, nil
	}
	out := LiveDemandPublishResult{}

	gateway := s.host.PreferredGatewayPubkeyHex()
	if gateway == "" {
		out.Error = "no gateway available"
		return out, nil
	}
	out.GatewayPubkeyHex = gateway

	buyerAddrs := s.host.LocalAdvertiseAddrs()
	body := &contractmessage.LiveDemandPublishReq{
		StreamID:         strings.ToLower(strings.TrimSpace(p.StreamID)),
		HaveSegmentIndex: p.HaveSegmentIndex,
		Window:           p.Window,
		BuyerAddrs:       buyerAddrs,
	}
	bodyRaw, err := marshalProto(body)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	resp, err := s.peerCall(ctx, gateway, ProtoBroadcastV1LiveDemandPublish, bodyRaw)
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