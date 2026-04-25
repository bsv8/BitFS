package gatewayclient

import (
	"context"
	"fmt"
	"strings"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtoBroadcastV1ListenCycle              = protocol.ID(contractprotoid.ProtoBroadcastV1ListenCycle)
	ProtoBroadcastV1DemandPublish            = protocol.ID(contractprotoid.ProtoBroadcastV1DemandPublish)
	ProtoBroadcastV1DemandPublishBatch       = protocol.ID(contractprotoid.ProtoBroadcastV1DemandPublishBatch)
	ProtoBroadcastV1LiveDemandPublish        = protocol.ID(contractprotoid.ProtoBroadcastV1LiveDemandPublish)
	ProtoBroadcastV1NodeReachabilityAnnounce = protocol.ID(contractprotoid.ProtoBroadcastV1NodeReachabilityAnnounce)
	ProtoBroadcastV1NodeReachabilityQuery    = protocol.ID(contractprotoid.ProtoBroadcastV1NodeReachabilityQuery)
)

func (s *service) peerCall(ctx context.Context, to string, protoID protocol.ID, body []byte) (moduleapi.PeerCallResponse, error) {
	return s.callPeer(ctx, to, protoID, body)
}

func (s *service) quoteCall(ctx context.Context, to string, protoID protocol.ID, body []byte) (moduleapi.PeerCallResponse, error) {
	return s.quotePeer(ctx, to, protoID, body)
}

func (s *service) payQuotedCall(ctx context.Context, to string, protoID protocol.ID, body []byte, paymentScheme string, serviceQuote []byte) (moduleapi.PeerCallResponse, error) {
	return s.payQuotedPeer(ctx, to, protoID, body, paymentScheme, serviceQuote)
}

func marshalProto(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	pm, ok := msg.(oldproto.Message)
	if !ok {
		return nil, fmt.Errorf("message does not implement proto.Message")
	}
	return oldproto.Marshal(pm)
}

func unmarshalProto[T any](data []byte) (T, error) {
	var zero T
	if len(data) == 0 {
		return zero, nil
	}
	target := new(T)
	pm, ok := any(target).(oldproto.Message)
	if !ok {
		return zero, fmt.Errorf("type does not implement proto.Message")
	}
	if err := oldproto.Unmarshal(data, pm); err != nil {
		return zero, err
	}
	return *target, nil
}

func callRespToResult(resp moduleapi.PeerCallResponse) (*ncall.CallResp, error) {
	if resp.Code == "" && !resp.Ok {
		return nil, nil
	}
	return &ncall.CallResp{
		Ok:                   resp.Ok,
		Code:                 resp.Code,
		Message:              resp.Message,
		ContentType:          resp.ContentType,
		Body:                 append([]byte(nil), resp.Body...),
		PaymentSchemes:       toNCallPaymentOptions(resp.PaymentSchemes),
		PaymentReceiptScheme: strings.TrimSpace(resp.PaymentReceiptScheme),
		PaymentReceipt:       append([]byte(nil), resp.PaymentReceipt...),
		ServiceQuote:         append([]byte(nil), resp.ServiceQuote...),
		ServiceReceipt:       append([]byte(nil), resp.ServiceReceipt...),
	}, nil
}

func toNCallPaymentOptions(in []*moduleapi.PaymentOption) []*ncall.PaymentOption {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ncall.PaymentOption, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, &ncall.PaymentOption{
			Scheme:                   strings.TrimSpace(item.Scheme),
			PaymentDomain:            strings.TrimSpace(item.PaymentDomain),
			AmountSatoshi:            item.AmountSatoshi,
			Description:              strings.TrimSpace(item.Description),
			MinimumPoolAmountSatoshi: item.MinimumPoolAmountSatoshi,
			FeeRateSatPerByteMilli:   item.FeeRateSatPerByteMilli,
			LockBlocks:               item.LockBlocks,
			PricingMode:              strings.TrimSpace(item.PricingMode),
			ServiceQuantity:          item.ServiceQuantity,
			ServiceQuantityUnit:      strings.TrimSpace(item.ServiceQuantityUnit),
			QuoteStatus:              strings.TrimSpace(item.QuoteStatus),
		})
	}
	return out
}
