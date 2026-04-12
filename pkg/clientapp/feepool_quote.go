package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	"github.com/libp2p/go-libp2p/core/peer"
)

type feePoolServiceQuoteArgs struct {
	Session       *feePoolSession
	GatewayPeerID peer.ID
	Route         string
	ContentType   string
	Body          []byte
}

type feePoolServiceQuoteBuilt struct {
	GatewayPub                 *ec.PublicKey
	QuoteStatus                string
	ServiceQuoteRaw            []byte
	ServiceQuote               payflow.ServiceQuote
	ServiceQuoteHash           string
	ChargeReason               string
	NextSequence               uint32
	NextServerAmount           uint64
	GrantedServiceDeadlineUnix int64
	GrantedDurationSeconds     uint32
}

func requestGatewayServiceQuote(ctx context.Context, rt *Runtime, args feePoolServiceQuoteArgs) (feePoolServiceQuoteBuilt, error) {
	if rt == nil || rt.Host == nil {
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("runtime not initialized")
	}
	quotedResp, err := callNodeRoute(ctx, rt, args.GatewayPeerID, ncall.CallReq{
		To:          gatewayBusinessID(rt, args.GatewayPeerID),
		Route:       strings.TrimSpace(args.Route),
		ContentType: strings.TrimSpace(args.ContentType),
		Body:        append([]byte(nil), args.Body...),
	})
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	if strings.TrimSpace(quotedResp.Code) != "PAYMENT_QUOTED" {
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("payment quote unavailable: code=%s message=%s", strings.TrimSpace(quotedResp.Code), strings.TrimSpace(quotedResp.Message))
	}
	if len(quotedResp.PaymentSchemes) == 0 || quotedResp.PaymentSchemes[0] == nil {
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("payment schemes missing")
	}
	gatewayPubKey := rt.Host.Peerstore().PubKey(args.GatewayPeerID)
	if gatewayPubKey == nil {
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("missing gateway pubkey")
	}
	rawGatewayPub, err := gatewayPubKey.Raw()
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	gatewayPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(rawGatewayPub)))
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	serviceParamsPayload, err := buildPeerCallServiceParamsPayload(strings.TrimSpace(args.Route), args.Body)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	offer := payflow.ServiceOffer{
		ServiceType:          peerCallQuotedServiceType(args.Route),
		ServiceNodePubkeyHex: strings.ToLower(hex.EncodeToString(rawGatewayPub)),
		ClientPubkeyHex:      strings.ToLower(strings.TrimSpace(rt.runIn.ClientID)),
		RequestParams:        append([]byte(nil), serviceParamsPayload...),
		CreatedAtUnix:        time.Now().Unix(),
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(quotedResp.ServiceQuote, gatewayPub)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, offer, offer.ServiceNodePubkeyHex, rt.runIn.ClientID, peerCallQuotedServiceType(args.Route), serviceParamsPayload, time.Now().Unix()); err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	chargeReason := strings.TrimSpace(quotedResp.PaymentSchemes[0].Description)
	if chargeReason == "" {
		chargeReason = strings.TrimSpace(args.Route)
	}
	nextSeq := uint32(0)
	nextServerAmount := uint64(0)
	if args.Session != nil {
		nextSeq = args.Session.Sequence + 1
		nextServerAmount = args.Session.ServerAmount + quote.ChargeAmountSatoshi
	}
	grantedDurationSeconds := uint32(0)
	grantedDeadlineUnix := int64(0)
	if strings.TrimSpace(args.Route) == broadcastmodule.RouteBroadcastV1ListenCycle {
		if args.Session == nil {
			return feePoolServiceQuoteBuilt{}, fmt.Errorf("fee pool session missing")
		}
		duration, err := poolcore.ListenOfferBudgetToDurationSeconds(quote.ChargeAmountSatoshi, args.Session.SingleCycleFeeSatoshi, args.Session.BillingCycleSeconds)
		if err != nil {
			return feePoolServiceQuoteBuilt{}, err
		}
		grantedDurationSeconds = uint32(duration)
		grantedDeadlineUnix = time.Now().Unix() + int64(duration)
	}
	return feePoolServiceQuoteBuilt{
		GatewayPub:                 gatewayPub,
		QuoteStatus:                strings.TrimSpace(quotedResp.PaymentSchemes[0].QuoteStatus),
		ServiceQuoteRaw:            append([]byte(nil), quotedResp.ServiceQuote...),
		ServiceQuote:               quote,
		ServiceQuoteHash:           quoteHash,
		ChargeReason:               chargeReason,
		NextSequence:               nextSeq,
		NextServerAmount:           nextServerAmount,
		GrantedDurationSeconds:     grantedDurationSeconds,
		GrantedServiceDeadlineUnix: grantedDeadlineUnix,
	}, nil
}
