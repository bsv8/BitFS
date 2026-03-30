package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/libp2p/go-libp2p/core/peer"
)

type feePoolServiceQuoteArgs struct {
	Session              *feePoolSession
	GatewayPeerID        peer.ID
	ServiceDomain        string
	ServiceType          string
	Target               string
	ChargeReason         string
	ServiceParamsPayload []byte
	PricingMode          string
	ProposedPaymentSat   uint64
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
	if args.Session == nil || strings.TrimSpace(args.Session.SpendTxID) == "" {
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("fee pool session missing")
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
	offer := payflow.ServiceOffer{
		ServiceType:          strings.TrimSpace(args.ServiceType),
		ServiceNodePubkeyHex: strings.ToLower(hex.EncodeToString(rawGatewayPub)),
		ClientPubkeyHex:      strings.ToLower(strings.TrimSpace(rt.runIn.ClientID)),
		RequestParams:        append([]byte(nil), args.ServiceParamsPayload...),
		CreatedAtUnix:        time.Now().Unix(),
	}
	rawOffer, err := payflow.MarshalServiceOffer(offer)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	resp, err := callNodeServiceQuote(ctx, rt, args.GatewayPeerID, poolcore.ServiceQuoteReq{
		ClientID:             rt.runIn.ClientID,
		ServiceOffer:         rawOffer,
		ServiceParamsPayload: append([]byte(nil), args.ServiceParamsPayload...),
	})
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "service quote rejected"
		}
		return feePoolServiceQuoteBuilt{}, fmt.Errorf("service quote rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(resp.ServiceQuote, gatewayPub)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, offer, offer.ServiceNodePubkeyHex, rt.runIn.ClientID, args.ServiceType, args.ServiceParamsPayload, time.Now().Unix()); err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	chargeReason := strings.TrimSpace(args.ChargeReason)
	if chargeReason == "" {
		chargeReason = strings.TrimSpace(args.ServiceType)
	}
	nextSeq := args.Session.Sequence + 1
	nextServerAmount := args.Session.ServerAmount + quote.ChargeAmountSatoshi
	grantedDurationSeconds := uint32(0)
	grantedDeadlineUnix := int64(0)
	if strings.TrimSpace(args.ServiceType) == poolcore.QuoteServiceTypeListenCycle {
		duration, err := poolcore.ListenOfferBudgetToDurationSeconds(quote.ChargeAmountSatoshi, args.Session.SingleCycleFeeSatoshi, args.Session.BillingCycleSeconds)
		if err != nil {
			return feePoolServiceQuoteBuilt{}, err
		}
		grantedDurationSeconds = uint32(duration)
		grantedDeadlineUnix = time.Now().Unix() + int64(duration)
	}
	return feePoolServiceQuoteBuilt{
		GatewayPub:                 gatewayPub,
		QuoteStatus:                strings.TrimSpace(resp.Status),
		ServiceQuoteRaw:            append([]byte(nil), resp.ServiceQuote...),
		ServiceQuote:               quote,
		ServiceQuoteHash:           quoteHash,
		ChargeReason:               chargeReason,
		NextSequence:               nextSeq,
		NextServerAmount:           nextServerAmount,
		GrantedDurationSeconds:     grantedDurationSeconds,
		GrantedServiceDeadlineUnix: grantedDeadlineUnix,
	}, nil
}
