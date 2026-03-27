package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/libp2p/go-libp2p/core/peer"
)

type feePoolServiceQuoteArgs struct {
	Session              *feePoolSession
	GatewayPeerID        peer.ID
	ServiceDomain        string
	ServiceType          string
	Target               string
	ServiceParamsPayload []byte
	PricingMode          string
	ProposedPaymentSat   uint64
}

type feePoolServiceQuoteBuilt struct {
	GatewayPub       *ec.PublicKey
	QuoteStatus      string
	ServiceQuoteRaw  []byte
	ServiceQuote     proof.ServiceQuote
	ServiceQuoteHash string
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
	offer := proof.ServiceOffer{
		Domain:                 normalizeFeePoolServiceDomain(args.ServiceDomain),
		ServiceType:            strings.TrimSpace(args.ServiceType),
		Target:                 strings.TrimSpace(args.Target),
		GatewayPubkeyHex:       strings.ToLower(hex.EncodeToString(rawGatewayPub)),
		ClientPubkeyHex:        strings.ToLower(strings.TrimSpace(rt.runIn.ClientID)),
		SpendTxID:              strings.TrimSpace(args.Session.SpendTxID),
		ServiceParamsHash:      dual2of2.HashServiceParamsPayload(args.ServiceParamsPayload),
		PricingMode:            strings.TrimSpace(args.PricingMode),
		ProposedPaymentSatoshi: args.ProposedPaymentSat,
		CreatedAtUnix:          time.Now().Unix(),
	}
	rawOffer, err := proof.MarshalServiceOffer(offer)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	resp, err := callNodePoolServiceQuote(ctx, rt, args.GatewayPeerID, dual2of2.ServiceQuoteReq{
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
	quote, quoteHash, err := dual2of2.ParseAndVerifyServiceQuote(resp.ServiceQuote, gatewayPub)
	if err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	if err := dual2of2.ValidateServiceQuoteBinding(quote, offer.GatewayPubkeyHex, rt.runIn.ClientID, args.Session.SpendTxID, args.ServiceType, args.ServiceParamsPayload, time.Now().Unix()); err != nil {
		return feePoolServiceQuoteBuilt{}, err
	}
	return feePoolServiceQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      strings.TrimSpace(resp.Status),
		ServiceQuoteRaw:  append([]byte(nil), resp.ServiceQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: quoteHash,
	}, nil
}

func normalizeFeePoolServiceDomain(raw string) string {
	value := strings.TrimSpace(raw)
	if value != "" {
		return value
	}
	return "bitcast-gateway"
}
