package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	domainmodule "github.com/bsv8/BFTP/pkg/modules/domain"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type feePoolChargeContext struct {
	Session            *feePoolSession
	Charge             uint64
	NextSeq            uint32
	NextServerAmount   uint64
	UpdatedTxHex       string
	AcceptedChargeHash string
	ClientSignature    []byte
	ProofIntent        []byte
	SignedProofCommit  []byte
}

func retryPeerCallWithAutoPayment(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, options []*ncall.PaymentOption) (ncall.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options)
	if !ok {
		return ncall.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case ncall.PaymentSchemePool2of2V1:
		quotedResp, quoted, info, err := quotePeerCallWithFeePool2of2(ctx, rt, peerID, req, option)
		if err != nil {
			return ncall.CallResp{}, err
		}
		if strings.TrimSpace(quotedResp.Code) == "PAYMENT_QUOTED" {
			return payPeerCallWithFeePool2of2Quote(ctx, rt, peerID, req, option, quoted, info)
		}
		return quotedResp, nil
	default:
		return ncall.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func quotePeerCallFromPaymentRequired(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, options []*ncall.PaymentOption) (ncall.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options)
	if !ok {
		return ncall.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case ncall.PaymentSchemePool2of2V1:
		resp, _, _, err := quotePeerCallWithFeePool2of2(ctx, rt, peerID, req, option)
		return resp, err
	default:
		return ncall.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func payPeerCallWithAcceptedQuote(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, rawQuote []byte) (ncall.CallResp, error) {
	if rt == nil || rt.Host == nil {
		return ncall.CallResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rawQuote) == 0 {
		return ncall.CallResp{}, fmt.Errorf("payment quote missing")
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, peerID)
	if err != nil {
		return ncall.CallResp{}, err
	}
	quote, _, err := poolcore.ParseAndVerifyServiceQuote(rawQuote, gatewayPub)
	if err != nil {
		return ncall.CallResp{}, err
	}
	option := &ncall.PaymentOption{
		Scheme:              ncall.PaymentSchemePool2of2V1,
		PaymentDomain:       "",
		AmountSatoshi:       quote.ChargeAmountSatoshi,
		Description:         strings.TrimSpace(req.Route),
		PricingMode:         poolcore.ServiceOfferPricingModeFixedPrice,
		ServiceQuantity:     1,
		ServiceQuantityUnit: "call",
		QuoteStatus:         "accepted",
	}
	payMu := rt.feePoolPayMutex(peerID.String())
	payMu.Lock()
	defer payMu.Unlock()

	info, _, err := ensurePeerFeePoolSessionForChargeLocked(ctx, rt, peerID, option.PaymentDomain, option.AmountSatoshi, option)
	if err != nil {
		return ncall.CallResp{}, err
	}
	session, ok := rt.getFeePool(peerID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return ncall.CallResp{}, fmt.Errorf("fee pool session missing for peer=%s", peerID)
	}
	return payPeerCallWithFeePool2of2Quote(ctx, rt, peerID, req, option, feePoolServiceQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      "accepted",
		ServiceQuoteRaw:  append([]byte(nil), rawQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: "",
		ChargeReason:     strings.TrimSpace(req.Route),
		NextSequence:     session.Sequence + 1,
		NextServerAmount: session.ServerAmount + quote.ChargeAmountSatoshi,
	}, info)
}

func choosePeerCallPaymentOption(options []*ncall.PaymentOption) (*ncall.PaymentOption, bool) {
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == ncall.PaymentSchemePool2of2V1 {
			return item, true
		}
	}
	return nil, false
}

func quotePeerCallWithFeePool2of2(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption) (ncall.CallResp, feePoolServiceQuoteBuilt, poolcore.InfoResp, error) {
	if rt == nil || rt.Host == nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, fmt.Errorf("runtime not initialized")
	}
	if option == nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, fmt.Errorf("payment option missing")
	}
	charge := option.AmountSatoshi
	if charge == 0 {
		charge = 1
	}
	payMu := rt.feePoolPayMutex(peerID.String())
	payMu.Lock()
	defer payMu.Unlock()

	info, gw, err := ensurePeerFeePoolSessionForChargeLocked(ctx, rt, peerID, option.PaymentDomain, charge, option)
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, fmt.Errorf("fee pool session missing for peer=%s", gw.ID)
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		Session:              session,
		GatewayPeerID:        gw.ID,
		ServiceDomain:        strings.TrimSpace(option.PaymentDomain),
		ServiceType:          strings.TrimSpace(req.Route),
		Target:               buildPeerCallQuoteTarget(req),
		ServiceParamsPayload: append([]byte(nil), req.Body...),
		PricingMode:          normalizePeerCallPricingMode(option),
		ProposedPaymentSat:   charge,
	})
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	quantity, unit := describePeerCallQuoteQuantity(quoted.ServiceQuote)
	return ncall.CallResp{
		Ok:             false,
		Code:           "PAYMENT_QUOTED",
		Message:        "payment quote ready",
		PaymentSchemes: []*ncall.PaymentOption{decorateQuotedPaymentOption(option, quoted, quantity, unit)},
		ServiceQuote:   append([]byte(nil), quoted.ServiceQuoteRaw...),
	}, quoted, info, nil
}

func payPeerCallWithFeePool2of2Quote(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted feePoolServiceQuoteBuilt, info poolcore.InfoResp) (ncall.CallResp, error) {
	chargeCtx, err := prepareFeePoolChargeFromQuote(rt, peerID, quoted)
	if err != nil {
		return ncall.CallResp{}, err
	}
	paymentPayload, err := oldproto.Marshal(&ncall.FeePool2of2Payment{
		SpendTxID:           chargeCtx.Session.SpendTxID,
		SequenceNumber:      chargeCtx.NextSeq,
		ServerAmount:        chargeCtx.NextServerAmount,
		ChargeAmountSatoshi: quoted.ServiceQuote.ChargeAmountSatoshi,
		Fee:                 chargeCtx.Session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), chargeCtx.ClientSignature...),
		ChargeReason:        strings.TrimSpace(quoted.ChargeReason),
		ProofIntent:         append([]byte(nil), chargeCtx.ProofIntent...),
		SignedProofCommit:   append([]byte(nil), chargeCtx.SignedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	})
	if err != nil {
		return ncall.CallResp{}, err
	}
	paidReq := req
	paidReq.PaymentScheme = ncall.PaymentSchemePool2of2V1
	paidReq.PaymentPayload = paymentPayload
	out, err := callNodeRoute(ctx, rt, peerID, paidReq)
	if err != nil {
		return ncall.CallResp{}, err
	}
	if strings.TrimSpace(out.PaymentReceiptScheme) == ncall.PaymentSchemePool2of2V1 && len(out.PaymentReceipt) > 0 {
		var receipt ncall.FeePool2of2Receipt
		if err := oldproto.Unmarshal(out.PaymentReceipt, &receipt); err != nil {
			return ncall.CallResp{}, err
		}
		resultPayloadBytes, err := expectedPeerCallResultPayload(req.Route, out.Body)
		if err != nil {
			return ncall.CallResp{}, err
		}
		if err := verifyServiceReceiptOrFreeze(ctx, rt, peerID, chargeCtx.Session, receipt.MergedCurrentTx, expectedServiceReceipt{
			ServiceType:        strings.TrimSpace(req.Route),
			OfferHash:          quoted.ServiceQuote.OfferHash,
			ResultPayloadBytes: resultPayloadBytes,
		}, out.ServiceReceipt); err != nil {
			return ncall.CallResp{}, err
		}
		nextTxHex := chargeCtx.UpdatedTxHex
		if len(receipt.MergedCurrentTx) > 0 {
			nextTxHex = strings.ToLower(hex.EncodeToString(receipt.MergedCurrentTx))
		}
		applyFeePoolChargeToSession(chargeCtx.Session, chargeCtx.NextSeq, chargeCtx.NextServerAmount, nextTxHex)
		appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + chargeCtx.Session.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           chargeCtx.Session.SpendTxID,
			Stage:           "use_peer_call",
			Direction:       "out",
			Purpose:         "peer_call_fee",
			AmountSatoshi:   -int64(chargeCtx.Charge),
			UsedSatoshi:     int64(chargeCtx.Charge),
			ReturnedSatoshi: 0,
			RelatedTxID:     receipt.UpdatedTxID,
			Note:            fmt.Sprintf("route=%s payment_domain=%s", strings.TrimSpace(req.Route), strings.TrimSpace(option.PaymentDomain)),
			Payload: map[string]any{
				"route":               strings.TrimSpace(req.Route),
				"payment_domain":      strings.TrimSpace(option.PaymentDomain),
				"charged_amount_sat":  receipt.ChargedAmountSatoshi,
				"sequence_number":     chargeCtx.NextSeq,
				"server_amount":       chargeCtx.NextServerAmount,
				"minimum_pool_amount": info.MinimumPoolAmountSatoshi,
			},
		})
	}
	return out, nil
}

func expectedPeerCallResultPayload(route string, body []byte) ([]byte, error) {
	switch strings.TrimSpace(route) {
	case broadcastmodule.RouteBroadcastV1DemandPublish:
		var resp broadcastmodule.DemandPublishPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish body failed: %w", err)
		}
		return broadcastmodule.MarshalDemandPublishServicePayload(resp)
	case broadcastmodule.RouteBroadcastV1DemandPublishBatch:
		var resp broadcastmodule.DemandPublishBatchPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish batch body failed: %w", err)
		}
		return broadcastmodule.MarshalDemandPublishBatchServicePayload(resp)
	case broadcastmodule.RouteBroadcastV1LiveDemandPublish:
		var resp broadcastmodule.LiveDemandPublishPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast live demand publish body failed: %w", err)
		}
		return broadcastmodule.MarshalLiveDemandPublishServicePayload(resp)
	case broadcastmodule.RouteBroadcastV1NodeReachabilityAnnounce:
		var resp broadcastmodule.NodeReachabilityAnnouncePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability announce body failed: %w", err)
		}
		return broadcastmodule.MarshalNodeReachabilityAnnounceServicePayload(resp)
	case broadcastmodule.RouteBroadcastV1NodeReachabilityQuery:
		var resp broadcastmodule.NodeReachabilityQueryPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability query body failed: %w", err)
		}
		return broadcastmodule.MarshalNodeReachabilityQueryServicePayload(resp)
	case domainmodule.RouteDomainV1Resolve:
		var resp domainmodule.ResolveNamePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain resolve body failed: %w", err)
		}
		return domainmodule.MarshalResolveNameServicePayload(resp)
	case domainmodule.RouteDomainV1Query:
		var resp domainmodule.QueryNamePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain query body failed: %w", err)
		}
		return domainmodule.MarshalQueryNameServicePayload(resp)
	case domainmodule.RouteDomainV1Lock:
		var resp domainmodule.RegisterLockPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain register lock body failed: %w", err)
		}
		return domainmodule.MarshalRegisterLockServicePayload(resp)
	case domainmodule.RouteDomainV1SetTarget:
		var resp domainmodule.SetTargetPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain set target body failed: %w", err)
		}
		return domainmodule.MarshalSetTargetServicePayload(resp)
	default:
		return append([]byte(nil), body...), nil
	}
}

func ensurePeerFeePoolSessionForChargeLocked(ctx context.Context, rt *Runtime, peerID peer.ID, paymentDomain string, charge uint64, option *ncall.PaymentOption) (poolcore.InfoResp, peer.AddrInfo, error) {
	var info poolcore.InfoResp
	var err error
	if rt == nil || rt.Host == nil {
		return info, peer.AddrInfo{}, fmt.Errorf("runtime not initialized")
	}
	info, err = callNodePoolInfo(ctx, rt, peerID)
	if err != nil {
		return info, peer.AddrInfo{}, err
	}
	gw := peer.AddrInfo{ID: peerID}
	if cur, ok := rt.getFeePool(gw.ID.String()); ok && cur != nil && strings.TrimSpace(cur.SpendTxID) != "" && cur.Status == "active" {
		if cur.ClientAmount >= charge+cur.SpendTxFeeSat {
			return info, gw, nil
		}
	}
	autoRenewRounds := rt.runIn.Listen.AutoRenewRounds
	if autoRenewRounds == 0 {
		autoRenewRounds = 1
	}
	targetMinimum := info.MinimumPoolAmountSatoshi
	if option != nil && option.MinimumPoolAmountSatoshi > targetMinimum {
		targetMinimum = option.MinimumPoolAmountSatoshi
	}
	if targetMinimum < charge+info.SingleQueryFeeSatoshi+info.SinglePublishFeeSatoshi+info.SingleCycleFeeSatoshi {
		targetMinimum = charge + info.SingleQueryFeeSatoshi + info.SinglePublishFeeSatoshi + info.SingleCycleFeeSatoshi
	}
	if targetMinimum == 0 {
		targetMinimum = charge
	}
	info.MinimumPoolAmountSatoshi = targetMinimum
	if option != nil {
		if option.LockBlocks > 0 {
			info.LockBlocks = option.LockBlocks
		}
		if option.FeeRateSatPerByteMilli > 0 {
			info.FeeRateSatPerByte = float64(option.FeeRateSatPerByteMilli) / 1000
		}
	}
	_, err = createFeePoolSessionWithSecurity(ctx, rt, gw, autoRenewRounds, info, gwSec(rt.rpcTrace))
	if err != nil {
		return info, gw, err
	}
	return info, gw, nil
}

func prepareFeePoolChargeFromQuote(rt *Runtime, targetPeerID peer.ID, quoted feePoolServiceQuoteBuilt) (feePoolChargeContext, error) {
	session, ok := rt.getFeePool(targetPeerID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return feePoolChargeContext{}, fmt.Errorf("fee pool session missing for peer=%s", targetPeerID)
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return feePoolChargeContext{}, err
	}
	built, err := buildFeePoolUpdatedTxWithProof(feePoolProofArgs{
		Session:             session,
		ClientActor:         clientActor,
		GatewayPub:          quoted.GatewayPub,
		ServiceQuoteRaw:     quoted.ServiceQuoteRaw,
		ServiceQuote:        quoted.ServiceQuote,
		ChargeReason:        quoted.ChargeReason,
		NextSequence:        quoted.NextSequence,
		NextServerAmount:    quoted.NextServerAmount,
		ServiceDeadlineUnix: quoted.GrantedServiceDeadlineUnix,
	})
	if err != nil {
		return feePoolChargeContext{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(built.UpdatedTx, clientActor.PrivKey, quoted.GatewayPub)
	if err != nil {
		return feePoolChargeContext{}, err
	}
	return feePoolChargeContext{
		Session:            session,
		Charge:             quoted.ServiceQuote.ChargeAmountSatoshi,
		NextSeq:            quoted.NextSequence,
		NextServerAmount:   quoted.NextServerAmount,
		UpdatedTxHex:       built.UpdatedTx.Hex(),
		AcceptedChargeHash: built.AcceptedChargeHash,
		ClientSignature:    append([]byte(nil), (*clientSig)...),
		ProofIntent:        append([]byte(nil), built.ProofIntent...),
		SignedProofCommit:  append([]byte(nil), built.SignedProofCommit...),
	}, nil
}

func normalizePeerCallPricingMode(option *ncall.PaymentOption) string {
	if option == nil {
		return poolcore.ServiceOfferPricingModeFixedPrice
	}
	mode := strings.TrimSpace(option.PricingMode)
	if mode != "" {
		return mode
	}
	return poolcore.ServiceOfferPricingModeFixedPrice
}

func buildPeerCallQuoteTarget(req ncall.CallReq) string {
	route := strings.TrimSpace(req.Route)
	if route != "" {
		return route
	}
	return strings.TrimSpace(req.To)
}

func describePeerCallQuoteQuantity(quote payflow.ServiceQuote) (uint64, string) {
	return 1, "call"
}

func decorateQuotedPaymentOption(option *ncall.PaymentOption, quoted feePoolServiceQuoteBuilt, quantity uint64, unit string) *ncall.PaymentOption {
	if option == nil {
		return nil
	}
	copyItem := *option
	copyItem.AmountSatoshi = quoted.ServiceQuote.ChargeAmountSatoshi
	copyItem.Description = strings.TrimSpace(quoted.ChargeReason)
	copyItem.PricingMode = normalizePeerCallPricingMode(option)
	copyItem.ServiceQuantity = quantity
	copyItem.ServiceQuantityUnit = strings.TrimSpace(unit)
	copyItem.QuoteStatus = strings.TrimSpace(quoted.QuoteStatus)
	return &copyItem
}
