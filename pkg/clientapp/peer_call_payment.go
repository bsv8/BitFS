package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
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

func retryPeerCallWithAutoPayment(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, options []*nodesvc.PaymentOption) (nodesvc.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options)
	if !ok {
		return nodesvc.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case nodesvc.PaymentSchemePool2of2V1:
		quotedResp, quoted, info, err := quotePeerCallWithFeePool2of2(ctx, rt, peerID, req, option)
		if err != nil {
			return nodesvc.CallResp{}, err
		}
		if strings.TrimSpace(quotedResp.Code) == "PAYMENT_QUOTED" {
			return payPeerCallWithFeePool2of2Quote(ctx, rt, peerID, req, option, quoted, info)
		}
		return quotedResp, nil
	default:
		return nodesvc.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func quotePeerCallFromPaymentRequired(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, options []*nodesvc.PaymentOption) (nodesvc.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options)
	if !ok {
		return nodesvc.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case nodesvc.PaymentSchemePool2of2V1:
		resp, _, _, err := quotePeerCallWithFeePool2of2(ctx, rt, peerID, req, option)
		return resp, err
	default:
		return nodesvc.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func payPeerCallWithAcceptedQuote(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, rawQuote []byte) (nodesvc.CallResp, error) {
	if rt == nil || rt.Host == nil {
		return nodesvc.CallResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rawQuote) == 0 {
		return nodesvc.CallResp{}, fmt.Errorf("payment quote missing")
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, peerID)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	quote, _, err := dual2of2.ParseAndVerifyServiceQuote(rawQuote, gatewayPub)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	option := &nodesvc.PaymentOption{
		Scheme:              nodesvc.PaymentSchemePool2of2V1,
		PaymentDomain:       strings.TrimSpace(quote.Domain),
		AmountSatoshi:       quote.ChargeAmountSatoshi,
		Description:         strings.TrimSpace(quote.ChargeReason),
		PricingMode:         dual2of2.ServiceOfferPricingModeFixedPrice,
		ServiceQuantity:     1,
		ServiceQuantityUnit: "call",
		QuoteStatus:         "accepted",
	}
	payMu := rt.feePoolPayMutex(peerID.String())
	payMu.Lock()
	defer payMu.Unlock()

	info, _, err := ensurePeerFeePoolSessionForChargeLocked(ctx, rt, peerID, option.PaymentDomain, option.AmountSatoshi, option)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	session, ok := rt.getFeePool(peerID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return nodesvc.CallResp{}, fmt.Errorf("fee pool session missing for peer=%s", peerID)
	}
	if err := dual2of2.ValidateServiceQuoteBinding(quote, strings.ToLower(hex.EncodeToString(gatewayPub.Compressed())), rt.runIn.ClientID, session.SpendTxID, strings.TrimSpace(req.Route), req.Body, time.Now().Unix()); err != nil {
		return nodesvc.CallResp{}, err
	}
	return payPeerCallWithFeePool2of2Quote(ctx, rt, peerID, req, option, feePoolServiceQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      "accepted",
		ServiceQuoteRaw:  append([]byte(nil), rawQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: "",
	}, info)
}

func choosePeerCallPaymentOption(options []*nodesvc.PaymentOption) (*nodesvc.PaymentOption, bool) {
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == nodesvc.PaymentSchemePool2of2V1 {
			return item, true
		}
	}
	return nil, false
}

func quotePeerCallWithFeePool2of2(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, option *nodesvc.PaymentOption) (nodesvc.CallResp, feePoolServiceQuoteBuilt, dual2of2.InfoResp, error) {
	if rt == nil || rt.Host == nil {
		return nodesvc.CallResp{}, feePoolServiceQuoteBuilt{}, dual2of2.InfoResp{}, fmt.Errorf("runtime not initialized")
	}
	if option == nil {
		return nodesvc.CallResp{}, feePoolServiceQuoteBuilt{}, dual2of2.InfoResp{}, fmt.Errorf("payment option missing")
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
		return nodesvc.CallResp{}, feePoolServiceQuoteBuilt{}, dual2of2.InfoResp{}, err
	}
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return nodesvc.CallResp{}, feePoolServiceQuoteBuilt{}, dual2of2.InfoResp{}, fmt.Errorf("fee pool session missing for peer=%s", gw.ID)
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
		return nodesvc.CallResp{}, feePoolServiceQuoteBuilt{}, dual2of2.InfoResp{}, err
	}
	quantity, unit := describePeerCallQuoteQuantity(quoted.ServiceQuote)
	return nodesvc.CallResp{
		Ok:                 false,
		Code:               "PAYMENT_QUOTED",
		Message:            "payment quote ready",
		PaymentOptions:     []*nodesvc.PaymentOption{decorateQuotedPaymentOption(option, quoted, quantity, unit)},
		PaymentQuoteScheme: nodesvc.PaymentSchemePool2of2V1,
		PaymentQuote:       append([]byte(nil), quoted.ServiceQuoteRaw...),
	}, quoted, info, nil
}

func payPeerCallWithFeePool2of2Quote(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, option *nodesvc.PaymentOption, quoted feePoolServiceQuoteBuilt, info dual2of2.InfoResp) (nodesvc.CallResp, error) {
	chargeCtx, err := prepareFeePoolChargeFromQuote(rt, peerID, quoted)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	paymentPayload, err := oldproto.Marshal(&nodesvc.FeePool2of2Payment{
		SpendTxID:           chargeCtx.Session.SpendTxID,
		SequenceNumber:      chargeCtx.NextSeq,
		ServerAmount:        chargeCtx.NextServerAmount,
		ChargeAmountSatoshi: quoted.ServiceQuote.ChargeAmountSatoshi,
		Fee:                 chargeCtx.Session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), chargeCtx.ClientSignature...),
		ChargeReason:        strings.TrimSpace(quoted.ServiceQuote.ChargeReason),
		ProofIntent:         append([]byte(nil), chargeCtx.ProofIntent...),
		SignedProofCommit:   append([]byte(nil), chargeCtx.SignedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	})
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	paidReq := req
	paidReq.PaymentScheme = nodesvc.PaymentSchemePool2of2V1
	paidReq.PaymentPayload = paymentPayload
	out, err := callNodeRoute(ctx, rt, peerID, paidReq)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	if strings.TrimSpace(out.PaymentReceiptScheme) == nodesvc.PaymentSchemePool2of2V1 && len(out.PaymentReceipt) > 0 {
		var receipt nodesvc.FeePool2of2Receipt
		if err := oldproto.Unmarshal(out.PaymentReceipt, &receipt); err != nil {
			return nodesvc.CallResp{}, err
		}
		if receipt.SequenceNumber != chargeCtx.NextSeq || receipt.ServerAmount != chargeCtx.NextServerAmount {
			return nodesvc.CallResp{}, fmt.Errorf("payment receipt state mismatch")
		}
		if err := verifyServiceReceiptOrFreeze(ctx, rt, peerID, chargeCtx.Session, receipt.MergedCurrentTx, expectedServiceReceipt{
			ServiceType:        strings.TrimSpace(req.Route),
			SpendTxID:          chargeCtx.Session.SpendTxID,
			SequenceNumber:     chargeCtx.NextSeq,
			AcceptedChargeHash: chargeCtx.AcceptedChargeHash,
			ResultCode:         strings.TrimSpace(out.Code),
			ResultPayloadBytes: append([]byte(nil), out.Body...),
		}, receipt.ServiceReceipt); err != nil {
			return nodesvc.CallResp{}, err
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
				"sequence_number":     receipt.SequenceNumber,
				"server_amount":       receipt.ServerAmount,
				"minimum_pool_amount": info.MinimumPoolAmountSatoshi,
			},
		})
	}
	return out, nil
}

func ensurePeerFeePoolSessionForChargeLocked(ctx context.Context, rt *Runtime, peerID peer.ID, paymentDomain string, charge uint64, option *nodesvc.PaymentOption) (dual2of2.InfoResp, peer.AddrInfo, error) {
	var info dual2of2.InfoResp
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
		Session:         session,
		ClientActor:     clientActor,
		GatewayPub:      quoted.GatewayPub,
		ServiceQuoteRaw: quoted.ServiceQuoteRaw,
		ServiceQuote:    quoted.ServiceQuote,
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
		NextSeq:            quoted.ServiceQuote.SequenceNumber,
		NextServerAmount:   quoted.ServiceQuote.ServerAmountAfter,
		UpdatedTxHex:       built.UpdatedTx.Hex(),
		AcceptedChargeHash: built.AcceptedChargeHash,
		ClientSignature:    append([]byte(nil), (*clientSig)...),
		ProofIntent:        append([]byte(nil), built.ProofIntent...),
		SignedProofCommit:  append([]byte(nil), built.SignedProofCommit...),
	}, nil
}

func normalizePeerCallPricingMode(option *nodesvc.PaymentOption) string {
	if option == nil {
		return dual2of2.ServiceOfferPricingModeFixedPrice
	}
	mode := strings.TrimSpace(option.PricingMode)
	if mode != "" {
		return mode
	}
	return dual2of2.ServiceOfferPricingModeFixedPrice
}

func buildPeerCallQuoteTarget(req nodesvc.CallReq) string {
	route := strings.TrimSpace(req.Route)
	if route != "" {
		return route
	}
	return strings.TrimSpace(req.To)
}

func describePeerCallQuoteQuantity(quote proof.ServiceQuote) (uint64, string) {
	if quote.GrantedDurationSeconds > 0 {
		return uint64(quote.GrantedDurationSeconds), "second"
	}
	return 1, "call"
}

func decorateQuotedPaymentOption(option *nodesvc.PaymentOption, quoted feePoolServiceQuoteBuilt, quantity uint64, unit string) *nodesvc.PaymentOption {
	if option == nil {
		return nil
	}
	copyItem := *option
	copyItem.AmountSatoshi = quoted.ServiceQuote.ChargeAmountSatoshi
	copyItem.Description = strings.TrimSpace(quoted.ServiceQuote.ChargeReason)
	copyItem.PricingMode = normalizePeerCallPricingMode(option)
	copyItem.ServiceQuantity = quantity
	copyItem.ServiceQuantityUnit = strings.TrimSpace(unit)
	copyItem.QuoteStatus = strings.TrimSpace(quoted.QuoteStatus)
	return &copyItem
}
