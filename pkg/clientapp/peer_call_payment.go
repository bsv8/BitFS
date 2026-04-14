package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
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

func retryPeerCallWithAutoPayment(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, options []*ncall.PaymentOption, requireActiveFeePool bool) (ncall.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options, preferredPaymentScheme(rt))
	if !ok {
		return ncall.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case ncall.PaymentSchemePool2of2V1:
		quotedResp, quoted, info, err := quotePeerCallWithFeePool2of2(ctx, rt, store, peerID, req, option, requireActiveFeePool)
		if err != nil {
			return ncall.CallResp{}, err
		}
		if strings.TrimSpace(quotedResp.Code) == "PAYMENT_QUOTED" {
			return payPeerCallWithFeePool2of2Quote(ctx, rt, store, peerID, req, option, quoted, info)
		}
		return quotedResp, nil
	case ncall.PaymentSchemeChainTxV1:
		quotedResp, quoted, err := quotePeerCallWithChainTx(ctx, rt, store, peerID, req, option)
		if err != nil {
			return ncall.CallResp{}, err
		}
		if strings.TrimSpace(quotedResp.Code) == "PAYMENT_QUOTED" {
			return payPeerCallWithChainTxQuote(ctx, rt, store, peerID, req, option, quoted)
		}
		return quotedResp, nil
	default:
		return ncall.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func quotePeerCallFromPaymentQuoted(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, options []*ncall.PaymentOption, requireActiveFeePool bool) (ncall.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options, preferredPaymentScheme(rt))
	if !ok {
		return ncall.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case ncall.PaymentSchemePool2of2V1:
		resp, _, _, err := quotePeerCallWithFeePool2of2(ctx, rt, store, peerID, req, option, requireActiveFeePool)
		return resp, err
	case ncall.PaymentSchemeChainTxV1:
		resp, _, err := quotePeerCallWithChainTx(ctx, rt, store, peerID, req, option)
		return resp, err
	default:
		return ncall.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
}

func payPeerCallWithAcceptedQuote(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, rawQuote []byte, preferredScheme string, requireActiveFeePool bool) (ncall.CallResp, error) {
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
	if strings.TrimSpace(preferredScheme) == "" {
		preferredScheme = preferredPaymentScheme(rt)
	}
	scheme, err := chooseAcceptedQuotePaymentScheme(preferredScheme)
	if err != nil {
		return ncall.CallResp{}, err
	}
	option := &ncall.PaymentOption{
		Scheme:              scheme,
		PaymentDomain:       "",
		AmountSatoshi:       quote.ChargeAmountSatoshi,
		Description:         strings.TrimSpace(req.Route),
		PricingMode:         poolcore.ServiceOfferPricingModeFixedPrice,
		ServiceQuantity:     1,
		ServiceQuantityUnit: "call",
		QuoteStatus:         "accepted",
	}
	switch scheme {
	case ncall.PaymentSchemePool2of2V1:
		payMu := rt.feePoolPayMutex(peerID.String())
		payMu.Lock()
		defer payMu.Unlock()

		info, _, err := ensurePeerFeePoolSessionForChargeLocked(ctx, rt, store, peerID, option.PaymentDomain, option.AmountSatoshi, option, requireActiveFeePool)
		if err != nil {
			return ncall.CallResp{}, err
		}
		session, ok := rt.getFeePool(peerID.String())
		if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
			return ncall.CallResp{}, fmt.Errorf("fee pool session missing for peer=%s", peerID)
		}
		return payPeerCallWithFeePool2of2Quote(ctx, rt, store, peerID, req, option, feePoolServiceQuoteBuilt{
			GatewayPub:       gatewayPub,
			QuoteStatus:      "accepted",
			ServiceQuoteRaw:  append([]byte(nil), rawQuote...),
			ServiceQuote:     quote,
			ServiceQuoteHash: "",
			ChargeReason:     strings.TrimSpace(req.Route),
			NextSequence:     session.Sequence + 1,
			NextServerAmount: session.ServerAmount + quote.ChargeAmountSatoshi,
		}, info)
	case ncall.PaymentSchemeChainTxV1:
		return payPeerCallWithChainTxQuote(ctx, rt, store, peerID, req, option, peerCallChainTxQuoteBuilt{
			GatewayPub:       gatewayPub,
			QuoteStatus:      "accepted",
			ServiceQuoteRaw:  append([]byte(nil), rawQuote...),
			ServiceQuote:     quote,
			ServiceQuoteHash: "",
			ChargeReason:     strings.TrimSpace(req.Route),
		})
	default:
		return ncall.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", scheme)
	}
}

func choosePeerCallPaymentOption(options []*ncall.PaymentOption, preferredScheme string) (*ncall.PaymentOption, bool) {
	preferredScheme, err := normalizePreferredPaymentScheme(preferredScheme)
	if err != nil {
		preferredScheme = defaultPreferredPaymentScheme
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == preferredScheme {
			return item, true
		}
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == ncall.PaymentSchemePool2of2V1 {
			return item, true
		}
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == ncall.PaymentSchemeChainTxV1 {
			return item, true
		}
	}
	return nil, false
}

func chooseAcceptedQuotePaymentScheme(preferredScheme string) (string, error) {
	return normalizePreferredPaymentScheme(preferredScheme)
}

func quotePeerCallWithFeePool2of2(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, requireActiveFeePool bool) (ncall.CallResp, feePoolServiceQuoteBuilt, poolcore.InfoResp, error) {
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

	info, gw, err := ensurePeerFeePoolSessionForChargeLocked(ctx, rt, store, peerID, option.PaymentDomain, charge, option, requireActiveFeePool)
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, fmt.Errorf("fee pool session missing for peer=%s", gw.ID)
	}
	serviceParamsPayload, err := buildPeerCallServiceParamsPayload(strings.TrimSpace(req.Route), req.Body)
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	quotedResp, err := callNodeRoute(ctx, rt, peerID, ncall.CallReq{
		To:          buildPeerCallQuoteTarget(req),
		Route:       strings.TrimSpace(req.Route),
		ContentType: strings.TrimSpace(req.ContentType),
		Body:        append([]byte(nil), req.Body...),
	})
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	if strings.TrimSpace(quotedResp.Code) != "PAYMENT_QUOTED" {
		return quotedResp, feePoolServiceQuoteBuilt{}, info, nil
	}
	if len(quotedResp.PaymentSchemes) == 0 || quotedResp.PaymentSchemes[0] == nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, fmt.Errorf("payment schemes missing")
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, peerID)
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(quotedResp.ServiceQuote, gatewayPub)
	if err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	offer := payflow.ServiceOffer{
		ServiceType:          peerCallQuotedServiceType(req.Route),
		ServiceNodePubkeyHex: strings.ToLower(gatewayPub.ToDERHex()),
		ClientPubkeyHex:      identity.ClientIDLower,
		RequestParams:        append([]byte(nil), serviceParamsPayload...),
		CreatedAtUnix:        time.Now().Unix(),
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, offer, offer.ServiceNodePubkeyHex, identity.ClientID, peerCallQuotedServiceType(req.Route), serviceParamsPayload, time.Now().Unix()); err != nil {
		return ncall.CallResp{}, feePoolServiceQuoteBuilt{}, poolcore.InfoResp{}, err
	}
	chargeReason := strings.TrimSpace(option.Description)
	if chargeReason == "" {
		chargeReason = peerCallQuotedServiceType(req.Route)
	}
	quantity, unit := describePeerCallQuoteQuantity(quote)
	if strings.TrimSpace(req.Route) == string(contractroute.RouteBroadcastV1ListenCycle) {
		quantity, unit = 1, "second"
		if duration, err := poolcore.ListenOfferBudgetToDurationSeconds(quote.ChargeAmountSatoshi, session.SingleCycleFeeSatoshi, session.BillingCycleSeconds); err == nil {
			quantity = duration
		}
	}
	quoted := feePoolServiceQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      strings.TrimSpace(quotedResp.PaymentSchemes[0].QuoteStatus),
		ServiceQuoteRaw:  append([]byte(nil), quotedResp.ServiceQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: quoteHash,
		ChargeReason:     chargeReason,
		NextSequence:     session.Sequence + 1,
		NextServerAmount: session.ServerAmount + quote.ChargeAmountSatoshi,
	}
	quotedResp.PaymentSchemes = []*ncall.PaymentOption{decorateQuotedPeerCallPaymentOption(option, quote.ChargeAmountSatoshi, quoted.ChargeReason, quoted.QuoteStatus, quantity, unit)}
	return quotedResp, quoted, info, nil
}

func payPeerCallWithFeePool2of2Quote(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted feePoolServiceQuoteBuilt, info poolcore.InfoResp) (ncall.CallResp, error) {
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
		if err := verifyServiceReceiptOrFreeze(ctx, rt, store, peerID, chargeCtx.Session, receipt.MergedCurrentTx, expectedServiceReceipt{
			ServiceType:        peerCallReceiptServiceType(req.Route),
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
		gatewayPubHex := gatewayBusinessID(rt, peerID)
		if quoted.GatewayPub != nil {
			if v := strings.TrimSpace(quoted.GatewayPub.ToDERHex()); v != "" {
				gatewayPubHex = strings.ToLower(v)
			}
		}
		if err := dbRecordFeePoolQuotePayAccounting(ctx, store, gatewayPubHex, chargeCtx.Session, receipt.UpdatedTxID, nextTxHex, quoted.ServiceQuote.ChargeAmountSatoshi, quoted.ChargeReason); err != nil {
			return ncall.CallResp{}, err
		}
		// 资金流水已迁移到 fact_* 事实表组装
	}
	return out, nil
}

func expectedPeerCallResultPayload(route string, body []byte) ([]byte, error) {
	switch strings.TrimSpace(route) {
	case string(contractroute.RouteBroadcastV1ListenCycle):
		var resp contractmessage.ListenCyclePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast listen cycle body failed: %w", err)
		}
		return marshalListenCycleServicePayload(resp)
	case string(contractroute.RouteBroadcastV1DemandPublish):
		var resp contractmessage.DemandPublishPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish body failed: %w", err)
		}
		return marshalDemandPublishServicePayload(resp)
	case string(contractroute.RouteBroadcastV1DemandPublishBatch):
		var resp contractmessage.DemandPublishBatchPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish batch body failed: %w", err)
		}
		return marshalDemandPublishBatchServicePayload(resp)
	case string(contractroute.RouteBroadcastV1LiveDemandPublish):
		var resp contractmessage.LiveDemandPublishPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast live demand publish body failed: %w", err)
		}
		return marshalLiveDemandPublishServicePayload(resp)
	case string(contractroute.RouteBroadcastV1NodeReachabilityAnnounce):
		var resp contractmessage.NodeReachabilityAnnouncePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability announce body failed: %w", err)
		}
		return marshalNodeReachabilityAnnounceServicePayload(resp)
	case string(contractroute.RouteBroadcastV1NodeReachabilityQuery):
		var resp contractmessage.NodeReachabilityQueryPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability query body failed: %w", err)
		}
		return marshalNodeReachabilityQueryServicePayload(resp)
	case string(contractroute.RouteDomainV1Resolve):
		var resp contractmessage.ResolveNamePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain resolve body failed: %w", err)
		}
		return marshalResolveNameServicePayload(resp)
	case string(contractroute.RouteDomainV1Query):
		var resp contractmessage.QueryNamePaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain query body failed: %w", err)
		}
		return marshalQueryNameServicePayload(resp)
	case string(contractroute.RouteDomainV1Lock):
		var resp contractmessage.RegisterLockPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain register lock body failed: %w", err)
		}
		return marshalRegisterLockServicePayload(resp)
	case string(contractroute.RouteDomainV1SetTarget):
		var resp contractmessage.SetTargetPaidResp
		if err := oldproto.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode domain set target body failed: %w", err)
		}
		return marshalSetTargetServicePayload(resp)
	default:
		return append([]byte(nil), body...), nil
	}
}

func buildPeerCallServiceParamsPayload(route string, body []byte) ([]byte, error) {
	switch strings.TrimSpace(route) {
	case string(contractroute.RouteBroadcastV1ListenCycle):
		var req contractmessage.ListenCycleReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast listen cycle request failed: %w", err)
		}
		return poolcore.MarshalListenCycleQuotePayload(req.RequestedDurationSeconds, req.RequestedUntilUnix, req.ProposedPaymentSatoshi)
	case string(contractroute.RouteBroadcastV1DemandPublish):
		var req contractmessage.DemandPublishReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish request failed: %w", err)
		}
		return marshalDemandPublishQuotePayload(req.SeedHash, req.ChunkCount, req.BuyerAddrs)
	case string(contractroute.RouteBroadcastV1DemandPublishBatch):
		var req contractmessage.DemandPublishBatchReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast demand publish batch request failed: %w", err)
		}
		return marshalDemandPublishBatchQuotePayload(req.Items, req.BuyerAddrs)
	case string(contractroute.RouteBroadcastV1LiveDemandPublish):
		var req contractmessage.LiveDemandPublishReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast live demand publish request failed: %w", err)
		}
		return marshalLiveDemandPublishQuotePayload(req.StreamID, req.HaveSegmentIndex, req.Window, req.BuyerAddrs)
	case string(contractroute.RouteBroadcastV1NodeReachabilityAnnounce):
		var req contractmessage.NodeReachabilityAnnounceReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability announce request failed: %w", err)
		}
		return marshalNodeReachabilityAnnounceQuotePayload(req.SignedAnnouncement)
	case string(contractroute.RouteBroadcastV1NodeReachabilityQuery):
		var req contractmessage.NodeReachabilityQueryReq
		if err := oldproto.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("decode broadcast node reachability query request failed: %w", err)
		}
		return marshalNodeReachabilityQueryQuotePayload(req.TargetNodePubkeyHex)
	default:
		return append([]byte(nil), body...), nil
	}
}

func ensurePeerFeePoolSessionForChargeLocked(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, paymentDomain string, charge uint64, option *ncall.PaymentOption, requireActiveFeePool bool) (poolcore.InfoResp, peer.AddrInfo, error) {
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
	if cur, ok := rt.getFeePool(gw.ID.String()); ok && cur != nil && strings.TrimSpace(cur.SpendTxID) != "" {
		if cur.Status != "active" {
			return info, gw, fmt.Errorf("fee pool session not active for peer=%s status=%s", gw.ID, strings.TrimSpace(cur.Status))
		}
		if cur.ClientAmount >= charge+cur.SpendTxFeeSat {
			return info, gw, nil
		}
		if requireActiveFeePool {
			return info, gw, errListenFeePoolRotateRequired
		}
	}
	if requireActiveFeePool {
		return info, gw, fmt.Errorf("active fee pool session missing for peer=%s", gw.ID)
	}
	autoRenewRounds := rt.ConfigSnapshot().Listen.AutoRenewRounds
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
	_, err = createFeePoolSessionWithSecurity(ctx, rt, store, gw, autoRenewRounds, info, gwSec(rt.rpcTrace), "")
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
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return feePoolChargeContext{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return feePoolChargeContext{}, fmt.Errorf("runtime not initialized")
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

func peerCallQuotedServiceType(route string) string {
	switch strings.TrimSpace(route) {
	case string(contractroute.RouteBroadcastV1ListenCycle):
		return poolcore.QuoteServiceTypeListenCycle
	case string(contractroute.RouteBroadcastV1DemandPublish):
		return quoteServiceTypeDemandPublish
	case string(contractroute.RouteBroadcastV1DemandPublishBatch):
		return quoteServiceTypeDemandPublishBatch
	case string(contractroute.RouteBroadcastV1LiveDemandPublish):
		return quoteServiceTypeLiveDemandPublish
	case string(contractroute.RouteBroadcastV1NodeReachabilityAnnounce):
		return quoteServiceTypeNodeReachabilityAnnounce
	case string(contractroute.RouteBroadcastV1NodeReachabilityQuery):
		return quoteServiceTypeNodeReachabilityQuery
	default:
		return strings.TrimSpace(route)
	}
}

func peerCallReceiptServiceType(route string) string {
	switch strings.TrimSpace(route) {
	case string(contractroute.RouteBroadcastV1ListenCycle):
		return serviceTypeListenCycle
	case string(contractroute.RouteBroadcastV1DemandPublish):
		return serviceTypeDemandPublish
	case string(contractroute.RouteBroadcastV1DemandPublishBatch):
		return serviceTypeDemandPublishBatch
	case string(contractroute.RouteBroadcastV1LiveDemandPublish):
		return serviceTypeLiveDemandPublish
	case string(contractroute.RouteBroadcastV1NodeReachabilityAnnounce):
		return serviceTypeNodeReachabilityAnnounce
	case string(contractroute.RouteBroadcastV1NodeReachabilityQuery):
		return serviceTypeNodeReachabilityQuery
	case string(contractroute.RouteDomainV1Resolve):
		return serviceTypeResolveName
	case string(contractroute.RouteDomainV1Query):
		return serviceTypeQueryName
	case string(contractroute.RouteDomainV1Lock):
		return serviceTypeRegisterLock
	case string(contractroute.RouteDomainV1SetTarget):
		return serviceTypeSetTarget
	default:
		return strings.TrimSpace(route)
	}
}

func describePeerCallQuoteQuantity(quote payflow.ServiceQuote) (uint64, string) {
	return 1, "call"
}

func decorateQuotedPeerCallPaymentOption(option *ncall.PaymentOption, amountSatoshi uint64, chargeReason string, quoteStatus string, quantity uint64, unit string) *ncall.PaymentOption {
	if option == nil {
		return nil
	}
	copyItem := *option
	copyItem.AmountSatoshi = amountSatoshi
	copyItem.Description = strings.TrimSpace(chargeReason)
	copyItem.PricingMode = normalizePeerCallPricingMode(option)
	copyItem.ServiceQuantity = quantity
	copyItem.ServiceQuantityUnit = strings.TrimSpace(unit)
	copyItem.QuoteStatus = strings.TrimSpace(quoteStatus)
	return &copyItem
}
