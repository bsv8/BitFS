package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/nodesvc"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type feePoolChargeContext struct {
	Session          *feePoolSession
	Charge           uint64
	NextSeq          uint32
	NextServerAmount uint64
	UpdatedTxHex     string
	ClientSignature  []byte
}

func retryPeerCallWithAutoPayment(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, options []*nodesvc.PaymentOption) (nodesvc.CallResp, error) {
	option, ok := choosePeerCallPaymentOption(options)
	if !ok {
		return nodesvc.CallResp{}, fmt.Errorf("no supported payment option")
	}
	switch strings.TrimSpace(option.Scheme) {
	case nodesvc.PaymentSchemePool2of2V1:
		return retryPeerCallWithFeePool2of2(ctx, rt, peerID, req, option)
	default:
		return nodesvc.CallResp{}, fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(option.Scheme))
	}
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

func retryPeerCallWithFeePool2of2(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq, option *nodesvc.PaymentOption) (nodesvc.CallResp, error) {
	if rt == nil || rt.Host == nil {
		return nodesvc.CallResp{}, fmt.Errorf("runtime not initialized")
	}
	if option == nil {
		return nodesvc.CallResp{}, fmt.Errorf("payment option missing")
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
		return nodesvc.CallResp{}, err
	}
	chargeCtx, err := prepareFeePoolCharge(rt, gw.ID, charge)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	paymentPayload, err := oldproto.Marshal(&nodesvc.FeePool2of2Payment{
		SpendTxID:           chargeCtx.Session.SpendTxID,
		SequenceNumber:      chargeCtx.NextSeq,
		ServerAmount:        chargeCtx.NextServerAmount,
		ChargeAmountSatoshi: charge,
		Fee:                 chargeCtx.Session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), chargeCtx.ClientSignature...),
		ChargeReason:        strings.TrimSpace(option.Description),
	})
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	paidReq := req
	paidReq.PaymentScheme = nodesvc.PaymentSchemePool2of2V1
	paidReq.PaymentPayload = paymentPayload
	out, err := callNodeRoute(ctx, rt, gw.ID, paidReq)
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
		applyFeePoolChargeToSession(chargeCtx.Session, chargeCtx.NextSeq, chargeCtx.NextServerAmount, chargeCtx.UpdatedTxHex)
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
	if rt == nil || rt.Host == nil {
		return info, peer.AddrInfo{}, fmt.Errorf("runtime not initialized")
	}
	sec, err := remotePaymentSec(rt, paymentDomain)
	if err != nil {
		return info, peer.AddrInfo{}, err
	}
	if err := p2prpc.CallProto(ctx, rt.Host, peerID, dual2of2.ProtoFeePoolInfo, sec, dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
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
	_, err = createFeePoolSessionWithSecurity(ctx, rt, gw, autoRenewRounds, info, sec)
	if err != nil {
		return info, gw, err
	}
	return info, gw, nil
}

func prepareFeePoolCharge(rt *Runtime, targetPeerID peer.ID, charge uint64) (feePoolChargeContext, error) {
	session, ok := rt.getFeePool(targetPeerID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return feePoolChargeContext{}, fmt.Errorf("fee pool session missing for peer=%s", targetPeerID)
	}
	if charge == 0 {
		charge = 1
	}
	serverPubKey := rt.Host.Peerstore().PubKey(targetPeerID)
	if serverPubKey == nil {
		return feePoolChargeContext{}, fmt.Errorf("missing peer pubkey")
	}
	raw, err := serverPubKey.Raw()
	if err != nil {
		return feePoolChargeContext{}, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return feePoolChargeContext{}, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return feePoolChargeContext{}, err
	}
	nextSeq := session.Sequence + 1
	nextServerAmount := session.ServerAmount + charge
	updatedTx, err := ce.LoadTx(session.CurrentTxHex, nil, nextSeq, nextServerAmount, serverPub, clientActor.PubKey, session.PoolAmountSat)
	if err != nil {
		return feePoolChargeContext{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return feePoolChargeContext{}, err
	}
	return feePoolChargeContext{
		Session:          session,
		Charge:           charge,
		NextSeq:          nextSeq,
		NextServerAmount: nextServerAmount,
		UpdatedTxHex:     updatedTx.Hex(),
		ClientSignature:  append([]byte(nil), (*clientSig)...),
	}, nil
}

func remotePaymentSec(rt *Runtime, paymentDomain string) (p2prpc.SecurityConfig, error) {
	domain := strings.TrimSpace(paymentDomain)
	if domain == "" {
		return p2prpc.SecurityConfig{}, fmt.Errorf("payment domain is required")
	}
	network := "test"
	if rt != nil && strings.TrimSpace(rt.runIn.BSV.Network) != "" {
		network = strings.ToLower(strings.TrimSpace(rt.runIn.BSV.Network))
	}
	return p2prpc.SecurityConfig{
		Domain:  domain,
		Network: network,
		TTL:     30 * time.Second,
		Trace:   rt.rpcTrace,
	}, nil
}
