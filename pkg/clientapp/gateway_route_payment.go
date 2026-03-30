package clientapp

import (
	"context"
	"fmt"
	"strings"

	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// gatewayRouteFeePoolCallArgs 把“按费用池支付调用 gateway route”收成一个统一动作。
// 设计说明：
// - 这里负责 quote、组 proof、签名、发起带支付的 node.call；
// - 调用方只需要关心业务 body 和业务回执，不再重复拼支付链；
// - 整个流程在 gateway 维度的 pay mutex 内完成，避免 sequence/server_amount 被并发打乱。
type gatewayRouteFeePoolCallArgs[T any] struct {
	GatewayPeerID peer.ID
	Charge        uint64
	ServiceType   string
	Target        string
	QuotePayload  []byte
	Route         string
	Body          oldproto.Message
	Decode        func(ncall.CallResp) (T, error)
}

type gatewayRouteFeePoolCallState[T any] struct {
	Session      *feePoolSession
	Quoted       feePoolServiceQuoteBuilt
	Response     T
	UpdatedTxHex string
}

type gatewayRouteChainTxCallArgs[T any] struct {
	GatewayPeerID peer.ID
	Charge        uint64
	ServiceType   string
	Target        string
	QuotePayload  []byte
	Route         string
	Body          oldproto.Message
	Decode        func(ncall.CallResp) (T, error)
}

type gatewayRouteChainTxCallState[T any] struct {
	Quoted   feePoolServiceQuoteBuilt
	Built    builtPeerCallChainTx
	Response T
	Receipt  ncall.ChainTxV1Receipt
}

func withGatewayRouteFeePoolCall[T any](ctx context.Context, rt *Runtime, args gatewayRouteFeePoolCallArgs[T], after func(gatewayRouteFeePoolCallState[T]) error) (T, error) {
	var zero T
	if rt == nil || rt.Host == nil {
		return zero, fmt.Errorf("runtime not initialized")
	}
	if args.Decode == nil {
		return zero, fmt.Errorf("route response decoder missing")
	}
	if args.Body == nil {
		return zero, fmt.Errorf("route body missing")
	}
	if args.GatewayPeerID == "" {
		return zero, fmt.Errorf("gateway peer id missing")
	}
	payMu := rt.feePoolPayMutex(args.GatewayPeerID.String())
	payMu.Lock()
	defer payMu.Unlock()

	session, ok := rt.getFeePool(args.GatewayPeerID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return zero, fmt.Errorf("fee pool session missing for gateway=%s", args.GatewayPeerID.String())
	}
	charge := args.Charge
	if charge == 0 {
		charge = 1
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		Session:              session,
		GatewayPeerID:        args.GatewayPeerID,
		ServiceType:          strings.TrimSpace(args.ServiceType),
		Target:               strings.TrimSpace(args.Target),
		ServiceParamsPayload: append([]byte(nil), args.QuotePayload...),
		PricingMode:          poolcore.ServiceOfferPricingModeFixedPrice,
		ProposedPaymentSat:   charge,
	})
	if err != nil {
		return zero, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return zero, err
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
		return zero, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(built.UpdatedTx, clientActor.PrivKey, quoted.GatewayPub)
	if err != nil {
		return zero, err
	}
	bodyRaw, err := oldproto.Marshal(args.Body)
	if err != nil {
		return zero, err
	}
	paymentPayload, err := marshalFeePoolPaymentPayload(session, quoted, append([]byte(nil), (*clientSig)...), built.ProofIntent, built.SignedProofCommit)
	if err != nil {
		return zero, err
	}
	callResp, err := callNodeRoute(ctx, rt, args.GatewayPeerID, ncall.CallReq{
		To:             gatewayBusinessID(rt, args.GatewayPeerID),
		Route:          strings.TrimSpace(args.Route),
		ContentType:    ncall.ContentTypeProto,
		Body:           bodyRaw,
		PaymentScheme:  ncall.PaymentSchemePool2of2V1,
		PaymentPayload: paymentPayload,
	})
	if err != nil {
		return zero, err
	}
	resp, err := args.Decode(callResp)
	if err != nil {
		return zero, err
	}
	state := gatewayRouteFeePoolCallState[T]{
		Session:      session,
		Quoted:       quoted,
		Response:     resp,
		UpdatedTxHex: built.UpdatedTx.Hex(),
	}
	if after != nil {
		if err := after(state); err != nil {
			return zero, err
		}
	}
	return resp, nil
}

func withGatewayRouteChainTxCall[T any](ctx context.Context, rt *Runtime, args gatewayRouteChainTxCallArgs[T], after func(gatewayRouteChainTxCallState[T]) error) (T, error) {
	var zero T
	if rt == nil || rt.Host == nil {
		return zero, fmt.Errorf("runtime not initialized")
	}
	if args.Decode == nil {
		return zero, fmt.Errorf("route response decoder missing")
	}
	if args.Body == nil {
		return zero, fmt.Errorf("route body missing")
	}
	if args.GatewayPeerID == "" {
		return zero, fmt.Errorf("gateway peer id missing")
	}
	charge := args.Charge
	if charge == 0 {
		charge = 1
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		GatewayPeerID:        args.GatewayPeerID,
		ServiceType:          strings.TrimSpace(args.ServiceType),
		Target:               strings.TrimSpace(args.Target),
		ServiceParamsPayload: append([]byte(nil), args.QuotePayload...),
		PricingMode:          poolcore.ServiceOfferPricingModeFixedPrice,
		ProposedPaymentSat:   charge,
	})
	if err != nil {
		return zero, err
	}
	built, err := buildPeerCallChainTx(rt, quoted.GatewayPub, quoted.ServiceQuoteRaw, quoted.ServiceQuote)
	if err != nil {
		return zero, err
	}
	bodyRaw, err := oldproto.Marshal(args.Body)
	if err != nil {
		return zero, err
	}
	paymentPayload, err := oldproto.Marshal(&ncall.ChainTxV1Payment{
		RawTx: append([]byte(nil), built.RawTx...),
	})
	if err != nil {
		return zero, err
	}
	callResp, err := callNodeRoute(ctx, rt, args.GatewayPeerID, ncall.CallReq{
		To:             gatewayBusinessID(rt, args.GatewayPeerID),
		Route:          strings.TrimSpace(args.Route),
		ContentType:    ncall.ContentTypeProto,
		Body:           bodyRaw,
		PaymentScheme:  ncall.PaymentSchemeChainTxV1,
		PaymentPayload: paymentPayload,
	})
	if err != nil {
		return zero, err
	}
	if strings.TrimSpace(callResp.PaymentReceiptScheme) != ncall.PaymentSchemeChainTxV1 || len(callResp.PaymentReceipt) == 0 {
		return zero, fmt.Errorf("payment receipt missing")
	}
	var receipt ncall.ChainTxV1Receipt
	if err := oldproto.Unmarshal(callResp.PaymentReceipt, &receipt); err != nil {
		return zero, err
	}
	if txid := strings.ToLower(strings.TrimSpace(receipt.PaymentTxID)); txid != "" && !strings.EqualFold(txid, built.TxID) {
		return zero, fmt.Errorf("payment receipt txid mismatch")
	}
	resp, err := args.Decode(callResp)
	if err != nil {
		return zero, err
	}
	state := gatewayRouteChainTxCallState[T]{
		Quoted:   quoted,
		Built:    built,
		Response: resp,
		Receipt:  receipt,
	}
	if after != nil {
		if err := after(state); err != nil {
			return zero, err
		}
	}
	return resp, nil
}
