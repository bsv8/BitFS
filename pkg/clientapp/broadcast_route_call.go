package clientapp

import (
	"context"
	"fmt"
	"strings"

	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	oldproto "github.com/golang/protobuf/proto"
)

func marshalFeePoolPaymentPayload(session *feePoolSession, quoted feePoolServiceQuoteBuilt, clientSignature []byte, proofIntent []byte, signedProofCommit []byte) ([]byte, error) {
	if session == nil {
		return nil, fmt.Errorf("fee pool session missing")
	}
	return oldproto.Marshal(&ncall.FeePool2of2Payment{
		SpendTxID:           strings.TrimSpace(session.SpendTxID),
		SequenceNumber:      quoted.NextSequence,
		ServerAmount:        quoted.NextServerAmount,
		ChargeAmountSatoshi: quoted.ServiceQuote.ChargeAmountSatoshi,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), clientSignature...),
		ChargeReason:        strings.TrimSpace(quoted.ChargeReason),
		ProofIntent:         append([]byte(nil), proofIntent...),
		SignedProofCommit:   append([]byte(nil), signedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	})
}

func triggerTypedPeerCall[T any](ctx context.Context, rt *Runtime, to string, route string, body oldproto.Message, decode func(ncall.CallResp) (T, error)) (T, ncall.CallResp, error) {
	var zero T
	if decode == nil {
		return zero, ncall.CallResp{}, fmt.Errorf("route response decoder missing")
	}
	rawBody, err := oldproto.Marshal(body)
	if err != nil {
		return zero, ncall.CallResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(to),
		Route:       strings.TrimSpace(route),
		ContentType: ncall.ContentTypeProto,
		Body:        rawBody,
	})
	if err != nil {
		return zero, ncall.CallResp{}, err
	}
	resp, err := decode(callResp)
	if err != nil {
		return zero, ncall.CallResp{}, err
	}
	return resp, callResp, nil
}

func decodeListenCycleRouteResp(callResp ncall.CallResp) (broadcastmodule.ListenCyclePaidResp, error) {
	var resp broadcastmodule.ListenCyclePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.ListenCyclePaidResp{}, err
	}
	if receipt, ok := parseFeePoolRouteReceipt(callResp); ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
	return resp, nil
}

func decodeDemandPublishRouteResp(callResp ncall.CallResp) (broadcastmodule.DemandPublishPaidResp, error) {
	var resp broadcastmodule.DemandPublishPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.DemandPublishPaidResp{}, err
	}
	applyFeePoolRouteReceiptToDemandPublishResp(&resp, callResp)
	return resp, nil
}

func decodeDemandPublishBatchRouteResp(callResp ncall.CallResp) (broadcastmodule.DemandPublishBatchPaidResp, error) {
	var resp broadcastmodule.DemandPublishBatchPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.DemandPublishBatchPaidResp{}, err
	}
	applyFeePoolRouteReceiptToDemandPublishBatchResp(&resp, callResp)
	return resp, nil
}

func decodeLiveDemandPublishRouteResp(callResp ncall.CallResp) (broadcastmodule.LiveDemandPublishPaidResp, error) {
	var resp broadcastmodule.LiveDemandPublishPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.LiveDemandPublishPaidResp{}, err
	}
	applyFeePoolRouteReceiptToLiveDemandPublishResp(&resp, callResp)
	return resp, nil
}

func decodeNodeReachabilityAnnounceRouteResp(callResp ncall.CallResp) (broadcastmodule.NodeReachabilityAnnouncePaidResp, error) {
	var resp broadcastmodule.NodeReachabilityAnnouncePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	applyFeePoolRouteReceiptToNodeReachabilityAnnounceResp(&resp, callResp)
	return resp, nil
}

func decodeNodeReachabilityQueryRouteResp(callResp ncall.CallResp) (broadcastmodule.NodeReachabilityQueryPaidResp, error) {
	var resp broadcastmodule.NodeReachabilityQueryPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	applyFeePoolRouteReceiptToNodeReachabilityQueryResp(&resp, callResp)
	return resp, nil
}

func applyFeePoolRouteReceiptToDemandPublishResp(resp *broadcastmodule.DemandPublishPaidResp, callResp ncall.CallResp) {
	if resp == nil {
		return
	}
	receipt, ok := parseFeePoolRouteReceipt(callResp)
	if ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
}

func applyFeePoolRouteReceiptToDemandPublishBatchResp(resp *broadcastmodule.DemandPublishBatchPaidResp, callResp ncall.CallResp) {
	if resp == nil {
		return
	}
	receipt, ok := parseFeePoolRouteReceipt(callResp)
	if ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
}

func applyFeePoolRouteReceiptToLiveDemandPublishResp(resp *broadcastmodule.LiveDemandPublishPaidResp, callResp ncall.CallResp) {
	if resp == nil {
		return
	}
	receipt, ok := parseFeePoolRouteReceipt(callResp)
	if ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
}

func applyFeePoolRouteReceiptToNodeReachabilityAnnounceResp(resp *broadcastmodule.NodeReachabilityAnnouncePaidResp, callResp ncall.CallResp) {
	if resp == nil {
		return
	}
	receipt, ok := parseFeePoolRouteReceipt(callResp)
	if ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
}

func applyFeePoolRouteReceiptToNodeReachabilityQueryResp(resp *broadcastmodule.NodeReachabilityQueryPaidResp, callResp ncall.CallResp) {
	if resp == nil {
		return
	}
	receipt, ok := parseFeePoolRouteReceipt(callResp)
	if ok {
		resp.ChargedAmount = receipt.ChargedAmountSatoshi
		resp.UpdatedTxID = receipt.UpdatedTxID
		resp.MergedCurrentTx = append([]byte(nil), receipt.MergedCurrentTx...)
		resp.ProofStatePayload = append([]byte(nil), receipt.ProofStatePayload...)
	}
	resp.ServiceReceipt = append([]byte(nil), callResp.ServiceReceipt...)
}

func parseFeePoolRouteReceipt(callResp ncall.CallResp) (ncall.FeePool2of2Receipt, bool) {
	if strings.TrimSpace(callResp.PaymentReceiptScheme) != ncall.PaymentSchemePool2of2V1 || len(callResp.PaymentReceipt) == 0 {
		return ncall.FeePool2of2Receipt{}, false
	}
	var receipt ncall.FeePool2of2Receipt
	if err := oldproto.Unmarshal(callResp.PaymentReceipt, &receipt); err != nil {
		return ncall.FeePool2of2Receipt{}, false
	}
	return receipt, true
}
