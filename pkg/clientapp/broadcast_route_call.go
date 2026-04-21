package clientapp

import (
	"context"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func triggerTypedPeerCall[T any](ctx context.Context, store *clientDB, rt *Runtime, to string, protoID protocol.ID, body oldproto.Message, decode func(ncall.CallResp) (T, error)) (T, ncall.CallResp, error) {
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
		ProtocolID:  protoID,
		ContentType: contractmessage.ContentTypeProto,
		Body:        rawBody,
		Store:       store,
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

func decodeListenCycleRouteResp(callResp ncall.CallResp) (contractmessage.ListenCyclePaidResp, error) {
	var resp contractmessage.ListenCyclePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.ListenCyclePaidResp{}, err
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

func decodeDemandPublishRouteResp(callResp ncall.CallResp) (contractmessage.DemandPublishPaidResp, error) {
	var resp contractmessage.DemandPublishPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.DemandPublishPaidResp{}, err
	}
	applyFeePoolRouteReceiptToDemandPublishResp(&resp, callResp)
	return resp, nil
}

func decodeDemandPublishBatchRouteResp(callResp ncall.CallResp) (contractmessage.DemandPublishBatchPaidResp, error) {
	var resp contractmessage.DemandPublishBatchPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.DemandPublishBatchPaidResp{}, err
	}
	applyFeePoolRouteReceiptToDemandPublishBatchResp(&resp, callResp)
	return resp, nil
}

func decodeLiveDemandPublishRouteResp(callResp ncall.CallResp) (contractmessage.LiveDemandPublishPaidResp, error) {
	var resp contractmessage.LiveDemandPublishPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.LiveDemandPublishPaidResp{}, err
	}
	applyFeePoolRouteReceiptToLiveDemandPublishResp(&resp, callResp)
	return resp, nil
}

func decodeNodeReachabilityAnnounceRouteResp(callResp ncall.CallResp) (contractmessage.NodeReachabilityAnnouncePaidResp, error) {
	var resp contractmessage.NodeReachabilityAnnouncePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	applyFeePoolRouteReceiptToNodeReachabilityAnnounceResp(&resp, callResp)
	return resp, nil
}

func decodeNodeReachabilityQueryRouteResp(callResp ncall.CallResp) (contractmessage.NodeReachabilityQueryPaidResp, error) {
	var resp contractmessage.NodeReachabilityQueryPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	applyFeePoolRouteReceiptToNodeReachabilityQueryResp(&resp, callResp)
	return resp, nil
}

func applyFeePoolRouteReceiptToDemandPublishResp(resp *contractmessage.DemandPublishPaidResp, callResp ncall.CallResp) {
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

func applyFeePoolRouteReceiptToDemandPublishBatchResp(resp *contractmessage.DemandPublishBatchPaidResp, callResp ncall.CallResp) {
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

func applyFeePoolRouteReceiptToLiveDemandPublishResp(resp *contractmessage.LiveDemandPublishPaidResp, callResp ncall.CallResp) {
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

func applyFeePoolRouteReceiptToNodeReachabilityAnnounceResp(resp *contractmessage.NodeReachabilityAnnouncePaidResp, callResp ncall.CallResp) {
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

func applyFeePoolRouteReceiptToNodeReachabilityQueryResp(resp *contractmessage.NodeReachabilityQueryPaidResp, callResp ncall.CallResp) {
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
