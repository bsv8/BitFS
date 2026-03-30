package clientapp

import (
	"testing"

	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	oldproto "github.com/golang/protobuf/proto"
)

func TestDecodeDemandPublishRouteRespUsesTopLevelReceipts(t *testing.T) {
	body, err := oldproto.Marshal(&broadcastmodule.DemandPublishPaidResp{
		Success:  true,
		Status:   "ok",
		DemandID: "dmd_1",
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	paymentReceipt, err := oldproto.Marshal(&ncall.FeePool2of2Receipt{
		ChargedAmountSatoshi: 9,
		UpdatedTxID:          "tx_9",
		MergedCurrentTx:      []byte("merged"),
		ProofStatePayload:    []byte("proof"),
	})
	if err != nil {
		t.Fatalf("marshal payment receipt: %v", err)
	}
	resp, err := decodeDemandPublishRouteResp(ncall.CallResp{
		Body:                 body,
		PaymentReceiptScheme: ncall.PaymentSchemePool2of2V1,
		PaymentReceipt:       paymentReceipt,
		ServiceReceipt:       []byte("service"),
	})
	if err != nil {
		t.Fatalf("decodeDemandPublishRouteResp() error = %v", err)
	}
	if resp.ChargedAmount != 9 {
		t.Fatalf("ChargedAmount = %d", resp.ChargedAmount)
	}
	if resp.UpdatedTxID != "tx_9" {
		t.Fatalf("UpdatedTxID = %q", resp.UpdatedTxID)
	}
	if string(resp.ServiceReceipt) != "service" {
		t.Fatalf("ServiceReceipt = %q", string(resp.ServiceReceipt))
	}
}
