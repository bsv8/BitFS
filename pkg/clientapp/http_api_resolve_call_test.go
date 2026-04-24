package clientapp

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	oldproto "github.com/golang/protobuf/proto"
)

func TestRouteCallPaymentHTTPExtrasIncludesUnifiedPaymentFields(t *testing.T) {
	receiptRaw, err := oldproto.Marshal(&ncall.FeePool2of2Receipt{
		ChargedAmountSatoshi: 7,
		UpdatedTxID:          "tx-7",
	})
	if err != nil {
		t.Fatalf("marshal payment receipt: %v", err)
	}
	serviceQuoteRaw := []byte(`{"status":"accepted"}`)
	serviceReceiptRaw := []byte("signed-service-receipt")

	out := routeCallHTTPResponse(true, "OK", "ok", "application/json", []byte(`{"ok":true}`), routeCallPaymentHTTPExtras(ncall.CallResp{
		PaymentSchemes: []*ncall.PaymentOption{
			{
				Scheme:        ncall.PaymentSchemePool2of2V1,
				AmountSatoshi: 7,
				QuoteStatus:   "accepted",
			},
		},
		PaymentReceiptScheme: ncall.PaymentSchemePool2of2V1,
		PaymentReceipt:       receiptRaw,
		ServiceQuote:         serviceQuoteRaw,
		ServiceReceipt:       serviceReceiptRaw,
	}))

	paymentReceiptBase64, _ := out["payment_receipt_base64"].(string)
	if paymentReceiptBase64 != base64.StdEncoding.EncodeToString(receiptRaw) {
		t.Fatalf("payment_receipt_base64 mismatch")
	}
	serviceQuoteBase64, _ := out["service_quote_base64"].(string)
	if serviceQuoteBase64 != base64.StdEncoding.EncodeToString(serviceQuoteRaw) {
		t.Fatalf("service_quote_base64 mismatch")
	}
	serviceReceiptBase64, _ := out["service_receipt_base64"].(string)
	if serviceReceiptBase64 != base64.StdEncoding.EncodeToString(serviceReceiptRaw) {
		t.Fatalf("service_receipt_base64 mismatch")
	}
	if _, ok := out["payment_receipt"]; !ok {
		t.Fatalf("payment_receipt missing")
	}
	serviceQuote, ok := out["service_quote"]
	if !ok {
		t.Fatalf("service_quote missing")
	}
	encoded, err := json.Marshal(serviceQuote)
	if err != nil {
		t.Fatalf("marshal service_quote: %v", err)
	}
	if string(encoded) != string(serviceQuoteRaw) {
		t.Fatalf("service_quote mismatch: got=%s want=%s", string(encoded), string(serviceQuoteRaw))
	}
}
