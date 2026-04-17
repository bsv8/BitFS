package clientapp

import (
	"testing"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	oldproto "github.com/golang/protobuf/proto"
)

func TestExpectedPeerCallResultPayloadForDomainQuery(t *testing.T) {
	resp := contractmessage.QueryNamePaidResp{
		Success:                  true,
		Status:                   "available",
		Name:                     "movie.david",
		Available:                true,
		RegisterPriceSatoshi:     1,
		RegisterSubmitFeeSatoshi: 1,
		RegisterLockFeeSatoshi:   1,
		SetTargetFeeSatoshi:      1,
		ResolveFeeSatoshi:        1,
		QueryFeeSatoshi:          1,
		ServiceReceipt:           []byte("signed-service-receipt"),
	}
	body, err := oldproto.Marshal(&resp)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := expectedPeerCallResultPayload(string(contractroute.RouteDomainV1Query), body)
	if err != nil {
		t.Fatalf("expectedPeerCallResultPayload() error = %v", err)
	}
	want, err := marshalQueryNameServicePayload(resp)
	if err != nil {
		t.Fatalf("MarshalQueryNameServicePayload() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch")
	}
}

func TestExpectedPeerCallResultPayloadForBroadcastDemandPublish(t *testing.T) {
	resp := contractmessage.DemandPublishPaidResp{
		Success:        true,
		Status:         "ok",
		DemandID:       "demand-1",
		Published:      true,
		Error:          "",
		ServiceReceipt: []byte("signed-service-receipt"),
	}
	body, err := oldproto.Marshal(&resp)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := expectedPeerCallResultPayload(string(contractroute.RouteBroadcastV1DemandPublish), body)
	if err != nil {
		t.Fatalf("expectedPeerCallResultPayload() error = %v", err)
	}
	want, err := marshalDemandPublishServicePayload(resp)
	if err != nil {
		t.Fatalf("MarshalDemandPublishServicePayload() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch")
	}
}

func TestExpectedPeerCallResultPayloadForBroadcastListenCycle(t *testing.T) {
	resp := contractmessage.ListenCyclePaidResp{
		Success:                true,
		Status:                 "ok",
		ChargedAmount:          123,
		UpdatedTxID:            "tx-1",
		GrantedDurationSeconds: 300,
		GrantedUntilUnix:       1234567890,
		ServiceReceipt:         []byte("signed-service-receipt"),
	}
	body, err := oldproto.Marshal(&resp)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := expectedPeerCallResultPayload(string(contractroute.RouteBroadcastV1ListenCycle), body)
	if err != nil {
		t.Fatalf("expectedPeerCallResultPayload() error = %v", err)
	}
	want, err := marshalListenCycleServicePayload(resp)
	if err != nil {
		t.Fatalf("MarshalListenCycleServicePayload() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch")
	}
}

func TestExpectedPeerCallResultPayloadForBroadcastNodeReachabilityQuery(t *testing.T) {
	resp := contractmessage.NodeReachabilityQueryPaidResp{
		Success:             true,
		Status:              "ok",
		Found:               true,
		TargetNodePubkeyHex: "021111111111111111111111111111111111111111111111111111111111111111",
		SignedAnnouncement:  []byte("announcement"),
		ServiceReceipt:      []byte("signed-service-receipt"),
	}
	body, err := oldproto.Marshal(&resp)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := expectedPeerCallResultPayload(string(contractroute.RouteBroadcastV1NodeReachabilityQuery), body)
	if err != nil {
		t.Fatalf("expectedPeerCallResultPayload() error = %v", err)
	}
	want, err := marshalNodeReachabilityQueryServicePayload(resp)
	if err != nil {
		t.Fatalf("MarshalNodeReachabilityQueryServicePayload() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch")
	}
}

func TestChoosePeerCallPaymentOptionFallsBackToChainTx(t *testing.T) {
	got, ok := choosePeerCallPaymentOption([]*ncall.PaymentOption{
		{Scheme: ncall.PaymentSchemeChainTxV1},
	}, "")
	if !ok || got == nil {
		t.Fatalf("choosePeerCallPaymentOption() did not return chain_tx_v1")
	}
	if got.Scheme != ncall.PaymentSchemeChainTxV1 {
		t.Fatalf("choosePeerCallPaymentOption() = %s, want %s", got.Scheme, ncall.PaymentSchemeChainTxV1)
	}
}

func TestChoosePeerCallPaymentOptionStillPrefersConfiguredScheme(t *testing.T) {
	got, ok := choosePeerCallPaymentOption([]*ncall.PaymentOption{
		{Scheme: ncall.PaymentSchemeChainTxV1},
		{Scheme: ncall.PaymentSchemePool2of2V1},
	}, ncall.PaymentSchemePool2of2V1)
	if !ok || got == nil {
		t.Fatalf("choosePeerCallPaymentOption() returned no option")
	}
	if got.Scheme != ncall.PaymentSchemePool2of2V1 {
		t.Fatalf("choosePeerCallPaymentOption() = %s, want %s", got.Scheme, ncall.PaymentSchemePool2of2V1)
	}
}

func TestChoosePeerCallPaymentOptionPrefersChainTxWhenConfigured(t *testing.T) {
	got, ok := choosePeerCallPaymentOption([]*ncall.PaymentOption{
		{Scheme: ncall.PaymentSchemePool2of2V1},
		{Scheme: ncall.PaymentSchemeChainTxV1},
	}, ncall.PaymentSchemeChainTxV1)
	if !ok || got == nil {
		t.Fatalf("choosePeerCallPaymentOption() returned no option")
	}
	if got.Scheme != ncall.PaymentSchemeChainTxV1 {
		t.Fatalf("choosePeerCallPaymentOption() = %s, want %s", got.Scheme, ncall.PaymentSchemeChainTxV1)
	}
}

func TestDecidePeerCallPaymentWithoutQuotedOptions(t *testing.T) {
	decision, err := decidePeerCallPayment(nil, "", nil)
	if err != nil {
		t.Fatalf("decidePeerCallPayment(default) error = %v", err)
	}
	if decision.Scheme != ncall.PaymentSchemePool2of2V1 {
		t.Fatalf("decidePeerCallPayment(default) = %s, want %s", decision.Scheme, ncall.PaymentSchemePool2of2V1)
	}
	decision, err = decidePeerCallPayment(nil, ncall.PaymentSchemeChainTxV1, nil)
	if err != nil {
		t.Fatalf("decidePeerCallPayment(chain_tx_v1) error = %v", err)
	}
	if decision.Scheme != ncall.PaymentSchemeChainTxV1 {
		t.Fatalf("decidePeerCallPayment(chain_tx_v1) = %s, want %s", decision.Scheme, ncall.PaymentSchemeChainTxV1)
	}
	if _, err := decidePeerCallPayment(nil, "unknown", nil); err == nil {
		t.Fatalf("decidePeerCallPayment(unknown) expected error")
	}
}

func TestPeerCallQuotedServiceTypeMapsBroadcastRoute(t *testing.T) {
	if got := peerCallQuotedServiceType(string(contractroute.RouteBroadcastV1DemandPublish)); got != quoteServiceTypeDemandPublish {
		t.Fatalf("peerCallQuotedServiceType(demand_publish) = %s, want %s", got, quoteServiceTypeDemandPublish)
	}
	if got := peerCallQuotedServiceType(string(contractroute.RouteBroadcastV1ListenCycle)); got != poolcore.QuoteServiceTypeListenCycle {
		t.Fatalf("peerCallQuotedServiceType(listen_cycle) = %s, want %s", got, poolcore.QuoteServiceTypeListenCycle)
	}
	if got := peerCallQuotedServiceType(string(contractroute.RouteDomainV1Query)); got != string(contractroute.RouteDomainV1Query) {
		t.Fatalf("peerCallQuotedServiceType(domain_query) = %s, want %s", got, string(contractroute.RouteDomainV1Query))
	}
}

func TestPeerCallReceiptServiceTypeMapsRoutes(t *testing.T) {
	if got := peerCallReceiptServiceType(string(contractroute.RouteBroadcastV1DemandPublish)); got != serviceTypeDemandPublish {
		t.Fatalf("peerCallReceiptServiceType(demand_publish) = %s, want %s", got, serviceTypeDemandPublish)
	}
	if got := peerCallReceiptServiceType(string(contractroute.RouteBroadcastV1ListenCycle)); got != serviceTypeListenCycle {
		t.Fatalf("peerCallReceiptServiceType(listen_cycle) = %s, want %s", got, serviceTypeListenCycle)
	}
	if got := peerCallReceiptServiceType(string(contractroute.RouteDomainV1SetTarget)); got != serviceTypeSetTarget {
		t.Fatalf("peerCallReceiptServiceType(set_target) = %s, want %s", got, serviceTypeSetTarget)
	}
}

func TestBuildPeerCallServiceParamsPayloadForListenCycle(t *testing.T) {
	body, err := oldproto.Marshal(&contractmessage.ListenCycleReq{
		RequestedDurationSeconds: 120,
		RequestedUntilUnix:       1234567890,
		ProposedPaymentSatoshi:   456,
	})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := buildPeerCallServiceParamsPayload(string(contractroute.RouteBroadcastV1ListenCycle), body)
	if err != nil {
		t.Fatalf("buildPeerCallServiceParamsPayload() error = %v", err)
	}
	payload, err := poolcore.UnmarshalListenCycleQuotePayload(got)
	if err != nil {
		t.Fatalf("UnmarshalListenCycleQuotePayload() error = %v", err)
	}
	if payload.RequestedDurationSeconds != 120 {
		t.Fatalf("RequestedDurationSeconds = %d", payload.RequestedDurationSeconds)
	}
	if payload.RequestedUntilUnix != 1234567890 {
		t.Fatalf("RequestedUntilUnix = %d", payload.RequestedUntilUnix)
	}
	if payload.ProposedPaymentSatoshi != 456 {
		t.Fatalf("ProposedPaymentSatoshi = %d", payload.ProposedPaymentSatoshi)
	}
}
