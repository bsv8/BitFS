package clientapp

import (
	"testing"

	domainmodule "github.com/bsv8/BFTP/pkg/modules/domain"
	oldproto "github.com/golang/protobuf/proto"
)

func TestExpectedPeerCallResultPayloadForDomainQuery(t *testing.T) {
	resp := domainmodule.QueryNamePaidResp{
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
	got, err := expectedPeerCallResultPayload(domainmodule.RouteDomainV1Query, body)
	if err != nil {
		t.Fatalf("expectedPeerCallResultPayload() error = %v", err)
	}
	want, err := domainmodule.MarshalQueryNameServicePayload(resp)
	if err != nil {
		t.Fatalf("MarshalQueryNameServicePayload() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch")
	}
}
