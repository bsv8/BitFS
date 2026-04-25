package domainclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type httpSettingsBackendStub struct {
	*fakeBusinessStore
	*fakeRuntimePorts
	summary    FrontOrderSettlementSummary
	summaryErr error
}

func (s httpSettingsBackendStub) GetFrontOrderSettlementSummary(context.Context, string) (FrontOrderSettlementSummary, error) {
	return s.summary, s.summaryErr
}

func TestHandleDomainSettlementStatusUsesStandardHTTPEnvelope(t *testing.T) {
	t.Parallel()

	svc := &service{
		backend: httpSettingsBackendStub{
			fakeBusinessStore: &fakeBusinessStore{},
			fakeRuntimePorts:  &fakeRuntimePorts{},
			summary: FrontOrderSettlementSummary{
				FrontOrderID: "fo-1",
				Summary: SettlementTotalSummary{
					OverallStatus:      "pending",
					TotalTargetSatoshi: 10,
				},
			},
		},
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/domains/settlement-status?front_order_id=fo-1", nil)
	svc.handleDomainSettlementStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["status"] != "ok" {
		t.Fatalf("unexpected status envelope: %#v", got["status"])
	}
	data, ok := got["data"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected data envelope: %#v", got["data"])
	}
	if data["front_order_id"] != "fo-1" {
		t.Fatalf("unexpected front_order_id: %#v", data["front_order_id"])
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/domains/settlement-status", nil)
	svc.handleDomainSettlementStatus(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected error status: %d", rr.Code)
	}
	got = nil
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got["status"] != "error" {
		t.Fatalf("unexpected error envelope: %#v", got["status"])
	}
	errBody, ok := got["error"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected error payload: %#v", got["error"])
	}
	if errBody["code"] != "BAD_REQUEST" {
		t.Fatalf("unexpected error code: %#v", errBody["code"])
	}
}
