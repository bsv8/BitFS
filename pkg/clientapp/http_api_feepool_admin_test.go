package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandleAdminFeePoolLegacy_BasicAvailability 验证旧分表接口仍可用（兼容口）。
// 第七轮：这些接口不再承载主产品语义，测试只保留基本可用性，但要求"至少查得到 1 条真实数据 + detail 能打开"。
func TestHandleAdminFeePoolLegacy_BasicAvailability(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	_ = dbAppendCommandJournal(nil, newClientDB(db, nil), commandJournalEntry{
		CommandID:     "cmd-1",
		CommandType:   "ensure_active",
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "e2e",
		RequestedAt:   1700000000,
		Accepted:      true,
		Status:        "paused",
		ErrorCode:     "wallet_insufficient",
		ErrorMessage:  "not enough balance",
		StateBefore:   "idle",
		StateAfter:    "paused_insufficient",
		DurationMS:    12,
		Payload:       map[string]any{"x": 1},
		Result:        map[string]any{"status": "paused"},
	})
	_ = dbAppendDomainEvent(nil, newClientDB(db, nil), domainEventEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_paused_insufficient",
		StateBefore:   "idle",
		StateAfter:    "paused_insufficient",
		Payload:       map[string]any{"need": 100000, "have": 12345},
	})
	_ = dbAppendStateSnapshot(nil, newClientDB(db, nil), stateSnapshotEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		State:         "paused_insufficient",
		PauseReason:   "wallet_insufficient",
		PauseNeedSat:  100000,
		PauseHaveSat:  12345,
		LastError:     "not enough balance",
		Payload:       map[string]any{"source": "test"},
	})
	_ = dbAppendObservedGatewayState(nil, newClientDB(db, nil), observedGatewayStateEntry{
		GatewayPeerID:  "gw1",
		SourceRef:      "gw1",
		ObservedAtUnix: 1700000002,
		EventName:      "fee_pool_resumed_by_wallet_probe",
		StateBefore:    "paused_insufficient",
		StateAfter:     "idle",
		PauseHaveSat:   999999,
		Payload:        observedGatewayStatePayload{ObservedReason: "wallet_probe", WalletBalanceSatoshi: 999999, Extra: map[string]any{}},
	})
	_ = dbAppendEffectLog(nil, newClientDB(db, nil), effectLogEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		EffectType:    "chain",
		Stage:         "fee_pool_open",
		Status:        "paused",
		ErrorMessage:  "not enough balance",
		Payload:       map[string]any{"need": 100000, "have": 12345},
	})

	// 统一的基本可用性断言：列表必须返回至少 1 条，detail 必须能打开且非空
	assertListOK := func(t *testing.T, name string, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusOK {
			t.Fatalf("%s list status mismatch: got=%d want=%d body=%s", name, rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int           `json:"total"`
			Items []interface{} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s list decode failed: %v", name, err)
		}
		if out.Total < 1 {
			t.Fatalf("%s list total mismatch: got=%d want>=1", name, out.Total)
		}
		if len(out.Items) < 1 {
			t.Fatalf("%s list items mismatch: got=%d want>=1", name, len(out.Items))
		}
	}
	assertDetailOK := func(t *testing.T, name string, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusOK {
			t.Fatalf("%s detail status mismatch: got=%d want=%d body=%s", name, rec.Code, http.StatusOK, rec.Body.String())
		}
		var out map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s detail decode failed: %v", name, err)
		}
		if len(out) == 0 {
			t.Fatalf("%s detail returned empty object", name)
		}
	}

	// commands
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?gateway_pubkey_hex=gw1&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolCommands(rec, req)
		assertListOK(t, "commands", rec)

		var list struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatalf("commands list unmarshal: %v", err)
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands/detail?id="+itoa64(list.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolCommandDetail(drec, dreq)
		assertDetailOK(t, "commands", drec)
	}

	// events
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/events?event_name=fee_pool_paused_insufficient&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolEvents(rec, req)
		assertListOK(t, "events", rec)

		var list struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatalf("events list unmarshal: %v", err)
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/events/detail?id="+itoa64(list.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolEventDetail(drec, dreq)
		assertDetailOK(t, "events", drec)
	}

	// states
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/states?state=paused_insufficient&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolStates(rec, req)
		assertListOK(t, "states", rec)

		var list struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatalf("states list unmarshal: %v", err)
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/states/detail?id="+itoa64(list.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolStateDetail(drec, dreq)
		assertDetailOK(t, "states", drec)
	}

	// observed-states
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states?event_name=fee_pool_resumed_by_wallet_probe&gateway_pubkey_hex=gw1&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolObservedStates(rec, req)
		assertListOK(t, "observed-states", rec)

		var list struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatalf("observed-states list unmarshal: %v", err)
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states/detail?id="+itoa64(list.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolObservedStateDetail(drec, dreq)
		assertDetailOK(t, "observed-states", drec)
	}

	// effects
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/effects?stage=fee_pool_open&status=paused&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolEffects(rec, req)
		assertListOK(t, "effects", rec)

		var list struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Fatalf("effects list unmarshal: %v", err)
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/effects/detail?id="+itoa64(list.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolEffectDetail(drec, dreq)
		assertDetailOK(t, "effects", drec)
	}
}
