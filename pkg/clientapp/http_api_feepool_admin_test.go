package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleAdminFeePoolLogs_ListAndDetail(t *testing.T) {
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
	var observedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM observed_gateway_states WHERE gateway_pubkey_hex='gw1'`).Scan(&observedCount); err != nil {
		t.Fatalf("count observed gateway states failed: %v", err)
	}
	if observedCount != 1 {
		t.Fatalf("expected exactly one observed gateway state, got=%d", observedCount)
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands?gateway_pubkey_hex=gw1&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolCommands(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("commands status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int `json:"total"`
			Items []struct {
				ID      int64           `json:"id"`
				Status  string          `json:"status"`
				Payload json.RawMessage `json:"payload"`
				Result  json.RawMessage `json:"result"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode commands list: %v", err)
		}
		if out.Total != 1 || len(out.Items) != 1 {
			t.Fatalf("commands total/items mismatch: total=%d items=%d", out.Total, len(out.Items))
		}
		if out.Items[0].Status != "paused" || len(out.Items[0].Payload) == 0 || len(out.Items[0].Result) == 0 {
			t.Fatalf("unexpected commands row: %+v", out.Items[0])
		}

		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands/detail?id="+itoa64(out.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolCommandDetail(drec, dreq)
		if drec.Code != http.StatusOK {
			t.Fatalf("commands detail status mismatch: got=%d want=%d body=%s", drec.Code, http.StatusOK, drec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/events?event_name=fee_pool_paused_insufficient&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolEvents(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("events status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int `json:"total"`
			Items []struct {
				ID      int64           `json:"id"`
				Event   string          `json:"event_name"`
				State   string          `json:"state_after"`
				Payload json.RawMessage `json:"payload"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode events list: %v", err)
		}
		if out.Total != 1 || len(out.Items) != 1 {
			t.Fatalf("events total/items mismatch: total=%d items=%d", out.Total, len(out.Items))
		}
		if out.Items[0].Event != "fee_pool_paused_insufficient" || out.Items[0].State != "paused_insufficient" || len(out.Items[0].Payload) == 0 {
			t.Fatalf("unexpected events row: %+v", out.Items[0])
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/events/detail?id="+itoa64(out.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolEventDetail(drec, dreq)
		if drec.Code != http.StatusOK {
			t.Fatalf("events detail status mismatch: got=%d want=%d body=%s", drec.Code, http.StatusOK, drec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/states?state=paused_insufficient&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolStates(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("states status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int `json:"total"`
			Items []struct {
				ID      int64  `json:"id"`
				State   string `json:"state"`
				NeedSat uint64 `json:"pause_need_satoshi"`
				HaveSat uint64 `json:"pause_have_satoshi"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode states list: %v", err)
		}
		if out.Total != 1 || len(out.Items) != 1 {
			t.Fatalf("states total/items mismatch: total=%d items=%d", out.Total, len(out.Items))
		}
		if out.Items[0].State != "paused_insufficient" || out.Items[0].HaveSat != 12345 {
			t.Fatalf("unexpected states row: %+v", out.Items[0])
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/states/detail?id="+itoa64(out.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolStateDetail(drec, dreq)
		if drec.Code != http.StatusOK {
			t.Fatalf("states detail status mismatch: got=%d want=%d body=%s", drec.Code, http.StatusOK, drec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states?event_name=fee_pool_resumed_by_wallet_probe&gateway_pubkey_hex=gw1&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolObservedStates(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("observed states status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int `json:"total"`
			Items []struct {
				ID         int64           `json:"id"`
				Event      string          `json:"event_name"`
				StateAfter string          `json:"state_after"`
				SourceRef  string          `json:"source_ref"`
				HaveSat    uint64          `json:"pause_have_satoshi"`
				Payload    json.RawMessage `json:"payload"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode observed states list: %v", err)
		}
		if out.Total != 1 || len(out.Items) != 1 {
			t.Fatalf("observed states total/items mismatch: total=%d items=%d", out.Total, len(out.Items))
		}
		var payload struct {
			ObservedReason       string         `json:"observed_reason"`
			WalletBalanceSatoshi uint64         `json:"wallet_balance_satoshi"`
			Extra                map[string]any `json:"extra"`
		}
		if err := json.Unmarshal(out.Items[0].Payload, &payload); err != nil {
			t.Fatalf("decode observed payload failed: %v", err)
		}
		if out.Items[0].Event != "fee_pool_resumed_by_wallet_probe" || out.Items[0].StateAfter != "idle" || out.Items[0].SourceRef != "gw1" || out.Items[0].HaveSat != 999999 || payload.ObservedReason != "wallet_probe" || payload.WalletBalanceSatoshi != 999999 {
			t.Fatalf("unexpected observed states row: %+v", out.Items[0])
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states/detail?id="+itoa64(out.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolObservedStateDetail(drec, dreq)
		if drec.Code != http.StatusOK {
			t.Fatalf("observed states detail status mismatch: got=%d want=%d body=%s", drec.Code, http.StatusOK, drec.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/effects?stage=fee_pool_open&status=paused&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminFeePoolEffects(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("effects status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var out struct {
			Total int `json:"total"`
			Items []struct {
				ID      int64           `json:"id"`
				Stage   string          `json:"stage"`
				Status  string          `json:"status"`
				Payload json.RawMessage `json:"payload"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode effects list: %v", err)
		}
		if out.Total != 1 || len(out.Items) != 1 {
			t.Fatalf("effects total/items mismatch: total=%d items=%d", out.Total, len(out.Items))
		}
		if out.Items[0].Stage != "fee_pool_open" || out.Items[0].Status != "paused" || len(out.Items[0].Payload) == 0 {
			t.Fatalf("unexpected effects row: %+v", out.Items[0])
		}
		dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/effects/detail?id="+itoa64(out.Items[0].ID), nil)
		drec := httptest.NewRecorder()
		srv.handleAdminFeePoolEffectDetail(drec, dreq)
		if drec.Code != http.StatusOK {
			t.Fatalf("effects detail status mismatch: got=%d want=%d body=%s", drec.Code, http.StatusOK, drec.Body.String())
		}
	}
}
