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

	appendCommandJournal(db, commandJournalEntry{
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
	appendDomainEvent(db, domainEventEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_paused_insufficient",
		StateBefore:   "idle",
		StateAfter:    "paused_insufficient",
		Payload:       map[string]any{"need": 100000, "have": 12345},
	})
	appendStateSnapshot(db, stateSnapshotEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		State:         "paused_insufficient",
		PauseReason:   "wallet_insufficient",
		PauseNeedSat:  100000,
		PauseHaveSat:  12345,
		LastError:     "not enough balance",
		Payload:       map[string]any{"source": "test"},
	})
	appendEffectLog(db, effectLogEntry{
		CommandID:     "cmd-1",
		GatewayPeerID: "gw1",
		EffectType:    "chain",
		Stage:         "fee_pool_open",
		Status:        "paused",
		ErrorMessage:  "not enough balance",
		Payload:       map[string]any{"need": 100000, "have": 12345},
	})

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
		if out.Items[0].State != "paused_insufficient" || out.Items[0].NeedSat != 100000 || out.Items[0].HaveSat != 12345 {
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
