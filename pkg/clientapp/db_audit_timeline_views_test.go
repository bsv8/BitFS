package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuditTimelineGatewayAndCommandOrdering(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	mustInsert := func(sqlText string, args ...any) {
		t.Helper()
		if _, err := db.Exec(sqlText, args...); err != nil {
			t.Fatalf("insert row failed: %v", err)
		}
	}

	mustInsert(`INSERT INTO command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000000, "cmd-1", "feepool.ensure_active", "gw1", "gateway:gw1", "e2e", 1700000000, 1, "applied", "", "", "idle", "active", 9, "", `{"scene":"cmd"}`, `{"ok":true}`,
	)
	mustInsert(`INSERT INTO gateway_events(
		created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		1700000000, "gw1", "cmd-1", "fee_pool_open", "msg-1", 1, "pool-1", 100, `{"scene":"gateway"}`,
	)
	mustInsert(`INSERT INTO domain_events(
		created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
	) VALUES(?,?,?,?,?,?,?)`,
		1700000000, "cmd-1", "gw1", "fee_pool_opened", "idle", "active", `{"scene":"domain"}`,
	)
	mustInsert(`INSERT INTO state_snapshots(
		created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		1700000000, "cmd-1", "gw1", "active", "", 0, 0, "", `{"scene":"snapshot"}`,
	)
	mustInsert(`INSERT INTO effect_logs(
		created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
	) VALUES(?,?,?,?,?,?,?,?)`,
		1700000000, "cmd-1", "gw1", "chain", "open", "done", "", `{"scene":"effect"}`,
	)
	mustInsert(`INSERT INTO observed_gateway_states(
		created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000000, "gw1", "gw1", 1700000000, "fee_pool_resumed_by_wallet_probe", "active", "idle", "", 0, 0, "", `{"scene":"observed_gateway"}`,
	)
	mustInsert(`INSERT INTO observed_gateway_states(
		created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000000, "gw1", "cmd-1", 1700000000, "fee_pool_reconciled", "active", "active", "", 0, 0, "", `{"source_command_id":"cmd-1"}`,
	)

	gatewayPage, err := dbListGatewayAuditTimeline(nil, store, AuditTimelineFilter{
		Limit:            50,
		Offset:           0,
		GatewayPubkeyHex: "gw1",
	})
	if err != nil {
		t.Fatalf("list gateway audit timeline failed: %v", err)
	}
	if gatewayPage.Total != 7 {
		t.Fatalf("unexpected gateway total: got=%d want=7", gatewayPage.Total)
	}
	wantKinds := []string{
		"command_journal",
		"gateway_event",
		"domain_event",
		"state_snapshot",
		"effect_log",
		"observed_gateway_state",
		"observed_gateway_state",
	}
	if len(gatewayPage.Items) != len(wantKinds) {
		t.Fatalf("unexpected gateway item size: got=%d want=%d", len(gatewayPage.Items), len(wantKinds))
	}
	for i, want := range wantKinds {
		if gatewayPage.Items[i].FactKind != want {
			t.Fatalf("gateway item %d fact kind mismatch: got=%s want=%s", i, gatewayPage.Items[i].FactKind, want)
		}
	}
	if gatewayPage.Items[0].OriginKind != "command" || gatewayPage.Items[5].OriginKind != "observed" {
		t.Fatalf("unexpected gateway origin kinds: %+v", gatewayPage.Items)
	}
	if gatewayPage.Items[0].Title != "命令 feepool.ensure_active / applied" {
		t.Fatalf("unexpected command title: %s", gatewayPage.Items[0].Title)
	}
	if len(gatewayPage.Items[0].Payload) == 0 || !json.Valid(gatewayPage.Items[0].Payload) {
		t.Fatalf("command payload should be valid json: %s", string(gatewayPage.Items[0].Payload))
	}

	commandPage, err := dbListCommandAuditTimeline(nil, store, AuditTimelineFilter{
		Limit:     50,
		Offset:    0,
		CommandID: "cmd-1",
	})
	if err != nil {
		t.Fatalf("list command audit timeline failed: %v", err)
	}
	if commandPage.Total != 5 {
		t.Fatalf("unexpected command total: got=%d want=5", commandPage.Total)
	}
	for _, item := range commandPage.Items {
		if item.FactKind == "observed_gateway_state" {
			t.Fatalf("command timeline should not contain observed fact: %+v", item)
		}
	}

	srv := &httpAPIServer{db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/audit/gateway-timeline?gateway_pubkey_hex=gw1&limit=50&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFeePoolGatewayAuditTimeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway audit http status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/audit/command-timeline?command_id=cmd-1&limit=50&offset=0", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFeePoolCommandAuditTimeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("command audit http status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/audit/command-timeline?command_id=not-exist&limit=50&offset=0", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFeePoolCommandAuditTimeline(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing command should return 404, got=%d body=%s", rec.Code, rec.Body.String())
	}
}
