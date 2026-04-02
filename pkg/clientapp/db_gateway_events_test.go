package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGatewayEventsSchemaHasCommandIDAndIndex(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cols, err := tableColumns(db, "gateway_events")
	if err != nil {
		t.Fatalf("inspect gateway_events columns failed: %v", err)
	}
	if _, ok := cols["command_id"]; !ok {
		t.Fatalf("gateway_events missing command_id")
	}

	hasIndex, err := tableHasIndex(db, "gateway_events", "idx_gateway_events_cmd_id")
	if err != nil {
		t.Fatalf("inspect gateway_events index failed: %v", err)
	}
	if !hasIndex {
		t.Fatalf("gateway_events missing idx_gateway_events_cmd_id")
	}
}

func TestGatewayEventWriteRequiresCommandIDAndQueryReturnsIt(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	if err := dbAppendGatewayEvent(context.Background(), store, gatewayEventEntry{
		GatewayPeerID: "gw1",
		Action:        "fee_pool_open",
		PoolID:        "pool-1",
		SequenceNum:   1,
		AmountSatoshi: 100,
		Payload:       map[string]any{"scene": "bad"},
	}); err == nil {
		t.Fatalf("expected empty command_id write to be rejected")
	}

	if err := dbAppendGatewayEvent(context.Background(), store, gatewayEventEntry{
		GatewayPeerID: "gw1",
		CommandID:     "cmd-fee-1",
		Action:        "fee_pool_open",
		PoolID:        "pool-1",
		SequenceNum:   1,
		AmountSatoshi: 100,
		Payload:       map[string]any{"scene": "fee_pool_open"},
	}); err != nil {
		t.Fatalf("append fee_pool_open gateway event failed: %v", err)
	}
	if err := dbAppendGatewayEvent(context.Background(), store, gatewayEventEntry{
		GatewayPeerID: "gw1",
		CommandID:     "cmd-fee-1",
		Action:        "listen_cycle_fee_open",
		PoolID:        "pool-1",
		SequenceNum:   1,
		AmountSatoshi: 20,
		Payload:       map[string]any{"scene": "listen_cycle_fee_open"},
	}); err != nil {
		t.Fatalf("append listen_cycle_fee_open gateway event failed: %v", err)
	}
	if err := dbAppendGatewayEvent(context.Background(), store, gatewayEventEntry{
		GatewayPeerID: "gw1",
		CommandID:     "cmd-fee-2",
		Action:        "listen_cycle_fee",
		PoolID:        "pool-1",
		SequenceNum:   2,
		AmountSatoshi: 30,
		Payload:       map[string]any{"scene": "listen_cycle_fee"},
	}); err != nil {
		t.Fatalf("append listen_cycle_fee gateway event failed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM gateway_events WHERE command_id IS NULL OR command_id=''`).Scan(&count); err != nil {
		t.Fatalf("count empty command_id rows failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("unexpected empty command_id rows: %d", count)
	}

	srv := &httpAPIServer{db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways/events?command_id=cmd-fee-1&gateway_pubkey_hex=gw1&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleGatewayEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway events list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			CommandID string `json:"command_id"`
			Action    string `json:"action"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode gateway events list failed: %v", err)
	}
	if out.Total != 2 || len(out.Items) != 2 {
		t.Fatalf("unexpected gateway events count: total=%d items=%d", out.Total, len(out.Items))
	}
	for _, item := range out.Items {
		if item.CommandID != "cmd-fee-1" {
			t.Fatalf("unexpected command_id in list: %+v", item)
		}
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/gateways/events/detail?id=1", nil)
	detailRec := httptest.NewRecorder()
	srv.handleGatewayEventDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("gateway event detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detail struct {
		CommandID string `json:"command_id"`
		Action    string `json:"action"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode gateway event detail failed: %v", err)
	}
	if detail.CommandID != "cmd-fee-1" {
		t.Fatalf("gateway event detail command_id mismatch: got=%s want=cmd-fee-1", detail.CommandID)
	}
}

func TestListenErrorWritesOrchestratorLog(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	h, _ := newSecpHost(t)
	defer h.Close()

	rt := &Runtime{}
	recordGatewayRuntimeError(rt, store, h.ID(), "fee_pool_open", context.Canceled)

	var eventType, source, aggregateKey, gatewayPubkeyHex, errorMessage, payloadJSON string
	if err := db.QueryRow(`
		SELECT event_type,source,aggregate_key,gateway_pubkey_hex,error_message,payload_json
		FROM orchestrator_logs
		WHERE event_type='listen_error'
		ORDER BY id DESC LIMIT 1`,
	).Scan(&eventType, &source, &aggregateKey, &gatewayPubkeyHex, &errorMessage, &payloadJSON); err != nil {
		t.Fatalf("query orchestrator_logs failed: %v", err)
	}
	if eventType != "listen_error" {
		t.Fatalf("event_type mismatch: got=%s want=listen_error", eventType)
	}
	if source != "listen_loop" {
		t.Fatalf("source mismatch: got=%s want=listen_loop", source)
	}
	if aggregateKey != "gateway:"+h.ID().String() {
		t.Fatalf("aggregate_key mismatch: got=%s", aggregateKey)
	}
	if gatewayPubkeyHex != h.ID().String() {
		t.Fatalf("gateway_pubkey_hex mismatch: got=%s", gatewayPubkeyHex)
	}
	if errorMessage != context.Canceled.Error() {
		t.Fatalf("error_message mismatch: got=%s", errorMessage)
	}
	if payloadJSON == "" || !json.Valid([]byte(payloadJSON)) {
		t.Fatalf("payload_json should be valid json, got=%s", payloadJSON)
	}

	var listenErrorCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM gateway_events WHERE action='listen_error'`).Scan(&listenErrorCount); err != nil {
		t.Fatalf("count gateway_events listen_error failed: %v", err)
	}
	if listenErrorCount != 0 {
		t.Fatalf("listen_error should not be written to gateway_events, got=%d", listenErrorCount)
	}
}
