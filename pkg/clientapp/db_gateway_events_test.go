package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGatewayEventsSchemaHasCommandIDAndIndex(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}

	cols, err := tableColumns(db, "proc_gateway_events")
	if err != nil {
		t.Fatalf("inspect proc_gateway_events columns failed: %v", err)
	}
	if _, ok := cols["command_id"]; !ok {
		t.Fatalf("proc_gateway_events missing command_id")
	}

	hasIndex, err := tableHasIndex(db, "proc_gateway_events", "idx_proc_gateway_events_cmd_id")
	if err != nil {
		t.Fatalf("inspect proc_gateway_events index failed: %v", err)
	}
	if !hasIndex {
		t.Fatalf("proc_gateway_events missing idx_proc_gateway_events_cmd_id")
	}

	notNull, err := tableColumnNotNull(db, "proc_gateway_events", "command_id")
	if err != nil {
		t.Fatalf("inspect proc_gateway_events command_id notnull failed: %v", err)
	}
	if !notNull {
		t.Fatalf("proc_gateway_events.command_id should be NOT NULL")
	}

	unique, err := tableHasUniqueIndexOnColumns(db, "proc_command_journal", []string{"command_id"})
	if err != nil {
		t.Fatalf("inspect proc_command_journal unique failed: %v", err)
	}
	if !unique {
		t.Fatalf("proc_command_journal missing unique constraint on command_id")
	}
}

func TestGatewayEventWriteRequiresCommandIDAndQueryReturnsIt(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	if err := enableForeignKeys(db); err != nil {
		t.Fatalf("enable foreign keys failed: %v", err)
	}
	insertGatewayEventTestCommandJournal(t, db, "cmd-fee-1", "gw1")
	insertGatewayEventTestCommandJournal(t, db, "cmd-fee-2", "gw1")

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
	if err := db.QueryRow(`SELECT COUNT(1) FROM proc_gateway_events WHERE command_id IS NULL OR command_id=''`).Scan(&count); err != nil {
		t.Fatalf("count empty command_id rows failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("unexpected empty command_id rows: %d", count)
	}

	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}
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

	rt := newRuntimeForTest(t, Config{}, "", withRuntimeStore(store))
	recordGatewayRuntimeError(rt, store, h.ID(), "fee_pool_open", context.Canceled)

	var eventType, source, aggregateKey, gatewayPubkeyHex, errorMessage, payloadJSON string
	if err := db.QueryRow(`
		SELECT event_type,source,aggregate_key,gateway_pubkey_hex,error_message,payload_json
		FROM proc_orchestrator_logs
		WHERE event_type='listen_error'
		ORDER BY id DESC LIMIT 1`,
	).Scan(&eventType, &source, &aggregateKey, &gatewayPubkeyHex, &errorMessage, &payloadJSON); err != nil {
		t.Fatalf("query proc_orchestrator_logs failed: %v", err)
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM proc_gateway_events WHERE action='listen_error'`).Scan(&listenErrorCount); err != nil {
		t.Fatalf("count proc_gateway_events listen_error failed: %v", err)
	}
	if listenErrorCount != 0 {
		t.Fatalf("listen_error should not be written to proc_gateway_events, got=%d", listenErrorCount)
	}
}

func createLegacyCommandJournalSchemaForGatewayTest(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`CREATE TABLE proc_command_journal(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_unix INTEGER NOT NULL,
		command_id TEXT NOT NULL,
		command_type TEXT NOT NULL,
		gateway_pubkey_hex TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		requested_by TEXT NOT NULL,
		requested_at_unix INTEGER NOT NULL,
		accepted INTEGER NOT NULL,
		status TEXT NOT NULL,
		error_code TEXT NOT NULL,
		error_message TEXT NOT NULL,
		state_before TEXT NOT NULL,
		state_after TEXT NOT NULL,
		duration_ms INTEGER NOT NULL,
		trigger_key TEXT NOT NULL DEFAULT '',
		payload_json TEXT NOT NULL,
		result_json TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("create legacy proc_command_journal failed: %v", err)
	}
}

func createLegacyGatewayEventsSchemaForGatewayTest(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`CREATE TABLE proc_gateway_events(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_unix INTEGER NOT NULL,
		gateway_pubkey_hex TEXT NOT NULL,
		command_id TEXT,
		action TEXT NOT NULL,
		msg_id TEXT NOT NULL,
		sequence_num INTEGER NOT NULL,
		pool_id TEXT NOT NULL,
		amount_satoshi INTEGER NOT NULL,
		payload_json TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("create legacy proc_gateway_events failed: %v", err)
	}
}

func insertGatewayEventTestCommandJournal(t *testing.T, db *sql.DB, commandID, gatewayPeerID string) {
	t.Helper()

	_, err := db.Exec(`INSERT INTO proc_command_journal(
		created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		1700000001,
		commandID,
		"fee_pool.open",
		gatewayPeerID,
		"agg-"+commandID,
		"tester",
		1700000001,
		1,
		"applied",
		"",
		"",
		"before",
		"after",
		10,
		"",
		"{}",
		"{}",
	)
	if err != nil {
		t.Fatalf("insert proc_command_journal failed: %v", err)
	}
}

func insertGatewayEventTestGatewayEvent(t *testing.T, db *sql.DB, commandID, action string, seq int64) {
	t.Helper()

	_, err := db.Exec(`INSERT INTO proc_gateway_events(
		created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?)`,
		1700000001,
		"gw1",
		commandID,
		action,
		"msg-"+action,
		seq,
		"pool-1",
		100,
		`{"scene":"legacy"}`,
	)
	if err != nil {
		t.Fatalf("insert proc_gateway_events failed: %v", err)
	}
}
