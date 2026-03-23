package clientapp

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func newWalletAPITestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

func TestHandleWalletFundFlows_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "direct_pool:s1",
		FlowType:        "direct_transfer_pool",
		RefID:           "s1",
		Stage:           "open_lock",
		Direction:       "lock",
		Purpose:         "direct_transfer_pool_open",
		AmountSatoshi:   1000,
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx_open_1",
		Note:            "deal_id=d1",
		Payload:         map[string]any{"sequence": 1},
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "direct_pool:s1",
		FlowType:        "direct_transfer_pool",
		RefID:           "s1",
		Stage:           "use_pay",
		Direction:       "out",
		Purpose:         "direct_transfer_chunk_pay",
		AmountSatoshi:   -120,
		UsedSatoshi:     120,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx_pay_1",
		Note:            "chunk_index=0",
		Payload:         map[string]any{"chunk_index": 0},
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "fee_pool:fp1",
		FlowType:        "fee_pool",
		RefID:           "fp1",
		Stage:           "close_settle",
		Direction:       "settle",
		Purpose:         "fee_pool_close",
		AmountSatoshi:   0,
		UsedSatoshi:     200,
		ReturnedSatoshi: 50,
		RelatedTxID:     "tx_close_1",
		Note:            "gateway=g1",
		Payload:         map[string]any{"status": "closed"},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?flow_type=direct_transfer_pool&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			ID       int64           `json:"id"`
			FlowType string          `json:"flow_type"`
			Payload  json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.Total != 2 {
		t.Fatalf("list total mismatch: got=%d want=2", list.Total)
	}
	if len(list.Items) != 2 {
		t.Fatalf("list item count mismatch: got=%d want=2", len(list.Items))
	}
	if list.Items[0].FlowType != "direct_transfer_pool" || len(list.Items[0].Payload) == 0 {
		t.Fatalf("unexpected list item: %+v", list.Items[0])
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id=999999", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusNotFound {
		t.Fatalf("detail not found status mismatch: got=%d want=%d", detailRec.Code, http.StatusNotFound)
	}

	detailID := list.Items[0].ID
	detailReqOK := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id="+itoa64(detailID), nil)
	detailRecOK := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(detailRecOK, detailReqOK)
	if detailRecOK.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRecOK.Code, http.StatusOK, detailRecOK.Body.String())
	}
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func TestHandleWalletSummary(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "open_lock",
		Direction:       "lock",
		Purpose:         "open",
		AmountSatoshi:   1000,
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx1",
		Note:            "n1",
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "use_pay",
		Direction:       "out",
		Purpose:         "pay",
		AmountSatoshi:   -400,
		UsedSatoshi:     400,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx2",
		Note:            "n2",
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "close_settle",
		Direction:       "settle",
		Purpose:         "close",
		AmountSatoshi:   0,
		UsedSatoshi:     0,
		ReturnedSatoshi: 600,
		RelatedTxID:     "tx3",
		Note:            "n3",
	})
	appendTxHistory(db, txHistoryEntry{GatewayPeerID: "g1", EventType: "evt_a", Direction: "out", AmountSatoshi: 1, Purpose: "p", Note: "n"})
	appendTxHistory(db, txHistoryEntry{GatewayPeerID: "g1", EventType: "evt_b", Direction: "in", AmountSatoshi: 2, Purpose: "p", Note: "n"})
	appendGatewayEvent(db, gatewayEventEntry{GatewayPeerID: "g1", Action: "a1", AmountSatoshi: 1, Payload: map[string]any{"x": 1}})
	appendSaleRecord(db, saleRecordEntry{SessionID: "s1", SeedHash: "seed1", ChunkIndex: 0, UnitPriceSatPer64K: 2, AmountSatoshi: 2, BuyerGatewayPeerID: "g1", ReleaseToken: "r"})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletSummary(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if int(body["flow_count"].(float64)) != 3 {
		t.Fatalf("flow_count mismatch: got=%v want=3", body["flow_count"])
	}
	if int(body["tx_event_count"].(float64)) != 2 {
		t.Fatalf("tx_event_count mismatch: got=%v want=2", body["tx_event_count"])
	}
	if int(body["sale_count"].(float64)) != 1 {
		t.Fatalf("sale_count mismatch: got=%v want=1", body["sale_count"])
	}
	if int(body["gateway_event_count"].(float64)) != 1 {
		t.Fatalf("gateway_event_count mismatch: got=%v want=1", body["gateway_event_count"])
	}
	if int64(body["total_in_satoshi"].(float64)) != 1000 {
		t.Fatalf("total_in_satoshi mismatch: got=%v want=1000", body["total_in_satoshi"])
	}
	if int64(body["total_out_satoshi"].(float64)) != 400 {
		t.Fatalf("total_out_satoshi mismatch: got=%v want=400", body["total_out_satoshi"])
	}
	if int64(body["total_used_satoshi"].(float64)) != 400 {
		t.Fatalf("total_used_satoshi mismatch: got=%v want=400", body["total_used_satoshi"])
	}
	if int64(body["total_returned_satoshi"].(float64)) != 600 {
		t.Fatalf("total_returned_satoshi mismatch: got=%v want=600", body["total_returned_satoshi"])
	}
	if int64(body["net_spent_satoshi"].(float64)) != -200 {
		t.Fatalf("net_spent_satoshi mismatch: got=%v want=-200", body["net_spent_satoshi"])
	}
	if int64(body["ledger_count"].(float64)) != 0 {
		t.Fatalf("ledger_count mismatch: got=%v want=0", body["ledger_count"])
	}
	if int64(body["ledger_total_in_satoshi"].(float64)) != 0 {
		t.Fatalf("ledger_total_in_satoshi mismatch: got=%v want=0", body["ledger_total_in_satoshi"])
	}
	if int64(body["ledger_total_out_satoshi"].(float64)) != 0 {
		t.Fatalf("ledger_total_out_satoshi mismatch: got=%v want=0", body["ledger_total_out_satoshi"])
	}
	if got, _ := body["balance_source"].(string); got != "wallet_utxo_db" {
		t.Fatalf("balance_source mismatch: got=%v want=wallet_utxo_db", body["balance_source"])
	}
}

func TestHandleWalletSummary_IncludesSyncFailureAnchors(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 0, 0, now, `http 502: {"error":"http 404: Not Found"}`, "chain_utxo_worker", "periodic_tick", 0, "round-http-1", "wallet_chain_get_unconfirmed_history", walletChainUnconfirmedHistoryUpstreamPath(addr), 502,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}

	srv := &httpAPIServer{db: db, rt: rt}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletSummary(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if got, _ := body["wallet_utxo_sync_last_round_id"].(string); got != "round-http-1" {
		t.Fatalf("wallet_utxo_sync_last_round_id mismatch: got=%v want=round-http-1", body["wallet_utxo_sync_last_round_id"])
	}
	if got, _ := body["wallet_utxo_sync_last_failed_step"].(string); got != "wallet_chain_get_unconfirmed_history" {
		t.Fatalf("wallet_utxo_sync_last_failed_step mismatch: got=%v", body["wallet_utxo_sync_last_failed_step"])
	}
	if got, _ := body["wallet_utxo_sync_last_upstream_path"].(string); got != walletChainUnconfirmedHistoryUpstreamPath(addr) {
		t.Fatalf("wallet_utxo_sync_last_upstream_path mismatch: got=%v want=%v", body["wallet_utxo_sync_last_upstream_path"], walletChainUnconfirmedHistoryUpstreamPath(addr))
	}
	if got := int(body["wallet_utxo_sync_last_http_status"].(float64)); got != 502 {
		t.Fatalf("wallet_utxo_sync_last_http_status mismatch: got=%d want=502", got)
	}
}

func TestHandleWalletSummary_IncludesStaleAndSchedulerDiagnostics(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	runtimeStartedAt := now + 600
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 0, 0, now, `http 502: {"error":"context canceled"}`, "chain_utxo_worker", "periodic_tick", 0, "", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO scheduler_tasks(
			task_name,owner,mode,status,interval_seconds,created_at_unix,updated_at_unix,closed_at_unix,
			last_trigger,last_started_at_unix,last_ended_at_unix,last_duration_ms,last_error,in_flight,
			run_count,success_count,failure_count,last_summary_json,meta_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"chain_utxo_sync", "chain_maintenance", "static", "active", 10, now, now, 0,
		"periodic_tick", now+10, now+10, 0, "context canceled", 1,
		1, 0, 1, "{}", "{}",
	); err != nil {
		t.Fatalf("seed scheduler_tasks failed: %v", err)
	}

	rt.StartedAtUnix = runtimeStartedAt
	rt.chainMaint = &chainMaintainer{
		queue: make(chan chainTask, 2),
		status: chainSchedulerStatus{
			InFlight:          false,
			InFlightTaskType:  "",
			LastTaskStartedAt: now + 20,
			LastTaskEndedAt:   now + 21,
			LastError:         "",
		},
	}
	rt.chainMaint.queue <- chainTask{TaskType: chainTaskUTXO}
	srv := &httpAPIServer{db: db, rt: rt, startedAt: time.Unix(runtimeStartedAt+60, 0)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletSummary(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if got := int64(body["runtime_started_at_unix"].(float64)); got != runtimeStartedAt {
		t.Fatalf("runtime_started_at_unix mismatch: got=%d want=%d", got, runtimeStartedAt)
	}
	if stale, _ := body["wallet_utxo_sync_state_is_stale"].(bool); !stale {
		t.Fatalf("wallet_utxo_sync_state_is_stale mismatch: got=%v want=true", body["wallet_utxo_sync_state_is_stale"])
	}
	if got, _ := body["wallet_utxo_sync_state_stale_reason"].(string); got != "sync_state_older_than_runtime" {
		t.Fatalf("wallet_utxo_sync_state_stale_reason mismatch: got=%v want=sync_state_older_than_runtime", body["wallet_utxo_sync_state_stale_reason"])
	}
	if got, _ := body["wallet_utxo_sync_scheduler_status"].(string); got != "active" {
		t.Fatalf("wallet_utxo_sync_scheduler_status mismatch: got=%v want=active", body["wallet_utxo_sync_scheduler_status"])
	}
	if inFlight, _ := body["wallet_utxo_sync_scheduler_in_flight"].(bool); !inFlight {
		t.Fatalf("wallet_utxo_sync_scheduler_in_flight mismatch: got=%v want=true", body["wallet_utxo_sync_scheduler_in_flight"])
	}
	if got, _ := body["wallet_utxo_sync_scheduler_last_error"].(string); got != "context canceled" {
		t.Fatalf("wallet_utxo_sync_scheduler_last_error mismatch: got=%v want=context canceled", body["wallet_utxo_sync_scheduler_last_error"])
	}
	if got := int(body["chain_maintainer_queue_length"].(float64)); got != 1 {
		t.Fatalf("chain_maintainer_queue_length mismatch: got=%d want=1", got)
	}
	if inFlight, _ := body["chain_maintainer_in_flight"].(bool); inFlight {
		t.Fatalf("chain_maintainer_in_flight mismatch: got=%v want=false", body["chain_maintainer_in_flight"])
	}
	if got, _ := body["chain_maintainer_in_flight_task_type"].(string); got != "" {
		t.Fatalf("chain_maintainer_in_flight_task_type mismatch: got=%q want=\"\"", got)
	}
	if got := int64(body["chain_maintainer_last_task_started_at_unix"].(float64)); got != now+20 {
		t.Fatalf("chain_maintainer_last_task_started_at_unix mismatch: got=%d want=%d", got, now+20)
	}
}

func TestHandleWalletLedger_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletLedgerEntry(db, walletLedgerEntry{
		TxID:              "tx001",
		Direction:         "OUT",
		Category:          "FEE_POOL",
		AmountSatoshi:     120,
		CounterpartyLabel: "gateway:g1",
		Status:            "CONFIRMED",
		BlockHeight:       100,
		OccurredAtUnix:    1000,
		RawRefID:          "fee_pool:spend1",
		Note:              "open fee pool",
		Payload:           map[string]any{"a": 1},
	})
	appendWalletLedgerEntry(db, walletLedgerEntry{
		TxID:              "tx002",
		Direction:         "IN",
		Category:          "CHANGE",
		AmountSatoshi:     80,
		CounterpartyLabel: "self",
		Status:            "CONFIRMED",
		BlockHeight:       101,
		OccurredAtUnix:    1001,
		RawRefID:          "change:1",
		Note:              "change back",
		Payload:           map[string]any{"b": 2},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger?direction=OUT&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletLedger(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			ID        int64           `json:"id"`
			Direction string          `json:"direction"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.Total != 1 {
		t.Fatalf("list total mismatch: got=%d want=1", list.Total)
	}
	if len(list.Items) != 1 {
		t.Fatalf("list item count mismatch: got=%d want=1", len(list.Items))
	}
	if list.Items[0].Direction != "OUT" || len(list.Items[0].Payload) == 0 {
		t.Fatalf("unexpected list item: %+v", list.Items[0])
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger/detail?id=999999", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletLedgerDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusNotFound {
		t.Fatalf("detail not found status mismatch: got=%d want=%d", detailRec.Code, http.StatusNotFound)
	}

	detailID := list.Items[0].ID
	detailReqOK := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger/detail?id="+itoa64(detailID), nil)
	detailRecOK := httptest.NewRecorder()
	srv.handleWalletLedgerDetail(detailRecOK, detailReqOK)
	if detailRecOK.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRecOK.Code, http.StatusOK, detailRecOK.Body.String())
	}
}
