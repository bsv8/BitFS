package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestObservedGatewayStatesSchemaAndIndexes(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	cols, err := tableColumns(db, "observed_gateway_states")
	if err != nil {
		t.Fatalf("inspect observed_gateway_states columns failed: %v", err)
	}
	for _, col := range []string{
		"id",
		"created_at_unix",
		"gateway_pubkey_hex",
		"source_ref",
		"observed_at_unix",
		"event_name",
		"state_before",
		"state_after",
		"pause_reason",
		"pause_need_satoshi",
		"pause_have_satoshi",
		"last_error",
		"payload_json",
	} {
		if _, ok := cols[col]; !ok {
			t.Fatalf("observed_gateway_states missing %s", col)
		}
	}

	for _, indexName := range []string{
		"idx_observed_gateway_states_created_at",
		"idx_observed_gateway_states_gateway",
		"idx_observed_gateway_states_event",
		"idx_observed_gateway_states_state",
	} {
		hasIndex, err := tableHasIndex(db, "observed_gateway_states", indexName)
		if err != nil {
			t.Fatalf("inspect %s failed: %v", indexName, err)
		}
		if !hasIndex {
			t.Fatalf("missing %s", indexName)
		}
	}

	for _, table := range []string{"domain_events", "state_snapshots", "effect_logs"} {
		notNull, err := tableColumnNotNull(db, table, "command_id")
		if err != nil {
			t.Fatalf("inspect %s command_id notnull failed: %v", table, err)
		}
		if !notNull {
			t.Fatalf("%s.command_id should be NOT NULL", table)
		}
	}
}

func TestObservedGatewayStateWriteAndQuery(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	_ = dbAppendObservedGatewayState(nil, store, observedGatewayStateEntry{
		GatewayPeerID:  "gw1",
		SourceRef:      "gw1",
		ObservedAtUnix: 1700000101,
		EventName:      "fee_pool_resumed_by_wallet_probe",
		StateBefore:    "paused_insufficient",
		StateAfter:     "idle",
		PauseHaveSat:   999999,
		LastError:      "",
		Payload:        map[string]any{"wallet_balance_satoshi": 999999},
	})

	page, err := dbListObservedGatewayStates(nil, store, observedGatewayStateFilter{
		Limit:         10,
		Offset:        0,
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_resumed_by_wallet_probe",
	})
	if err != nil {
		t.Fatalf("list observed gateway states failed: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("unexpected observed page size: total=%d items=%d", page.Total, len(page.Items))
	}
	if page.Items[0].GatewayPeerID != "gw1" || page.Items[0].SourceRef != "gw1" || page.Items[0].StateAfter != "idle" || page.Items[0].PauseHaveSat != 999999 {
		t.Fatalf("unexpected observed row: %+v", page.Items[0])
	}

	srv := &httpAPIServer{db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states?gateway_pubkey_hex=gw1&event_name=fee_pool_resumed_by_wallet_probe&limit=10&offset=0", nil)
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
			Payload    json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode observed states list failed: %v", err)
	}
	if out.Total != 1 || len(out.Items) != 1 {
		t.Fatalf("unexpected observed api size: total=%d items=%d", out.Total, len(out.Items))
	}
	if out.Items[0].Event != "fee_pool_resumed_by_wallet_probe" || out.Items[0].StateAfter != "idle" || out.Items[0].SourceRef != "gw1" || len(out.Items[0].Payload) == 0 {
		t.Fatalf("unexpected observed api row: %+v", out.Items[0])
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/observed-states/detail?id="+itoa64(out.Items[0].ID), nil)
	detailRec := httptest.NewRecorder()
	srv.handleAdminFeePoolObservedStateDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("observed detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
}

func TestObservedGatewayStateResumeChainWritesBeforeAndAfter(t *testing.T) {
	t.Parallel()

	db := newKernelTestDB(t)
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: strings.Repeat("1", 64),
		},
	}
	rt.runIn.BSV.Network = "test"

	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("derive wallet address failed: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, addr, []poolcore.UTXO{
		{TxID: "tx1", Vout: 0, Value: 150000},
	}); err != nil {
		t.Fatalf("seed wallet utxos failed: %v", err)
	}

	gwHost, gwPubHex := newSecpHost(t)
	defer gwHost.Close()
	gwAddr := gwHost.Addrs()[0].String() + "/p2p/" + gwHost.ID().String()
	rt.runIn.Network.Gateways = []PeerNode{
		{Enabled: true, Addr: gwAddr, Pubkey: gwPubHex},
	}

	k := newFeePoolKernel(rt, newClientDB(db, nil))
	k.setState(gwHost.ID().String(), feePoolKernelGatewayState{
		State:        feePoolKernelStatePausedInsufficient,
		PauseReason:  "wallet_insufficient",
		PauseNeedSat: 100000,
		PauseHaveSat: 12345,
		LastError:    "not enough balance",
	})

	k.tryResumePausedGateway(context.Background(), peer.AddrInfo{ID: gwHost.ID(), Addrs: gwHost.Addrs()})

	page, err := dbListObservedGatewayStates(nil, newClientDB(db, nil), observedGatewayStateFilter{
		Limit:         10,
		Offset:        0,
		GatewayPeerID: gwPubHex,
		EventName:     "fee_pool_resumed_by_wallet_probe",
	})
	if err != nil {
		t.Fatalf("list observed gateway states failed: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("unexpected observed page size: total=%d items=%d", page.Total, len(page.Items))
	}
	item := page.Items[0]
	if item.StateBefore != feePoolKernelStatePausedInsufficient || item.StateAfter != feePoolKernelStateIdle {
		t.Fatalf("unexpected observed state transition: %+v", item)
	}
	if item.PauseHaveSat != 150000 || item.PauseNeedSat != 0 || item.PauseReason != "" || item.LastError != "" {
		t.Fatalf("unexpected observed state detail: %+v", item)
	}
	if item.SourceRef != gwPubHex {
		t.Fatalf("unexpected source_ref: got=%s want=%s", item.SourceRef, gwPubHex)
	}
}

func TestObservedGatewayStateMigrationMovesStateSnapshotsAndMergesDomainEvent(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	_ = dbAppendDomainEvent(nil, store, domainEventEntry{
		CommandID:      "cmd-domain-1",
		GatewayPeerID:  "gw1",
		ObservedAtUnix: now,
		EventName:      "fee_pool_paused_insufficient",
		StateBefore:    "idle",
		StateAfter:     "paused_insufficient",
		Payload:        map[string]any{"source": "command"},
	})
	_ = dbAppendStateSnapshot(nil, store, stateSnapshotEntry{
		CommandID:      "cmd-state-1",
		GatewayPeerID:  "gw1",
		ObservedAtUnix: now,
		State:          "paused_insufficient",
		PauseReason:    "wallet_insufficient",
		PauseNeedSat:   100000,
		PauseHaveSat:   12345,
		LastError:      "not enough balance",
		Payload:        map[string]any{"source": "command"},
	})

	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		now, "legacy_obs_domain", "gw1", "observed_gateway_state", "gw1", now, "fee_pool_resumed_by_wallet_probe", "paused_insufficient", "idle", `{"source":"observed"}`,
	); err != nil {
		t.Fatalf("insert observed domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		now+1, "legacy_obs_domain_2", "gw1", "observed_gateway_state", "gw1-missing", now+1, "fee_pool_resumed_by_wallet_probe", "paused_insufficient", "idle", `{"source":"observed-unmatched"}`,
	); err != nil {
		t.Fatalf("insert unmatched observed domain event failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		now, "legacy_obs_state", "gw1", "observed_gateway_state", "gw1", now, "idle", "", 0, 999999, "", `{"source":"observed"}`,
	); err != nil {
		t.Fatalf("insert observed state snapshot failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		now+1, "legacy_obs_state_2", "gw1", "observed_gateway_state", "gw1-other", now+1, "paused_insufficient", "wallet_insufficient", 100000, 12345, "not enough balance", `{"source":"observed-unmatched"}`,
	); err != nil {
		t.Fatalf("insert unmatched observed state snapshot failed: %v", err)
	}

	report, err := migrateObservedGatewayStateFactsWithReport(db)
	if err != nil {
		t.Fatalf("migrate observed gateway facts failed: %v", err)
	}
	if report.SnapshotRowsRead != 2 || report.SnapshotRowsInserted != 2 || report.SnapshotRowsDeleted != 2 {
		t.Fatalf("unexpected snapshot migration report: %+v", report)
	}
	if report.DomainEventRowsRead != 2 || report.DomainEventRowsMerged != 1 || report.DomainEventRowsDeleted != 2 || report.DomainEventRowsUnmatched != 1 {
		t.Fatalf("unexpected domain event migration report: %+v", report)
	}

	cmdEvents, err := dbListDomainEvents(nil, store, domainEventFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list command domain events failed: %v", err)
	}
	if cmdEvents.Total != 1 || len(cmdEvents.Items) != 1 || cmdEvents.Items[0].CommandID != "cmd-domain-1" {
		t.Fatalf("default domain events should only return command facts: %+v", cmdEvents)
	}
	cmdStates, err := dbListStateSnapshots(nil, store, stateSnapshotFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list command state snapshots failed: %v", err)
	}
	if cmdStates.Total != 1 || len(cmdStates.Items) != 1 || cmdStates.Items[0].CommandID != "cmd-state-1" {
		t.Fatalf("default state snapshots should only return command facts: %+v", cmdStates)
	}

	obsPage, err := dbListObservedGatewayStates(nil, store, observedGatewayStateFilter{
		Limit:         10,
		Offset:        0,
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_resumed_by_wallet_probe",
	})
	if err != nil {
		t.Fatalf("list observed gateway states failed: %v", err)
	}
	if obsPage.Total != 1 || len(obsPage.Items) != 1 {
		t.Fatalf("unexpected observed gateway states page: %+v", obsPage)
	}
	obsItem := obsPage.Items[0]
	if obsItem.StateBefore != "paused_insufficient" || obsItem.StateAfter != "idle" || obsItem.SourceRef != "gw1" {
		t.Fatalf("unexpected observed gateway state row: %+v", obsItem)
	}
	var payload struct {
		SourceTable     string         `json:"source_table"`
		SnapshotPayload map[string]any `json:"snapshot_payload"`
		ObservedEvent   map[string]any `json:"observed_event"`
	}
	if err := json.Unmarshal(obsItem.Payload, &payload); err != nil {
		t.Fatalf("unmarshal observed payload failed: %v", err)
	}
	if payload.SourceTable != "state_snapshots" || payload.ObservedEvent == nil {
		t.Fatalf("unexpected observed payload wrapper: %+v", payload)
	}
	if payload.ObservedEvent["event_name"] != "fee_pool_resumed_by_wallet_probe" {
		t.Fatalf("unexpected merged observed event payload: %+v", payload.ObservedEvent)
	}

	obsDomainResult, err := db.Exec(`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		now+2, "manual_obs_domain", "gw1", "observed_gateway_state", "gw1-manual", now+2, "fee_pool_resumed_by_wallet_probe", "paused_insufficient", "idle", `{"source":"manual-observed"}`,
	)
	if err != nil {
		t.Fatalf("insert manual observed domain event failed: %v", err)
	}
	obsDomainID, err := obsDomainResult.LastInsertId()
	if err != nil {
		t.Fatalf("load manual observed domain id failed: %v", err)
	}
	obsStateResult, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		now+2, "manual_obs_state", "gw1", "observed_gateway_state", "gw1-manual", now+2, "idle", "", 0, 888888, "", `{"source":"manual-observed"}`,
	)
	if err != nil {
		t.Fatalf("insert manual observed state snapshot failed: %v", err)
	}
	obsStateID, err := obsStateResult.LastInsertId()
	if err != nil {
		t.Fatalf("load manual observed state id failed: %v", err)
	}

	if page, err := dbListDomainEvents(nil, store, domainEventFilter{Limit: 10, Offset: 0}); err != nil {
		t.Fatalf("list command domain events after manual observed insert failed: %v", err)
	} else if page.Total != 1 || len(page.Items) != 1 || page.Items[0].CommandID != "cmd-domain-1" {
		t.Fatalf("command domain events should ignore observed rows: %+v", page)
	}
	if page, err := dbListStateSnapshots(nil, store, stateSnapshotFilter{Limit: 10, Offset: 0}); err != nil {
		t.Fatalf("list command state snapshots after manual observed insert failed: %v", err)
	} else if page.Total != 1 || len(page.Items) != 1 || page.Items[0].CommandID != "cmd-state-1" {
		t.Fatalf("command state snapshots should ignore observed rows: %+v", page)
	}

	if _, err := dbGetDomainEventItem(nil, store, obsDomainID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("observed domain detail should be hidden, got err=%v", err)
	}
	if _, err := dbGetStateSnapshotItem(nil, store, obsStateID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("observed state detail should be hidden, got err=%v", err)
	}

	srv := &httpAPIServer{db: db}
	dreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/events/detail?id="+itoa64(obsDomainID), nil)
	drec := httptest.NewRecorder()
	srv.handleAdminFeePoolEventDetail(drec, dreq)
	if drec.Code != http.StatusNotFound {
		t.Fatalf("observed domain http detail should be 404, got=%d body=%s", drec.Code, drec.Body.String())
	}
	sreq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/states/detail?id="+itoa64(obsStateID), nil)
	srec := httptest.NewRecorder()
	srv.handleAdminFeePoolStateDetail(srec, sreq)
	if srec.Code != http.StatusNotFound {
		t.Fatalf("observed state http detail should be 404, got=%d body=%s", srec.Code, srec.Body.String())
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("second initIndexDB failed: %v", err)
	}
	obsPage, err = dbListObservedGatewayStates(nil, store, observedGatewayStateFilter{
		Limit:         10,
		Offset:        0,
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_resumed_by_wallet_probe",
	})
	if err != nil {
		t.Fatalf("list observed gateway states after second init failed: %v", err)
	}
	if obsPage.Total != 2 || len(obsPage.Items) != 2 {
		t.Fatalf("unexpected observed gateway states page after second init: %+v", obsPage)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("third initIndexDB failed: %v", err)
	}
	obsPage, err = dbListObservedGatewayStates(nil, store, observedGatewayStateFilter{
		Limit:         10,
		Offset:        0,
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_resumed_by_wallet_probe",
	})
	if err != nil {
		t.Fatalf("list observed gateway states after third init failed: %v", err)
	}
	if obsPage.Total != 2 || len(obsPage.Items) != 2 {
		t.Fatalf("observed gateway states should stay stable after third init: %+v", obsPage)
	}
}
