package clientapp

import (
	"context"
	"encoding/json"
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

	for _, table := range []string{"domain_events", "state_snapshots"} {
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

func TestCommandQueriesDefaultToCommandFacts(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO state_snapshots(created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		now, "legacy_obs_state", "gw1", "observed_gateway_state", "gw1", now, "idle", "", 0, 999999, "", `{"source":"observed"}`,
	); err != nil {
		t.Fatalf("insert observed state snapshot failed: %v", err)
	}

	cmdEvents, err := dbListDomainEvents(nil, store, domainEventFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list command domain events failed: %v", err)
	}
	if cmdEvents.Total != 1 || len(cmdEvents.Items) != 1 || cmdEvents.Items[0].CommandID != "cmd-domain-1" {
		t.Fatalf("default domain events should only return command facts: %+v", cmdEvents)
	}
	obsEvents, err := dbListDomainEvents(nil, store, domainEventFilter{Limit: 10, Offset: 0, SourceKind: "observed_gateway_state"})
	if err != nil {
		t.Fatalf("list observed domain events failed: %v", err)
	}
	if obsEvents.Total != 1 || len(obsEvents.Items) != 1 || obsEvents.Items[0].CommandID != "legacy_obs_domain" {
		t.Fatalf("observed domain events should still be queryable: %+v", obsEvents)
	}

	cmdStates, err := dbListStateSnapshots(nil, store, stateSnapshotFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("list command state snapshots failed: %v", err)
	}
	if cmdStates.Total != 1 || len(cmdStates.Items) != 1 || cmdStates.Items[0].CommandID != "cmd-state-1" {
		t.Fatalf("default state snapshots should only return command facts: %+v", cmdStates)
	}
	obsStates, err := dbListStateSnapshots(nil, store, stateSnapshotFilter{Limit: 10, Offset: 0, SourceKind: "observed_gateway_state"})
	if err != nil {
		t.Fatalf("list observed state snapshots failed: %v", err)
	}
	if obsStates.Total != 1 || len(obsStates.Items) != 1 || obsStates.Items[0].CommandID != "legacy_obs_state" {
		t.Fatalf("observed state snapshots should still be queryable: %+v", obsStates)
	}
}
