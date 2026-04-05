package clientapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCommandDetailHydratesRelatedFacts_Timeline 验证 commands/detail 时间线入口
// 仍会填充关联事实（内部已使用 timeline 逻辑）。主入口请用 command-timeline。
func TestCommandDetailHydratesRelatedFacts_Timeline(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	_ = dbAppendCommandJournal(context.Background(), store, commandJournalEntry{
		CommandID:     "cmd-timeline-1",
		CommandType:   "ensure_active",
		GatewayPeerID: "gw1",
		AggregateID:   "gateway:gw1",
		RequestedBy:   "e2e",
		RequestedAt:   1700001000,
		Accepted:      true,
		Status:        "applied",
		StateBefore:   "idle",
		StateAfter:    "active",
		DurationMS:    8,
		Payload:       map[string]any{"scene": "timeline"},
		Result:        map[string]any{"ok": true},
	})
	_ = dbAppendGatewayEvent(context.Background(), store, gatewayEventEntry{
		GatewayPeerID: "gw1",
		CommandID:     "cmd-timeline-1",
		Action:        "fee_pool_open",
		PoolID:        "pool-1",
		SequenceNum:   1,
		AmountSatoshi: 100,
		Payload:       map[string]any{"scene": "gateway"},
	})
	_ = dbAppendDomainEvent(context.Background(), store, domainEventEntry{
		CommandID:     "cmd-timeline-1",
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_opened",
		StateBefore:   "idle",
		StateAfter:    "active",
		Payload:       map[string]any{"scene": "domain"},
	})
	_ = dbAppendStateSnapshot(context.Background(), store, stateSnapshotEntry{
		CommandID:     "cmd-timeline-1",
		GatewayPeerID: "gw1",
		State:         "active",
		Payload:       map[string]any{"scene": "snapshot"},
	})
	_ = dbAppendEffectLog(context.Background(), store, effectLogEntry{
		CommandID:     "cmd-timeline-1",
		GatewayPeerID: "gw1",
		EffectType:    "chain",
		Stage:         "open",
		Status:        "done",
		Payload:       map[string]any{"scene": "effect"},
	})

	it, err := dbGetCommandTimelineItem(context.Background(), store, 1)
	if err != nil {
		t.Fatalf("get command timeline failed: %v", err)
	}
	if it.CommandID != "cmd-timeline-1" {
		t.Fatalf("unexpected command id: %s", it.CommandID)
	}
	if len(it.GatewayEvents) != 1 || len(it.DomainEvents) != 1 || len(it.StateSnapshots) != 1 || len(it.EffectLogs) != 1 {
		t.Fatalf("unexpected related facts: %+v", it)
	}

	srv := &httpAPIServer{db: db}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/feepool/commands/detail?id=1", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFeePoolCommandDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("timeline detail status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
