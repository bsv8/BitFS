package clientapp

import "testing"

func TestDbAppendCommandChainRejectsBlankCommandID(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)

	if err := dbAppendCommandJournal(nil, store, commandJournalEntry{
		CommandID:     "   ",
		CommandType:   "ensure_active",
		GatewayPeerID: "gw1",
	}); err == nil {
		t.Fatalf("expected blank command_id command journal write to be rejected")
	}
	if err := dbAppendDomainEvent(nil, store, domainEventEntry{
		CommandID:     "   ",
		GatewayPeerID: "gw1",
		EventName:     "fee_pool_paused_insufficient",
		StateBefore:   "idle",
		StateAfter:    "paused_insufficient",
	}); err == nil {
		t.Fatalf("expected blank command_id domain event write to be rejected")
	}
	if err := dbAppendStateSnapshot(nil, store, stateSnapshotEntry{
		CommandID:     "   ",
		GatewayPeerID: "gw1",
		State:         "paused_insufficient",
	}); err == nil {
		t.Fatalf("expected blank command_id state snapshot write to be rejected")
	}
	if err := dbAppendEffectLog(nil, store, effectLogEntry{
		CommandID:     "   ",
		GatewayPeerID: "gw1",
		EffectType:    "chain",
		Stage:         "fee_pool_open",
		Status:        "paused",
	}); err == nil {
		t.Fatalf("expected blank command_id effect log write to be rejected")
	}

	for _, table := range []string{"proc_command_journal", "proc_domain_events", "proc_state_snapshots", "proc_effect_logs"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(1) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s failed: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("unexpected rows in %s: %d", table, count)
		}
	}
}
