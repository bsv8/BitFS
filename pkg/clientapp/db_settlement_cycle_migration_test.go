package clientapp

import "testing"

func TestSettlementCycleSchema_UsesSourceModelOnly(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB failed: %v", err)
	}

	cols, err := tableColumns(db, "fact_settlement_cycles")
	if err != nil {
		t.Fatalf("inspect fact_settlement_cycles columns failed: %v", err)
	}
	for _, col := range []string{"id", "cycle_id", "source_type", "source_id", "state", "gross_amount_satoshi", "gate_fee_satoshi", "net_amount_satoshi", "cycle_index", "occurred_at_unix", "confirmed_at_unix", "note", "payload_json"} {
		if _, ok := cols[col]; !ok {
			t.Fatalf("fact_settlement_cycles missing column %s", col)
		}
	}
	if len(cols) != 13 {
		t.Fatalf("fact_settlement_cycles column count mismatch: got %d want %d", len(cols), 13)
	}

	if unique, err := tableHasUniqueIndexOnColumns(db, "fact_settlement_cycles", []string{"source_type", "source_id"}); err != nil {
		t.Fatalf("inspect unique index failed: %v", err)
	} else if !unique {
		t.Fatal("fact_settlement_cycles should keep unique constraint on (source_type, source_id)")
	}
}
