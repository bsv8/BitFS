package clientapp

import (
	"context"
	"testing"
)

func TestChainWorkerLogTrimKeepsLimitWithoutTypeMismatch(t *testing.T) {
	t.Parallel()

	db := openSchemaTestDB(t)
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	store := newClientDB(db, nil)

	for i := 0; i < chainWorkerLogKeepCount+1; i++ {
		dbAppendChainTipWorkerLog(context.Background(), store, chainWorkerLogEntry{
			TriggeredAtUnix: int64(1700000000 + i),
			StartedAtUnix:   int64(1700000000 + i),
			EndedAtUnix:     int64(1700000000 + i + 1),
			DurationMS:      int64(1),
			TriggerSource:   "unit_test",
			Status:          "ok",
			Result:          map[string]any{"seq": i},
		})
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM proc_chain_tip_worker_logs`).Scan(&count); err != nil {
		t.Fatalf("count worker logs failed: %v", err)
	}
	if count != int64(chainWorkerLogKeepCount) {
		t.Fatalf("trim count mismatch: got=%d want=%d", count, chainWorkerLogKeepCount)
	}
}
