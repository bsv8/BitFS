package clientapp

import (
	"context"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

func (m *chainMaintainer) runUTXOWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt, m.store)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_utxo_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainUTXOWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskUTXO, trigger)
			return map[string]any{"task_type": chainTaskUTXO, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_utxo_task_register_failed", map[string]any{"error": err.Error()})
		return
	}
	obs.Info("bitcast-client", "chain_utxo_task_registered", map[string]any{
		"interval_sec": int64(chainUTXOWorkerInterval / time.Second),
	})
}
