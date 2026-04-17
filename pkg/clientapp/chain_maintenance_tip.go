package clientapp

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

func (m *chainMaintainer) runTipWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt, m.store)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_tip_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainTipWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskTip, trigger)
			return map[string]any{"task_type": chainTaskTip, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_tip_task_register_failed", map[string]any{"error": err.Error()})
		return
	}
	obs.Info("bitcast-client", "chain_tip_task_registered", map[string]any{
		"interval_sec": int64(chainTipWorkerInterval / time.Second),
	})
}

func (m *chainMaintainer) executeTipTask(ctx context.Context, task chainTask) (map[string]any, error) {
	if m.rt == nil || m.rt.WalletChain == nil {
		return map[string]any{"task_type": chainTaskTip}, fmt.Errorf("runtime wallet chain not initialized")
	}
	before, _ := dbLoadChainTipState(ctx, m.store)
	tip, err := m.rt.WalletChain.GetChainInfo(ctx)
	if err != nil {
		_ = dbUpdateChainTipStateError(ctx, m.store, err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskTip}, err
	}
	if err := dbUpsertChainTipState(ctx, m.store, tip, "", task.TriggerSource, time.Now().Unix(), 0); err != nil {
		return map[string]any{"task_type": chainTaskTip, "tip_height": tip}, err
	}
	emitted := false
	if before.TipHeight > 0 && tip > before.TipHeight {
		if m.rt != nil && m.rt.orch != nil {
			m.rt.orch.EmitSignal(orchestratorSignal{
				Source:       "chain_tip_worker",
				Type:         orchestratorSignalChainTip,
				AggregateKey: "chain:tip",
				Payload: map[string]any{
					"tip_from": before.TipHeight,
					"tip_to":   tip,
				},
			})
			emitted = true
		}
	}
	return map[string]any{
		"task_type":   chainTaskTip,
		"tip_from":    before.TipHeight,
		"tip_to":      tip,
		"signal_emit": emitted,
	}, nil
}
