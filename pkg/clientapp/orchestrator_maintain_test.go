package clientapp

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestOrchestratorReconcileSignal_FeePoolTickUsesMaintain(t *testing.T) {
	t.Parallel()

	o := newOrchestrator(&Runtime{}, nil)
	if o == nil {
		t.Fatal("newOrchestrator returned nil")
	}
	now := time.Unix(1700000000, 0)
	out := o.reconcileSignal(orchestratorSignal{
		Type:         orchestratorSignalFeePoolTick,
		AggregateKey: "gw-1",
	}, now)
	if len(out) != 1 {
		t.Fatalf("task count mismatch: got=%d want=1", len(out))
	}
	if got := out[0].TaskType; got != commandTypeFeePoolMaintain {
		t.Fatalf("task type mismatch: got=%s want=%s", got, commandTypeFeePoolMaintain)
	}
}

func TestOrchestratorReconcileSignal_ChainTipUsesMaintain(t *testing.T) {
	t.Parallel()

	rt := newRuntimeForTest(t, Config{}, "")
	rt.HealthyGWs = []peer.AddrInfo{
		{ID: peer.ID("gw-chain-1")},
		{ID: peer.ID("gw-chain-2")},
	}
	mustUpdateRuntimeConfigMemoryOnly(t, rt, func(cfg *Config) {
		cfg.Listen.Enabled = boolPtr(true)
	})
	o := newOrchestrator(rt, nil)
	if o == nil {
		t.Fatal("newOrchestrator returned nil")
	}
	now := time.Unix(1700000000, 0)
	out := o.reconcileSignal(orchestratorSignal{
		Type: orchestratorSignalChainTip,
		Payload: map[string]any{
			"tip_to": int64(123),
		},
	}, now)
	if len(out) != 2 {
		t.Fatalf("task count mismatch: got=%d want=2", len(out))
	}
	for i, task := range out {
if got := task.TaskType; got != commandTypeFeePoolMaintain {
		t.Fatalf("task[%d] task type mismatch: got=%s want=%s", i, got, commandTypeFeePoolMaintain)
		}
	}
}

func TestOrchestratorReconcileSignal_ChainTipSkipsWhenListenDisabled(t *testing.T) {
	t.Parallel()

	rt := newRuntimeForTest(t, Config{}, "")
	rt.HealthyGWs = []peer.AddrInfo{
		{ID: peer.ID("gw-chain-1")},
	}
	mustUpdateRuntimeConfigMemoryOnly(t, rt, func(cfg *Config) {
		cfg.Listen.Enabled = boolPtr(false)
	})
	o := newOrchestrator(rt, nil)
	if o == nil {
		t.Fatal("newOrchestrator returned nil")
	}
	out := o.reconcileSignal(orchestratorSignal{
		Type: orchestratorSignalChainTip,
		Payload: map[string]any{
			"tip_to": int64(123),
		},
	}, time.Unix(1700000000, 0))
	if len(out) != 0 {
		t.Fatalf("task count mismatch: got=%d want=0", len(out))
	}
}
