package clientapp

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestOrchestratorReconcileSignal_FeePoolTickUsesMaintain(t *testing.T) {
	t.Parallel()

	o := newOrchestrator(&Runtime{})
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
	if got := out[0].Command.CommandType; got != clientKernelCommandFeePoolMaintain {
		t.Fatalf("command type mismatch: got=%s want=%s", got, clientKernelCommandFeePoolMaintain)
	}
}

func TestOrchestratorReconcileSignal_ChainTipUsesMaintain(t *testing.T) {
	t.Parallel()

	rt := &Runtime{
		HealthyGWs: []peer.AddrInfo{
			{ID: peer.ID("gw-chain-1")},
			{ID: peer.ID("gw-chain-2")},
		},
	}
	rt.runIn.Listen.Enabled = boolPtr(true)
	o := newOrchestrator(rt)
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
		if got := task.Command.CommandType; got != clientKernelCommandFeePoolMaintain {
			t.Fatalf("task[%d] command type mismatch: got=%s want=%s", i, got, clientKernelCommandFeePoolMaintain)
		}
	}
}

func TestOrchestratorReconcileSignal_ChainTipSkipsWhenListenDisabled(t *testing.T) {
	t.Parallel()

	rt := &Runtime{
		HealthyGWs: []peer.AddrInfo{
			{ID: peer.ID("gw-chain-1")},
		},
	}
	rt.runIn.Listen.Enabled = boolPtr(false)
	o := newOrchestrator(rt)
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
