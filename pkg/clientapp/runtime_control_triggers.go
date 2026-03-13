package clientapp

import (
	"fmt"
	"strings"
)

type ListenAutoRenewRoundsResult struct {
	Before uint64 `json:"before"`
	After  uint64 `json:"after"`
}

// TriggerSetListenAutoRenewRounds 动态修改 listen.auto_renew_rounds（用于 e2e 场景触发）。
func TriggerSetListenAutoRenewRounds(rt *Runtime, rounds uint64) (ListenAutoRenewRoundsResult, error) {
	if rt == nil {
		return ListenAutoRenewRoundsResult{}, fmt.Errorf("runtime not initialized")
	}
	if rounds == 0 {
		return ListenAutoRenewRoundsResult{}, fmt.Errorf("auto renew rounds must be greater than zero")
	}
	before := rt.runIn.Listen.AutoRenewRounds
	rt.runIn.Listen.AutoRenewRounds = rounds
	return ListenAutoRenewRoundsResult{Before: before, After: rounds}, nil
}

type EmitFeePoolTickSignalParams struct {
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
	Count         int    `json:"count,omitempty"`
	Source        string `json:"source,omitempty"`
}

type EmitFeePoolTickSignalResult struct {
	GatewayPeerID string `json:"gateway_pubkey_hex"`
	EmittedCount  int    `json:"emitted_count"`
}

// TriggerEmitFeePoolTickSignal 向 orchestrator 发出 feepool.billing_tick（用于 e2e 可控触发）。
func TriggerEmitFeePoolTickSignal(rt *Runtime, p EmitFeePoolTickSignalParams) (EmitFeePoolTickSignalResult, error) {
	if rt == nil {
		return EmitFeePoolTickSignalResult{}, fmt.Errorf("runtime not initialized")
	}
	orch := getClientOrchestrator(rt)
	if orch == nil {
		return EmitFeePoolTickSignalResult{}, fmt.Errorf("orchestrator not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return EmitFeePoolTickSignalResult{}, err
	}
	n := p.Count
	if n <= 0 {
		n = 1
	}
	source := strings.TrimSpace(p.Source)
	if source == "" {
		source = "trigger_emit_feepool_tick"
	}
	for i := 0; i < n; i++ {
		orch.EmitSignal(orchestratorSignal{
			Source:       source,
			Type:         orchestratorSignalFeePoolTick,
			AggregateKey: gw.ID.String(),
			Payload: map[string]any{
				"trigger": "manual_feepool_tick_signal",
			},
		})
	}
	return EmitFeePoolTickSignalResult{
		GatewayPeerID: gatewayBusinessID(rt, gw.ID),
		EmittedCount:  n,
	}, nil
}
