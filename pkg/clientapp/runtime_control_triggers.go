package clientapp

import (
	"fmt"
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
