package poolcore

import (
	"fmt"
	"strings"
)

const (
	lifecyclePendingBaseTx = "pending_base_tx"
	lifecycleActive        = "active"
	lifecycleFrozen        = "frozen"
	lifecycleShouldSubmit  = "should_submit"
	// 历史遗留状态：旧实现把“已广播待观察”持久化到了库里。
	// 现实现以“广播被 WOC 接受”为关闭真值，不再新写入该状态，仅用于启动后顺手收口旧数据。
	lifecycleCloseSubmitted = "close_submitted"
	lifecycleClosed         = "closed"
	lifecycleSettledExtern  = "settled_external"
)

const (
	payabilityPayable = "payable"
	payabilityBlocked = "blocked"
)

const (
	phasePayable      = "payable"
	phaseNearExpiry   = "near_expiry"
	phaseShouldSubmit = "should_submit"
)

func normalizeLifecycleState(v string) string {
	out := strings.ToLower(strings.TrimSpace(v))
	if out == "" {
		return lifecyclePendingBaseTx
	}
	return out
}

func guardBlocksOrDefault(g uint32) uint32 {
	if g == 0 {
		return defaultPayGuardBlocks
	}
	return g
}

// computePhaseAndPayability 按链高实时计算业务阶段。
// 说明：
// - phase/payability 是运行时视图，不作为数据库真值；
// - 当无法解析 expire_height 或拿不到 tip 时，由调用方决定是否拒绝业务操作。
func computePhaseAndPayability(tip uint32, expireHeight uint32, guardBlocks uint32) (string, string) {
	g := guardBlocksOrDefault(guardBlocks)
	if tip+g < expireHeight {
		return phasePayable, payabilityPayable
	}
	if tip < expireHeight+1 {
		return phaseNearExpiry, payabilityBlocked
	}
	return phaseShouldSubmit, payabilityBlocked
}

func ensureActivePoolGate(row GatewaySessionRow) error {
	if normalizeLifecycleState(row.LifecycleState) != lifecycleActive {
		return fmt.Errorf("not_active_pool")
	}
	return nil
}
