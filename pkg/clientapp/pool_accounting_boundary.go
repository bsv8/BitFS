package clientapp

import "strings"

// 费用池边界口径。
// 设计说明：
// - 这里把“业务动作”和“fact 表事件”分开定义，避免把两层口径混成一层。
// - 旧路径仍然保留现状行为，第1步只做统一引用和可见告警，不做硬切断。

const (
	// 业务层动作口径。
	PoolBusinessActionOpen       = "open"
	PoolBusinessActionClose      = "close"
	PoolBusinessActionServicePay = "service_pay"
	PoolBusinessActionCycleFee   = "cycle_fee"
	PoolBusinessActionPayLegacy  = "pay"

	// fact 表事件口径。
	PoolFactEventKindPoolEvent = "pool_event"
	PoolFactEventKindTxHistory = "tx_history"
)

// NormalizePoolBusinessAction 把历史写法收敛成统一口径。
// 说明：这里只做口径识别，不回写、不改业务结果。
func NormalizePoolBusinessAction(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch kind {
	case PoolBusinessActionPayLegacy:
		return PoolBusinessActionServicePay
	default:
		return kind
	}
}

// IsPoolFactEventKind 判断是否属于当前费用池 fact 事件口径。
func IsPoolFactEventKind(kind string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return kind == PoolFactEventKindPoolEvent || kind == PoolFactEventKindTxHistory
}

// IsPoolFactOpenCloseAction 判断该业务动作是否仍允许沿用 fact 口径。
// 第1步只做识别，不做拦截。
func IsPoolFactOpenCloseAction(kind string) bool {
	kind = NormalizePoolBusinessAction(kind)
	return kind == PoolBusinessActionOpen || kind == PoolBusinessActionClose
}

// IsPoolFactAllocationDisallowed 判断某个业务动作是否不该再写进 fact 主路径。
// 说明：open/close 仍保留在现有链路里，其余动作视为旧口径。
func IsPoolFactAllocationDisallowed(kind string) bool {
	return !IsPoolFactOpenCloseAction(kind)
}
