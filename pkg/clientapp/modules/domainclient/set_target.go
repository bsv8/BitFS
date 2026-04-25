package domainclient

import "context"

// TriggerDomainSetTarget 通过 domain 系统更新域名指向。
func TriggerDomainSetTarget(ctx context.Context, store BusinessStore, rt RuntimePorts, p TriggerDomainSetTargetParams) (TriggerDomainSetTargetResult, error) {
	return triggerDomainSetTarget(ctx, backendBridge{BusinessStore: store, RuntimePorts: rt}, p)
}
