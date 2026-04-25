package domainclient

import "context"

// TriggerDomainPrepareRegister 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易”，但不提交交易。
func TriggerDomainPrepareRegister(ctx context.Context, store BusinessStore, rt RuntimePorts, p TriggerDomainPrepareRegisterParams) (TriggerDomainPrepareRegisterResult, error) {
	return triggerDomainPrepareRegister(ctx, backendBridge{BusinessStore: store, RuntimePorts: rt}, p)
}
