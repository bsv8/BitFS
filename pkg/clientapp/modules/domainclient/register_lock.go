package domainclient

import "context"

// TriggerDomainRegisterLock 通过 domain 系统完成“查询 -> 锁名”，但不提交注册交易。
func TriggerDomainRegisterLock(ctx context.Context, store BusinessStore, rt RuntimePorts, p TriggerDomainRegisterLockParams) (TriggerDomainRegisterLockResult, error) {
	return triggerDomainRegisterLock(ctx, backendBridge{BusinessStore: store, RuntimePorts: rt}, p)
}
