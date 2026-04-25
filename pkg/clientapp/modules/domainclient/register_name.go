package domainclient

import "context"

// TriggerDomainRegisterName 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易 -> 远端提交”。
func TriggerDomainRegisterName(ctx context.Context, store BusinessStore, rt RuntimePorts, p TriggerDomainRegisterNameParams) (TriggerDomainRegisterNameResult, error) {
	return triggerDomainRegisterName(ctx, backendBridge{BusinessStore: store, RuntimePorts: rt}, p)
}
