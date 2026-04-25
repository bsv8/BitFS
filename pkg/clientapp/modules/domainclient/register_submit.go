package domainclient

import "context"

// TriggerDomainSubmitPreparedRegister 把已准备好的正式注册交易提交给 domain。
func TriggerDomainSubmitPreparedRegister(ctx context.Context, store BusinessStore, rt RuntimePorts, p TriggerDomainSubmitPreparedRegisterParams) (TriggerDomainSubmitPreparedRegisterResult, error) {
	return triggerDomainSubmitPreparedRegister(ctx, backendBridge{BusinessStore: store, RuntimePorts: rt}, p)
}
