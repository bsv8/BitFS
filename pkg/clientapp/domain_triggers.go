package clientapp

import (
	"context"
	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
)

const (
	domainQuotePayloadVersion = "bsv8-domain-register-quote-v1"
)

type TriggerDomainRegisterNameParams = domainbiz.TriggerDomainRegisterNameParams

type TriggerDomainRegisterNameResult = domainbiz.TriggerDomainRegisterNameResult

type TriggerDomainRegisterLockParams = domainbiz.TriggerDomainRegisterLockParams

type TriggerDomainRegisterLockResult = domainbiz.TriggerDomainRegisterLockResult

type TriggerDomainPrepareRegisterParams = domainbiz.TriggerDomainPrepareRegisterParams

type TriggerDomainPrepareRegisterResult = domainbiz.TriggerDomainPrepareRegisterResult

type TriggerDomainSubmitPreparedRegisterParams = domainbiz.TriggerDomainSubmitPreparedRegisterParams

type TriggerDomainSubmitPreparedRegisterResult = domainbiz.TriggerDomainSubmitPreparedRegisterResult

type TriggerDomainSetTargetParams = domainbiz.TriggerDomainSetTargetParams

type TriggerDomainSetTargetResult = domainbiz.TriggerDomainSetTargetResult

// domain 相关 trigger 的收费边界说明：
// - resolve/query/register_lock/set_target 这类请求，都是在已有 2of2 费用池会话上推进一次 PayConfirm；
// - 只有 register_submit 会把客户端本地构造好的独立注册交易交给 domain 去广播；
// - 因此读这组代码时，要把“费用池状态推进”和“独立注册交易广播”看成两件不同的事。

// TriggerDomainRegisterName 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易 -> 远端提交”。
// 设计说明：
// - 这里返回时，要么注册已经成功写链并拿到服务端回执，要么明确失败且不会偷偷广播；
// - 注册交易在客户端本地构造，但不自行广播，保持“确认资格后才上链”的语义。
func TriggerDomainRegisterName(ctx context.Context, store *clientDB, rt *Runtime, p TriggerDomainRegisterNameParams) (TriggerDomainRegisterNameResult, error) {
	adapter := newDomainModuleAdapter(rt, store)
	out, err := domainbiz.TriggerDomainRegisterName(ctx, adapter, adapter, domainbiz.TriggerDomainRegisterNameParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	return TriggerDomainRegisterNameResult(out), err
}

// TriggerDomainRegisterLock 通过 domain 系统完成“查询 -> 锁名”，但不提交注册交易。
// 设计意图：
// - 让 e2e 可以验证锁冲突、锁过期等边界，而不绕过真实费用池与签名链路；
// - 返回时若成功，代表服务端已为该 client 持有一个有效锁，可继续在锁期内构造并提交注册交易。
func TriggerDomainRegisterLock(ctx context.Context, store *clientDB, rt *Runtime, p TriggerDomainRegisterLockParams) (TriggerDomainRegisterLockResult, error) {
	adapter := newDomainModuleAdapter(rt, store)
	out, err := domainbiz.TriggerDomainRegisterLock(ctx, adapter, adapter, domainbiz.TriggerDomainRegisterLockParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	return TriggerDomainRegisterLockResult(out), err
}

// TriggerDomainPrepareRegister 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易”，但不提交交易。
// 设计意图：
// - 让 e2e 可以验证 lock_missing、broadcast_failed 等 RegisterSubmit 边界；
// - 返回的 tx_hex 仍然是按正式注册流程构造的真实交易，可直接再提交给 domain。
func TriggerDomainPrepareRegister(ctx context.Context, store *clientDB, rt *Runtime, p TriggerDomainPrepareRegisterParams) (TriggerDomainPrepareRegisterResult, error) {
	adapter := newDomainModuleAdapter(rt, store)
	out, err := domainbiz.TriggerDomainPrepareRegister(ctx, adapter, adapter, domainbiz.TriggerDomainPrepareRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	return TriggerDomainPrepareRegisterResult(out), err
}

// TriggerDomainSubmitPreparedRegister 把已准备好的正式注册交易提交给 domain。
// 设计意图：
// - 与 TriggerDomainPrepareRegister 配合，覆盖“锁还在 / 锁过期 / 广播失败”三类 RegisterSubmit 行为；
// - 提交成功后，仍然会把本地已广播交易投影回钱包，保持与正式注册入口一致。
func TriggerDomainSubmitPreparedRegister(ctx context.Context, store *clientDB, rt *Runtime, p TriggerDomainSubmitPreparedRegisterParams) (TriggerDomainSubmitPreparedRegisterResult, error) {
	adapter := newDomainModuleAdapter(rt, store)
	out, err := domainbiz.TriggerDomainSubmitPreparedRegister(ctx, adapter, adapter, domainbiz.TriggerDomainSubmitPreparedRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		RegisterTxHex:     p.RegisterTxHex,
	})
	return TriggerDomainSubmitPreparedRegisterResult(out), err
}

// TriggerDomainSetTarget 通过 domain 系统更新域名指向。
// 设计说明：
// - 这里先付费查询一次，拿到最新状态与精确 set_target 费率，避免用 publish 最大值粗扣；
// - 只有当前 owner 才允许更新 target。
func TriggerDomainSetTarget(ctx context.Context, store *clientDB, rt *Runtime, p TriggerDomainSetTargetParams) (TriggerDomainSetTargetResult, error) {
	adapter := newDomainModuleAdapter(rt, store)
	out, err := domainbiz.TriggerDomainSetTarget(ctx, adapter, adapter, domainbiz.TriggerDomainSetTargetParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	return TriggerDomainSetTargetResult(out), err
}
