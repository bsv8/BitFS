package clientapp

import (
	"context"
	"fmt"
	"strings"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
)

// ResolveDomainToPubkey 是 BitFS 基座统一域名解析入口。
//
// 设计说明：
// - 这里只收 domain，只返回 pubkey hex；
// - 真正的解析顺序由运行配置 domain.resolve_order 控制；
// - 基座不关心 provider 细节，只做调度、归一化和错误码收口。
func ResolveDomainToPubkey(ctx context.Context, rt *Runtime, rawDomain string) (string, error) {
	if ctx == nil {
		return "", domainbiz.NewError(domainbiz.CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return "", domainbiz.NewError(domainbiz.CodeRequestCanceled, ctx.Err().Error())
	}
	if rt == nil || rt.modules == nil {
		return "", domainbiz.NewError(domainbiz.CodeDomainResolverUnavailable, "domain resolver is unavailable")
	}

	domain, err := domainwire.NormalizeName(rawDomain)
	if err != nil {
		return "", domainbiz.NewError(domainbiz.CodeBadRequest, err.Error())
	}

	var order []string
	if cfgSvc := rt.RuntimeConfigService(); cfgSvc != nil {
		order = cfgSvc.DomainResolveOrder()
	}
	out, err := rt.modules.DispatchDomainResolve(ctx, domain, order...)
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(out.PubkeyHex)), nil
}

// validateDomainResolveOrderForRuntime 只校验“写进去的顺序”是否来自当前已注册 provider。
//
// 设计说明：
// - 允许空：空代表按当前注册顺序执行；
// - 只校验当前注册表，不依赖静态白名单；
// - 这样模块是否编译启用，由接线层和注册表共同决定。
func validateDomainResolveOrderForRuntime(rt *Runtime, order []string) error {
	if len(order) == 0 {
		return nil
	}
	if rt == nil || rt.modules == nil {
		return fmt.Errorf("runtime is not initialized")
	}
	for _, raw := range order {
		name := strings.TrimSpace(raw)
		if name == "" {
			return fmt.Errorf("domain.resolve_order contains empty provider")
		}
		if !rt.modules.domainResolveHasProvider(name) {
			return fmt.Errorf("domain.resolve_order contains disabled provider: %s", name)
		}
	}
	return nil
}
