package domain

import (
	"context"
	"strings"

	domainmodule "github.com/bsv8/BFTP/pkg/modules/domain"
)

// BizResolve 是模块自己的最小业务入口。
//
// 设计说明：
// - 这里只做参数校验、归一化和错误码映射；
// - 真正的解析能力由基座注册的 provider 提供；
// - 这层不碰网络、不碰 DB，只收最小能力。
func BizResolve(ctx context.Context, resolver Resolver, rawDomain string) (ResolveResult, error) {
	if ctx == nil {
		return ResolveResult{}, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return ResolveResult{}, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if resolver == nil {
		return ResolveResult{}, moduleDisabledErr()
	}

	domainName, err := domainmodule.NormalizeName(rawDomain)
	if err != nil {
		return ResolveResult{}, NewError(CodeBadRequest, err.Error())
	}
	pubkeyHex, err := resolver.ResolveDomainToPubkey(ctx, domainName)
	if err != nil {
		if CodeOf(err) == "" {
			return ResolveResult{}, err
		}
		return ResolveResult{}, err
	}
	pubkeyHex = strings.ToLower(strings.TrimSpace(pubkeyHex))
	if pubkeyHex == "" {
		return ResolveResult{}, NewError(CodeInvalidPubkey, "invalid pubkey")
	}
	return ResolveResult{
		Domain:    domainName,
		PubkeyHex: pubkeyHex,
	}, nil
}

// ResolveHookRegistrar 是基座解析钩子的最小注册能力。
type ResolveHookRegistrar interface {
	RegisterDomainResolveHook(name string, hook Hook) (func(), error)
}

// RegisterResolveHook 把 domain 解析 provider 接到基座上。
//
// 设计说明：
// - provider 自己只负责单一解析后端；
// - 注册顺序由基座统一决定；
// - 这里只保留模块内的命名，不把接线逻辑散到别处。
func RegisterResolveHook(reg ResolveHookRegistrar, name string, hook Hook) (func(), error) {
	if reg == nil {
		return func() {}, nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = ResolveProviderName
	}
	return reg.RegisterDomainResolveHook(name, hook)
}
