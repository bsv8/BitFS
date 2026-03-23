package clientapp

import (
	"fmt"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

// NewWalletChainClient 负责把钱包同步所需的 WOC 客户端装配出来。
// 设计约束：
// - 这里仅做 runtime 装配，不再复制一层同构接口；
// - 钱包同步直接依赖 whatsonchain 原始语义，ctx 必须由调用方显式传递；
// - 若后续钱包需要脱离 WOC 语义，应在业务层重画边界，而不是再补一层转发壳。
func NewWalletChainClient(route chainbridge.Route) (walletChainClient, error) {
	return NewWalletChainClientWithBaseURL(route, chainbridge.ResolveAPIBaseURLFromEnv(route.Network), whatsonchain.AuthConfig{})
}

// NewWalletChainClientWithBaseURL 允许上层把已经决议好的 WOC baseURL / auth 明确注入进来。
// 设计约束：
// - 仅上层运行时装配可调用，不把“从哪里决议 URL / auth”继续渗透进钱包同步域；
// - 桌面托管模式可以用它把本地 wocproxy 或直连 WOC 的选择统一收口在主进程。
func NewWalletChainClientWithBaseURL(route chainbridge.Route, baseURL string, auth whatsonchain.AuthConfig) (walletChainClient, error) {
	route = route.Normalize()
	if route.Provider == "" || route.Network == "" {
		return nil, fmt.Errorf("wallet chain route provider and network are required")
	}
	if route.Provider != chainbridge.WhatsOnChainProvider {
		return nil, fmt.Errorf("wallet chain only supports provider %s", chainbridge.WhatsOnChainProvider)
	}
	return whatsonchain.NewClient(baseURL, auth), nil
}
