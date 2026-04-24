package chainbridge

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
)

// FeePoolChainBaseURLEnv 统一描述业务进程使用的 WOC API baseURL。
// 默认值落到本机透明代理的同网络 baseURL，保证独立运行与 e2e 走同一条协议链。
const FeePoolChainBaseURLEnv = "BSV_CHAIN_API_URL"

type RouteConfig struct {
	Provider string
	Network  string
	Profile  string
	BaseURL  string
	Auth     AuthConfig
}

// FeePoolChainPlan 只描述 route 清单、一个读链 route，以及广播回退顺序。
// 设计约束：
// - 不再把提交策略放进外部中间层；
// - 费用池自己决定“按哪几条 route 顺序广播”。
type FeePoolChainPlan struct {
	Routes       []RouteConfig
	ReadRoute    Route
	SubmitRoutes []Route
}

func (c RouteConfig) Route() Route {
	return Route{
		Provider: c.Provider,
		Network:  c.Network,
		Profile:  c.Profile,
	}.Normalize()
}

func (c RouteConfig) Validate() error {
	route := c.Route()
	if route.Provider == "" {
		return fmt.Errorf("route provider is required")
	}
	if route.Network == "" {
		return fmt.Errorf("route network is required")
	}
	return c.Auth.Validate()
}

func (c RouteConfig) Normalize() Route {
	return c.Route()
}

type FeePoolChain struct {
	manager      *routeManager
	readRoute    Route
	submitRoutes []Route
	baseURL      string
}

func NewEmbeddedFeePoolChain(routeCfg RouteConfig) (*FeePoolChain, error) {
	plan, err := singleRouteFeePoolChainPlan(routeCfg)
	if err != nil {
		return nil, err
	}
	return NewEmbeddedFeePoolChainFromPlan(plan)
}

func NewEmbeddedFeePoolChainFromPlan(plan FeePoolChainPlan) (*FeePoolChain, error) {
	normalized, err := normalizeFeePoolChainPlan(plan)
	if err != nil {
		return nil, err
	}
	manager, err := newRouteManager(normalized.Routes)
	if err != nil {
		return nil, err
	}
	return newFeePoolChain(manager, normalized.ReadRoute, normalized.SubmitRoutes, routeBaseURLForPlan(normalized, normalized.ReadRoute)), nil
}

func DefaultAPIBaseURL(network string) string {
	return wocproxy.DefaultBaseURLForNetwork(network)
}

func ResolveAPIBaseURLFromEnv(network string) string {
	if u := strings.TrimRight(strings.TrimSpace(os.Getenv(FeePoolChainBaseURLEnv)), "/"); u != "" {
		return u
	}
	return DefaultAPIBaseURL(network)
}

// NewDefaultFeePoolChain 返回真正使用配置 URL 的费用池链客户端。
// 设计约束：
// - Whatsonchain 默认始终指向透明代理 URL；
// - 业务日志与状态页展示的 BaseURL 必须和真实访问地址一致。
func NewDefaultFeePoolChain(routeCfg RouteConfig) (*FeePoolChain, error) {
	routeCfg.BaseURL = effectiveRouteBaseURL(routeCfg)
	return NewEmbeddedFeePoolChain(routeCfg)
}

func NewPortFeePoolChain(baseURL string, routeCfg RouteConfig) (*FeePoolChain, error) {
	routeCfg.BaseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return NewEmbeddedFeePoolChain(routeCfg)
}

func (c *FeePoolChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	if c == nil || c.manager == nil {
		return nil, fmt.Errorf("fee pool chain is nil")
	}
	address = strings.TrimSpace(address)
	client, err := c.manager.Open(c.readRoute)
	if err != nil {
		return nil, err
	}
	items, err := getUTXOsByRouteClient(context.Background(), client, address)
	if err != nil {
		return nil, err
	}
	out := make([]poolcore.UTXO, 0, len(items))
	for _, item := range items {
		out = append(out, poolcore.UTXO{TxID: item.TxID, Vout: item.Vout, Value: item.Value})
	}
	return out, nil
}

func (c *FeePoolChain) GetTipHeight() (uint32, error) {
	if c == nil || c.manager == nil {
		return 0, fmt.Errorf("fee pool chain is nil")
	}
	client, err := c.manager.Open(c.readRoute)
	if err != nil {
		return 0, err
	}
	return getTipByRouteClient(context.Background(), client)
}

func (c *FeePoolChain) Broadcast(txHex string) (string, error) {
	if c == nil || c.manager == nil {
		return "", fmt.Errorf("fee pool chain is nil")
	}
	txHex = strings.TrimSpace(txHex)
	if txHex == "" {
		return "", fmt.Errorf("tx_hex is required")
	}
	var lastErr error
	for _, route := range c.submitRoutes {
		client, err := c.manager.Open(route)
		if err != nil {
			lastErr = err
			continue
		}
		txid, err := broadcastByRouteClient(context.Background(), client, txHex)
		if err == nil {
			return txid, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return "", fmt.Errorf("submit routes are empty")
	}
	return "", lastErr
}

func (c *FeePoolChain) BaseURL() string {
	if c == nil {
		return ""
	}
	return c.baseURL
}

func (c *FeePoolChain) ReadRoute() Route {
	if c == nil {
		return Route{}
	}
	return c.readRoute
}

func (c *FeePoolChain) SubmitRoutes() []Route {
	if c == nil {
		return nil
	}
	return append([]Route(nil), c.submitRoutes...)
}

func newFeePoolChain(manager *routeManager, readRoute Route, submitRoutes []Route, baseURL string) *FeePoolChain {
	return &FeePoolChain{
		manager:      manager,
		readRoute:    readRoute.Normalize(),
		submitRoutes: append([]Route(nil), submitRoutes...),
		baseURL:      baseURL,
	}
}

func singleRouteFeePoolChainPlan(routeCfg RouteConfig) (FeePoolChainPlan, error) {
	route := routeCfg.Normalize()
	return FeePoolChainPlan{
		Routes: []RouteConfig{
			{
				Provider: route.Provider,
				Network:  route.Network,
				Profile:  route.Profile,
				BaseURL:  effectiveRouteBaseURL(routeCfg),
				Auth:     routeCfg.Auth,
			},
		},
		ReadRoute:    route,
		SubmitRoutes: []Route{route},
	}, nil
}

func normalizeFeePoolChainPlan(plan FeePoolChainPlan) (FeePoolChainPlan, error) {
	if len(plan.Routes) == 0 {
		return FeePoolChainPlan{}, fmt.Errorf("fee pool chain routes are required")
	}
	routeConfigs := make([]RouteConfig, 0, len(plan.Routes))
	seenRoutes := map[string]struct{}{}
	for _, rc := range plan.Routes {
		route := rc.Route()
		if route.Provider == "" {
			return FeePoolChainPlan{}, fmt.Errorf("route provider is required")
		}
		if route.Network == "" {
			return FeePoolChainPlan{}, fmt.Errorf("route network is required")
		}
		key := route.Key()
		if _, exists := seenRoutes[key]; exists {
			return FeePoolChainPlan{}, fmt.Errorf("duplicate route: %s", key)
		}
		seenRoutes[key] = struct{}{}
		routeConfigs = append(routeConfigs, rc)
	}
	readRoute := plan.ReadRoute.Normalize()
	if readRoute.Provider == "" {
		return FeePoolChainPlan{}, fmt.Errorf("read route provider is required")
	}
	if readRoute.Network == "" {
		return FeePoolChainPlan{}, fmt.Errorf("read route network is required")
	}
	if _, ok := seenRoutes[readRoute.Key()]; !ok {
		return FeePoolChainPlan{}, fmt.Errorf("read route is not present in route list: %s", readRoute.Key())
	}
	if !routeSupportsRead(readRoute.Provider) {
		return FeePoolChainPlan{}, fmt.Errorf("read route does not support fee pool reads: %s", readRoute.Key())
	}
	submitRoutes, err := normalizeChainBridgeSubmitRoutes(plan.SubmitRoutes)
	if err != nil {
		return FeePoolChainPlan{}, err
	}
	for _, route := range submitRoutes {
		if _, ok := seenRoutes[route.Key()]; !ok {
			return FeePoolChainPlan{}, fmt.Errorf("submit route is not present in route list: %s", route.Key())
		}
	}
	return FeePoolChainPlan{
		Routes:       routeConfigs,
		ReadRoute:    readRoute,
		SubmitRoutes: submitRoutes,
	}, nil
}

func normalizeChainBridgeSubmitRoutes(in []Route) ([]Route, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("submit routes are required")
	}
	out := make([]Route, 0, len(in))
	seen := map[string]struct{}{}
	for _, route := range in {
		n := route.Normalize()
		if n.Provider == "" {
			return nil, fmt.Errorf("submit route provider is required")
		}
		if n.Network == "" {
			return nil, fmt.Errorf("submit route network is required")
		}
		key := n.Key()
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate submit route: %s", key)
		}
		seen[key] = struct{}{}
		out = append(out, n)
	}
	return out, nil
}

func effectiveRouteBaseURL(routeCfg RouteConfig) string {
	baseURL := strings.TrimRight(strings.TrimSpace(routeCfg.BaseURL), "/")
	if baseURL != "" {
		return baseURL
	}
	if strings.EqualFold(strings.TrimSpace(routeCfg.Provider), WhatsOnChainProvider) {
		return ResolveAPIBaseURLFromEnv(routeCfg.Network)
	}
	return ""
}

func routeBaseURLForPlan(plan FeePoolChainPlan, route Route) string {
	want := route.Normalize().Key()
	for _, rc := range plan.Routes {
		if rc.Normalize().Key() != want {
			continue
		}
		return strings.TrimRight(strings.TrimSpace(rc.BaseURL), "/")
	}
	return ""
}

func routeSupportsRead(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case WhatsOnChainProvider:
		return true
	default:
		return false
	}
}

func getUTXOsByRouteClient(ctx context.Context, client *routeClient, address string) ([]whatsonchain.UTXO, error) {
	switch strings.ToLower(strings.TrimSpace(client.Provider())) {
	case WhatsOnChainProvider:
		if client == nil || client.whatsonchain == nil {
			return nil, fmt.Errorf("route %s does not support GetUTXOs", client.Route().Key())
		}
		return client.whatsonchain.GetAddressSpendableUnspent(ctx, address)
	default:
		return nil, fmt.Errorf("route %s does not support GetUTXOs", client.Route().Key())
	}
}

func getTipByRouteClient(ctx context.Context, client *routeClient) (uint32, error) {
	switch strings.ToLower(strings.TrimSpace(client.Provider())) {
	case WhatsOnChainProvider:
		if client == nil || client.whatsonchain == nil {
			return 0, fmt.Errorf("route %s does not support GetTipHeight", client.Route().Key())
		}
		return client.whatsonchain.GetChainInfo(ctx)
	default:
		return 0, fmt.Errorf("route %s does not support GetTipHeight", client.Route().Key())
	}
}

func broadcastByRouteClient(ctx context.Context, client *routeClient, txHex string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(client.Provider())) {
	case WhatsOnChainProvider:
		if client == nil || client.whatsonchain == nil {
			return "", fmt.Errorf("route %s does not support Broadcast", client.Route().Key())
		}
		return client.whatsonchain.PostTxRaw(ctx, txHex)
	case GorillaPoolARCProvider:
		if client == nil || client.gorillapoolARC == nil {
			return "", fmt.Errorf("route %s does not support Broadcast", client.Route().Key())
		}
		return client.gorillapoolARC.PostTxRaw(ctx, txHex)
	case TAALARCProvider:
		if client == nil || client.taalARC == nil {
			return "", fmt.Errorf("route %s does not support Broadcast", client.Route().Key())
		}
		return client.taalARC.PostTxRaw(ctx, txHex)
	case TAALLegacyProvider:
		if client == nil || client.taalLegacy == nil {
			return "", fmt.Errorf("route %s does not support Broadcast", client.Route().Key())
		}
		return client.taalLegacy.PostTxRaw(ctx, txHex)
	default:
		return "", fmt.Errorf("route %s does not support Broadcast", client.Route().Key())
	}
}
