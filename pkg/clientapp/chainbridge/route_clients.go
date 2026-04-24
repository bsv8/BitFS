package chainbridge

import (
	"fmt"
	"strings"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

const (
	gorillaPoolARCBaseURL    = "https://arc.gorillapool.io"
	gorillaPoolARCSubmitPath = "/v1/tx"

	taalARCBaseURL    = "https://arc.taal.com"
	taalARCSubmitPath = "/v1/tx"

	taalLegacyBaseURL    = "https://api.taal.com"
	taalLegacySubmitPath = "/api/v1/broadcast"
)

// routeClient 是费用池自己的最小 route 句柄。
// 设计约束：
// - 这里只保留费用池真正需要的 vendor client；
// - 不再额外抽一层“通用链 API 平台”。
type routeClient struct {
	route          Route
	whatsonchain   *whatsonchain.Client
	gorillapoolARC rawTxSubmitClient
	taalARC        rawTxSubmitClient
	taalLegacy     rawTxSubmitClient
}

func (c *routeClient) Route() Route {
	if c == nil {
		return Route{}
	}
	return c.route
}

func (c *routeClient) Provider() string {
	if c == nil {
		return ""
	}
	return c.route.Provider
}

type routeManager struct {
	routes map[string]*routeClient
}

func newRouteManager(routes []RouteConfig) (*routeManager, error) {
	if len(routes) == 0 {
		return nil, fmt.Errorf("at least one route is required")
	}
	out := &routeManager{routes: map[string]*routeClient{}}
	for _, rc := range routes {
		if err := rc.Validate(); err != nil {
			return nil, err
		}
		route := rc.Route()
		key := route.Key()
		if _, exists := out.routes[key]; exists {
			return nil, fmt.Errorf("duplicate route: %s", key)
		}
		client, err := buildRouteClient(rc)
		if err != nil {
			return nil, err
		}
		out.routes[key] = client
	}
	return out, nil
}

func (m *routeManager) Open(route Route) (*routeClient, error) {
	if m == nil {
		return nil, fmt.Errorf("manager is nil")
	}
	key := route.Key()
	client := m.routes[key]
	if client == nil {
		return nil, fmt.Errorf("route not found: %s", key)
	}
	return client, nil
}

func buildRouteClient(cfg RouteConfig) (*routeClient, error) {
	route := cfg.Route()
	switch route.Provider {
	case WhatsOnChainProvider:
		baseURL := strings.TrimSpace(cfg.BaseURL)
		if baseURL == "" {
			baseURL = whatsonchain.BaseURLForNetwork(route.Network)
		}
		return &routeClient{
			route:        route,
			whatsonchain: whatsonchain.NewClient(baseURL, whatsonchain.AuthConfig(cfg.Auth)),
		}, nil
	case GorillaPoolARCProvider:
		if route.Network != "main" {
			return nil, fmt.Errorf("gorillapool_arc only supports main network")
		}
		baseURL := strings.TrimSpace(cfg.BaseURL)
		if baseURL == "" {
			baseURL = gorillaPoolARCBaseURL
		}
		auth := cfg.Auth
		if strings.TrimSpace(auth.Value) != "" && strings.TrimSpace(auth.Mode) == "" {
			auth.Mode = "bearer"
		}
		return &routeClient{
			route:          route,
			gorillapoolARC: newRawTxClient(baseURL, gorillaPoolARCSubmitPath, auth),
		}, nil
	case TAALARCProvider:
		if route.Network != "main" {
			return nil, fmt.Errorf("taal_arc only supports main network")
		}
		baseURL := strings.TrimSpace(cfg.BaseURL)
		if baseURL == "" {
			baseURL = taalARCBaseURL
		}
		auth := cfg.Auth
		if strings.TrimSpace(auth.Value) != "" && strings.TrimSpace(auth.Mode) == "" {
			auth.Mode = "bearer"
		}
		return &routeClient{
			route:   route,
			taalARC: newRawTxClient(baseURL, taalARCSubmitPath, auth),
		}, nil
	case TAALLegacyProvider:
		if route.Network != "main" {
			return nil, fmt.Errorf("taal_legacy only supports main network")
		}
		baseURL := strings.TrimSpace(cfg.BaseURL)
		if baseURL == "" {
			baseURL = taalLegacyBaseURL
		}
		auth := cfg.Auth
		if strings.TrimSpace(auth.Value) != "" && strings.TrimSpace(auth.Mode) == "" {
			auth.Mode = "header"
			auth.Name = "Authorization"
		}
		return &routeClient{
			route:      route,
			taalLegacy: newRawTxClient(baseURL, taalLegacySubmitPath, auth),
		}, nil
	default:
		return nil, fmt.Errorf("provider not registered: %s", route.Provider)
	}
}
