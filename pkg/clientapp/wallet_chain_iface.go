package clientapp

import (
	"context"
	"fmt"
	"time"

	chainapi "github.com/bsv8/BSVChainAPI"
	"github.com/bsv8/BSVChainAPI/whatsonchain"
)

// WalletChainClient 只承载钱包同步所需的 WOC 观察能力。
// 设计约束：
// - 钱包同步当前明确绑定 whatsonchain 语义；
// - BSVChainAPI 只负责 route/protect/runtime，不再额外定义中间数据结构。
type WalletChainClient interface {
	BaseURL() string
	GetAddressConfirmedUnspent(address string) ([]whatsonchain.UTXO, error)
	GetChainInfo() (uint32, error)
	GetAddressConfirmedHistoryPage(ctx context.Context, address string, q whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error)
	GetAddressUnconfirmedHistory(ctx context.Context, address string) ([]string, error)
	GetTxHash(ctx context.Context, txid string) (whatsonchain.TxDetail, error)
}

type walletRouteChainClient struct {
	manager *chainapi.Manager
	route   chainapi.Route
	baseURL string
}

func NewWalletRouteChainClient(route chainapi.Route, protectInterval time.Duration) (WalletChainClient, error) {
	route = route.Normalize()
	if route.Provider == "" || route.Network == "" {
		return nil, fmt.Errorf("wallet chain route provider and network are required")
	}
	if route.Provider != chainapi.WhatsOnChainProvider {
		return nil, fmt.Errorf("wallet chain only supports provider %s", chainapi.WhatsOnChainProvider)
	}
	manager, err := chainapi.NewManager(chainapi.Config{
		Routes: []chainapi.RouteConfig{
			{
				Provider: route.Provider,
				Network:  route.Network,
				Profile:  route.Profile,
				Protect:  chainapi.ProtectConfig{MinInterval: protectInterval},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	client := &walletRouteChainClient{
		manager: manager,
		route:   route,
		baseURL: "embedded",
	}
	if rc, err := manager.Open(route); err == nil {
		if baseURL, baseErr := walletRouteBaseURL(rc); baseErr == nil {
			client.baseURL = baseURL
		}
	}
	return client, nil
}

func (c *walletRouteChainClient) BaseURL() string {
	if c == nil {
		return ""
	}
	return c.baseURL
}

func (c *walletRouteChainClient) GetAddressConfirmedUnspent(address string) ([]whatsonchain.UTXO, error) {
	client, err := c.open()
	if err != nil {
		return nil, err
	}
	return client.GetAddressConfirmedUnspent(context.Background(), address)
}

func (c *walletRouteChainClient) GetChainInfo() (uint32, error) {
	client, err := c.open()
	if err != nil {
		return 0, err
	}
	return client.GetChainInfo(context.Background())
}

func (c *walletRouteChainClient) GetAddressConfirmedHistoryPage(ctx context.Context, address string, q whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error) {
	client, err := c.open()
	if err != nil {
		return whatsonchain.ConfirmedHistoryPage{}, err
	}
	return client.GetAddressConfirmedHistoryPage(ctx, address, q)
}

func (c *walletRouteChainClient) GetAddressUnconfirmedHistory(ctx context.Context, address string) ([]string, error) {
	client, err := c.open()
	if err != nil {
		return nil, err
	}
	return client.GetAddressUnconfirmedHistory(ctx, address)
}

func (c *walletRouteChainClient) GetTxHash(ctx context.Context, txid string) (whatsonchain.TxDetail, error) {
	client, err := c.open()
	if err != nil {
		return whatsonchain.TxDetail{}, err
	}
	return client.GetTxHash(ctx, txid)
}

func (c *walletRouteChainClient) open() (chainapi.WhatsOnChainClient, error) {
	if c == nil || c.manager == nil {
		return nil, fmt.Errorf("wallet chain is nil")
	}
	rc, err := c.manager.Open(c.route)
	if err != nil {
		return nil, err
	}
	return rc.WhatsOnChain()
}

func walletRouteBaseURL(client *chainapi.RouteClient) (string, error) {
	if client == nil {
		return "", fmt.Errorf("route client is nil")
	}
	wocClient, err := client.WhatsOnChain()
	if err != nil {
		return "", err
	}
	return wocClient.BaseURL(), nil
}
