package clientapp

import (
	"context"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

// walletChainClient 只保留钱包同步真正依赖的 WOC 原始语义。
// 设计约束：
// - BitFS 运行时不再持有外部“链 API 平台”的抽象；
// - 钱包同步直接面对 WOC 领域对象，减少一层历史转发壳。
type walletChainClient interface {
	BaseURL() string
	GetAddressConfirmedUnspent(ctx context.Context, address string) ([]whatsonchain.UTXO, error)
	GetAddressBSV21TokenUnspent(ctx context.Context, address string) ([]whatsonchain.BSV21TokenUTXO, error)
	GetChainInfo(ctx context.Context) (uint32, error)
	GetAddressConfirmedHistory(ctx context.Context, address string) ([]whatsonchain.AddressHistoryItem, error)
	GetAddressConfirmedHistoryPage(ctx context.Context, address string, q whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error)
	GetAddressUnconfirmedHistory(ctx context.Context, address string) ([]string, error)
	GetTxHash(ctx context.Context, txid string) (whatsonchain.TxDetail, error)
	GetTxHex(ctx context.Context, txid string) (string, error)
}
