package clientapp

import (
	"context"

	"github.com/bsv8/BFTP/pkg/woc"
)

// WalletChainClient 只承载钱包同步所需的链能力。
// 设计约束：钱包历史/未确认观察仍走旧链语义，不把这些接口塞进 BSVChainAPI。
type WalletChainClient interface {
	GetUTXOs(address string) ([]woc.UTXO, error)
	GetTipHeight() (uint32, error)
	GetConfirmedHistoryPageContext(ctx context.Context, address string, q woc.ConfirmedHistoryQuery) (woc.ConfirmedHistoryPage, error)
	GetUnconfirmedHistoryContext(ctx context.Context, address string) ([]string, error)
	GetTxDetailContext(ctx context.Context, txid string) (woc.TxDetail, error)
}
