package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// applyLocalBroadcastWalletTx 把“已成功广播的钱包交易”投影回本地 wallet_utxo。
// 设计说明：
// - 这里不碰 sql，只把业务输入转给 db 层；
// - 本地广播投影和费用池回填共用同一条显式 store 路径。
func applyLocalBroadcastWalletTx(ctx context.Context, store ClientStore, rt *Runtime, txHex string, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return fmt.Errorf("parse local broadcast tx: %w", err)
	}
	return applyLocalBroadcastWalletProjection(ctx, store, rt, parsed, trigger)
}

func applyLocalBroadcastWalletTxBytes(ctx context.Context, store ClientStore, rt *Runtime, rawTx []byte, trigger string) error {
	if len(rawTx) == 0 {
		return fmt.Errorf("raw tx is empty")
	}
	return applyLocalBroadcastWalletTx(ctx, store, rt, hex.EncodeToString(rawTx), trigger)
}

// applyLocalBroadcastWalletProjection 只保留业务入口，真正的落库细节放到 db 文件里。
func applyLocalBroadcastWalletProjection(ctx context.Context, store ClientStore, rt *Runtime, tx *txsdk.Transaction, trigger string) error {
	return dbApplyLocalBroadcastWalletProjection(ctx, store, rt, tx, trigger)
}
