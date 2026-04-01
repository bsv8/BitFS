package clientapp

import (
	"context"
	"fmt"
)

const (
	walletAssetGroupToken   = "token"
	walletAssetGroupOrdinal = "ordinal"
)

type walletUTXOAssetRow struct {
	UTXOID        string `json:"utxo_id"`
	WalletID      string `json:"wallet_id"`
	Address       string `json:"address"`
	AssetGroup    string `json:"asset_group"`
	AssetStandard string `json:"asset_standard"`
	AssetKey      string `json:"asset_key"`
	AssetSymbol   string `json:"asset_symbol"`
	QuantityText  string `json:"quantity_text"`
	SourceName    string `json:"source_name"`
	PayloadJSON   string `json:"payload_json"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// defaultWalletUTXOProtectionForValue 按当前 `1 sat` 风险模型给新 UTXO 一个默认保护分类。
// 设计说明：
// - 当前不再在本地判定 token / ordinal 是否成立；
// - 但 1 聪输出仍然不能直接当普通零钱放行，否则会把潜在资产输出误花；
// - 因此只有 value=1 的输出，先进入 unknown，等待后续业务显式按证据面处理；
// - 非 1 聪输出默认仍按 plain_bsv 处理，避免把保护范围错误放大。
func defaultWalletUTXOProtectionForValue(value uint64) (string, string) {
	if value == 1 {
		return walletUTXOAllocationUnknown, "awaiting external token evidence"
	}
	return walletUTXOAllocationPlainBSV, ""
}

// refreshWalletAssetProjection 在链同步后清理旧资产影子，并把 `1 sat` 输出维持在受保护状态。
// 设计说明：
// - 1Sat overlay 识别能力已经下线，wallet 不再在本地或私有索引里判定资产成立；
// - 这里仍要清空 wallet_utxo_assets，避免旧版本写下的资产影子继续误导查询面；
// - 同时把当前未花费重新归一回默认保护口径：1 聪保持 unknown，其余回 plain_bsv。
func refreshWalletAssetProjection(ctx context.Context, store *clientDB, address string, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	if err := dbRefreshWalletAssetProjection(ctx, store, address, trigger); err != nil {
		return err
	}
	return nil
}
