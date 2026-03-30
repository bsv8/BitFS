package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"github.com/bsv8/BFTP/pkg/obs"
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

func loadCurrentWalletUTXOStateRows(db *sql.DB, address string) (map[string]utxoStateRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	walletID := walletIDByAddress(address)
	rows, err := db.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, strings.TrimSpace(address))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if err := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.AllocationClass, &r.AllocationReason, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix); err != nil {
			return nil, err
		}
		r.AllocationClass = normalizeWalletUTXOAllocationClass(r.AllocationClass)
		r.AllocationReason = strings.TrimSpace(r.AllocationReason)
		out[strings.ToLower(strings.TrimSpace(r.UTXOID))] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func listWalletUTXOAssetRows(db *sql.DB, utxoID string) ([]walletUTXOAssetRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(
		`SELECT utxo_id,wallet_id,address,asset_group,asset_standard,asset_key,asset_symbol,quantity_text,source_name,payload_json,updated_at_unix
		 FROM wallet_utxo_assets
		 WHERE utxo_id=?
		 ORDER BY asset_group ASC,asset_standard ASC,asset_key ASC`,
		strings.ToLower(strings.TrimSpace(utxoID)),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]walletUTXOAssetRow, 0, 4)
	for rows.Next() {
		var item walletUTXOAssetRow
		if err := rows.Scan(&item.UTXOID, &item.WalletID, &item.Address, &item.AssetGroup, &item.AssetStandard, &item.AssetKey, &item.AssetSymbol, &item.QuantityText, &item.SourceName, &item.PayloadJSON, &item.UpdatedAtUnix); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// refreshWalletAssetProjection 在链同步后清理旧资产影子，并把 `1 sat` 输出维持在受保护状态。
// 设计说明：
// - 1Sat overlay 识别能力已经下线，wallet 不再在本地或私有索引里判定资产成立；
// - 这里仍要清空 wallet_utxo_assets，避免旧版本写下的资产影子继续误导查询面；
// - 同时把当前未花费重新归一回默认保护口径：1 聪保持 unknown，其余回 plain_bsv。
func refreshWalletAssetProjection(ctx context.Context, rt *Runtime, address string, trigger string) error {
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	loadCurrent := func(db *sql.DB) (map[string]utxoStateRow, error) {
		return loadCurrentWalletUTXOStateRows(db, address)
	}
	var current map[string]utxoStateRow
	var err error
	if rt.DBActor != nil {
		current, err = runtimeDBValue(rt, ctx, loadCurrent)
	} else if rt.DB != nil {
		current, err = loadCurrent(rt.DB)
	} else {
		return fmt.Errorf("runtime not initialized")
	}
	if err != nil {
		return err
	}
	apply := func(db *sql.DB) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		walletID := walletIDByAddress(address)
		if _, err := tx.Exec(`DELETE FROM wallet_utxo_assets WHERE wallet_id=? AND address=?`, walletID, address); err != nil {
			return err
		}
		updatedAt := time.Now().Unix()
		for utxoID, row := range current {
			if strings.TrimSpace(strings.ToLower(row.State)) != "unspent" {
				continue
			}
			allocationClass, allocationReason := defaultWalletUTXOProtectionForValue(row.Value)
			allocationClass = normalizeWalletUTXOAllocationClass(allocationClass)
			if _, err := tx.Exec(`UPDATE wallet_utxo SET allocation_class=?,allocation_reason=? WHERE utxo_id=?`, allocationClass, allocationReason, utxoID); err != nil {
				return err
			}
		}
		stats, err := loadWalletUTXOAggregateTx(tx, address)
		if err != nil {
			return err
		}
		if err := updateWalletUTXOSyncStateStatsTx(tx, address, stats, updatedAt); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	if rt.DBActor != nil {
		err = runtimeDBDo(rt, ctx, apply)
	} else if rt.DB != nil {
		err = apply(rt.DB)
	} else {
		return fmt.Errorf("runtime not initialized")
	}
	if err != nil {
		return err
	}
	payload := map[string]any{
		"address":            address,
		"trigger":            strings.TrimSpace(trigger),
		"current_utxo_count": len(current),
		"asset_rows_cleared": true,
	}
	obs.Info("bitcast-client", "wallet_asset_projection_refreshed", payload)
	return nil
}

func loadWalletUTXOAggregateTx(tx *sql.Tx, address string) (walletUTXOAggregateStats, error) {
	if tx == nil {
		return walletUTXOAggregateStats{}, fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOAggregateStats{}, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	rows, err := tx.Query(
		`SELECT allocation_class,COUNT(1),COALESCE(SUM(value_satoshi),0)
		 FROM wallet_utxo
		 WHERE wallet_id=? AND address=? AND state='unspent'
		 GROUP BY allocation_class`,
		walletID,
		address,
	)
	if err != nil {
		return walletUTXOAggregateStats{}, err
	}
	defer rows.Close()
	var stats walletUTXOAggregateStats
	for rows.Next() {
		var class string
		var count int
		var balance uint64
		if err := rows.Scan(&class, &count, &balance); err != nil {
			return walletUTXOAggregateStats{}, err
		}
		class = normalizeWalletUTXOAllocationClass(class)
		stats.UTXOCount += count
		stats.BalanceSatoshi += balance
		switch class {
		case walletUTXOAllocationPlainBSV:
			stats.PlainBSVUTXOCount += count
			stats.PlainBSVBalanceSatoshi += balance
		case walletUTXOAllocationProtectedAsset:
			stats.ProtectedUTXOCount += count
			stats.ProtectedBalanceSatoshi += balance
		default:
			stats.UnknownUTXOCount += count
			stats.UnknownBalanceSatoshi += balance
		}
	}
	if err := rows.Err(); err != nil {
		return walletUTXOAggregateStats{}, err
	}
	return stats, nil
}

func updateWalletUTXOSyncStateStatsTx(tx *sql.Tx, address string, stats walletUTXOAggregateStats, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	res, err := tx.Exec(
		`UPDATE wallet_utxo_sync_state
		 SET wallet_id=?,
		     utxo_count=?,
		     balance_satoshi=?,
		     plain_bsv_utxo_count=?,
		     plain_bsv_balance_satoshi=?,
		     protected_utxo_count=?,
		     protected_balance_satoshi=?,
		     unknown_utxo_count=?,
		     unknown_balance_satoshi=?,
		     updated_at_unix=?
		 WHERE address=?`,
		walletID,
		stats.UTXOCount,
		stats.BalanceSatoshi,
		stats.PlainBSVUTXOCount,
		stats.PlainBSVBalanceSatoshi,
		stats.ProtectedUTXOCount,
		stats.ProtectedBalanceSatoshi,
		stats.UnknownUTXOCount,
		stats.UnknownBalanceSatoshi,
		updatedAt,
		address,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	_, err = tx.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		address,
		walletID,
		stats.UTXOCount,
		stats.BalanceSatoshi,
		stats.PlainBSVUTXOCount,
		stats.PlainBSVBalanceSatoshi,
		stats.ProtectedUTXOCount,
		stats.ProtectedBalanceSatoshi,
		stats.UnknownUTXOCount,
		stats.UnknownBalanceSatoshi,
		updatedAt,
		"",
		"wallet_asset_projection",
		"",
		0,
		"",
		"",
		"",
		0,
	)
	return err
}
