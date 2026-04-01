package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
)

// dbApplyLocalBroadcastWalletProjection 只负责把本地广播交易写回钱包视图。
// 设计说明：
// - 业务层只传交易和显式 store，这里统一处理钱包 UTXO 回填；
// - 这样 local broadcast 这条链路就不会再散落 sql 细节。
func dbApplyLocalBroadcastWalletProjection(ctx context.Context, store *clientDB, rt *Runtime, tx *txsdk.Transaction, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return err
	}
	walletScriptHex, err := walletAddressLockScriptHex(addr)
	if err != nil {
		return err
	}
	txid := strings.ToLower(strings.TrimSpace(tx.TxID().String()))
	if txid == "" {
		return fmt.Errorf("local broadcast txid is empty")
	}
	txHex := strings.ToLower(strings.TrimSpace(tx.Hex()))
	if txHex == "" {
		return fmt.Errorf("local broadcast tx hex is empty")
	}
	walletID := walletIDByAddress(addr)
	updatedAt := time.Now().Unix()

	if err := store.Tx(ctx, func(dbtx *sql.Tx) error {
		existing, err := dbLoadWalletUTXOStateRowsTx(dbtx, walletID, addr)
		if err != nil {
			return err
		}
		for _, in := range tx.Inputs {
			if in == nil || in.SourceTXID == nil {
				continue
			}
			utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
			if err := setWalletUTXOSpentTxWithNote(dbtx, existing, utxoID, txid, updatedAt, "local_broadcast_spent", "utxo spent by local broadcast", map[string]any{
				"trigger": trigger,
			}); err != nil {
				return err
			}
		}
		for idx, out := range tx.Outputs {
			if out == nil || out.LockingScript == nil {
				continue
			}
			// 设计说明：
			// - 本地广播投影不能只认纯 p2pkh，因为 token change 常常是“协议前缀 + 钱包 p2pkh 后缀”；
			// - 这里只判断“最终是否仍由本钱包控制”，和链同步保持同一套脚本匹配口径；
			// - 这样 submit 成功后，钱包不会把自己的 1sat 找零误看成外部输出。
			if !walletScriptHexMatchesAddressControl(hex.EncodeToString(out.LockingScript.Bytes()), walletScriptHex) {
				continue
			}
			utxoID := txid + ":" + fmt.Sprint(idx)
			if err := upsertWalletUTXORowTxWithEvent(dbtx, existing, walletID, addr, utxoID, txid, uint32(idx), out.Satoshis, "unspent", "", updatedAt, "local_broadcast_detected", "utxo detected by local broadcast", map[string]any{
				"trigger": trigger,
			}); err != nil {
				return err
			}
		}
		if err := dbUpsertWalletLocalBroadcastTx(dbtx, walletID, addr, txid, txHex, updatedAt); err != nil {
			return err
		}
		stats := summarizeWalletUTXOState(existing)
		if _, err := dbtx.Exec(
			`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(address) DO UPDATE SET
			 	utxo_count=excluded.utxo_count,
			 	balance_satoshi=excluded.balance_satoshi,
			 	plain_bsv_utxo_count=excluded.plain_bsv_utxo_count,
			 	plain_bsv_balance_satoshi=excluded.plain_bsv_balance_satoshi,
			 	protected_utxo_count=excluded.protected_utxo_count,
			 	protected_balance_satoshi=excluded.protected_balance_satoshi,
			 	unknown_utxo_count=excluded.unknown_utxo_count,
			 	unknown_balance_satoshi=excluded.unknown_balance_satoshi,
			 	updated_at_unix=excluded.updated_at_unix,
			 	last_updated_by=excluded.last_updated_by,
			 	last_trigger=excluded.last_trigger,
			 	last_duration_ms=excluded.last_duration_ms`,
			addr,
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
			"local_wallet_projection",
			strings.TrimSpace(trigger),
			0,
			"",
			"",
			"",
			0,
		); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func dbLoadWalletUTXOStateRowsTx(tx *sql.Tx, walletID string, address string) (map[string]utxoStateRow, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	rows, err := tx.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
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

// summarizeWalletUTXOState 只做内存汇总，不碰数据库。
func summarizeWalletUTXOState(existing map[string]utxoStateRow) walletUTXOAggregateStats {
	stats := walletUTXOAggregateStats{}
	for _, row := range existing {
		if strings.TrimSpace(strings.ToLower(row.State)) != "unspent" {
			continue
		}
		stats.UTXOCount++
		stats.BalanceSatoshi += row.Value
		switch normalizeWalletUTXOAllocationClass(row.AllocationClass) {
		case walletUTXOAllocationPlainBSV:
			stats.PlainBSVUTXOCount++
			stats.PlainBSVBalanceSatoshi += row.Value
		case walletUTXOAllocationProtectedAsset:
			stats.ProtectedUTXOCount++
			stats.ProtectedBalanceSatoshi += row.Value
		default:
			stats.UnknownUTXOCount++
			stats.UnknownBalanceSatoshi += row.Value
		}
	}
	return stats
}

func dbUpsertWalletLocalBroadcastTx(tx *sql.Tx, walletID string, address string, txid string, txHex string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	if txid == "" || txHex == "" {
		return fmt.Errorf("local broadcast txid and tx hex are required")
	}
	if _, err := tx.Exec(
		`INSERT INTO wallet_local_broadcast_txs(txid,wallet_id,address,tx_hex,created_at_unix,updated_at_unix,observed_at_unix)
		 VALUES(?,?,?,?,?,?,0)
		 ON CONFLICT(txid) DO UPDATE SET
		 	wallet_id=excluded.wallet_id,
		 	address=excluded.address,
		 	tx_hex=excluded.tx_hex,
		 	updated_at_unix=excluded.updated_at_unix`,
		txid,
		strings.TrimSpace(walletID),
		strings.TrimSpace(address),
		txHex,
		updatedAt,
		updatedAt,
	); err != nil {
		return err
	}
	return nil
}

// dbRefreshWalletAssetProjection 负责清掉旧资产影子，并把当前 wallet_utxo 重新归一。
// 设计说明：
// - 这里只做数据库投影，不再碰 Runtime 里的旧 DB 字段；
// - 这样资产视图和本地广播投影都统一走 clientDB。
func dbRefreshWalletAssetProjection(ctx context.Context, store *clientDB, address string, trigger string) error {
	if store == nil {
		return fmt.Errorf("store not initialized")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	current, err := dbLoadCurrentWalletUTXOStateRows(ctx, store, address)
	if err != nil {
		return err
	}
	if err := store.Tx(ctx, func(tx *sql.Tx) error {
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
		stats, err := dbLoadWalletUTXOAggregateTx(tx, address)
		if err != nil {
			return err
		}
		if err := dbUpdateWalletUTXOSyncStateStatsTx(tx, address, stats, updatedAt); err != nil {
			return err
		}
		return nil
	}); err != nil {
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

func dbLoadWalletUTXOAggregateTx(tx *sql.Tx, address string) (walletUTXOAggregateStats, error) {
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

func dbUpdateWalletUTXOSyncStateStatsTx(tx *sql.Tx, address string, stats walletUTXOAggregateStats, updatedAt int64) error {
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
