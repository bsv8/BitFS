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

	roundID := strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey)))
	ctx = sqlTraceContextWithMeta(ctx, roundID, trigger, "wallet_local_projection", "dbApplyLocalBroadcastWalletProjection")
	if err := store.Tx(ctx, func(dbtx *sql.Tx) error {
		current, err := dbLoadWalletUTXOStateRowsTx(dbtx, walletID, addr)
		if err != nil {
			return err
		}
		desired := cloneWalletUTXOStateRows(current)
		inputFacts := make([]chainPaymentUTXOLinkEntry, 0, len(tx.Inputs))
		for _, in := range tx.Inputs {
			if in == nil || in.SourceTXID == nil {
				continue
			}
			utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
			_ = applyWalletUTXOSpentState(desired, utxoID, txid, updatedAt)
			if err := markBSVUTXOSpentConn(dbtx, utxoID, txid, updatedAt); err != nil {
				return err
			}
			if row, ok := desired[strings.ToLower(strings.TrimSpace(utxoID))]; ok {
				inputFacts = append(inputFacts, chainPaymentUTXOLinkEntry{
					UTXOID:        utxoID,
					IOSide:        "input",
					UTXORole:      "wallet_input",
					AmountSatoshi: int64(row.Value),
					CreatedAtUnix: updatedAt,
					Note:          "wallet local broadcast input",
				})
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
			_ = applyWalletUTXOUpsertState(desired, walletID, addr, utxoID, txid, uint32(idx), out.Satoshis, "unspent", "", updatedAt)
			if err := dbUpsertBSVUTXODB(dbtx, bsvUTXOEntry{
				UTXOID:         utxoID,
				OwnerPubkeyHex: strings.ToLower(strings.TrimSpace(addr)),
				Address:        addr,
				TxID:           txid,
				Vout:           uint32(idx),
				ValueSatoshi:   int64(out.Satoshis),
				UTXOState:      "unspent",
				CarrierType:    "plain_bsv",
				CreatedAtUnix:  updatedAt,
				UpdatedAtUnix:  updatedAt,
				Note:           "wallet local broadcast output",
			}); err != nil {
				return err
			}
		}
		if err := dbUpsertWalletLocalBroadcastFactTx(dbtx, walletID, addr, txid, txHex, updatedAt); err != nil {
			return err
		}
		if strings.TrimSpace(trigger) == "peer_call_chain_tx" {
			settlementCycleID, err := dbGetSettlementCycleBySource(dbtx, "chain_bsv", txid)
			if err != nil {
				return err
			}
			if len(inputFacts) > 0 {
				if err := dbAppendBSVConsumptionsForSettlementCycle(dbtx, settlementCycleID, inputFacts, updatedAt); err != nil {
					return err
				}
			}
		}
		if err := applyWalletUTXODiffTx(ctx, dbtx, current, desired, walletID, addr, updatedAt); err != nil {
			return err
		}
		stats := summarizeWalletUTXOState(desired)
		utxoCount := int64(stats.UTXOCount)
		balanceSatoshi := int64(stats.BalanceSatoshi)
		plainBSVUTXOCount := int64(stats.PlainBSVUTXOCount)
		plainBSVBalanceSatoshi := int64(stats.PlainBSVBalanceSatoshi)
		protectedUTXOCount := int64(stats.ProtectedUTXOCount)
		protectedBalanceSatoshi := int64(stats.ProtectedBalanceSatoshi)
		unknownUTXOCount := int64(stats.UnknownUTXOCount)
		unknownBalanceSatoshi := int64(stats.UnknownBalanceSatoshi)
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
			utxoCount,
			balanceSatoshi,
			plainBSVUTXOCount,
			plainBSVBalanceSatoshi,
			protectedUTXOCount,
			protectedBalanceSatoshi,
			unknownUTXOCount,
			unknownBalanceSatoshi,
			updatedAt,
			"",
			"local_wallet_projection",
			strings.TrimSpace(trigger),
			int64(0),
			"",
			"",
			"",
			int64(0),
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
	rows, err := tx.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,spent_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if err := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.AllocationClass, &r.AllocationReason, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix, &r.SpentAtUnix); err != nil {
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

func dbUpsertWalletLocalBroadcastFactTx(tx *sql.Tx, walletID string, address string, txid string, txHex string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	if txid == "" || txHex == "" {
		return fmt.Errorf("local broadcast txid and tx hex are required")
	}
	if err := dbUpsertSettlementCycle(tx, fmt.Sprintf("cycle_chain_bsv_%s", txid), "chain_bsv", txid, "confirmed", 0, 0, 0, 0, updatedAt, "wallet local broadcast", map[string]any{
		"tx_hex":    txHex,
		"wallet_id": strings.TrimSpace(walletID),
		"address":   strings.TrimSpace(address),
	}); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO fact_chain_payments(
			txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,
			block_height,occurred_at_unix,submitted_at_unix,wallet_observed_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(txid) DO UPDATE SET
			payment_subtype=excluded.payment_subtype,
			status=excluded.status,
			wallet_input_satoshi=excluded.wallet_input_satoshi,
			wallet_output_satoshi=excluded.wallet_output_satoshi,
			net_amount_satoshi=excluded.net_amount_satoshi,
			block_height=excluded.block_height,
			occurred_at_unix=excluded.occurred_at_unix,
			submitted_at_unix=excluded.submitted_at_unix,
			wallet_observed_at_unix=excluded.wallet_observed_at_unix,
			from_party_id=excluded.from_party_id,
			to_party_id=excluded.to_party_id,
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		txid,
		"wallet_local_broadcast",
		"submitted",
		int64(0),
		int64(0),
		int64(0),
		int64(0),
		updatedAt,
		updatedAt,
		int64(0),
		strings.TrimSpace(walletID),
		"external:unknown",
		mustJSONString(map[string]any{
			"tx_hex":    txHex,
			"wallet_id": strings.TrimSpace(walletID),
			"address":   strings.TrimSpace(address),
		}),
		updatedAt,
	)
	if err != nil {
		return err
	}
	return nil
}

// dbRefreshWalletAssetProjection 把当前 wallet_utxo 未花费输出重新归一保护分类。
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
		int64(stats.UTXOCount),
		int64(stats.BalanceSatoshi),
		int64(stats.PlainBSVUTXOCount),
		int64(stats.PlainBSVBalanceSatoshi),
		int64(stats.ProtectedUTXOCount),
		int64(stats.ProtectedBalanceSatoshi),
		int64(stats.UnknownUTXOCount),
		int64(stats.UnknownBalanceSatoshi),
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
		int64(stats.UTXOCount),
		int64(stats.BalanceSatoshi),
		int64(stats.PlainBSVUTXOCount),
		int64(stats.PlainBSVBalanceSatoshi),
		int64(stats.ProtectedUTXOCount),
		int64(stats.ProtectedBalanceSatoshi),
		int64(stats.UnknownUTXOCount),
		int64(stats.UnknownBalanceSatoshi),
		updatedAt,
		"",
		"wallet_asset_projection",
		"",
		int64(0),
		"",
		"",
		"",
		int64(0),
	)
	return err
}
