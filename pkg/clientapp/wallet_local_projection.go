package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// applyLocalBroadcastWalletTx 把“已成功广播的钱包交易”立即投影到本地 wallet_utxo。
// 设计说明：
// - 真实链上广播成功后，不能再等链同步慢慢追上，否则后续业务会误选已花输入；
// - 这里先把本地钱包视图更新到“至少不自相矛盾”，链同步后续再做最终校正。
func applyLocalBroadcastWalletTx(rt *Runtime, txHex string, trigger string) error {
	if rt == nil || rt.DB == nil {
		return fmt.Errorf("runtime not initialized")
	}
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return fmt.Errorf("parse local broadcast tx: %w", err)
	}
	return applyLocalBroadcastWalletProjection(rt, parsed, trigger)
}

func applyLocalBroadcastWalletTxBytes(rt *Runtime, rawTx []byte, trigger string) error {
	if len(rawTx) == 0 {
		return fmt.Errorf("raw tx is empty")
	}
	return applyLocalBroadcastWalletTx(rt, hex.EncodeToString(rawTx), trigger)
}

func applyLocalBroadcastWalletProjection(rt *Runtime, tx *txsdk.Transaction, trigger string) error {
	if rt == nil || rt.DB == nil {
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

	dbtx, err := rt.DB.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = dbtx.Rollback() }()

	existing, err := loadWalletUTXOStateRowsTx(dbtx, walletID, addr)
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
	if err := upsertWalletLocalBroadcastTx(dbtx, walletID, addr, txid, txHex, updatedAt); err != nil {
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
	if err := dbtx.Commit(); err != nil {
		return err
	}
	return refreshWalletAssetProjection(context.Background(), rt, addr, trigger)
}

func upsertWalletLocalBroadcastTx(tx *sql.Tx, walletID string, address string, txid string, txHex string, updatedAt int64) error {
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

func loadWalletUTXOStateRowsTx(tx *sql.Tx, walletID string, address string) (map[string]utxoStateRow, error) {
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
