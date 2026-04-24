package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/walletutxo"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/walletutxosyncstate"
)

// dbApplyLocalBroadcastWalletProjection 只负责把本地广播交易写回钱包视图。
// 设计说明：
// - 业务层只传交易和显式 store，这里统一处理钱包 UTXO 回填；
// - 本地广播只是钱包运行态，不是结算事实；这里不允许补任何 settlement payment attempt；
// - 这样 local broadcast 这条链路就不会再散落 sql 细节。
func dbApplyLocalBroadcastWalletProjection(ctx context.Context, store moduleBootstrapStore, rt transferRuntimeCaps, tx *txsdk.Transaction, trigger string) error {
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
	classifier := defaultWalletScriptClassifier(newRuntimeWalletScriptEvidenceSource(rt))

	roundID := strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey)))
	ctx = sqlTraceContextWithMeta(ctx, roundID, trigger, "wallet_local_projection", "dbApplyLocalBroadcastWalletProjection")
	if err := store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		current, err := dbLoadWalletUTXOStateRowsTx(ctx, wtx, walletID, addr)
		if err != nil {
			return err
		}
		desired := cloneWalletUTXOStateRows(current)
		for _, in := range tx.Inputs {
			if in == nil || in.SourceTXID == nil {
				continue
			}
			utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
			_ = applyWalletUTXOSpentState(desired, utxoID, txid, updatedAt)
			if err := markBSVUTXOSpentConn(ctx, wtx, utxoID, txid, updatedAt); err != nil {
				return err
			}
		}
		for idx, out := range tx.Outputs {
			if out == nil || out.LockingScript == nil {
				continue
			}
			if !walletScriptHexMatchesAddressControl(hex.EncodeToString(out.LockingScript.Bytes()), walletScriptHex) {
				continue
			}
			utxoID := txid + ":" + fmt.Sprint(idx)
			classified := classifyWalletUTXOWithClassifier(ctx, classifier, newRuntimeWalletScriptEvidenceSource(rt), addr, utxoID, txid, uint32(idx), out.Satoshis, hex.EncodeToString(out.LockingScript.Bytes()), txHex)
			_ = applyWalletUTXOUpsertState(desired, walletID, addr, utxoID, txid, uint32(idx), out.Satoshis, "unspent", "", string(classified.ScriptType), classified.Reason, updatedAt, updatedAt)
			if err := dbUpsertBSVUTXODB(ctx, wtx, bsvUTXOEntry{
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
		if err := dbUpsertWalletLocalBroadcastStoreTx(ctx, wtx, walletID, addr, txid, txHex, 0, updatedAt); err != nil {
			return err
		}
		if err := applyWalletUTXODiffTx(ctx, wtx, current, desired, walletID, addr, updatedAt); err != nil {
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
		if _, err := wtx.ExecContext(ctx,
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

func dbLoadWalletUTXOStateRowsTx(ctx context.Context, tx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, walletID string, address string) (map[string]utxoStateRow, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	rows, err := tx.QueryContext(ctx, `SELECT utxo_id,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,spent_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if err := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.ScriptType, &r.ScriptTypeReason, &r.ScriptTypeUpdatedAtUnix, &r.AllocationClass, &r.AllocationReason, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix, &r.SpentAtUnix); err != nil {
			return nil, err
		}
		r.ScriptType = string(normalizeWalletScriptType(r.ScriptType))
		r.AllocationClass = walletScriptTypeAllocationClass(r.ScriptType)
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
		switch walletScriptTypeAllocationClass(row.ScriptType) {
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

func dbUpsertWalletLocalBroadcastStoreTx(ctx context.Context, wtx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, walletID string, address string, txid string, txHex string, observedAtUnix int64, updatedAt int64) error {
	if wtx == nil {
		return fmt.Errorf("tx is nil")
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	if txid == "" || txHex == "" {
		return fmt.Errorf("local broadcast txid and tx hex are required")
	}
	if observedAtUnix < 0 {
		observedAtUnix = 0
	}
	_, err := wtx.ExecContext(ctx,
		`INSERT INTO wallet_local_broadcast_txs(
			txid,wallet_id,address,tx_hex,created_at_unix,updated_at_unix,observed_at_unix
		) VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(txid) DO UPDATE SET
			observed_at_unix=CASE
				WHEN excluded.observed_at_unix > wallet_local_broadcast_txs.observed_at_unix THEN excluded.observed_at_unix
				ELSE wallet_local_broadcast_txs.observed_at_unix
			END,
			updated_at_unix=excluded.updated_at_unix`,
		txid,
		strings.TrimSpace(walletID),
		strings.TrimSpace(address),
		txHex,
		updatedAt,
		updatedAt,
		observedAtUnix,
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
	if err := store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updatedAt := time.Now().Unix()
		for utxoID, row := range current {
			if strings.TrimSpace(strings.ToLower(row.State)) != "unspent" {
				continue
			}
			allocationClass := walletScriptTypeAllocationClass(row.ScriptType)
			allocationReason := strings.TrimSpace(row.ScriptTypeReason)
			if allocationReason == "" {
				allocationReason = strings.TrimSpace(row.AllocationReason)
			}
			if _, err := tx.WalletUtxo.Update().
				Where(walletutxo.UtxoIDEQ(utxoID)).
				SetAllocationClass(allocationClass).
				SetAllocationReason(allocationReason).
				Save(ctx); err != nil {
				return err
			}
		}
		stats := summarizeWalletUTXOState(current)
		if err := dbUpsertWalletUTXOSyncStateEntTx(ctx, tx, address, stats, updatedAt); err != nil {
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
	obs.Info(ServiceName, "wallet_asset_projection_refreshed", payload)
	return nil
}

func dbUpsertWalletUTXOSyncStateEntTx(ctx context.Context, tx EntWriteRoot, address string, stats walletUTXOAggregateStats, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	existing, err := tx.WalletUtxoSyncState.Query().Where(walletutxosyncstate.AddressEQ(address)).Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetWalletID(walletID).
			SetUtxoCount(int64(stats.UTXOCount)).
			SetBalanceSatoshi(int64(stats.BalanceSatoshi)).
			SetPlainBsvUtxoCount(int64(stats.PlainBSVUTXOCount)).
			SetPlainBsvBalanceSatoshi(int64(stats.PlainBSVBalanceSatoshi)).
			SetProtectedUtxoCount(int64(stats.ProtectedUTXOCount)).
			SetProtectedBalanceSatoshi(int64(stats.ProtectedBalanceSatoshi)).
			SetUnknownUtxoCount(int64(stats.UnknownUTXOCount)).
			SetUnknownBalanceSatoshi(int64(stats.UnknownBalanceSatoshi)).
			SetUpdatedAtUnix(updatedAt).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.WalletUtxoSyncState.Create().
		SetAddress(address).
		SetWalletID(walletID).
		SetUtxoCount(int64(stats.UTXOCount)).
		SetBalanceSatoshi(int64(stats.BalanceSatoshi)).
		SetPlainBsvUtxoCount(int64(stats.PlainBSVUTXOCount)).
		SetPlainBsvBalanceSatoshi(int64(stats.PlainBSVBalanceSatoshi)).
		SetProtectedUtxoCount(int64(stats.ProtectedUTXOCount)).
		SetProtectedBalanceSatoshi(int64(stats.ProtectedBalanceSatoshi)).
		SetUnknownUtxoCount(int64(stats.UnknownUTXOCount)).
		SetUnknownBalanceSatoshi(int64(stats.UnknownBalanceSatoshi)).
		SetUpdatedAtUnix(updatedAt).
		SetLastError("").
		SetLastUpdatedBy("wallet_asset_projection").
		SetLastTrigger("").
		SetLastDurationMs(0).
		SetLastSyncRoundID("").
		SetLastFailedStep("").
		SetLastUpstreamPath("").
		SetLastHTTPStatus(0).
		Save(ctx)
	return err
}
