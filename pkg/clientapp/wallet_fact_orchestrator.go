package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// confirmedUTXOChange 已确认 UTXO 变化（来自同步层，供 fact 层消费）
// 设计说明：
// - 只包含 confirmed（snapshot.Live）+ unspent + plain_bsv 的 UTXO
// - 这是同步层到 fact 层的唯一传递格式
type confirmedUTXOChange struct {
	UTXOID           string
	WalletID         string
	Address          string
	TxID             string
	Vout             uint32
	Value            uint64
	ScriptType       string
	ScriptTypeReason string
	AllocationClass  string
	CreatedAtUnix    int64
}

// SyncWalletAndApplyFacts 同步钱包状态并写入 fact 的编排函数
// 设计说明：
// - 先执行同步状态（reconcileWalletUTXOSet），再执行 fact 写入
// - 两阶段错误处理：同步失败直接返回，fact 失败可重试
// - 这是唯一的钱包同步入口，以后所有同步入口都只调它
func SyncWalletAndApplyFacts(ctx context.Context, store *clientDB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOSyncCursor, source walletScriptEvidenceSource, syncRoundID string, lastError string, trigger string, updatedAt int64, durationMS int64) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)

	// 第一阶段：同步钱包状态，先算最终态，再统一落库
	ctx = sqlTraceContextWithMeta(ctx, syncRoundID, trigger, "reconcile_wallet_utxo", "wallet_fact_orchestrator")
	changes, err := reconcileWalletUTXOSetAndReturnChanges(ctx, store, address, snapshot, history, cursor, source, syncRoundID, lastError, trigger, updatedAt, durationMS)
	if err != nil {
		return fmt.Errorf("sync wallet utxo set: %w", err)
	}

	// 第二阶段：写入 fact（幂等，可重试）
	if len(changes) > 0 {
		if applyErr := ApplyConfirmedUTXOChanges(sqlTraceContextWithMeta(ctx, syncRoundID, trigger, "apply_confirmed_utxo_facts", "wallet_fact_orchestrator"), store, changes, updatedAt); applyErr != nil {
			obs.Error("bitcast-client", "apply_confirmed_utxo_changes_failed", map[string]any{
				"wallet_id":    walletID,
				"address":      address,
				"change_count": len(changes),
				"error":        applyErr.Error(),
			})
			return fmt.Errorf("apply confirmed utxo changes: %w", applyErr)
		}
		obs.Info("bitcast-client", "apply_confirmed_utxo_changes_success", map[string]any{
			"wallet_id":    walletID,
			"address":      address,
			"change_count": len(changes),
		})
	}
	return nil
}

// reconcileWalletUTXOSetAndReturnChanges 同步钱包状态并返回变化列表
// 设计说明：
// - 内部执行完整的钱包 UTXO 对账，但不写 fact
// - 返回 confirmed + unspent + plain_bsv 的 UTXO 变化，供上层调用方写入 fact
func reconcileWalletUTXOSetAndReturnChanges(ctx context.Context, store *clientDB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOSyncCursor, source walletScriptEvidenceSource, syncRoundID string, lastError string, trigger string, updatedAt int64, durationMS int64) ([]confirmedUTXOChange, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	historyByTxID := make(map[string]walletHistoryTxRecord, len(history))
	for _, item := range history {
		txid := strings.ToLower(strings.TrimSpace(item.TxID))
		if txid != "" {
			historyByTxID[txid] = item
		}
	}
	classifier := defaultWalletScriptClassifier(source)
	var changes []confirmedUTXOChange
	err := store.Tx(ctx, func(tx *sql.Tx) error {
		current, err := dbLoadWalletUTXOStateRowsTx(tx, walletID, address)
		if err != nil {
			return err
		}
		desired := cloneWalletUTXOStateRows(current)
		scriptHex, err := walletAddressLockScriptHex(address)
		if err != nil {
			return err
		}
		localBroadcasts, err := loadWalletLocalBroadcastTxsTx(ctx, tx, walletID, address)
		if err != nil {
			return err
		}
		for _, hist := range history {
			historyTxID := strings.ToLower(strings.TrimSpace(hist.TxID))
			for _, out := range hist.Tx.Vout {
				utxoID, value, ok := matchWalletOutput(historyTxID, out, scriptHex)
				if !ok {
					continue
				}
				classified := classifyWalletUTXOWithClassifier(ctx, classifier, source, address, utxoID, historyTxID, out.N, value, out.ScriptPubKey.Hex, "")
				_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, historyTxID, out.N, value, "spent", "", string(classified.ScriptType), classified.Reason, updatedAt, updatedAt)
			}
			for _, in := range hist.Tx.Vin {
				spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
				if _, ok := desired[spentID]; !ok {
					continue
				}
				_ = applyWalletUTXOSpentState(desired, spentID, historyTxID, updatedAt)
			}
		}

		for _, detail := range snapshot.ObservedMempoolTxs {
			mempoolTxID := strings.ToLower(strings.TrimSpace(detail.TxID))
			for _, out := range detail.Vout {
				utxoID, value, ok := matchWalletOutput(mempoolTxID, out, scriptHex)
				if !ok {
					continue
				}
				classified := classifyWalletUTXOWithClassifier(ctx, classifier, source, address, utxoID, mempoolTxID, out.N, value, out.ScriptPubKey.Hex, "")
				_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, mempoolTxID, out.N, value, "spent", "", string(classified.ScriptType), classified.Reason, updatedAt, updatedAt)
			}
			for _, in := range detail.Vin {
				spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
				if _, ok := desired[spentID]; !ok {
					continue
				}
				_ = applyWalletUTXOSpentState(desired, spentID, mempoolTxID, updatedAt)
			}
		}

		for utxoID, u := range snapshot.Live {
			liveTxID := strings.ToLower(strings.TrimSpace(u.TxID))
			scriptHex := ""
			if hist, ok := historyByTxID[liveTxID]; ok && int(u.Vout) < len(hist.Tx.Vout) {
				scriptHex = strings.TrimSpace(hist.Tx.Vout[u.Vout].ScriptPubKey.Hex)
			}
			classified := classifyWalletUTXOWithClassifier(ctx, classifier, source, address, utxoID, liveTxID, u.Vout, u.Value, scriptHex, "")
			_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, liveTxID, u.Vout, u.Value, "unspent", "", string(classified.ScriptType), classified.Reason, updatedAt, updatedAt)
		}
		observedLocalTxIDs := collectObservedWalletTxIDs(history, snapshot.ObservedMempoolTxs)
		if err = overlayPendingLocalBroadcastsTx(ctx, classifier, desired, walletID, address, scriptHex, localBroadcasts, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}
		if err = markObservedWalletLocalBroadcastTxsTx(ctx, tx, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}

		if err = applyWalletUTXODiffTx(ctx, tx, current, desired, walletID, address, updatedAt); err != nil {
			return err
		}

		stats := summarizeWalletUTXOState(desired)
		confirmedUTXOSet := make(map[string]struct{}, len(snapshot.Live))
		for utxoID := range snapshot.Live {
			confirmedUTXOSet[strings.ToLower(strings.TrimSpace(utxoID))] = struct{}{}
		}

		// 收集 confirmed unspent plain_bsv UTXO 变化
		changes = make([]confirmedUTXOChange, 0, len(snapshot.Live))
		for utxoID, row := range desired {
			if _, confirmed := confirmedUTXOSet[strings.ToLower(strings.TrimSpace(utxoID))]; !confirmed {
				continue
			}
			state := strings.ToLower(strings.TrimSpace(row.State))
			scriptType := normalizeWalletScriptType(row.ScriptType)
			if state != "unspent" || !walletScriptTypeIsPlain(string(scriptType)) {
				continue
			}
			changes = append(changes, confirmedUTXOChange{
				UTXOID:           utxoID,
				WalletID:         walletID,
				Address:          address,
				TxID:             strings.ToLower(strings.TrimSpace(row.CreatedTxID)),
				Vout:             row.Vout,
				Value:            row.Value,
				ScriptType:       string(scriptType),
				ScriptTypeReason: row.ScriptTypeReason,
				AllocationClass:  walletScriptTypeAllocationClass(string(scriptType)),
				CreatedAtUnix:    row.CreatedAtUnix,
			})
		}

		utxoCount := int64(stats.UTXOCount)
		balanceSatoshi := int64(stats.BalanceSatoshi)
		plainBSVUTXOCount := int64(stats.PlainBSVUTXOCount)
		plainBSVBalanceSatoshi := int64(stats.PlainBSVBalanceSatoshi)
		protectedUTXOCount := int64(stats.ProtectedUTXOCount)
		protectedBalanceSatoshi := int64(stats.ProtectedBalanceSatoshi)
		unknownUTXOCount := int64(stats.UnknownUTXOCount)
		unknownBalanceSatoshi := int64(stats.UnknownBalanceSatoshi)
		lastDurationMS := int64(durationMS)
		if _, err = ExecContext(ctx, tx,
			`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(address) DO UPDATE SET
				wallet_id=excluded.wallet_id,
				utxo_count=excluded.utxo_count,
				balance_satoshi=excluded.balance_satoshi,
				plain_bsv_utxo_count=excluded.plain_bsv_utxo_count,
				plain_bsv_balance_satoshi=excluded.plain_bsv_balance_satoshi,
				protected_utxo_count=excluded.protected_utxo_count,
				protected_balance_satoshi=excluded.protected_balance_satoshi,
				unknown_utxo_count=excluded.unknown_utxo_count,
				unknown_balance_satoshi=excluded.unknown_balance_satoshi,
				updated_at_unix=excluded.updated_at_unix,
				last_error=excluded.last_error,
				last_updated_by=excluded.last_updated_by,
				last_trigger=excluded.last_trigger,
				last_duration_ms=excluded.last_duration_ms,
				last_sync_round_id=excluded.last_sync_round_id,
				last_failed_step=excluded.last_failed_step,
				last_upstream_path=excluded.last_upstream_path,
				last_http_status=excluded.last_http_status`,
			address, walletID,
			utxoCount,
			balanceSatoshi,
			plainBSVUTXOCount,
			plainBSVBalanceSatoshi,
			protectedUTXOCount,
			protectedBalanceSatoshi,
			unknownUTXOCount,
			unknownBalanceSatoshi,
			updatedAt,
			strings.TrimSpace(lastError),
			"chain_utxo_worker",
			strings.TrimSpace(trigger),
			lastDurationMS,
			strings.TrimSpace(syncRoundID),
			"",
			"",
			int64(0),
		); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx,
			`INSERT INTO wallet_utxo_sync_cursor(address,wallet_id,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error)
			 VALUES(?,?,?,?,?,?,?,?)
			 ON CONFLICT(address) DO UPDATE SET
				wallet_id=excluded.wallet_id,
				next_confirmed_height=excluded.next_confirmed_height,
				next_page_token=excluded.next_page_token,
				anchor_height=excluded.anchor_height,
				round_tip_height=excluded.round_tip_height,
				updated_at_unix=excluded.updated_at_unix,
				last_error=excluded.last_error`,
			address, walletID, cursor.NextConfirmedHeight, strings.TrimSpace(cursor.NextPageToken), cursor.AnchorHeight, cursor.RoundTipHeight, updatedAt, strings.TrimSpace(cursor.LastError),
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

// ApplyConfirmedUTXOChanges 将已确认 UTXO 变化写入 fact_bsv_utxos
// 设计说明（硬切版）：
// - 只写入 confirmed + unspent + plain_bsv 的 UTXO
// - 幂等写入，重复触发不会重复写入
// - 这是 fact 层的唯一写入入口
// - 改为写入新表 fact_bsv_utxos，不再写 fact_chain_asset_flows
func ApplyConfirmedUTXOChanges(ctx context.Context, store *clientDB, changes []confirmedUTXOChange, updatedAt int64) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	if len(changes) == 0 {
		return nil
	}
	startedAt := time.Now()
	obs.Info("bitcast-client", "fact_apply_confirmed_utxo_changes_started", map[string]any{
		"change_count": len(changes),
		"updated_at":   updatedAt,
	})
	return store.Do(ctx, func(db *sql.DB) error {
		addressWallet := make(map[string]string, len(changes))
		for _, change := range changes {
			// 从 wallet_id 提取 owner_pubkey_hex（去掉 "wallet:" 前缀）
			ownerPubkeyHex := strings.ToLower(strings.TrimSpace(change.WalletID))
			ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

			entry := bsvUTXOEntry{
				UTXOID:         change.UTXOID,
				OwnerPubkeyHex: ownerPubkeyHex,
				Address:        change.Address,
				TxID:           change.TxID,
				Vout:           change.Vout,
				ValueSatoshi:   int64(change.Value),
				UTXOState:      "unspent",
				CarrierType:    "plain_bsv",
				CreatedAtUnix: func() int64 {
					if change.CreatedAtUnix > 0 {
						return change.CreatedAtUnix
					}
					return updatedAt
				}(),
				UpdatedAtUnix: updatedAt,
				Note:          "plain_bsv utxo detected by chain sync",
				Payload: map[string]any{
					"allocation_class":   change.AllocationClass,
					"script_type":        change.ScriptType,
					"script_type_reason": change.ScriptTypeReason,
				},
			}
			if err := dbUpsertBSVUTXODB(ctx, db, entry); err != nil {
				return fmt.Errorf("upsert bsv utxo for %s: %w", change.UTXOID, err)
			}
			addr := strings.TrimSpace(change.Address)
			wid := strings.TrimSpace(change.WalletID)
			if addr != "" && wid != "" {
				addressWallet[addr] = wid
			}
		}
		// 把 wallet_utxo 已花费状态回填到 fact_bsv_utxos，确保 fact 与钱包投影一致。
		for address, walletID := range addressWallet {
			if err := backfillFactSpentFromWalletUTXO(ctx, db, walletID, address, updatedAt); err != nil {
				return fmt.Errorf("backfill fact spent from wallet_utxo failed: wallet_id=%s address=%s: %w", walletID, address, err)
			}
		}
		obs.Info("bitcast-client", "fact_apply_confirmed_utxo_changes_finished", map[string]any{
			"change_count": len(changes),
			"updated_at":   updatedAt,
			"duration_ms":  time.Since(startedAt).Milliseconds(),
		})
		return nil
	})
}

func backfillFactSpentFromWalletUTXO(ctx context.Context, db *sql.DB, walletID string, address string, updatedAt int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	walletID = strings.TrimSpace(walletID)
	address = strings.TrimSpace(address)
	if walletID == "" || address == "" {
		return fmt.Errorf("wallet_id and address are required")
	}
	startedAt := time.Now()
	// 先拿“本轮刚变化为 spent”的明细，再按 utxo_id 定点回填，避免大范围更新导致锁等待。
	rows, err := QueryContext(ctx, db, `SELECT LOWER(TRIM(utxo_id)), LOWER(TRIM(spent_txid))
FROM wallet_utxo
WHERE wallet_id=? AND address=? AND state='spent' AND COALESCE(TRIM(spent_txid),'')<>'' AND updated_at_unix=?`, walletID, address, updatedAt)
	if err != nil {
		return fmt.Errorf("query changed spent rows: %w", err)
	}
	defer rows.Close()
	spentByUTXO := make(map[string]string)
	spentTxIDs := make(map[string]struct{})
	for rows.Next() {
		var utxoID string
		var spentTxID string
		if err := rows.Scan(&utxoID, &spentTxID); err != nil {
			return fmt.Errorf("scan changed spent rows: %w", err)
		}
		utxoID = strings.ToLower(strings.TrimSpace(utxoID))
		spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
		if utxoID == "" || spentTxID == "" {
			continue
		}
		spentByUTXO[utxoID] = spentTxID
		spentTxIDs[spentTxID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate changed spent rows: %w", err)
	}
	if len(spentByUTXO) == 0 {
		return nil
	}
	for utxoID, spentTxID := range spentByUTXO {
		if _, err := ExecContext(ctx, db, `UPDATE fact_bsv_utxos
SET utxo_state='spent',
    spent_by_txid=?,
    spent_at_unix=CASE WHEN spent_at_unix=0 THEN ? ELSE spent_at_unix END,
    updated_at_unix=?
WHERE utxo_id=? AND (utxo_state<>'spent' OR COALESCE(spent_by_txid,'')='')`,
			spentTxID, updatedAt, updatedAt, utxoID,
		); err != nil {
			return fmt.Errorf("update fact spent by utxo_id=%s: %w", utxoID, err)
		}
	}
	for spentTxID := range spentTxIDs {
		emitFactBSVSpentAppliedEvent(ctx, db, address, spentTxID)
	}
	obs.Info("bitcast-client", "fact_backfill_spent_from_wallet_finished", map[string]any{
		"wallet_id":        walletID,
		"address":          address,
		"updated_at":       updatedAt,
		"spent_utxo_count": len(spentByUTXO),
		"spent_tx_count":   len(spentTxIDs),
		"duration_ms":      time.Since(startedAt).Milliseconds(),
	})
	return nil
}
