package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

// confirmedUTXOChange 已确认 UTXO 变化（来自同步层，供 fact 层消费）
// 设计说明：
// - 只包含 confirmed（snapshot.Live）+ unspent + plain_bsv 的 UTXO
// - 这是同步层到 fact 层的唯一传递格式
type confirmedUTXOChange struct {
	UTXOID          string
	WalletID        string
	Address         string
	TxID            string
	Vout            uint32
	Value           uint64
	AllocationClass string
	CreatedAtUnix   int64
}

// SyncWalletAndApplyFacts 同步钱包状态并写入 fact 的编排函数
// 设计说明：
// - 先执行同步状态（reconcileWalletUTXOSet），再执行 fact 写入
// - 两阶段错误处理：同步失败直接返回，fact 失败可重试
// - 这是唯一的钱包同步入口，以后所有同步入口都只调它
func SyncWalletAndApplyFacts(ctx context.Context, store *clientDB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOSyncCursor, syncRoundID string, lastError string, trigger string, updatedAt int64, durationMS int64) error {
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
	changes, err := reconcileWalletUTXOSetAndReturnChanges(ctx, store, address, snapshot, history, cursor, syncRoundID, lastError, trigger, updatedAt, durationMS)
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
func reconcileWalletUTXOSetAndReturnChanges(ctx context.Context, store *clientDB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOSyncCursor, syncRoundID string, lastError string, trigger string, updatedAt int64, durationMS int64) ([]confirmedUTXOChange, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
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
		localBroadcasts, err := loadWalletLocalBroadcastTxsTx(tx, walletID, address)
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
				_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, historyTxID, out.N, value, "spent", "", updatedAt)
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
				_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, mempoolTxID, out.N, value, "spent", "", updatedAt)
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
			_ = applyWalletUTXOUpsertState(desired, walletID, address, utxoID, strings.ToLower(strings.TrimSpace(u.TxID)), u.Vout, u.Value, "unspent", "", updatedAt)
		}
		observedLocalTxIDs := collectObservedWalletTxIDs(history, snapshot.ObservedMempoolTxs)
		if err = overlayPendingLocalBroadcastsTx(ctx, desired, walletID, address, scriptHex, localBroadcasts, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}
		if err = markObservedWalletLocalBroadcastTxsTx(tx, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}

		// 写入 chain accounting（与原 reconcileWalletUTXOSet 行为一致）
		accountedTxIDs := map[string]struct{}{}
		recordWalletChainTx := func(detail whatsonchain.TxDetail) error {
			inputs, err := buildWalletChainAccountingInputsFromTxDetail(tx, address, detail)
			if err != nil {
				return err
			}
			if len(inputs) == 0 {
				return nil
			}
			for _, input := range inputs {
				key := input.SourceType + ":" + input.SourceID
				if _, seen := accountedTxIDs[key]; seen {
					continue
				}
				if err := recordWalletChainAccountingConn(tx, input); err != nil {
					return err
				}
				accountedTxIDs[key] = struct{}{}
			}
			return nil
		}
		for _, hist := range history {
			if err := recordWalletChainTx(hist.Tx); err != nil {
				return err
			}
		}
		for _, detail := range snapshot.ObservedMempoolTxs {
			if err := recordWalletChainTx(detail); err != nil {
				return err
			}
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
			allocationClass := normalizeWalletUTXOAllocationClass(row.AllocationClass)
			if state != "unspent" || allocationClass != walletUTXOAllocationPlainBSV {
				continue
			}
			changes = append(changes, confirmedUTXOChange{
				UTXOID:          utxoID,
				WalletID:        walletID,
				Address:         address,
				TxID:            strings.ToLower(strings.TrimSpace(row.CreatedTxID)),
				Vout:            row.Vout,
				Value:           row.Value,
				AllocationClass: allocationClass,
				CreatedAtUnix:   row.CreatedAtUnix,
			})
		}

		// 将 unknown UTXO 加入待确认队列
		for utxoID, row := range desired {
			if normalizeWalletUTXOAllocationClass(row.AllocationClass) == walletUTXOAllocationUnknown {
				if _, isConfirmed := confirmedUTXOSet[strings.ToLower(strings.TrimSpace(utxoID))]; isConfirmed {
					if err := enqueueUnknownUTXOToVerificationTx(tx, walletID, address, utxoID, row.TxID, row.Vout, row.Value); err != nil {
						obs.Error("bitcast-client", "enqueue_token_verification_failed", map[string]any{
							"utxo_id": utxoID,
							"error":   err.Error(),
						})
					}
				}
			}
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
		if _, err = tx.Exec(
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
		if _, err = tx.Exec(
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
	return store.Do(ctx, func(db *sql.DB) error {
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
				Payload:       map[string]any{"allocation_class": change.AllocationClass},
			}
			if err := dbUpsertBSVUTXODB(db, entry); err != nil {
				return fmt.Errorf("upsert bsv utxo for %s: %w", change.UTXOID, err)
			}
		}
		return nil
	})
}
