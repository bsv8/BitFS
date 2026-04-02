package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func appendWalletUTXOEventTx(tx *sql.Tx, utxoID string, eventType string, refTxID string, refBusinessID string, note string, payload any) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	p := "{}"
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			p = string(b)
		}
	}
	_, err := tx.Exec(
		`INSERT INTO wallet_utxo_events(created_at_unix,utxo_id,event_type,ref_txid,ref_business_id,note,payload_json) VALUES(?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		strings.TrimSpace(utxoID),
		strings.TrimSpace(eventType),
		strings.ToLower(strings.TrimSpace(refTxID)),
		strings.TrimSpace(refBusinessID),
		strings.TrimSpace(note),
		p,
	)
	return err
}

func reconcileWalletUTXOSet(ctx context.Context, store *clientDB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOHistoryCursor, syncRoundID string, lastError string, trigger string, updatedAt int64, durationMS int64) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	return store.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
		if err != nil {
			return err
		}
		existing := map[string]utxoStateRow{}
		for rows.Next() {
			var r utxoStateRow
			if scanErr := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.AllocationClass, &r.AllocationReason, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix); scanErr != nil {
				_ = rows.Close()
				return scanErr
			}
			r.AllocationClass = normalizeWalletUTXOAllocationClass(r.AllocationClass)
			r.AllocationReason = strings.TrimSpace(r.AllocationReason)
			existing[strings.ToLower(strings.TrimSpace(r.UTXOID))] = r
		}
		if err = rows.Close(); err != nil {
			return err
		}
		scriptHex, err := walletAddressLockScriptHex(address)
		if err != nil {
			return err
		}
		localBroadcasts, err := loadWalletLocalBroadcastTxsTx(tx, walletID, address)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`UPDATE wallet_utxo SET state='spent',updated_at_unix=?,spent_at_unix=CASE WHEN spent_at_unix>0 THEN spent_at_unix ELSE ? END WHERE wallet_id=? AND address=? AND state<>'spent'`, updatedAt, updatedAt, walletID, address); err != nil {
			return err
		}
		for utxoID, row := range existing {
			if strings.TrimSpace(strings.ToLower(row.State)) != "spent" {
				row.State = "spent"
				existing[utxoID] = row
			}
		}

		for _, hist := range history {
			historyTxID := strings.ToLower(strings.TrimSpace(hist.TxID))
			for _, out := range hist.Tx.Vout {
				utxoID, value, ok := matchWalletOutput(historyTxID, out, scriptHex)
				if !ok {
					continue
				}
				if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, historyTxID, out.N, value, "spent", "", updatedAt); err != nil {
					return err
				}
			}
			for _, in := range hist.Tx.Vin {
				spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
				if _, ok := existing[spentID]; !ok {
					continue
				}
				if err = setWalletUTXOSpentTx(tx, existing, spentID, historyTxID, updatedAt, "confirmed_history_spent"); err != nil {
					return err
				}
			}
		}

		for _, detail := range snapshot.ObservedMempoolTxs {
			mempoolTxID := strings.ToLower(strings.TrimSpace(detail.TxID))
			for _, out := range detail.Vout {
				utxoID, value, ok := matchWalletOutput(mempoolTxID, out, scriptHex)
				if !ok {
					continue
				}
				if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, mempoolTxID, out.N, value, "spent", "", updatedAt); err != nil {
					return err
				}
			}
			for _, in := range detail.Vin {
				spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
				if _, ok := existing[spentID]; !ok {
					continue
				}
				if err = setWalletUTXOSpentTx(tx, existing, spentID, mempoolTxID, updatedAt, "mempool_spent"); err != nil {
					return err
				}
			}
		}

		for utxoID, u := range snapshot.Live {
			if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, strings.ToLower(strings.TrimSpace(u.TxID)), u.Vout, u.Value, "unspent", "", updatedAt); err != nil {
				return err
			}
		}
		observedLocalTxIDs := collectObservedWalletTxIDs(history, snapshot.ObservedMempoolTxs)
		if err = overlayPendingLocalBroadcastsTx(tx, existing, walletID, address, scriptHex, localBroadcasts, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}
		if err = markObservedWalletLocalBroadcastTxsTx(tx, observedLocalTxIDs, updatedAt); err != nil {
			return err
		}
		accountedTxIDs := map[string]struct{}{}
		recordWalletChainTx := func(detail whatsonchain.TxDetail) error {
			input, ok, err := buildWalletChainAccountingInputFromTxDetail(tx, address, detail)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if _, seen := accountedTxIDs[input.TxID]; seen {
				return nil
			}
			if err := recordWalletChainAccountingConn(tx, input); err != nil {
				return err
			}
			accountedTxIDs[input.TxID] = struct{}{}
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
		stats := summarizeWalletUTXOState(existing)

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
			stats.UTXOCount,
			stats.BalanceSatoshi,
			stats.PlainBSVUTXOCount,
			stats.PlainBSVBalanceSatoshi,
			stats.ProtectedUTXOCount,
			stats.ProtectedBalanceSatoshi,
			stats.UnknownUTXOCount,
			stats.UnknownBalanceSatoshi,
			updatedAt,
			strings.TrimSpace(lastError),
			"chain_utxo_worker",
			strings.TrimSpace(trigger),
			durationMS,
			strings.TrimSpace(syncRoundID),
			"",
			"",
			0,
		); err != nil {
			return err
		}
		if _, err = tx.Exec(
			`INSERT INTO wallet_utxo_history_cursor(address,wallet_id,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error)
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
}

func upsertWalletUTXORowTx(tx *sql.Tx, existing map[string]utxoStateRow, walletID string, address string, utxoID string, txid string, vout uint32, value uint64, state string, spentTxID string, updatedAt int64) error {
	return upsertWalletUTXORowTxWithEvent(tx, existing, walletID, address, utxoID, txid, vout, value, state, spentTxID, updatedAt, "detected", "utxo detected by chain sync", map[string]any{"state": state})
}

func loadWalletLocalBroadcastTxsTx(tx *sql.Tx, walletID string, address string) ([]walletLocalBroadcastRow, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	rows, err := tx.Query(
		`SELECT txid,tx_hex,created_at_unix,updated_at_unix,observed_at_unix
		 FROM wallet_local_broadcast_txs
		 WHERE wallet_id=? AND address=?
		 ORDER BY created_at_unix ASC, updated_at_unix ASC, txid ASC`,
		walletID,
		address,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]walletLocalBroadcastRow, 0, 8)
	for rows.Next() {
		var row walletLocalBroadcastRow
		if err := rows.Scan(&row.TxID, &row.TxHex, &row.CreatedAtUnix, &row.UpdatedAtUnix, &row.ObservedAtUnix); err != nil {
			return nil, err
		}
		row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
		row.TxHex = strings.ToLower(strings.TrimSpace(row.TxHex))
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func collectObservedWalletTxIDs(history []walletHistoryTxRecord, mempool []whatsonchain.TxDetail) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range history {
		txid := strings.ToLower(strings.TrimSpace(item.TxID))
		if txid != "" {
			out[txid] = struct{}{}
		}
	}
	for _, item := range mempool {
		txid := strings.ToLower(strings.TrimSpace(item.TxID))
		if txid != "" {
			out[txid] = struct{}{}
		}
	}
	return out
}

func overlayPendingLocalBroadcastsTx(tx *sql.Tx, existing map[string]utxoStateRow, walletID string, address string, scriptHex string, rows []walletLocalBroadcastRow, observedTxIDs map[string]struct{}, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if len(rows) == 0 {
		return nil
	}
	ordered, err := orderWalletLocalBroadcastRows(rows)
	if err != nil {
		return err
	}
	for _, row := range ordered {
		if row.ObservedAtUnix > 0 {
			continue
		}
		if _, ok := observedTxIDs[row.TxID]; ok {
			continue
		}
		parsed, err := txsdk.NewTransactionFromHex(row.TxHex)
		if err != nil {
			return fmt.Errorf("parse local broadcast tx %s failed: %w", row.TxID, err)
		}
		for _, in := range parsed.Inputs {
			if in == nil || in.SourceTXID == nil {
				continue
			}
			utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
			if err := setWalletUTXOSpentTxWithNote(tx, existing, utxoID, row.TxID, updatedAt, "local_pending_spent", "utxo preserved by local pending broadcast", map[string]any{
				"txid": row.TxID,
			}); err != nil {
				return err
			}
		}
		for idx, out := range parsed.Outputs {
			if out == nil || out.LockingScript == nil {
				continue
			}
			if !walletScriptHexMatchesAddressControl(hex.EncodeToString(out.LockingScript.Bytes()), scriptHex) {
				continue
			}
			utxoID := row.TxID + ":" + fmt.Sprint(idx)
			if err := upsertWalletUTXORowTxWithEvent(tx, existing, walletID, address, utxoID, row.TxID, uint32(idx), out.Satoshis, "unspent", "", updatedAt, "local_pending_detected", "utxo preserved by local pending broadcast", map[string]any{
				"txid": row.TxID,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func orderWalletLocalBroadcastRows(rows []walletLocalBroadcastRow) ([]walletLocalBroadcastRow, error) {
	if len(rows) <= 1 {
		return rows, nil
	}
	parsed := make(map[string]*txsdk.Transaction, len(rows))
	pending := make(map[string]walletLocalBroadcastRow, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.TxID) == "" || strings.TrimSpace(row.TxHex) == "" {
			continue
		}
		parsedTx, err := txsdk.NewTransactionFromHex(row.TxHex)
		if err != nil {
			return nil, fmt.Errorf("parse local broadcast tx %s failed: %w", row.TxID, err)
		}
		parsed[row.TxID] = parsedTx
		pending[row.TxID] = row
	}
	out := make([]walletLocalBroadcastRow, 0, len(pending))
	for len(pending) > 0 {
		progressed := false
		for txid, row := range pending {
			ready := true
			for _, in := range parsed[txid].Inputs {
				if in == nil || in.SourceTXID == nil {
					continue
				}
				prevTxID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String()))
				if prevTxID == "" || prevTxID == txid {
					continue
				}
				if _, waiting := pending[prevTxID]; waiting {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			out = append(out, row)
			delete(pending, txid)
			progressed = true
		}
		if progressed {
			continue
		}
		remaining := make([]walletLocalBroadcastRow, 0, len(pending))
		for _, row := range pending {
			remaining = append(remaining, row)
		}
		sort.Slice(remaining, func(i, j int) bool {
			if remaining[i].CreatedAtUnix != remaining[j].CreatedAtUnix {
				return remaining[i].CreatedAtUnix < remaining[j].CreatedAtUnix
			}
			return remaining[i].TxID < remaining[j].TxID
		})
		out = append(out, remaining...)
		break
	}
	return out, nil
}

func markObservedWalletLocalBroadcastTxsTx(tx *sql.Tx, observedTxIDs map[string]struct{}, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if len(observedTxIDs) == 0 {
		return nil
	}
	holders := make([]string, 0, len(observedTxIDs))
	args := make([]any, 0, len(observedTxIDs)+2)
	args = append(args, updatedAt, updatedAt)
	for txid := range observedTxIDs {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			continue
		}
		holders = append(holders, "?")
		args = append(args, txid)
	}
	if len(holders) == 0 {
		return nil
	}
	_, err := tx.Exec(
		`UPDATE wallet_local_broadcast_txs
		 SET observed_at_unix=CASE WHEN observed_at_unix>0 THEN observed_at_unix ELSE ? END,
		     updated_at_unix=?
		 WHERE txid IN (`+strings.Join(holders, ",")+`)`,
		args...,
	)
	return err
}

func upsertWalletUTXORowTxWithEvent(tx *sql.Tx, existing map[string]utxoStateRow, walletID string, address string, utxoID string, txid string, vout uint32, value uint64, state string, spentTxID string, updatedAt int64, eventType string, eventNote string, payload any) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	txid = strings.ToLower(strings.TrimSpace(txid))
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	row, ok := existing[utxoID]
	createdAtUnix := updatedAt
	if ok && row.CreatedAtUnix > 0 {
		createdAtUnix = row.CreatedAtUnix
	}
	// 设计说明：
	// - 按当前 1sat 资产模型，真正需要先保护的是“1 聪输出”；
	// - 这些输出在链上先出现、索引后确认之间有时间差，若先按 plain_bsv 放行，可能被分配器误花；
	// - 因此 value=1 的新 UTXO 先进入 unknown，等索引明确确认“不是 1sat 资产输出”后才放行；
	// - 非 1 聪输出默认仍按 plain_bsv 处理，避免把保护范围错误扩大到整个钱包。
	allocationClass, allocationReason := defaultWalletUTXOProtectionForValue(value)
	if ok {
		allocationClass = normalizeWalletUTXOAllocationClass(row.AllocationClass)
		allocationReason = strings.TrimSpace(row.AllocationReason)
	}
	spentAtUnix := int64(0)
	if state == "spent" {
		spentAtUnix = updatedAt
	}
	if ok {
		_, err := tx.Exec(
			`UPDATE wallet_utxo
			 SET txid=?,vout=?,value_satoshi=?,state=?,allocation_class=?,allocation_reason=?,created_txid=?,spent_txid=?,updated_at_unix=?,spent_at_unix=?
			 WHERE utxo_id=?`,
			txid, vout, value, strings.TrimSpace(state), allocationClass, allocationReason, txid, spentTxID, updatedAt, spentAtUnix, utxoID,
		)
		if err != nil {
			return err
		}
		row.TxID = txid
		row.Vout = vout
		row.Value = value
		row.State = state
		row.CreatedTxID = txid
		row.SpentTxID = spentTxID
		row.AllocationClass = allocationClass
		row.AllocationReason = allocationReason
		existing[utxoID] = row
		return nil
	}
	_, err := tx.Exec(
		`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, txid, vout, value, strings.TrimSpace(state), allocationClass, allocationReason, txid, spentTxID, createdAtUnix, updatedAt, spentAtUnix,
	)
	if err != nil {
		return err
	}
	existing[utxoID] = utxoStateRow{
		UTXOID:           utxoID,
		TxID:             txid,
		Vout:             vout,
		Value:            value,
		State:            state,
		AllocationClass:  allocationClass,
		AllocationReason: allocationReason,
		CreatedTxID:      txid,
		SpentTxID:        spentTxID,
		CreatedAtUnix:    createdAtUnix,
	}
	return appendWalletUTXOEventTx(tx, utxoID, strings.TrimSpace(eventType), txid, "", strings.TrimSpace(eventNote), payload)
}

func setWalletUTXOSpentTx(tx *sql.Tx, existing map[string]utxoStateRow, utxoID string, spentTxID string, updatedAt int64, eventType string) error {
	return setWalletUTXOSpentTxWithNote(tx, existing, utxoID, spentTxID, updatedAt, eventType, "utxo spent by chain sync", nil)
}

func setWalletUTXOSpentTxWithNote(tx *sql.Tx, existing map[string]utxoStateRow, utxoID string, spentTxID string, updatedAt int64, eventType string, eventNote string, payload any) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	row, ok := existing[utxoID]
	if !ok {
		return nil
	}
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	prevState := strings.TrimSpace(strings.ToLower(row.State))
	prevSpentTxID := strings.TrimSpace(strings.ToLower(row.SpentTxID))
	if _, err := tx.Exec(`UPDATE wallet_utxo SET state='spent',spent_txid=?,spent_at_unix=?,updated_at_unix=? WHERE utxo_id=?`, spentTxID, updatedAt, updatedAt, utxoID); err != nil {
		return err
	}
	row.State = "spent"
	row.SpentTxID = spentTxID
	existing[utxoID] = row
	if prevState == "spent" && prevSpentTxID == spentTxID {
		return nil
	}
	return appendWalletUTXOEventTx(tx, utxoID, strings.TrimSpace(eventType), spentTxID, "", strings.TrimSpace(eventNote), payload)
}
