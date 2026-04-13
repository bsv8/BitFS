package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func loadWalletLocalBroadcastTxsTx(ctx context.Context, tx *sql.Tx, walletID string, address string) ([]walletLocalBroadcastRow, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	walletID = strings.TrimSpace(walletID)
	address = strings.TrimSpace(address)
	rows, err := QueryContext(ctx, tx,
		`SELECT txid,tx_hex,created_at_unix,observed_at_unix,updated_at_unix
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
		if err := rows.Scan(&row.TxID, &row.TxHex, &row.CreatedAtUnix, &row.ObservedAtUnix, &row.UpdatedAtUnix); err != nil {
			return nil, err
		}
		row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
		row.TxHex = strings.ToLower(strings.TrimSpace(row.TxHex))
		if row.TxHex == "" {
			return nil, fmt.Errorf("wallet local broadcast tx_hex is empty for txid=%s", row.TxID)
		}
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

func cloneWalletUTXOStateRows(src map[string]utxoStateRow) map[string]utxoStateRow {
	if len(src) == 0 {
		return map[string]utxoStateRow{}
	}
	out := make(map[string]utxoStateRow, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// applyWalletUTXOUpsertState 只改目标态，不直接落库。
// 设计说明：
// - 先在内存里把最终态算出来，再由统一的 diff 落库；
// - 脚本语义只认 script_type，不再靠 value=1 推导；
// - 这样同一轮里的中间态不会把 updated_at_unix 顶上去。
func applyWalletUTXOUpsertState(target map[string]utxoStateRow, walletID string, address string, utxoID string, txid string, vout uint32, value uint64, state string, spentTxID string, scriptType string, scriptTypeReason string, scriptTypeUpdatedAtUnix int64, updatedAt int64) bool {
	if target == nil {
		return false
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	txid = strings.ToLower(strings.TrimSpace(txid))
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	state = strings.ToLower(strings.TrimSpace(state))
	scriptType = string(normalizeWalletScriptType(scriptType))
	scriptTypeReason = strings.TrimSpace(scriptTypeReason)
	if scriptTypeUpdatedAtUnix <= 0 {
		scriptTypeUpdatedAtUnix = updatedAt
	}
	row, ok := target[utxoID]
	createdAtUnix := updatedAt
	if ok && row.CreatedAtUnix > 0 {
		createdAtUnix = row.CreatedAtUnix
	}
	if ok {
		currentScriptType := normalizeWalletScriptType(row.ScriptType)
		currentScriptTypeReason := strings.TrimSpace(row.ScriptTypeReason)
		if currentScriptType == normalizeWalletScriptType(scriptType) && currentScriptTypeReason == scriptTypeReason && row.ScriptTypeUpdatedAtUnix > 0 {
			scriptTypeUpdatedAtUnix = row.ScriptTypeUpdatedAtUnix
		}
		if currentScriptType != walletScriptTypeUnknown && normalizeWalletScriptType(scriptType) == walletScriptTypeUnknown {
			scriptType = string(currentScriptType)
			scriptTypeReason = currentScriptTypeReason
			scriptTypeUpdatedAtUnix = row.ScriptTypeUpdatedAtUnix
		}
		if scriptTypeReason == "" && currentScriptTypeReason != "" && normalizeWalletScriptType(scriptType) == currentScriptType {
			scriptTypeReason = currentScriptTypeReason
		}
		if scriptTypeUpdatedAtUnix <= 0 && row.ScriptTypeUpdatedAtUnix > 0 {
			scriptTypeUpdatedAtUnix = row.ScriptTypeUpdatedAtUnix
		}
	}
	allocationClass := walletScriptTypeAllocationClass(scriptType)
	allocationReason := scriptTypeReason
	if ok && allocationReason == "" {
		allocationReason = strings.TrimSpace(row.AllocationReason)
	}
	spentAtUnix := int64(0)
	if state == "spent" {
		if ok && row.SpentAtUnix > 0 {
			spentAtUnix = row.SpentAtUnix
		} else {
			spentAtUnix = updatedAt
		}
	}
	if ok {
		currentState := strings.ToLower(strings.TrimSpace(row.State))
		currentSpentTxID := strings.ToLower(strings.TrimSpace(row.SpentTxID))
		currentAllocationClass := normalizeWalletUTXOAllocationClass(row.AllocationClass)
		currentAllocationReason := strings.TrimSpace(row.AllocationReason)
		currentScriptType := normalizeWalletScriptType(row.ScriptType)
		currentScriptTypeReason := strings.TrimSpace(row.ScriptTypeReason)
		currentScriptTypeUpdatedAtUnix := row.ScriptTypeUpdatedAtUnix
		currentCreatedTxID := strings.ToLower(strings.TrimSpace(row.CreatedTxID))
		// 旧行已经被判定为 spent 时，不再允许被“回滚”为 unspent。
		if state == "unspent" && currentState == "spent" && currentSpentTxID != "" {
			return false
		}
		if currentState == state &&
			currentSpentTxID == spentTxID &&
			currentAllocationClass == allocationClass &&
			currentAllocationReason == allocationReason &&
			currentScriptType == normalizeWalletScriptType(scriptType) &&
			currentScriptTypeReason == scriptTypeReason &&
			currentScriptTypeUpdatedAtUnix == scriptTypeUpdatedAtUnix &&
			currentCreatedTxID == txid &&
			strings.ToLower(strings.TrimSpace(row.TxID)) == txid &&
			row.Vout == vout &&
			row.Value == value &&
			row.SpentAtUnix == spentAtUnix {
			return false
		}
	}
	target[utxoID] = utxoStateRow{
		UTXOID:                  utxoID,
		TxID:                    txid,
		Vout:                    vout,
		Value:                   value,
		State:                   state,
		ScriptType:              normalizeWalletScriptType(scriptType).String(),
		ScriptTypeReason:        scriptTypeReason,
		ScriptTypeUpdatedAtUnix: scriptTypeUpdatedAtUnix,
		AllocationClass:         allocationClass,
		AllocationReason:        allocationReason,
		CreatedTxID:             txid,
		SpentTxID:               spentTxID,
		CreatedAtUnix:           createdAtUnix,
		SpentAtUnix:             spentAtUnix,
	}
	return true
}

// applyWalletUTXOSpentState 只改目标态，不直接落库。
func applyWalletUTXOSpentState(target map[string]utxoStateRow, utxoID string, spentTxID string, updatedAt int64) bool {
	if target == nil {
		return false
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	row, ok := target[utxoID]
	if !ok {
		return false
	}
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	currentState := strings.ToLower(strings.TrimSpace(row.State))
	currentSpentTxID := strings.ToLower(strings.TrimSpace(row.SpentTxID))
	if currentState == "spent" && currentSpentTxID == spentTxID && row.SpentAtUnix > 0 {
		return false
	}
	spentAtUnix := updatedAt
	if row.SpentAtUnix > 0 {
		spentAtUnix = row.SpentAtUnix
	}
	row.State = "spent"
	row.SpentTxID = spentTxID
	row.SpentAtUnix = spentAtUnix
	target[utxoID] = row
	return true
}

func walletUTXOBusinessEqual(current utxoStateRow, desired utxoStateRow) bool {
	return strings.ToLower(strings.TrimSpace(current.TxID)) == strings.ToLower(strings.TrimSpace(desired.TxID)) &&
		current.Vout == desired.Vout &&
		current.Value == desired.Value &&
		strings.ToLower(strings.TrimSpace(current.State)) == strings.ToLower(strings.TrimSpace(desired.State)) &&
		normalizeWalletScriptType(current.ScriptType) == normalizeWalletScriptType(desired.ScriptType) &&
		strings.TrimSpace(current.ScriptTypeReason) == strings.TrimSpace(desired.ScriptTypeReason) &&
		current.ScriptTypeUpdatedAtUnix == desired.ScriptTypeUpdatedAtUnix &&
		normalizeWalletUTXOAllocationClass(current.AllocationClass) == normalizeWalletUTXOAllocationClass(desired.AllocationClass) &&
		strings.TrimSpace(current.AllocationReason) == strings.TrimSpace(desired.AllocationReason) &&
		strings.ToLower(strings.TrimSpace(current.CreatedTxID)) == strings.ToLower(strings.TrimSpace(desired.CreatedTxID)) &&
		strings.ToLower(strings.TrimSpace(current.SpentTxID)) == strings.ToLower(strings.TrimSpace(desired.SpentTxID)) &&
		current.SpentAtUnix == desired.SpentAtUnix
}

// applyWalletUTXODiffTx 统一比较业务字段，只把真正变化的行写回数据库。
func applyWalletUTXODiffTx(ctx context.Context, tx *sql.Tx, current map[string]utxoStateRow, desired map[string]utxoStateRow, walletID string, address string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if desired == nil {
		return nil
	}
	scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
	walletID = strings.ToLower(strings.TrimSpace(walletID))
	address = strings.TrimSpace(address)
	for utxoID, target := range desired {
		utxoID = strings.ToLower(strings.TrimSpace(utxoID))
		if utxoID == "" {
			continue
		}
		currentRow, ok := current[utxoID]
		if ok && walletUTXOBusinessEqual(currentRow, target) {
			continue
		}
		if ok {
			updateScope := scope
			updateScope.Intent = "wallet_utxo_upsert_update"
			var writeErr error
			sqliteactor.WithTraceScope(updateScope, func() {
				_, writeErr = ExecContext(ctx, tx,
					`UPDATE wallet_utxo
					 SET txid=?,vout=?,value_satoshi=?,state=?,script_type=?,script_type_reason=?,script_type_updated_at_unix=?,allocation_class=?,allocation_reason=?,created_txid=?,spent_txid=?,updated_at_unix=?,spent_at_unix=?
					 WHERE utxo_id=? AND wallet_id=? AND address=?`,
					target.TxID, int64(target.Vout), int64(target.Value), target.State, target.ScriptType, target.ScriptTypeReason, target.ScriptTypeUpdatedAtUnix, target.AllocationClass, target.AllocationReason, target.CreatedTxID, target.SpentTxID, updatedAt, target.SpentAtUnix,
					utxoID, walletID, address,
				)
			})
			if writeErr != nil {
				return writeErr
			}
			continue
		}
		createdAtUnix := target.CreatedAtUnix
		if createdAtUnix <= 0 {
			createdAtUnix = updatedAt
		}
		insertScope := scope
		insertScope.Intent = "wallet_utxo_upsert_insert"
		var writeErr error
		sqliteactor.WithTraceScope(insertScope, func() {
			_, writeErr = ExecContext(ctx, tx,
				`INSERT INTO wallet_utxo(
					utxo_id,wallet_id,address,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
				) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				utxoID, walletID, address, target.TxID, int64(target.Vout), int64(target.Value), target.State, target.ScriptType, target.ScriptTypeReason, target.ScriptTypeUpdatedAtUnix, target.AllocationClass, target.AllocationReason, target.CreatedTxID, target.SpentTxID, createdAtUnix, updatedAt, target.SpentAtUnix,
			)
		})
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func overlayPendingLocalBroadcastsTx(ctx context.Context, classifier *walletScriptClassifier, target map[string]utxoStateRow, walletID string, address string, scriptHex string, rows []walletLocalBroadcastRow, observedTxIDs map[string]struct{}, updatedAt int64) error {
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
			_ = applyWalletUTXOSpentState(target, utxoID, row.TxID, updatedAt)
		}
		for idx, out := range parsed.Outputs {
			if out == nil || out.LockingScript == nil {
				continue
			}
			if !walletScriptHexMatchesAddressControl(hex.EncodeToString(out.LockingScript.Bytes()), scriptHex) {
				continue
			}
			utxoID := row.TxID + ":" + fmt.Sprint(idx)
			classified := classifyWalletUTXOWithClassifier(ctx, classifier, nil, address, utxoID, row.TxID, uint32(idx), out.Satoshis, hex.EncodeToString(out.LockingScript.Bytes()), row.TxHex)
			_ = applyWalletUTXOUpsertState(target, walletID, address, utxoID, row.TxID, uint32(idx), out.Satoshis, "unspent", "", string(classified.ScriptType), classified.Reason, updatedAt, updatedAt)
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

func markObservedWalletLocalBroadcastTxsTx(ctx context.Context, tx *sql.Tx, observedTxIDs map[string]struct{}, updatedAt int64) error {
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
	_, err := ExecContext(ctx, tx,
		`UPDATE wallet_local_broadcast_txs
		 SET observed_at_unix=CASE WHEN observed_at_unix>0 THEN observed_at_unix ELSE ? END,
		     updated_at_unix=?
		 WHERE txid IN (`+strings.Join(holders, ",")+`)`,
		args...,
	)
	return err
}
