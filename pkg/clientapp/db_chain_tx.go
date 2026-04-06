package clientapp

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func loadWalletLocalBroadcastTxsTx(tx *sql.Tx, walletID string, address string) ([]walletLocalBroadcastRow, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	rows, err := tx.Query(
		`SELECT txid,payload_json,submitted_at_unix,wallet_observed_at_unix,updated_at_unix
		 FROM fact_chain_payments
		 WHERE from_party_id=? AND payment_subtype='wallet_local_broadcast' AND submitted_at_unix>0
		 ORDER BY submitted_at_unix ASC, updated_at_unix ASC, txid ASC`,
		walletID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]walletLocalBroadcastRow, 0, 8)
	for rows.Next() {
		var row walletLocalBroadcastRow
		var payloadJSON string
		if err := rows.Scan(&row.TxID, &payloadJSON, &row.CreatedAtUnix, &row.ObservedAtUnix, &row.UpdatedAtUnix); err != nil {
			return nil, err
		}
		row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
		txHex, err := factChainPaymentPayloadTxHex(payloadJSON)
		if err != nil {
			return nil, fmt.Errorf("parse fact_chain_payments payload for txid=%s: %w", row.TxID, err)
		}
		row.TxHex = txHex
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
			if err := setWalletUTXOSpentTx(tx, existing, utxoID, row.TxID, updatedAt); err != nil {
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
			if err := upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, row.TxID, uint32(idx), out.Satoshis, "unspent", "", updatedAt); err != nil {
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
		`UPDATE fact_chain_payments
		 SET wallet_observed_at_unix=CASE WHEN wallet_observed_at_unix>0 THEN wallet_observed_at_unix ELSE ? END,
		     updated_at_unix=?
		 WHERE payment_subtype='wallet_local_broadcast' AND txid IN (`+strings.Join(holders, ",")+`)`,
		args...,
	)
	return err
}

func upsertWalletUTXORowTx(tx *sql.Tx, existing map[string]utxoStateRow, walletID string, address string, utxoID string, txid string, vout uint32, value uint64, state string, spentTxID string, updatedAt int64) error {
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
	return nil
}

func setWalletUTXOSpentTx(tx *sql.Tx, existing map[string]utxoStateRow, utxoID string, spentTxID string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	row, ok := existing[utxoID]
	if !ok {
		return nil
	}
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	if _, err := tx.Exec(`UPDATE wallet_utxo SET state='spent',spent_txid=?,spent_at_unix=?,updated_at_unix=? WHERE utxo_id=?`, spentTxID, updatedAt, updatedAt, utxoID); err != nil {
		return err
	}
	row.State = "spent"
	row.SpentTxID = spentTxID
	existing[utxoID] = row
	return nil
}
