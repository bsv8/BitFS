package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// 设计说明：
// - 这里收口钱包运行期常见的读查询，避免业务代码到处直连 SQL；
// - 只保留当前还在用的查询，不再带旧 create 状态逻辑。

func dbLoadCurrentWalletUTXOStateRows(ctx context.Context, store *clientDB, address string) (map[string]utxoStateRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (map[string]utxoStateRow, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return map[string]utxoStateRow{}, fmt.Errorf("wallet address is empty")
		}
		walletID := walletIDByAddress(address)
		rows, err := QueryContext(ctx, db, `SELECT utxo_id,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,spent_at_unix
			FROM wallet_utxo
			WHERE wallet_id=? AND address=?
			ORDER BY created_at_unix ASC, utxo_id ASC`, walletID, address)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[string]utxoStateRow{}
		for rows.Next() {
			var row utxoStateRow
			if err := rows.Scan(&row.UTXOID, &row.TxID, &row.Vout, &row.Value, &row.State, &row.ScriptType, &row.ScriptTypeReason, &row.ScriptTypeUpdatedAtUnix, &row.AllocationClass, &row.AllocationReason, &row.CreatedTxID, &row.SpentTxID, &row.CreatedAtUnix, &row.SpentAtUnix); err != nil {
				return nil, err
			}
			row.ScriptType = string(normalizeWalletScriptType(row.ScriptType))
			row.AllocationClass = walletScriptTypeAllocationClass(row.ScriptType)
			row.AllocationReason = strings.TrimSpace(row.AllocationReason)
			out[strings.ToLower(strings.TrimSpace(row.UTXOID))] = row
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbLoadWalletUTXOsByID(ctx context.Context, store *clientDB, address string, utxoIDs []string) ([]poolcore.UTXO, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]poolcore.UTXO, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return []poolcore.UTXO{}, fmt.Errorf("wallet address is empty")
		}
		if len(utxoIDs) == 0 {
			return []poolcore.UTXO{}, nil
		}
		walletID := walletIDByAddress(address)
		args := make([]any, 0, len(utxoIDs)+2)
		args = append(args, walletID, address)
		placeholders := make([]string, 0, len(utxoIDs))
		requested := make(map[string]struct{}, len(utxoIDs))
		for _, raw := range utxoIDs {
			utxoID := strings.ToLower(strings.TrimSpace(raw))
			if utxoID == "" {
				continue
			}
			requested[utxoID] = struct{}{}
			placeholders = append(placeholders, "?")
			args = append(args, utxoID)
		}
		if len(placeholders) == 0 {
			return []poolcore.UTXO{}, nil
		}
		query := `SELECT utxo_id,txid,vout,value_satoshi
			FROM wallet_utxo
			WHERE wallet_id=? AND address=? AND state='unspent' AND utxo_id IN (` + strings.Join(placeholders, ",") + `)
			ORDER BY created_at_unix ASC, utxo_id ASC`
		rows, err := QueryContext(ctx, db, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]poolcore.UTXO, 0, len(placeholders))
		for rows.Next() {
			var utxoID string
			var item poolcore.UTXO
			if err := rows.Scan(&utxoID, &item.TxID, &item.Vout, &item.Value); err != nil {
				return nil, err
			}
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(out) != len(requested) {
			return nil, fmt.Errorf("requested utxo count mismatch: want=%d got=%d", len(requested), len(out))
		}
		return out, nil
	})
}

func dbListPlainBSVFundingCandidates(ctx context.Context, store *clientDB, address string) ([]fundalloc.Candidate, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]fundalloc.Candidate, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return []fundalloc.Candidate{}, nil
		}
		ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(address, "wallet:"))
		walletID := walletIDByAddress(ownerPubkeyHex)
		rows, err := QueryContext(ctx, db, `SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,carrier_type,note
			FROM fact_bsv_utxos
			WHERE (owner_pubkey_hex=? OR owner_pubkey_hex=?)
			  AND utxo_state='unspent' AND carrier_type='plain_bsv'
			ORDER BY created_at_unix ASC, value_satoshi ASC, txid ASC, vout ASC`, ownerPubkeyHex, walletID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]fundalloc.Candidate, 0, 16)
		for rows.Next() {
			var item fundalloc.Candidate
			var carrierType string
			var note string
			if err := rows.Scan(&item.ID, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.CreatedAtUnix, &carrierType, &note); err != nil {
				return nil, err
			}
			item.ID = strings.ToLower(strings.TrimSpace(item.ID))
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
			item.ProtectionClass = fundalloc.ProtectionClass(walletUTXOAllocationPlainBSV)
			item.ProtectionReason = strings.TrimSpace(note)
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbListWalletFundingCandidates(ctx context.Context, store *clientDB, address string) ([]walletFundingCandidate, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletFundingCandidate, error) {
		s, err := dbLoadWalletUTXOSyncState(ctx, store, address)
		if err != nil {
			return nil, err
		}
		if s.UpdatedAtUnix <= 0 {
			return nil, fmt.Errorf("wallet utxo sync state not ready")
		}
		if strings.TrimSpace(s.LastError) != "" {
			return nil, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
		}
		walletID := walletIDByAddress(address)
		rows, err := QueryContext(ctx, db, `SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,script_type,script_type_reason,allocation_class,allocation_reason
			FROM wallet_utxo
			WHERE wallet_id=? AND address=? AND state='unspent'
			ORDER BY created_at_unix ASC, value_satoshi ASC, txid ASC, vout ASC`, walletID, address)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletFundingCandidate, 0, s.UTXOCount)
		for rows.Next() {
			var item walletFundingCandidate
			if err := rows.Scan(&item.UTXOID, &item.UTXO.TxID, &item.UTXO.Vout, &item.UTXO.Value, &item.CreatedAtUnix, &item.ScriptType, &item.ScriptTypeReason, &item.AllocationClass, &item.AllocationReason); err != nil {
				return nil, err
			}
			item.UTXOID = strings.ToLower(strings.TrimSpace(item.UTXOID))
			item.UTXO.TxID = strings.ToLower(strings.TrimSpace(item.UTXO.TxID))
			item.ScriptType = string(normalizeWalletScriptType(item.ScriptType))
			item.AllocationClass = normalizeWalletUTXOAllocationClass(item.AllocationClass)
			item.AllocationReason = strings.TrimSpace(item.AllocationReason)
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbListWalletUnspentOneSatRows(ctx context.Context, store *clientDB, address string) ([]walletUTXOBasicRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletUTXOBasicRow, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return []walletUTXOBasicRow{}, nil
		}
		walletID := walletIDByAddress(address)
		rows, err := QueryContext(ctx, db, `SELECT utxo_id,txid,vout,value_satoshi,script_type,script_type_reason,allocation_class,allocation_reason,created_at_unix
			FROM wallet_utxo
			WHERE wallet_id=? AND address=? AND state='unspent' AND value_satoshi=1
			ORDER BY created_at_unix ASC, utxo_id ASC`, walletID, address)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletUTXOBasicRow, 0, 16)
		for rows.Next() {
			var item walletUTXOBasicRow
			if err := rows.Scan(&item.UTXOID, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.ScriptType, &item.ScriptTypeReason, &item.AllocationClass, &item.AllocationReason, &item.CreatedAtUnix); err != nil {
				return nil, err
			}
			item.UTXOID = strings.ToLower(strings.TrimSpace(item.UTXOID))
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
			item.ScriptType = string(normalizeWalletScriptType(item.ScriptType))
			item.AllocationClass = normalizeWalletUTXOAllocationClass(item.AllocationClass)
			item.AllocationReason = strings.TrimSpace(item.AllocationReason)
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbLoadWalletLocalBroadcastRows(ctx context.Context, store *clientDB, walletID string, address string) ([]walletLocalBroadcastRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletLocalBroadcastRow, error) {
		walletID = strings.TrimSpace(walletID)
		address = strings.TrimSpace(address)
		if walletID == "" || address == "" {
			return []walletLocalBroadcastRow{}, nil
		}
		rows, err := QueryContext(ctx, db, `SELECT txid,tx_hex,created_at_unix,observed_at_unix,updated_at_unix
			FROM wallet_local_broadcast_txs
			WHERE wallet_id=? AND address=?
			ORDER BY created_at_unix ASC, updated_at_unix ASC, txid ASC`, walletID, address)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletLocalBroadcastRow, 0, 8)
		for rows.Next() {
			var item walletLocalBroadcastRow
			if err := rows.Scan(&item.TxID, &item.TxHex, &item.CreatedAtUnix, &item.ObservedAtUnix, &item.UpdatedAtUnix); err != nil {
				return nil, err
			}
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
			item.TxHex = strings.ToLower(strings.TrimSpace(item.TxHex))
			if item.TxHex == "" {
				return nil, fmt.Errorf("wallet local broadcast tx_hex is empty for txid=%s", item.TxID)
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbResolveWalletAddress(ctx context.Context, store *clientDB) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var address string
		err := QueryRowContext(ctx, db, `SELECT address FROM wallet_utxo_sync_state ORDER BY updated_at_unix DESC, address ASC LIMIT 1`).Scan(&address)
		if err == nil && strings.TrimSpace(address) != "" {
			return strings.TrimSpace(address), nil
		}
		err = QueryRowContext(ctx, db, `SELECT address FROM wallet_utxo ORDER BY updated_at_unix DESC, address ASC LIMIT 1`).Scan(&address)
		if err != nil {
			return "", err
		}
		address = strings.TrimSpace(address)
		if address == "" {
			return "", fmt.Errorf("wallet address not found")
		}
		return address, nil
	})
}

func factChainPaymentPayloadTxHex(payloadJSON string) (string, error) {
	payloadJSON = strings.TrimSpace(payloadJSON)
	if payloadJSON == "" || payloadJSON == "{}" {
		return "", fmt.Errorf("fact chain payment payload is empty")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", err
	}
	txHex := strings.ToLower(strings.TrimSpace(firstNonEmptyStringField(payload, "tx_hex", "txHex")))
	if txHex == "" {
		return "", fmt.Errorf("fact chain payment payload missing tx_hex")
	}
	return txHex, nil
}
