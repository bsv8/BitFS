package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// 设计说明：
// - 这一组 helper 收口钱包运行期常见的只读/轻写动作；
// - wallet 过程函数不再自己散落 Query/Exec，只在这里取快照或更新状态。

func dbLoadWalletBSV21CreateStatusByTokenID(ctx context.Context, store *clientDB, tokenID string) (walletBSV21CreateStatusItem, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (walletBSV21CreateStatusItem, error) {
		tokenID = strings.ToLower(strings.TrimSpace(tokenID))
		if tokenID == "" {
			return walletBSV21CreateStatusItem{}, fmt.Errorf("token_id is required")
		}
		var item walletBSV21CreateStatusItem
		err := db.QueryRow(
			`SELECT token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,status,created_at_unix,submitted_at_unix,confirmed_at_unix,last_check_at_unix,next_auto_check_at_unix,updated_at_unix,last_check_error
			 FROM wallet_bsv21_create_status
			 WHERE token_id=?`,
			tokenID,
		).Scan(
			&item.TokenID,
			&item.CreateTxID,
			&item.WalletID,
			&item.Address,
			&item.TokenStandard,
			&item.Symbol,
			&item.MaxSupply,
			&item.Decimals,
			&item.Icon,
			&item.Status,
			&item.CreatedAtUnix,
			&item.SubmittedAtUnix,
			&item.VerifiedAtUnix,
			&item.LastVerificationAtUnix,
			&item.NextVerificationAtUnix,
			&item.UpdatedAtUnix,
			&item.LastVerificationError,
		)
		if err != nil {
			return walletBSV21CreateStatusItem{}, err
		}
		item.TokenID = strings.ToLower(strings.TrimSpace(item.TokenID))
		item.CreateTxID = strings.ToLower(strings.TrimSpace(item.CreateTxID))
		item.WalletID = strings.TrimSpace(item.WalletID)
		item.Address = strings.TrimSpace(item.Address)
		item.TokenStandard = strings.TrimSpace(item.TokenStandard)
		item.Symbol = strings.TrimSpace(item.Symbol)
		item.MaxSupply = strings.TrimSpace(item.MaxSupply)
		item.Icon = strings.TrimSpace(item.Icon)
		item.Status = normalizeWalletBSV21CreateStatus(item.Status)
		item.LastVerificationError = strings.TrimSpace(item.LastVerificationError)
		return item, nil
	})
}

func dbUpsertWalletBSV21CreateStatus(ctx context.Context, store *clientDB, item walletBSV21CreateStatusItem) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	item.TokenID = strings.ToLower(strings.TrimSpace(item.TokenID))
	item.CreateTxID = strings.ToLower(strings.TrimSpace(item.CreateTxID))
	item.WalletID = strings.TrimSpace(item.WalletID)
	item.Address = strings.TrimSpace(item.Address)
	item.TokenStandard = strings.TrimSpace(item.TokenStandard)
	item.Symbol = strings.TrimSpace(item.Symbol)
	item.MaxSupply = strings.TrimSpace(item.MaxSupply)
	item.Icon = strings.TrimSpace(item.Icon)
	item.Status = normalizeWalletBSV21CreateStatus(item.Status)
	item.LastVerificationError = strings.TrimSpace(item.LastVerificationError)
	if item.TokenID == "" || item.CreateTxID == "" {
		return fmt.Errorf("token_id and create_txid are required")
	}
	if item.TokenStandard == "" {
		item.TokenStandard = "bsv21"
	}
	if item.CreatedAtUnix <= 0 {
		item.CreatedAtUnix = time.Now().Unix()
	}
	if item.SubmittedAtUnix <= 0 {
		item.SubmittedAtUnix = item.CreatedAtUnix
	}
	if item.UpdatedAtUnix <= 0 {
		item.UpdatedAtUnix = time.Now().Unix()
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO wallet_bsv21_create_status(
				token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,status,created_at_unix,submitted_at_unix,confirmed_at_unix,last_check_at_unix,next_auto_check_at_unix,updated_at_unix,last_check_error
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(token_id) DO UPDATE SET
				create_txid=excluded.create_txid,
				wallet_id=excluded.wallet_id,
				address=excluded.address,
				token_standard=excluded.token_standard,
				symbol=excluded.symbol,
				max_supply=excluded.max_supply,
				decimals=excluded.decimals,
				icon=excluded.icon,
				status=excluded.status,
				submitted_at_unix=excluded.submitted_at_unix,
				confirmed_at_unix=excluded.confirmed_at_unix,
				last_check_at_unix=excluded.last_check_at_unix,
				next_auto_check_at_unix=excluded.next_auto_check_at_unix,
				updated_at_unix=excluded.updated_at_unix,
				last_check_error=excluded.last_check_error`,
			item.TokenID,
			item.CreateTxID,
			item.WalletID,
			item.Address,
			item.TokenStandard,
			item.Symbol,
			item.MaxSupply,
			item.Decimals,
			item.Icon,
			item.Status,
			item.CreatedAtUnix,
			item.SubmittedAtUnix,
			item.VerifiedAtUnix,
			item.LastVerificationAtUnix,
			item.NextVerificationAtUnix,
			item.UpdatedAtUnix,
			item.LastVerificationError,
		)
		return err
	})
}

func dbScheduleWalletBSV21CreateAutoCheckAfterTipChange(ctx context.Context, store *clientDB, dueAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	if dueAtUnix <= 0 {
		dueAtUnix = time.Now().Add(walletBSV21CreateAutoCheckDelay).Unix()
	}
	updatedAt := time.Now().Unix()
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE wallet_bsv21_create_status
			 SET next_auto_check_at_unix=?,updated_at_unix=?
			 WHERE status=?`,
			dueAtUnix,
			updatedAt,
			walletBSV21CreateStatusPendingExternalVerification,
		)
		return err
	})
}

func dbListDueWalletBSV21CreateStatuses(ctx context.Context, store *clientDB, nowUnix int64) ([]walletBSV21CreateStatusItem, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletBSV21CreateStatusItem, error) {
		if nowUnix <= 0 {
			nowUnix = time.Now().Unix()
		}
		rows, err := db.Query(
			`SELECT token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,status,created_at_unix,submitted_at_unix,confirmed_at_unix,last_check_at_unix,next_auto_check_at_unix,updated_at_unix,last_check_error
			 FROM wallet_bsv21_create_status
			 WHERE status=? AND next_auto_check_at_unix>0 AND next_auto_check_at_unix<=?
			 ORDER BY next_auto_check_at_unix ASC,token_id ASC`,
			walletBSV21CreateStatusPendingExternalVerification,
			nowUnix,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletBSV21CreateStatusItem, 0, 8)
		for rows.Next() {
			var item walletBSV21CreateStatusItem
			if err := rows.Scan(
				&item.TokenID,
				&item.CreateTxID,
				&item.WalletID,
				&item.Address,
				&item.TokenStandard,
				&item.Symbol,
				&item.MaxSupply,
				&item.Decimals,
				&item.Icon,
				&item.Status,
				&item.CreatedAtUnix,
				&item.SubmittedAtUnix,
				&item.VerifiedAtUnix,
				&item.LastVerificationAtUnix,
				&item.NextVerificationAtUnix,
				&item.UpdatedAtUnix,
				&item.LastVerificationError,
			); err != nil {
				return nil, err
			}
			item.Status = normalizeWalletBSV21CreateStatus(item.Status)
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
		rows, err := db.Query(
			`SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,allocation_class,allocation_reason
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent'
			 ORDER BY created_at_unix ASC,value_satoshi ASC,txid ASC,vout ASC`,
			walletID,
			address,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletFundingCandidate, 0, s.UTXOCount)
		for rows.Next() {
			var item walletFundingCandidate
			if err := rows.Scan(
				&item.UTXOID,
				&item.UTXO.TxID,
				&item.UTXO.Vout,
				&item.UTXO.Value,
				&item.CreatedAtUnix,
				&item.AllocationClass,
				&item.AllocationReason,
			); err != nil {
				return nil, err
			}
			item.UTXOID = strings.ToLower(strings.TrimSpace(item.UTXOID))
			item.UTXO.TxID = strings.ToLower(strings.TrimSpace(item.UTXO.TxID))
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

func dbLoadCurrentWalletUTXOStateRows(ctx context.Context, store *clientDB, address string) (map[string]utxoStateRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (map[string]utxoStateRow, error) {
		walletID := walletIDByAddress(address)
		rows, err := db.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, strings.TrimSpace(address))
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
	})
}

func dbListWalletUTXOAssetRows(ctx context.Context, store *clientDB, utxoID string) ([]walletUTXOAssetRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletUTXOAssetRow, error) {
		rows, err := db.Query(
			`SELECT utxo_id,wallet_id,address,asset_group,asset_standard,asset_key,asset_symbol,quantity_text,source_name,payload_json,updated_at_unix
			 FROM wallet_utxo_assets
			 WHERE utxo_id=?
			 ORDER BY asset_group ASC,asset_standard ASC,asset_key ASC`,
			strings.ToLower(strings.TrimSpace(utxoID)),
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletUTXOAssetRow, 0, 4)
		for rows.Next() {
			var item walletUTXOAssetRow
			if err := rows.Scan(&item.UTXOID, &item.WalletID, &item.Address, &item.AssetGroup, &item.AssetStandard, &item.AssetKey, &item.AssetSymbol, &item.QuantityText, &item.SourceName, &item.PayloadJSON, &item.UpdatedAtUnix); err != nil {
				return nil, err
			}
			out = append(out, item)
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
		if address == "" || len(utxoIDs) == 0 {
			return []poolcore.UTXO{}, nil
		}
		walletID := walletIDByAddress(address)
		out := make([]poolcore.UTXO, 0, len(utxoIDs))
		for _, rawID := range utxoIDs {
			utxoID := strings.ToLower(strings.TrimSpace(rawID))
			if utxoID == "" {
				continue
			}
			var item poolcore.UTXO
			err := db.QueryRow(
				`SELECT txid,vout,value_satoshi
				 FROM wallet_utxo
				 WHERE wallet_id=? AND address=? AND utxo_id=? AND state='unspent'`,
				walletID,
				address,
				utxoID,
			).Scan(&item.TxID, &item.Vout, &item.Value)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, fmt.Errorf("wallet utxo not found: %s", utxoID)
				}
				return nil, err
			}
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
			out = append(out, item)
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
		walletID := walletIDByAddress(address)
		rows, err := db.Query(
			`SELECT utxo_id,txid,vout,value_satoshi,created_at_unix,allocation_class,allocation_reason
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent'
			 ORDER BY created_at_unix ASC,value_satoshi ASC,txid ASC,vout ASC`,
			walletID,
			address,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		items := make([]fundalloc.Candidate, 0, 8)
		for rows.Next() {
			var item fundalloc.Candidate
			var allocationClass string
			var allocationReason string
			if err := rows.Scan(&item.ID, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.CreatedAtUnix, &allocationClass, &allocationReason); err != nil {
				return nil, err
			}
			item.ProtectionClass = fundalloc.ProtectionClass(allocationClass)
			item.ProtectionReason = strings.TrimSpace(allocationReason)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return items, nil
	})
}

func dbResolveWalletAddress(ctx context.Context, store *clientDB) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var addr string
		err := db.QueryRow(`SELECT address FROM wallet_utxo ORDER BY updated_at_unix DESC,created_at_unix DESC,utxo_id ASC LIMIT 1`).Scan(&addr)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return strings.TrimSpace(addr), nil
	})
}

func dbListWalletUnspentOneSatRows(ctx context.Context, store *clientDB, address string) ([]walletUTXOBasicRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletUTXOBasicRow, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return []walletUTXOBasicRow{}, nil
		}
		walletID := walletIDByAddress(address)
		rows, err := db.Query(
			`SELECT utxo_id,txid,vout,value_satoshi,allocation_class,allocation_reason,created_at_unix
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent' AND value_satoshi=1
			 ORDER BY created_at_unix ASC,txid ASC,vout ASC`,
			walletID,
			address,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]walletUTXOBasicRow, 0, 8)
		for rows.Next() {
			var item walletUTXOBasicRow
			if err := rows.Scan(&item.UTXOID, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.AllocationClass, &item.AllocationReason, &item.CreatedAtUnix); err != nil {
				return nil, err
			}
			item.UTXOID = strings.ToLower(strings.TrimSpace(item.UTXOID))
			item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
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
		rows, err := db.Query(
			`SELECT txid,tx_hex,created_at_unix,updated_at_unix,observed_at_unix
			 FROM wallet_local_broadcast_txs
			 WHERE wallet_id=? AND address=?
			 ORDER BY created_at_unix ASC, updated_at_unix ASC, txid ASC`,
			strings.TrimSpace(walletID),
			strings.TrimSpace(address),
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
		return orderWalletLocalBroadcastRows(out)
	})
}

func dbLoadWalletLocalBroadcastHex(ctx context.Context, store *clientDB, txid string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var txHex string
		err := db.QueryRow(`SELECT tx_hex FROM wallet_local_broadcast_txs WHERE txid=?`, strings.ToLower(strings.TrimSpace(txid))).Scan(&txHex)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(txHex)), nil
	})
}
