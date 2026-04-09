package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// ==================== Chain Tip State ====================

func dbLoadChainTipState(ctx context.Context, store *clientDB) (chainTipState, error) {
	if store == nil {
		return chainTipState{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (chainTipState, error) {
		var s chainTipState
		err := db.QueryRow(`SELECT tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms FROM proc_chain_tip_state WHERE id=1`).Scan(
			&s.TipHeight, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS,
		)
		if err == sql.ErrNoRows {
			return chainTipState{}, nil
		}
		return s, err
	})
}

func dbUpsertChainTipState(ctx context.Context, store *clientDB, tip uint32, lastError, updatedBy string, updatedAt, durationMS int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO proc_chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
			 VALUES(1,?,?,?,?,?,?)
			 ON CONFLICT(id) DO UPDATE SET
				tip_height=excluded.tip_height,
				updated_at_unix=excluded.updated_at_unix,
				last_error=excluded.last_error,
				last_updated_by=excluded.last_updated_by,
				last_trigger=excluded.last_trigger,
				last_duration_ms=excluded.last_duration_ms`,
			tip, updatedAt, strings.TrimSpace(lastError), "chain_tip_worker", strings.TrimSpace(updatedBy), durationMS,
		)
		return err
	})
}

func dbUpdateChainTipStateError(ctx context.Context, store *clientDB, errMsg, trigger string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	now := time.Now().Unix()
	cur, loadErr := dbLoadChainTipState(ctx, store)
	if loadErr != nil {
		obs.Error("bitcast-client", "proc_chain_tip_state_load_failed", map[string]any{"error": loadErr.Error()})
		return loadErr
	}
	return dbUpsertChainTipState(ctx, store, cur.TipHeight, errMsg, trigger, now, 0)
}

// ==================== Wallet UTXO Sync State ====================

func dbResetWalletUTXOSyncStateOnStartup(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE wallet_utxo_sync_state SET
				last_error='',
				last_trigger='',
				last_duration_ms=0,
				last_sync_round_id='',
				last_failed_step='',
				last_upstream_path='',
				last_http_status=0`,
		)
		return err
	})
}

func dbLoadWalletUTXOSyncState(ctx context.Context, store *clientDB, address string) (walletUTXOSyncState, error) {
	if store == nil {
		return walletUTXOSyncState{}, fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOSyncState{}, fmt.Errorf("wallet address is empty")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOSyncState, error) {
		var s walletUTXOSyncState
		err := db.QueryRow(
			`SELECT wallet_id,address,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status FROM wallet_utxo_sync_state WHERE address=?`,
			address,
		).Scan(&s.WalletID, &s.Address, &s.UTXOCount, &s.BalanceSatoshi, &s.PlainBSVUTXOCount, &s.PlainBSVBalanceSatoshi, &s.ProtectedUTXOCount, &s.ProtectedBalanceSatoshi, &s.UnknownUTXOCount, &s.UnknownBalanceSatoshi, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS, &s.LastSyncRoundID, &s.LastFailedStep, &s.LastUpstreamPath, &s.LastHTTPStatus)
		if err == sql.ErrNoRows {
			return walletUTXOSyncState{}, nil
		}
		return s, err
	})
}

func dbLoadWalletUTXOSyncCursor(ctx context.Context, store *clientDB, address string) (walletUTXOSyncCursor, error) {
	if store == nil {
		return walletUTXOSyncCursor{}, fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOSyncCursor{}, fmt.Errorf("wallet address is empty")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOSyncCursor, error) {
		var s walletUTXOSyncCursor
		err := db.QueryRow(
			`SELECT wallet_id,address,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error
			 FROM wallet_utxo_sync_cursor WHERE address=?`,
			address,
		).Scan(&s.WalletID, &s.Address, &s.NextConfirmedHeight, &s.NextPageToken, &s.AnchorHeight, &s.RoundTipHeight, &s.UpdatedAtUnix, &s.LastError)
		if err == sql.ErrNoRows {
			return walletUTXOSyncCursor{}, nil
		}
		return s, err
	})
}

func dbLoadWalletUTXOAggregate(ctx context.Context, store *clientDB, address string) (walletUTXOAggregateStats, error) {
	var stats walletUTXOAggregateStats
	if store == nil {
		return stats, fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return stats, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOAggregateStats, error) {
		rows, err := db.Query(
			`SELECT allocation_class,COUNT(1),COALESCE(SUM(value_satoshi),0)
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent'
			 GROUP BY allocation_class`,
			walletID, address,
		)
		if err != nil {
			return stats, err
		}
		defer rows.Close()
		for rows.Next() {
			var class string
			var count int
			var balance uint64
			if err := rows.Scan(&class, &count, &balance); err != nil {
				return stats, err
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
		return stats, rows.Err()
	})
}

func dbUpdateWalletUTXOSyncStateError(ctx context.Context, store *clientDB, address string, meta walletSyncRoundMeta, err error, trigger string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	roundID, failedStep, upstreamPath, httpStatus := walletSyncFailureDetails(meta, err)
	return store.Do(ctx, func(db *sql.DB) error {
		zeroCount := int64(0)
		lastHTTPStatus := int64(httpStatus)
		_, execErr := db.Exec(
			`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(address) DO UPDATE SET
				wallet_id=excluded.wallet_id,
				updated_at_unix=excluded.updated_at_unix,
				last_error=excluded.last_error,
				last_updated_by=excluded.last_updated_by,
				last_trigger=excluded.last_trigger,
				last_duration_ms=excluded.last_duration_ms,
				last_sync_round_id=excluded.last_sync_round_id,
				last_failed_step=excluded.last_failed_step,
				last_upstream_path=excluded.last_upstream_path,
				last_http_status=excluded.last_http_status`,
			address, walletID, zeroCount, zeroCount, zeroCount, zeroCount, zeroCount, zeroCount, zeroCount, zeroCount, now, strings.TrimSpace(errMsg), "chain_utxo_worker", strings.TrimSpace(trigger), zeroCount, roundID, failedStep, upstreamPath, lastHTTPStatus,
		)
		if execErr != nil {
			obs.Error("bitcast-client", "wallet_utxo_sync_state_upsert_failed", map[string]any{"error": execErr.Error(), "address": address})
		}
		return execErr
	})
}

func dbUpdateWalletUTXOSyncCursorError(ctx context.Context, store *clientDB, address, errMsg string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	return store.Do(ctx, func(db *sql.DB) error {
		zeroInt64 := int64(0)
		_, err := db.Exec(
			`INSERT INTO wallet_utxo_sync_cursor(address,wallet_id,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error)
			 VALUES(?,?,?,?,?,?,?,?)
			 ON CONFLICT(address) DO UPDATE SET
				wallet_id=excluded.wallet_id,
				updated_at_unix=excluded.updated_at_unix,
				last_error=excluded.last_error`,
			address, walletID, zeroInt64, "", zeroInt64, zeroInt64, now, strings.TrimSpace(errMsg),
		)
		if err != nil {
			obs.Error("bitcast-client", "wallet_utxo_sync_cursor_upsert_failed", map[string]any{"error": err.Error(), "address": address})
		}
		return err
	})
}
