package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procchaintipstate"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxo"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxosynccursor"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxosyncstate"
)

// ==================== Chain Tip State ====================

func dbLoadChainTipState(ctx context.Context, store *clientDB) (chainTipState, error) {
	if store == nil {
		return chainTipState{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (chainTipState, error) {
		var s chainTipState
		row, err := tx.ProcChainTipState.Query().Where(procchaintipstate.IDEQ(1)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return chainTipState{}, nil
			}
			return chainTipState{}, err
		}
		s.TipHeight = uint32(row.TipHeight)
		s.UpdatedAtUnix = row.UpdatedAtUnix
		s.LastError = row.LastError
		s.LastUpdatedBy = row.LastUpdatedBy
		s.LastTrigger = row.LastTrigger
		s.LastDurationMS = row.LastDurationMs
		return s, nil
	})
}

func dbUpsertChainTipState(ctx context.Context, store *clientDB, tip uint32, lastError, updatedBy string, updatedAt, durationMS int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, err := tx.ProcChainTipState.Query().Where(procchaintipstate.IDEQ(1)).Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetTipHeight(int64(tip)).
				SetUpdatedAtUnix(updatedAt).
				SetLastError(strings.TrimSpace(lastError)).
				SetLastUpdatedBy("chain_tip_worker").
				SetLastTrigger(strings.TrimSpace(updatedBy)).
				SetLastDurationMs(durationMS).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcChainTipState.Create().
			SetTipHeight(int64(tip)).
			SetUpdatedAtUnix(updatedAt).
			SetLastError(strings.TrimSpace(lastError)).
			SetLastUpdatedBy("chain_tip_worker").
			SetLastTrigger(strings.TrimSpace(updatedBy)).
			SetLastDurationMs(durationMS).
			Save(ctx)
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
		obs.Error(ServiceName, "proc_chain_tip_state_load_failed", map[string]any{"error": loadErr.Error()})
		return loadErr
	}
	return dbUpsertChainTipState(ctx, store, cur.TipHeight, errMsg, trigger, now, 0)
}

// ==================== Wallet UTXO Sync State ====================

func dbResetWalletUTXOSyncStateOnStartup(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		_, err := tx.WalletUtxoSyncState.Update().
			SetLastError("").
			SetLastTrigger("").
			SetLastDurationMs(0).
			SetLastSyncRoundID("").
			SetLastFailedStep("").
			SetLastUpstreamPath("").
			SetLastHTTPStatus(0).
			Save(ctx)
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (walletUTXOSyncState, error) {
		var s walletUTXOSyncState
		row, err := tx.WalletUtxoSyncState.Query().Where(walletutxosyncstate.AddressEQ(address)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return walletUTXOSyncState{}, nil
			}
			return walletUTXOSyncState{}, err
		}
		s.WalletID = row.WalletID
		s.Address = row.Address
		s.UTXOCount = int(row.UtxoCount)
		s.BalanceSatoshi = uint64(row.BalanceSatoshi)
		s.PlainBSVUTXOCount = int(row.PlainBsvUtxoCount)
		s.PlainBSVBalanceSatoshi = uint64(row.PlainBsvBalanceSatoshi)
		s.ProtectedUTXOCount = int(row.ProtectedUtxoCount)
		s.ProtectedBalanceSatoshi = uint64(row.ProtectedBalanceSatoshi)
		s.UnknownUTXOCount = int(row.UnknownUtxoCount)
		s.UnknownBalanceSatoshi = uint64(row.UnknownBalanceSatoshi)
		s.UpdatedAtUnix = row.UpdatedAtUnix
		s.LastError = row.LastError
		s.LastUpdatedBy = row.LastUpdatedBy
		s.LastTrigger = row.LastTrigger
		s.LastDurationMS = row.LastDurationMs
		s.LastSyncRoundID = row.LastSyncRoundID
		s.LastFailedStep = row.LastFailedStep
		s.LastUpstreamPath = row.LastUpstreamPath
		s.LastHTTPStatus = int(row.LastHTTPStatus)
		return s, nil
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (walletUTXOSyncCursor, error) {
		var s walletUTXOSyncCursor
		row, err := tx.WalletUtxoSyncCursor.Query().Where(walletutxosynccursor.AddressEQ(address)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return walletUTXOSyncCursor{}, nil
			}
			return walletUTXOSyncCursor{}, err
		}
		s.WalletID = row.WalletID
		s.Address = row.Address
		s.NextConfirmedHeight = row.NextConfirmedHeight
		s.NextPageToken = row.NextPageToken
		s.AnchorHeight = row.AnchorHeight
		s.RoundTipHeight = row.RoundTipHeight
		s.UpdatedAtUnix = row.UpdatedAtUnix
		s.LastError = row.LastError
		return s, nil
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (walletUTXOAggregateStats, error) {
		rows, err := tx.WalletUtxo.Query().
			Where(
				walletutxo.WalletIDEQ(walletID),
				walletutxo.AddressEQ(address),
				walletutxo.StateEQ("unspent"),
			).
			All(ctx)
		if err != nil {
			return stats, err
		}
		for _, row := range rows {
			class := string(normalizeWalletScriptType(row.ScriptType))
			stats.UTXOCount++
			stats.BalanceSatoshi += uint64(row.ValueSatoshi)
			switch walletScriptTypeAllocationClass(class) {
			case walletUTXOAllocationPlainBSV:
				stats.PlainBSVUTXOCount++
				stats.PlainBSVBalanceSatoshi += uint64(row.ValueSatoshi)
			case walletUTXOAllocationProtectedAsset:
				stats.ProtectedUTXOCount++
				stats.ProtectedBalanceSatoshi += uint64(row.ValueSatoshi)
			default:
				stats.UnknownUTXOCount++
				stats.UnknownBalanceSatoshi += uint64(row.ValueSatoshi)
			}
		}
		return stats, nil
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
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, loadErr := tx.WalletUtxoSyncState.Query().Where(walletutxosyncstate.AddressEQ(address)).Only(ctx)
		if loadErr == nil {
			_, err = existing.Update().
				SetWalletID(walletID).
				SetUtxoCount(0).
				SetBalanceSatoshi(0).
				SetPlainBsvUtxoCount(0).
				SetPlainBsvBalanceSatoshi(0).
				SetProtectedUtxoCount(0).
				SetProtectedBalanceSatoshi(0).
				SetUnknownUtxoCount(0).
				SetUnknownBalanceSatoshi(0).
				SetUpdatedAtUnix(now).
				SetLastError(strings.TrimSpace(errMsg)).
				SetLastUpdatedBy("chain_utxo_worker").
				SetLastTrigger(strings.TrimSpace(trigger)).
				SetLastDurationMs(0).
				SetLastSyncRoundID(roundID).
				SetLastFailedStep(failedStep).
				SetLastUpstreamPath(upstreamPath).
				SetLastHTTPStatus(int64(httpStatus)).
				Save(ctx)
			return err
		}
		if loadErr != nil && !gen.IsNotFound(loadErr) {
			return loadErr
		}
		_, err := tx.WalletUtxoSyncState.Create().
			SetAddress(address).
			SetWalletID(walletID).
			SetUtxoCount(0).
			SetBalanceSatoshi(0).
			SetPlainBsvUtxoCount(0).
			SetPlainBsvBalanceSatoshi(0).
			SetProtectedUtxoCount(0).
			SetProtectedBalanceSatoshi(0).
			SetUnknownUtxoCount(0).
			SetUnknownBalanceSatoshi(0).
			SetUpdatedAtUnix(now).
			SetLastError(strings.TrimSpace(errMsg)).
			SetLastUpdatedBy("chain_utxo_worker").
			SetLastTrigger(strings.TrimSpace(trigger)).
			SetLastDurationMs(0).
			SetLastSyncRoundID(roundID).
			SetLastFailedStep(failedStep).
			SetLastUpstreamPath(upstreamPath).
			SetLastHTTPStatus(int64(httpStatus)).
			Save(ctx)
		return err
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
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, loadErr := tx.WalletUtxoSyncCursor.Query().Where(walletutxosynccursor.AddressEQ(address)).Only(ctx)
		if loadErr == nil {
			_, err := existing.Update().
				SetWalletID(walletID).
				SetUpdatedAtUnix(now).
				SetLastError(strings.TrimSpace(errMsg)).
				Save(ctx)
			return err
		}
		if loadErr != nil && !gen.IsNotFound(loadErr) {
			return loadErr
		}
		_, err := tx.WalletUtxoSyncCursor.Create().
			SetAddress(address).
			SetWalletID(walletID).
			SetNextConfirmedHeight(0).
			SetNextPageToken("").
			SetAnchorHeight(0).
			SetRoundTipHeight(0).
			SetUpdatedAtUnix(now).
			SetLastError(strings.TrimSpace(errMsg)).
			Save(ctx)
		return err
	})
}
