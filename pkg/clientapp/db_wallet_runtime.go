package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factbsvutxos"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxo"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxosyncstate"
)

// 设计说明：
// - 这里收口钱包运行期常见的读查询，避免业务代码到处直连 SQL；
// - 只保留当前还在用的查询，不再带旧 create 状态逻辑。

func dbLoadCurrentWalletUTXOStateRows(ctx context.Context, store *clientDB, address string) (map[string]utxoStateRow, error) {
	return readEntValue(ctx, store, func(root EntReadRoot) (map[string]utxoStateRow, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return map[string]utxoStateRow{}, fmt.Errorf("wallet address is empty")
		}
		walletID := walletIDByAddress(address)
		rows, err := root.WalletUtxo.Query().
			Where(walletutxo.WalletIDEQ(walletID), walletutxo.AddressEQ(address)).
			Order(walletutxo.ByCreatedAtUnix(), walletutxo.ByUtxoID()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := map[string]utxoStateRow{}
		for _, row := range rows {
			item := utxoStateRow{
				UTXOID:                  row.UtxoID,
				TxID:                    row.Txid,
				Vout:                    uint32(row.Vout),
				Value:                   uint64(row.ValueSatoshi),
				State:                   row.State,
				ScriptType:              row.ScriptType,
				ScriptTypeReason:        row.ScriptTypeReason,
				ScriptTypeUpdatedAtUnix: row.ScriptTypeUpdatedAtUnix,
				AllocationClass:         row.AllocationClass,
				AllocationReason:        row.AllocationReason,
				CreatedTxID:             row.CreatedTxid,
				SpentTxID:               row.SpentTxid,
				CreatedAtUnix:           row.CreatedAtUnix,
				SpentAtUnix:             row.SpentAtUnix,
			}
			item.ScriptType = string(normalizeWalletScriptType(item.ScriptType))
			item.AllocationClass = walletScriptTypeAllocationClass(item.ScriptType)
			item.AllocationReason = strings.TrimSpace(item.AllocationReason)
			out[strings.ToLower(strings.TrimSpace(item.UTXOID))] = item
		}
		return out, nil
	})
}

func dbLoadWalletUTXOsByID(ctx context.Context, store *clientDB, address string, utxoIDs []string) ([]poolcore.UTXO, error) {
	return readEntValue(ctx, store, func(root EntReadRoot) ([]poolcore.UTXO, error) {
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
		rows, err := root.WalletUtxo.Query().
			Where(
				walletutxo.WalletIDEQ(walletID),
				walletutxo.AddressEQ(address),
				walletutxo.StateEQ("unspent"),
				walletutxo.UtxoIDIn(utxoIDs...),
			).
			Order(walletutxo.ByCreatedAtUnix(), walletutxo.ByUtxoID()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]poolcore.UTXO, 0, len(rows))
		for _, row := range rows {
			item := poolcore.UTXO{
				TxID:  strings.ToLower(strings.TrimSpace(row.Txid)),
				Vout:  uint32(row.Vout),
				Value: uint64(row.ValueSatoshi),
			}
			out = append(out, item)
		}
		if len(out) != len(requested) {
			return nil, fmt.Errorf("requested utxo count mismatch: want=%d got=%d", len(requested), len(out))
		}
		return out, nil
	})
}

func dbListPlainBSVFundingCandidates(ctx context.Context, store *clientDB, address string) ([]fundalloc.Candidate, error) {
	return readEntValue(ctx, store, func(root EntReadRoot) ([]fundalloc.Candidate, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return []fundalloc.Candidate{}, nil
		}
		ownerPubkeyHex := strings.ToLower(strings.TrimPrefix(address, "wallet:"))
		walletID := walletIDByAddress(ownerPubkeyHex)
		rows, err := root.FactBsvUtxos.Query().
			Where(
				factbsvutxos.OwnerPubkeyHexIn(ownerPubkeyHex, walletID),
				factbsvutxos.UtxoStateEQ("unspent"),
				factbsvutxos.CarrierTypeEQ("plain_bsv"),
			).
			Order(factbsvutxos.ByCreatedAtUnix(), factbsvutxos.ByValueSatoshi(), factbsvutxos.ByTxid(), factbsvutxos.ByVout()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]fundalloc.Candidate, 0, len(rows))
		for _, row := range rows {
			item := fundalloc.Candidate{
				ID:               row.UtxoID,
				TxID:             strings.ToLower(strings.TrimSpace(row.Txid)),
				Vout:             uint32(row.Vout),
				ValueSatoshi:     uint64(row.ValueSatoshi),
				CreatedAtUnix:    row.CreatedAtUnix,
				ProtectionClass:  fundalloc.ProtectionClass(walletUTXOAllocationPlainBSV),
				ProtectionReason: strings.TrimSpace(row.Note),
			}
			out = append(out, item)
		}
		return out, nil
	})
}

func dbResolveWalletAddress(ctx context.Context, store *clientDB) (string, error) {
	return readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		var address string
		row, err := root.WalletUtxoSyncState.Query().
			Order(walletutxosyncstate.ByUpdatedAtUnix()).
			First(ctx)
		if err == nil && strings.TrimSpace(row.Address) != "" {
			return strings.TrimSpace(row.Address), nil
		}
		if row2, err := root.WalletUtxo.Query().Order(walletutxo.ByUpdatedAtUnix()).First(ctx); err == nil {
			address = row2.Address
			if strings.TrimSpace(address) != "" {
				return strings.TrimSpace(address), nil
			}
		}
		return "", fmt.Errorf("wallet address not found")
	})
}
