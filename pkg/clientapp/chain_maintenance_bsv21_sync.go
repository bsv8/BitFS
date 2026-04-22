package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type walletBSV21UnspentSnapshotItem struct {
	UTXOID     string
	TxID       string
	Vout       uint32
	TokenID    string
	AmountText string
}

// syncWalletBSV21HoldingsFromUnspent 把 WOC bsv21 unspent 快照回写到本地事实表。
// 设计说明：
// - 空快照就是“当前持仓为 0”，不加缓冲、不加延迟；
// - 下一轮如果又查到 unspent，再按同一 lot/link id 恢复为 unspent；
// - 这样直接用快照面对上游抖动，符合“清零后可恢复”的业务语义。
func syncWalletBSV21HoldingsFromUnspent(ctx context.Context, store *clientDB, address string, snapshot []whatsonchain.BSV21TokenUTXO, updatedAt int64) (int, int, error) {
	if store == nil {
		return 0, 0, fmt.Errorf("store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return 0, 0, fmt.Errorf("wallet address is required")
	}
	if updatedAt <= 0 {
		return 0, 0, fmt.Errorf("updated_at is required")
	}
	ownerPubkeyHex := strings.ToLower(address)
	present := make(map[string]walletBSV21UnspentSnapshotItem, len(snapshot))
	for _, row := range snapshot {
		txid := strings.ToLower(strings.TrimSpace(row.TxID))
		tokenID := strings.ToLower(strings.TrimSpace(row.TokenID))
		if txid == "" || tokenID == "" {
			continue
		}
		amountText := strings.TrimSpace(row.AmountText)
		if amountText == "" {
			amountText = "0"
		}
		utxoID := txid + ":" + fmt.Sprint(row.Vout)
		present[utxoID] = walletBSV21UnspentSnapshotItem{
			UTXOID:     utxoID,
			TxID:       txid,
			Vout:       row.Vout,
			TokenID:    tokenID,
			AmountText: amountText,
		}
	}

	upserted := 0
	cleared := 0
	err := store.WriteTx(ctx, func(db moduleapi.WriteTx) error {
		for _, item := range present {
			lotID := walletBSV21SnapshotLotID(item.TokenID, item.TxID, item.Vout)
			linkID := walletBSV21SnapshotLinkID(item.TokenID, item.TxID, item.Vout)
			if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
				UTXOID:         item.UTXOID,
				OwnerPubkeyHex: ownerPubkeyHex,
				Address:        address,
				TxID:           item.TxID,
				Vout:           item.Vout,
				ValueSatoshi:   1,
				UTXOState:      "unspent",
				CarrierType:    "token_carrier",
				CreatedAtUnix:  updatedAt,
				UpdatedAtUnix:  updatedAt,
				Note:           "bsv21 unspent snapshot active",
				Payload: map[string]any{
					"source":     "woc_token_bsv21_unspent",
					"token_id":   item.TokenID,
					"amount_txt": item.AmountText,
				},
			}); err != nil {
				return fmt.Errorf("upsert bsv21 carrier utxo failed: %w", err)
			}
			if err := dbUpsertTokenLotDB(ctx, db, tokenLotEntry{
				LotID:            lotID,
				OwnerPubkeyHex:   ownerPubkeyHex,
				TokenID:          item.TokenID,
				TokenStandard:    "BSV21",
				QuantityText:     item.AmountText,
				UsedQuantityText: "0",
				LotState:         "unspent",
				MintTxid:         item.TxID,
				LastSpendTxid:    "",
				CreatedAtUnix:    updatedAt,
				UpdatedAtUnix:    updatedAt,
				Note:             "bsv21 unspent snapshot active",
				Payload: map[string]any{
					"source":       "woc_token_bsv21_unspent",
					"carrier_utxo": item.UTXOID,
				},
			}); err != nil {
				return fmt.Errorf("upsert bsv21 token lot failed: %w", err)
			}
			if err := dbUpsertTokenCarrierLinkDB(ctx, db, tokenCarrierLinkEntry{
				LinkID:         linkID,
				LotID:          lotID,
				CarrierUTXOID:  item.UTXOID,
				OwnerPubkeyHex: ownerPubkeyHex,
				LinkState:      "active",
				BindTxid:       item.TxID,
				UnbindTxid:     "",
				CreatedAtUnix:  updatedAt,
				UpdatedAtUnix:  updatedAt,
				Note:           "bsv21 carrier active by snapshot",
				Payload: map[string]any{
					"source":     "woc_token_bsv21_unspent",
					"token_id":   item.TokenID,
					"amount_txt": item.AmountText,
				},
			}); err != nil {
				return fmt.Errorf("upsert bsv21 carrier link failed: %w", err)
			}
			upserted++
		}

		rows, err := QueryContext(ctx, db, `SELECT l.link_id,l.lot_id,LOWER(TRIM(l.carrier_utxo_id)),LOWER(TRIM(COALESCE(t.token_standard,'')))
FROM fact_token_carrier_links l
LEFT JOIN fact_token_lots t ON t.lot_id=l.lot_id
WHERE l.owner_pubkey_hex=? AND l.link_state='active'`, ownerPubkeyHex)
		if err != nil {
			return fmt.Errorf("list active bsv21 carrier links failed: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var linkID string
			var lotID string
			var carrierUTXOID string
			var tokenStandard string
			if err := rows.Scan(&linkID, &lotID, &carrierUTXOID, &tokenStandard); err != nil {
				return fmt.Errorf("scan active carrier link failed: %w", err)
			}
			if tokenStandard != "bsv21" {
				continue
			}
			if _, exists := present[carrierUTXOID]; exists {
				continue
			}
			if _, err := ExecContext(ctx, db, `UPDATE fact_token_carrier_links
SET link_state='released',updated_at_unix=?,note=?,payload_json=?
WHERE link_id=?`, updatedAt, "bsv21 snapshot missing", mustJSONString(map[string]any{"source": "woc_token_bsv21_unspent", "action": "release_missing"}), strings.TrimSpace(linkID)); err != nil {
				return fmt.Errorf("release bsv21 carrier link failed: %w", err)
			}
			if _, err := ExecContext(ctx, db, `UPDATE fact_token_lots
SET used_quantity_text=quantity_text,lot_state='spent',updated_at_unix=?,note=?,payload_json=?
WHERE lot_id=? AND UPPER(COALESCE(token_standard,''))='BSV21'`, updatedAt, "bsv21 snapshot missing", mustJSONString(map[string]any{"source": "woc_token_bsv21_unspent", "action": "clear_missing"}), strings.TrimSpace(lotID)); err != nil {
				return fmt.Errorf("clear bsv21 token lot failed: %w", err)
			}
			if txid, vout, ok := splitUTXOID(carrierUTXOID); ok {
				if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
					UTXOID:         carrierUTXOID,
					OwnerPubkeyHex: ownerPubkeyHex,
					Address:        address,
					TxID:           txid,
					Vout:           vout,
					ValueSatoshi:   1,
					UTXOState:      "spent",
					CarrierType:    "token_carrier",
					CreatedAtUnix:  updatedAt,
					UpdatedAtUnix:  updatedAt,
					SpentAtUnix:    updatedAt,
					Note:           "bsv21 snapshot missing",
					Payload: map[string]any{
						"source": "woc_token_bsv21_unspent",
						"action": "mark_spent_missing",
					},
				}); err != nil {
					return fmt.Errorf("mark bsv21 carrier utxo spent failed: %w", err)
				}
			}
			cleared++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate active carrier links failed: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return upserted, cleared, nil
}

func walletBSV21SnapshotLotID(tokenID string, txid string, vout uint32) string {
	return fmt.Sprintf("lot_bsv21_%s_%s_%d", strings.ToLower(strings.TrimSpace(tokenID)), strings.ToLower(strings.TrimSpace(txid)), vout)
}

func walletBSV21SnapshotLinkID(tokenID string, txid string, vout uint32) string {
	return fmt.Sprintf("link_bsv21_%s_%s_%d", strings.ToLower(strings.TrimSpace(tokenID)), strings.ToLower(strings.TrimSpace(txid)), vout)
}
