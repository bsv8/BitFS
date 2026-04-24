package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// ==================== 测试/初始化支架 ====================

// applySQLitePragmas 统一给测试库加上基础 SQLite 参数。
// 设计说明：
// - WAL 提高并发写稳定性；
// - busy_timeout 避免短锁竞争直接失败。
func applySQLitePragmas(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("sqlite pragma foreign_keys: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("sqlite pragma journal_mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=15000`); err != nil {
		return fmt.Errorf("sqlite pragma busy_timeout: %w", err)
	}
	return nil
}

// shortHex 取字符串前4...后4，用于日志和 API 显示长 ID。
func shortHex(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}

// ==================== Token / BSV 兼容入口 ====================

// businessTriggerEntry order_settlement_events schema 已删除，保留结构体定义仅供编译通过
type businessTriggerEntry struct {
	TriggerID      string
	OrderID        string
	SettlementID   string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
	CreatedAtUnix  int64
	Note           string
	Payload        any
}



// dbUpsertTokenLot 幂等写入/更新 Token Lot。
func dbUpsertTokenLot(ctx context.Context, store *clientDB, e tokenLotEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertTokenLotEntTx(ctx, tx, e)
	})
}

// dbGetTokenLot 查询单个 Token Lot。
func dbGetTokenLot(ctx context.Context, store *clientDB, lotID string) (*tokenLotEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	var result *tokenLotEntry
	err := store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		item, err := dbGetTokenLotEntTx(ctx, tx, lotID)
		if err != nil {
			return err
		}
		result = item
		return nil
	})
	return result, err
}

// dbUpsertTokenCarrierLink 幂等写入/更新 Token Carrier Link。
func dbUpsertTokenCarrierLink(ctx context.Context, store *clientDB, e tokenCarrierLinkEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertTokenCarrierLinkEntTx(ctx, tx, e)
	})
}

// dbListActiveCarrierLinksByOwner 查询某个 owner 下所有 active carrier link。
func dbListActiveCarrierLinksByOwner(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]tokenCarrierLinkEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	var result []tokenCarrierLinkEntry
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid,
			       created_at_unix, updated_at_unix, note, payload_json
			  FROM fact_token_carrier_links
			 WHERE owner_pubkey_hex=? AND link_state='active'
			 ORDER BY created_at_unix ASC, link_id ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		out := make([]tokenCarrierLinkEntry, 0, 16)
		for rows.Next() {
			var row tokenCarrierLinkEntry
			var payloadJSON string
			if err := rows.Scan(&row.LinkID, &row.LotID, &row.CarrierUTXOID, &row.OwnerPubkeyHex, &row.LinkState,
				&row.BindTxid, &row.UnbindTxid, &row.CreatedAtUnix, &row.UpdatedAtUnix, &row.Note, &payloadJSON); err != nil {
				return err
			}
			if strings.TrimSpace(payloadJSON) != "" {
				row.Payload = json.RawMessage(payloadJSON)
			}
			out = append(out, row)
		}
		result = out
		return rows.Err()
	})
	return result, err
}

// dbAppendSettlementRecord 写入结算消耗记录。
func dbAppendSettlementRecord(ctx context.Context, store *clientDB, e settlementRecordEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbAppendSettlementRecordEntTx(ctx, tx, e)
	})
}

// dbListSettlementRecordsByCycle 查询某个 settlement_payment_attempt 的消耗记录。
func dbListSettlementRecordsByCycle(ctx context.Context, store *clientDB, settlementPaymentAttemptID int64) ([]settlementRecordEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return nil, fmt.Errorf("settlement_payment_attempt_id is required")
	}
	var result []settlementRecordEntry
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
			       COALESCE(source_utxo_id,''), COALESCE(source_lot_id,''),
			       used_satoshi, COALESCE(used_quantity_text,''), state,
			       occurred_at_unix, confirmed_at_unix, COALESCE(note,''), COALESCE(payload_json,'{}')
			  FROM fact_settlement_records
			 WHERE settlement_payment_attempt_id=?
			 ORDER BY id ASC`,
			settlementPaymentAttemptID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		out := make([]settlementRecordEntry, 0, 8)
		for rows.Next() {
			var row settlementRecordEntry
			var payloadJSON string
			if err := rows.Scan(&row.RecordID, &row.SettlementPaymentAttemptID, &row.AssetType, &row.OwnerPubkeyHex,
				&row.SourceUTXOID, &row.SourceLotID, &row.UsedSatoshi, &row.UsedQuantityText, &row.State,
				&row.OccurredAtUnix, &row.ConfirmedAtUnix, &row.Note, &payloadJSON); err != nil {
				return err
			}
			if strings.TrimSpace(payloadJSON) != "" {
				row.Payload = json.RawMessage(payloadJSON)
			}
			out = append(out, row)
		}
		result = out
		return rows.Err()
	})
	return result, err
}

// dbSelectBSVUTXOsForTarget 按目标金额做小额优先选币。
func dbSelectBSVUTXOsForTarget(ctx context.Context, store *clientDB, ownerPubkeyHex string, target uint64) ([]selectedBSVUTXO, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if target == 0 {
		return []selectedBSVUTXO{}, nil
	}
	utxos, err := dbListSpendableBSVUTXOs(ctx, store, ownerPubkeyHex)
	if err != nil {
		return nil, err
	}
	if len(utxos) == 0 {
		return nil, fmt.Errorf("no spendable bsv utxos available")
	}
	remaining := int64(target)
	out := make([]selectedBSVUTXO, 0, len(utxos))
	for _, utxo := range utxos {
		if remaining <= 0 {
			break
		}
		use := utxo.ValueSatoshi
		if use > remaining {
			use = remaining
		}
		out = append(out, selectedBSVUTXO{
			bsvUTXOEntry: utxo,
			UseAmount:    use,
		})
		remaining -= use
	}
	if remaining > 0 {
		var total int64
		for _, utxo := range utxos {
			total += utxo.ValueSatoshi
		}
		return nil, fmt.Errorf("insufficient balance: target=%d, available=%d, missing=%d", target, total, remaining)
	}
	return out, nil
}

// dbLoadWalletBSVBalance 加载钱包本币余额。
func dbLoadWalletBSVBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string) (walletBSVBalance, error) {
	if store == nil {
		return walletBSVBalance{}, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return walletBSVBalance{}, fmt.Errorf("owner_pubkey_hex is required")
	}
	var result walletBSVBalance
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		result.OwnerPubkeyHex = ownerPubkeyHex

		rows, err := rc.QueryContext(ctx, `
			SELECT value_satoshi, carrier_type
			  FROM fact_bsv_utxos
			 WHERE owner_pubkey_hex=? AND utxo_state='unspent'
			 ORDER BY created_at_unix ASC, utxo_id ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var value int64
			var carrierType string
			if err := rows.Scan(&value, &carrierType); err != nil {
				return err
			}
			result.TotalSatoshi += uint64(value)
			if strings.ToLower(strings.TrimSpace(carrierType)) == "plain_bsv" {
				result.ConfirmedSatoshi += uint64(value)
				result.SpendableUTXOCount++
			}
		}
		return rows.Err()
	})
	return result, err
}

// dbLoadAllWalletTokenBalances 加载钱包所有 Token 余额。
func dbLoadAllWalletTokenBalances(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]walletTokenBalance, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	type tokenAgg struct {
		standard  string
		tokenID   string
		totalIn   string
		totalUsed string
	}
	var result []walletTokenBalance
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT token_standard, token_id, quantity_text, used_quantity_text
			  FROM fact_token_lots
			 WHERE owner_pubkey_hex=? AND lot_state='unspent'
			 ORDER BY token_standard ASC, token_id ASC, created_at_unix ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		aggs := make(map[string]*tokenAgg)
		for rows.Next() {
			var standard, tokenID, qtyText, usedText string
			if err := rows.Scan(&standard, &tokenID, &qtyText, &usedText); err != nil {
				return err
			}
			key := standard + ":" + tokenID
			agg, ok := aggs[key]
			if !ok {
				agg = &tokenAgg{standard: standard, tokenID: tokenID, totalIn: "0", totalUsed: "0"}
				aggs[key] = agg
			}
			nextIn, err := sumDecimalTexts(agg.totalIn + "," + qtyText)
			if err != nil {
				return err
			}
			nextUsed, err := sumDecimalTexts(agg.totalUsed + "," + usedText)
			if err != nil {
				return err
			}
			agg.totalIn = nextIn
			agg.totalUsed = nextUsed
		}
		if err := rows.Err(); err != nil {
			return err
		}

		keys := make([]string, 0, len(aggs))
		for key := range aggs {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		out := make([]walletTokenBalance, 0, len(keys))
		for _, key := range keys {
			agg := aggs[key]
			remaining, err := subtractTokenDecimalText(agg.totalIn, agg.totalUsed)
			if err != nil {
				return err
			}
			if compareDecimalText(remaining, "0") <= 0 {
				continue
			}
			out = append(out, walletTokenBalance{
				OwnerPubkeyHex: ownerPubkeyHex,
				TokenStandard:  agg.standard,
				TokenID:        agg.tokenID,
				BalanceText:    remaining,
			})
		}
		result = out
		return nil
	})
	return result, err
}

// dbListTokenSpendableSourceFlows 保持旧接口，但只查新 fact 表。
func dbListTokenSpendableSourceFlows(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) ([]tokenSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind != "BSV20" && assetKind != "BSV21" {
		return nil, fmt.Errorf("token_standard must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("token_id is required")
	}
	lots, err := dbListSpendableTokenLots(ctx, store, ownerPubkeyHex, assetKind, tokenID)
	if err != nil {
		return nil, err
	}
	out := make([]tokenSourceFlow, 0, len(lots))
	for i, lot := range lots {
		link, err := dbGetActiveCarrierForLot(ctx, store, lot.LotID)
		if err != nil || link == nil {
			continue
		}
		var walletUtxoAllocationClass string
		err = store.Read(ctx, func(rc moduleapi.ReadConn) error {
			row := rc.QueryRowContext(ctx, `SELECT allocation_class FROM wallet_utxo WHERE utxo_id=?`, link.CarrierUTXOID)
			if row != nil {
				if err := row.Scan(&walletUtxoAllocationClass); err != nil {
					if err != sql.ErrNoRows {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if strings.ToLower(strings.TrimSpace(walletUtxoAllocationClass)) == walletUTXOAllocationUnknown {
			continue
		}
		remainingText, err := subtractTokenDecimalText(lot.QuantityText, lot.UsedQuantityText)
		if err != nil {
			return nil, err
		}
		if compareDecimalText(remainingText, "0") <= 0 {
			continue
		}
		txid, vout, ok := splitUTXOID(link.CarrierUTXOID)
		if !ok {
			continue
		}
		out = append(out, tokenSourceFlow{
			FlowID:         int64(i + 1),
			WalletID:       walletID,
			Address:        "",
			AssetKind:      assetKind,
			TokenID:        tokenID,
			UTXOID:         link.CarrierUTXOID,
			TxID:           txid,
			Vout:           vout,
			QuantityText:   remainingText,
			TotalUsedText:  lot.UsedQuantityText,
			OccurredAtUnix: lot.CreatedAtUnix,
		})
	}
	return out, nil
}

// dbLoadTokenBalanceFact 保持旧接口，但直接从新 fact 表计算。
func dbLoadTokenBalanceFact(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (tokenBalanceResult, error) {
	if store == nil {
		return tokenBalanceResult{}, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind != "BSV20" && assetKind != "BSV21" {
		return tokenBalanceResult{}, fmt.Errorf("token_standard must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return tokenBalanceResult{}, fmt.Errorf("token_id is required")
	}
	var result tokenBalanceResult
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		result.WalletID = walletID
		result.AssetKind = assetKind
		result.TokenID = tokenID

		rows, err := rc.QueryContext(ctx, `
			SELECT quantity_text, used_quantity_text
			  FROM fact_token_lots
			 WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=? AND lot_state='unspent'
			 ORDER BY created_at_unix ASC, lot_id ASC`,
			ownerPubkeyHex, assetKind, tokenID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		totalIn := "0"
		totalUsed := "0"
		for rows.Next() {
			var qtyText, usedText string
			if err := rows.Scan(&qtyText, &usedText); err != nil {
				return err
			}
			nextIn, err := sumDecimalTexts(totalIn + "," + qtyText)
			if err != nil {
				return err
			}
			nextUsed, err := sumDecimalTexts(totalUsed + "," + usedText)
			if err != nil {
				return err
			}
			totalIn = nextIn
			totalUsed = nextUsed
		}
		if err := rows.Err(); err != nil {
			return err
		}
		result.TotalInText = totalIn
		result.TotalUsedText = totalUsed
		return nil
	})
	return result, err
}

// dbGetBSVUTXO 查询单个本币 UTXO。
func dbGetBSVUTXO(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return nil, fmt.Errorf("utxo_id is required")
	}
	var result *bsvUTXOEntry
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		row := rc.QueryRowContext(ctx, `
			SELECT utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
			       utxo_state, carrier_type, spent_by_txid, created_at_unix, updated_at_unix,
			       spent_at_unix, note, COALESCE(payload_json,'{}')
			  FROM fact_bsv_utxos
			 WHERE utxo_id=?
			 LIMIT 1`,
			utxoID,
		)
		if row == nil {
			return nil
		}
		var out bsvUTXOEntry
		var payloadJSON string
		if err := row.Scan(&out.UTXOID, &out.OwnerPubkeyHex, &out.Address, &out.TxID, &out.Vout, &out.ValueSatoshi,
			&out.UTXOState, &out.CarrierType, &out.SpentByTxid, &out.CreatedAtUnix, &out.UpdatedAtUnix,
			&out.SpentAtUnix, &out.Note, &payloadJSON); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		if strings.TrimSpace(payloadJSON) != "" {
			out.Payload = json.RawMessage(payloadJSON)
		}
		result = &out
		return nil
	})
	return result, err
}

// dbMarkBSVUTXOSpent 标记本币 UTXO 为已花费。
func dbMarkBSVUTXOSpent(ctx context.Context, store *clientDB, utxoID string, spentByTxid string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if spentByTxid == "" {
		return fmt.Errorf("spent_by_txid is required")
	}
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		now := time.Now().Unix()
		result, err := wtx.ExecContext(ctx, `
			UPDATE fact_bsv_utxos
			   SET utxo_state='spent',
			       spent_by_txid=?,
			       spent_at_unix=CASE WHEN spent_at_unix=0 THEN ? ELSE spent_at_unix END,
			       updated_at_unix=?
			 WHERE utxo_id=?`,
			spentByTxid, now, now, utxoID,
		)
		if err != nil {
			return err
		}
		if result == nil {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		return nil
	})
}

// dbCalcBSVBalance 兼容旧签名。
func dbCalcBSVBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string) (uint64, uint64, error) {
	if store == nil {
		return 0, 0, fmt.Errorf("client db is nil")
	}
	var result struct {
		confirmed uint64
		total     uint64
	}
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		confirmed, total, err := dbCalcBSVBalanceEntRoot(ctx, root, ownerPubkeyHex)
		if err != nil {
			return err
		}
		result.confirmed = confirmed
		result.total = total
		return nil
	})
	return result.confirmed, result.total, err
}

// dbCalcTokenBalance 兼容旧签名。
func dbCalcTokenBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	var result string
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		balance, err := dbCalcTokenBalanceEntRoot(ctx, root, ownerPubkeyHex, tokenStandard, tokenID)
		if err != nil {
			return err
		}
		result = balance
		return nil
	})
	return result, err
}

// getFactBSV21ByTokenID 按 token_id 查询 BSV21 主事实。
func getFactBSV21ByTokenID(ctx context.Context, store *clientDB, tokenID string) (factBSV21CreateItem, error) {
	if store == nil {
		return factBSV21CreateItem{}, fmt.Errorf("client db is nil")
	}
	tokenID = strings.ToLower(strings.TrimSpace(tokenID))
	if tokenID == "" {
		return factBSV21CreateItem{}, fmt.Errorf("token_id is required")
	}
	var result factBSV21CreateItem
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		var item factBSV21CreateItem
		var payloadJSON string
		err := rc.QueryRowContext(ctx, `
			SELECT token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,created_at_unix,submitted_at_unix,updated_at_unix,COALESCE(payload_json,'{}')
			  FROM fact_bsv21
			 WHERE token_id=?
			 LIMIT 1`,
			tokenID,
		).Scan(
			&item.TokenID, &item.CreateTxID, &item.WalletID, &item.Address, &item.TokenStandard, &item.Symbol,
			&item.MaxSupply, &item.Decimals, &item.Icon, &item.CreatedAtUnix, &item.SubmittedAtUnix, &item.UpdatedAtUnix, &payloadJSON,
		)
		if err != nil {
			return err
		}
		item.PayloadJSON = payloadJSON
		result = item
		return nil
	})
	return result, err
}

// ==================== 财务业务与前台单 ====================
func dbAppendFinBusiness(ctx context.Context, store *clientDB, e finBusinessEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbAppendFinBusinessTx(ctx, tx, e)
	})
}

// dbListFrontOrders 查询前台业务单列表。
func dbListFrontOrders(ctx context.Context, store *clientDB, filter frontOrderFilter) (frontOrderPage, error) {
	if store == nil {
		return frontOrderPage{}, fmt.Errorf("client db is nil")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	var result frontOrderPage
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		where := make([]string, 0, 6)
		args := make([]any, 0, 6)
		if filter.FrontOrderID != "" {
			where = append(where, "order_id=?")
			args = append(args, strings.TrimSpace(filter.FrontOrderID))
		}
		if filter.FrontType != "" {
			where = append(where, "order_type=?")
			args = append(args, strings.TrimSpace(filter.FrontType))
		}
		if filter.FrontSubtype != "" {
			where = append(where, "order_subtype=?")
			args = append(args, strings.TrimSpace(filter.FrontSubtype))
		}
		if filter.OwnerPubkeyHex != "" {
			where = append(where, "owner_pubkey_hex=?")
			args = append(args, strings.TrimSpace(filter.OwnerPubkeyHex))
		}
		if filter.TargetObjectType != "" {
			where = append(where, "target_object_type=?")
			args = append(args, strings.TrimSpace(filter.TargetObjectType))
		}
		if filter.TargetObjectID != "" {
			where = append(where, "target_object_id=?")
			args = append(args, strings.TrimSpace(filter.TargetObjectID))
		}
		if filter.Status != "" {
			where = append(where, "status=?")
			args = append(args, strings.TrimSpace(filter.Status))
		}

		countSQL := "SELECT COUNT(1) FROM orders"
		listSQL := `
			SELECT order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,COALESCE(note,''),COALESCE(payload_json,'{}')
			  FROM orders`
		if len(where) > 0 {
			clause := " WHERE " + strings.Join(where, " AND ")
			countSQL += clause
			listSQL += clause
		}
		listSQL += " ORDER BY created_at_unix DESC, updated_at_unix DESC, order_id DESC LIMIT ? OFFSET ?"

		var page frontOrderPage
		if err := rc.QueryRowContext(ctx, countSQL, args...).Scan(&page.Total); err != nil {
			return err
		}
		rows, err := rc.QueryContext(ctx, listSQL, append(args, filter.Limit, filter.Offset)...)
		if err != nil {
			return err
		}
		defer rows.Close()

		items := make([]frontOrderItem, 0, filter.Limit)
		for rows.Next() {
			var item frontOrderItem
			var note string
			var payload string
			if err := rows.Scan(&item.FrontOrderID, &item.FrontType, &item.FrontSubtype, &item.OwnerPubkeyHex, &item.TargetObjectType, &item.TargetObjectID,
				&item.Status, &item.CreatedAtUnix, &item.UpdatedAtUnix, &note, &payload); err != nil {
				return err
			}
			item.Note = note
			item.Payload = json.RawMessage(payload)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		page.Items = items
		result = page
		return nil
	})
	return result, err
}

// defaultArbiterPubHex 统一给下载链路挑一个默认仲裁者。
// 设计说明：
// - 优先返回已连通仲裁者的 pub hex；
// - 不返回 peer ID 字符串，避免把系统外标识带回系统内。
func defaultArbiterPubHex(rt *Runtime) string {
	if rt == nil || rt.Host == nil {
		return ""
	}
	for _, ai := range rt.HealthyArbiters {
		pub := rt.Host.Peerstore().PubKey(ai.ID)
		if pub == nil {
			continue
		}
		raw, err := pub.Raw()
		if err != nil {
			continue
		}
		if hexPub, err := normalizeCompressedPubKeyHex(strings.ToLower(fmt.Sprintf("%x", raw))); err == nil {
			return hexPub
		}
	}
	return ""
}
