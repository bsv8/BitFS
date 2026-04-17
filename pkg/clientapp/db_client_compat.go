package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
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

// dbUpsertTokenLot 幂等写入/更新 Token Lot。
func dbUpsertTokenLot(ctx context.Context, store *clientDB, e tokenLotEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return dbUpsertTokenLotEntTx(ctx, tx, e)
	})
}

// dbGetTokenLot 查询单个 Token Lot。
func dbGetTokenLot(ctx context.Context, store *clientDB, lotID string) (*tokenLotEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (*tokenLotEntry, error) {
		return dbGetTokenLotEntTx(ctx, tx, lotID)
	})
}

// dbUpsertTokenCarrierLink 幂等写入/更新 Token Carrier Link。
func dbUpsertTokenCarrierLink(ctx context.Context, store *clientDB, e tokenCarrierLinkEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	_, err := clientDBValue(ctx, store, func(db sqlConn) (struct{}, error) {
		return struct{}{}, dbUpsertTokenCarrierLinkDB(ctx, db, e)
	})
	return err
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
	return clientDBValue(ctx, store, func(db sqlConn) ([]tokenCarrierLinkEntry, error) {
		rows, err := QueryContext(ctx, db, `
			SELECT link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid,
			       created_at_unix, updated_at_unix, note, payload_json
			  FROM fact_token_carrier_links
			 WHERE owner_pubkey_hex=? AND link_state='active'
			 ORDER BY created_at_unix ASC, link_id ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		out := make([]tokenCarrierLinkEntry, 0, 16)
		for rows.Next() {
			var row tokenCarrierLinkEntry
			var payloadJSON string
			if err := rows.Scan(&row.LinkID, &row.LotID, &row.CarrierUTXOID, &row.OwnerPubkeyHex, &row.LinkState,
				&row.BindTxid, &row.UnbindTxid, &row.CreatedAtUnix, &row.UpdatedAtUnix, &row.Note, &payloadJSON); err != nil {
				return nil, err
			}
			if strings.TrimSpace(payloadJSON) != "" {
				row.Payload = json.RawMessage(payloadJSON)
			}
			out = append(out, row)
		}
		return out, rows.Err()
	})
}

// dbAppendSettlementRecord 写入结算消耗记录。
func dbAppendSettlementRecord(ctx context.Context, store *clientDB, e settlementRecordEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
	return clientDBValue(ctx, store, func(db sqlConn) ([]settlementRecordEntry, error) {
		rows, err := QueryContext(ctx, db, `
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
			return nil, err
		}
		defer rows.Close()

		out := make([]settlementRecordEntry, 0, 8)
		for rows.Next() {
			var row settlementRecordEntry
			var payloadJSON string
			if err := rows.Scan(&row.RecordID, &row.SettlementPaymentAttemptID, &row.AssetType, &row.OwnerPubkeyHex,
				&row.SourceUTXOID, &row.SourceLotID, &row.UsedSatoshi, &row.UsedQuantityText, &row.State,
				&row.OccurredAtUnix, &row.ConfirmedAtUnix, &row.Note, &payloadJSON); err != nil {
				return nil, err
			}
			if strings.TrimSpace(payloadJSON) != "" {
				row.Payload = json.RawMessage(payloadJSON)
			}
			out = append(out, row)
		}
		return out, rows.Err()
	})
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
	return clientDBValue(ctx, store, func(db sqlConn) (walletBSVBalance, error) {
		var out walletBSVBalance
		out.OwnerPubkeyHex = ownerPubkeyHex

		rows, err := QueryContext(ctx, db, `
			SELECT value_satoshi, carrier_type
			  FROM fact_bsv_utxos
			 WHERE owner_pubkey_hex=? AND utxo_state='unspent'
			 ORDER BY created_at_unix ASC, utxo_id ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return walletBSVBalance{}, err
		}
		defer rows.Close()

		for rows.Next() {
			var value int64
			var carrierType string
			if err := rows.Scan(&value, &carrierType); err != nil {
				return walletBSVBalance{}, err
			}
			out.TotalSatoshi += uint64(value)
			if strings.ToLower(strings.TrimSpace(carrierType)) == "plain_bsv" {
				out.ConfirmedSatoshi += uint64(value)
				out.SpendableUTXOCount++
			}
		}
		if err := rows.Err(); err != nil {
			return walletBSVBalance{}, err
		}
		return out, nil
	})
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
	return clientDBValue(ctx, store, func(db sqlConn) ([]walletTokenBalance, error) {
		rows, err := QueryContext(ctx, db, `
			SELECT token_standard, token_id, quantity_text, used_quantity_text
			  FROM fact_token_lots
			 WHERE owner_pubkey_hex=? AND lot_state='unspent'
			 ORDER BY token_standard ASC, token_id ASC, created_at_unix ASC`,
			ownerPubkeyHex,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		aggs := make(map[string]*tokenAgg)
		for rows.Next() {
			var standard, tokenID, qtyText, usedText string
			if err := rows.Scan(&standard, &tokenID, &qtyText, &usedText); err != nil {
				return nil, err
			}
			key := standard + ":" + tokenID
			agg, ok := aggs[key]
			if !ok {
				agg = &tokenAgg{standard: standard, tokenID: tokenID, totalIn: "0", totalUsed: "0"}
				aggs[key] = agg
			}
			nextIn, err := sumDecimalTexts(agg.totalIn + "," + qtyText)
			if err != nil {
				return nil, err
			}
			nextUsed, err := sumDecimalTexts(agg.totalUsed + "," + usedText)
			if err != nil {
				return nil, err
			}
			agg.totalIn = nextIn
			agg.totalUsed = nextUsed
		}
		if err := rows.Err(); err != nil {
			return nil, err
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
				return nil, err
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
		return out, nil
	})
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
		row := QueryRowContext(ctx, store, `SELECT allocation_class FROM wallet_utxo WHERE utxo_id=?`, link.CarrierUTXOID)
		if row != nil {
			if err := row.Scan(&walletUtxoAllocationClass); err != nil {
				if err != sql.ErrNoRows {
					return nil, err
				}
			}
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
	return clientDBValue(ctx, store, func(db sqlConn) (tokenBalanceResult, error) {
		var result tokenBalanceResult
		result.WalletID = walletID
		result.AssetKind = assetKind
		result.TokenID = tokenID

		rows, err := QueryContext(ctx, db, `
			SELECT quantity_text, used_quantity_text
			  FROM fact_token_lots
			 WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=? AND lot_state='unspent'
			 ORDER BY created_at_unix ASC, lot_id ASC`,
			ownerPubkeyHex, assetKind, tokenID,
		)
		if err != nil {
			return tokenBalanceResult{}, err
		}
		defer rows.Close()

		totalIn := "0"
		totalUsed := "0"
		for rows.Next() {
			var qtyText, usedText string
			if err := rows.Scan(&qtyText, &usedText); err != nil {
				return tokenBalanceResult{}, err
			}
			nextIn, err := sumDecimalTexts(totalIn + "," + qtyText)
			if err != nil {
				return tokenBalanceResult{}, err
			}
			nextUsed, err := sumDecimalTexts(totalUsed + "," + usedText)
			if err != nil {
				return tokenBalanceResult{}, err
			}
			totalIn = nextIn
			totalUsed = nextUsed
		}
		if err := rows.Err(); err != nil {
			return tokenBalanceResult{}, err
		}
		result.TotalInText = totalIn
		result.TotalUsedText = totalUsed
		return result, nil
	})
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
	return clientDBValue(ctx, store, func(db sqlConn) (*bsvUTXOEntry, error) {
		row := QueryRowContext(ctx, db, `
			SELECT utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
			       utxo_state, carrier_type, spent_by_txid, created_at_unix, updated_at_unix,
			       spent_at_unix, note, COALESCE(payload_json,'{}')
			  FROM fact_bsv_utxos
			 WHERE utxo_id=?
			 LIMIT 1`,
			utxoID,
		)
		if row == nil {
			return nil, nil
		}
		var out bsvUTXOEntry
		var payloadJSON string
		if err := row.Scan(&out.UTXOID, &out.OwnerPubkeyHex, &out.Address, &out.TxID, &out.Vout, &out.ValueSatoshi,
			&out.UTXOState, &out.CarrierType, &out.SpentByTxid, &out.CreatedAtUnix, &out.UpdatedAtUnix,
			&out.SpentAtUnix, &out.Note, &payloadJSON); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if strings.TrimSpace(payloadJSON) != "" {
			out.Payload = json.RawMessage(payloadJSON)
		}
		return &out, nil
	})
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
	_, err := clientDBValue(ctx, store, func(db sqlConn) (struct{}, error) {
		now := time.Now().Unix()
		result, err := ExecContext(ctx, db, `
			UPDATE fact_bsv_utxos
			   SET utxo_state='spent',
			       spent_by_txid=?,
			       spent_at_unix=CASE WHEN spent_at_unix=0 THEN ? ELSE spent_at_unix END,
			       updated_at_unix=?
			 WHERE utxo_id=?`,
			spentByTxid, now, now, utxoID,
		)
		if err != nil {
			return struct{}{}, err
		}
		if result == nil {
			return struct{}{}, fmt.Errorf("utxo_id %s not found", utxoID)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return struct{}{}, err
		}
		if affected == 0 {
			return struct{}{}, fmt.Errorf("utxo_id %s not found", utxoID)
		}
		return struct{}{}, nil
	})
	return err
}

// dbCalcBSVBalance 兼容旧签名。
func dbCalcBSVBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string) (uint64, uint64, error) {
	if store == nil {
		return 0, 0, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return 0, 0, fmt.Errorf("client db ent client is nil")
	}
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (struct {
		confirmed uint64
		total     uint64
	}, error) {
		confirmed, total, err := dbCalcBSVBalanceEntTx(ctx, tx, ownerPubkeyHex)
		if err != nil {
			return struct {
				confirmed uint64
				total     uint64
			}{}, err
		}
		return struct {
			confirmed uint64
			total     uint64
		}{confirmed: confirmed, total: total}, nil
	})
	return out.confirmed, out.total, err
}

// dbCalcTokenBalance 兼容旧签名。
func dbCalcTokenBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return "", fmt.Errorf("client db ent client is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		return dbCalcTokenBalanceEntTx(ctx, tx, ownerPubkeyHex, tokenStandard, tokenID)
	})
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
	return clientDBValue(ctx, store, func(db sqlConn) (factBSV21CreateItem, error) {
		var item factBSV21CreateItem
		var payloadJSON string
		err := QueryRowContext(ctx, db, `
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
			return factBSV21CreateItem{}, err
		}
		item.PayloadJSON = payloadJSON
		return item, nil
	})
}

// dbUpsertChainPayment 按 txid 写入链支付事实。
func dbUpsertChainPayment(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		return dbUpsertChainPaymentEntTx(ctx, tx, e, false)
	})
}

// dbUpsertChainPaymentWithSettlementPaymentAttempt 按 txid 写入链支付事实，并补 settlement_payment_attempt。
func dbUpsertChainPaymentWithSettlementPaymentAttempt(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		return dbUpsertChainPaymentEntTx(ctx, tx, e, true)
	})
}

// dbUpsertChainDirectPayWithSettlementPaymentAttempt 写入 chain_direct_pay。
// 设计说明：旧测试还在用这个名字，内部直接走统一的 chain payment 事务入口。
func dbUpsertChainDirectPayWithSettlementPaymentAttempt(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		channelID, _, err := dbUpsertChainChannelWithSettlementPaymentAttempt(ctx, tx, e, "chain_direct_pay", "payment_attempt_chain_direct_pay", "bind chain direct pay channel id")
		return channelID, err
	})
}

// dbUpsertBusinessSettlement 写入业务结算出口。
func dbUpsertBusinessSettlement(ctx context.Context, store *clientDB, e businessSettlementEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return dbUpsertBusinessSettlementEntTx(ctx, tx, e)
	})
}

// dbGetBusinessSettlement 按 settlement_id 查询业务结算出口。
func dbGetBusinessSettlement(ctx context.Context, store *clientDB, settlementID string) (BusinessSettlementItem, error) {
	if store == nil {
		return BusinessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return BusinessSettlementItem{}, fmt.Errorf("settlement_id is required")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (BusinessSettlementItem, error) {
		var item BusinessSettlementItem
		var payload string
		err := QueryRowContext(ctx, db, `
			SELECT settlement_id,order_id,settlement_method,settlement_status,target_type,target_id,error_message,created_at_unix,updated_at_unix,COALESCE(settlement_payload_json,'{}')
			  FROM order_settlements
			 WHERE settlement_id=?
			 LIMIT 1`,
			settlementID,
		).Scan(
			&item.SettlementID, &item.OrderID, &item.SettlementMethod, &item.Status, &item.TargetType, &item.TargetID,
			&item.ErrorMessage, &item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return BusinessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListBusinessSettlementsByTarget 按 target_type + target_id 查询业务结算出口列表。
func dbListBusinessSettlementsByTarget(ctx context.Context, store *clientDB, targetType string, targetID string, limit int, offset int) (businessSettlementPage, error) {
	return dbListBusinessSettlements(ctx, store, businessSettlementFilter{
		Limit:      limit,
		Offset:     offset,
		TargetType: strings.TrimSpace(targetType),
		TargetID:   strings.TrimSpace(targetID),
	})
}

// dbListBusinessSettlements 查询业务结算出口列表。
func dbListBusinessSettlements(ctx context.Context, store *clientDB, filter businessSettlementFilter) (businessSettlementPage, error) {
	if store == nil {
		return businessSettlementPage{}, fmt.Errorf("client db is nil")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	type rowItem struct {
		item    BusinessSettlementItem
		payload string
	}
	return clientDBValue(ctx, store, func(db sqlConn) (businessSettlementPage, error) {
		where := make([]string, 0, 6)
		args := make([]any, 0, 6)
		if filter.SettlementID != "" {
			where = append(where, "settlement_id=?")
			args = append(args, strings.TrimSpace(filter.SettlementID))
		}
		if filter.OrderID != "" {
			where = append(where, "order_id=?")
			args = append(args, strings.TrimSpace(filter.OrderID))
		}
		if filter.SettlementMethod != "" {
			where = append(where, "settlement_method=?")
			args = append(args, strings.TrimSpace(filter.SettlementMethod))
		}
		if filter.Status != "" {
			where = append(where, "settlement_status=?")
			args = append(args, strings.TrimSpace(filter.Status))
		}
		if filter.TargetType != "" {
			where = append(where, "target_type=?")
			args = append(args, strings.TrimSpace(filter.TargetType))
		}
		if filter.TargetID != "" {
			where = append(where, "target_id=?")
			args = append(args, strings.TrimSpace(filter.TargetID))
		}

		countSQL := "SELECT COUNT(1) FROM order_settlements"
		listSQL := `
			SELECT settlement_id,order_id,settlement_method,settlement_status,target_type,target_id,error_message,created_at_unix,updated_at_unix,COALESCE(settlement_payload_json,'{}')
			  FROM order_settlements`
		if len(where) > 0 {
			clause := " WHERE " + strings.Join(where, " AND ")
			countSQL += clause
			listSQL += clause
		}
		countSQL += ""
		listSQL += " ORDER BY settlement_no DESC, updated_at_unix DESC, settlement_id DESC LIMIT ? OFFSET ?"

		var page businessSettlementPage
		if err := QueryRowContext(ctx, db, countSQL, args...).Scan(&page.Total); err != nil {
			return businessSettlementPage{}, err
		}
		rows, err := QueryContext(ctx, db, listSQL, append(args, filter.Limit, filter.Offset)...)
		if err != nil {
			return businessSettlementPage{}, err
		}
		defer rows.Close()

		items := make([]BusinessSettlementItem, 0, filter.Limit)
		for rows.Next() {
			var item BusinessSettlementItem
			var payload string
			if err := rows.Scan(&item.SettlementID, &item.OrderID, &item.SettlementMethod, &item.Status, &item.TargetType, &item.TargetID,
				&item.ErrorMessage, &item.CreatedAtUnix, &item.UpdatedAtUnix, &payload); err != nil {
				return businessSettlementPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return businessSettlementPage{}, err
		}
		page.Items = items
		return page, nil
	})
}

// dbUpdateBusinessSettlementOutcome 同步回写业务状态和结算出口状态。
func dbUpdateBusinessSettlementOutcome(ctx context.Context, store *clientDB, e businessSettlementOutcomeEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return dbUpdateBusinessSettlementOutcomeEntTx(ctx, tx, e)
	})
}

// dbAppendBusinessTrigger 新版业务触发桥接写入口。
func dbAppendBusinessTrigger(ctx context.Context, store *clientDB, e businessTriggerEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return dbAppendBusinessTriggerTx(ctx, tx, e)
	})
}

// dbGetBusinessTrigger 按 trigger_id 查询旧桥接记录。
func dbGetBusinessTrigger(ctx context.Context, store *clientDB, triggerID string) (businessTriggerItem, error) {
	if store == nil {
		return businessTriggerItem{}, fmt.Errorf("client db is nil")
	}
	triggerID = strings.TrimSpace(triggerID)
	if triggerID == "" {
		return businessTriggerItem{}, fmt.Errorf("trigger_id is required")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (businessTriggerItem, error) {
		var item businessTriggerItem
		var payload string
		err := QueryRowContext(ctx, db, `
			SELECT e.process_id, COALESCE(s.order_id,''), e.source_type, e.source_id, e.accounting_subtype, e.occurred_at_unix, e.note, COALESCE(e.payload_json,'{}')
			  FROM order_settlement_events e
			  LEFT JOIN order_settlements s ON s.settlement_id=e.settlement_id
			 WHERE e.event_type='bridge_trigger' AND e.process_id=?
			 ORDER BY e.occurred_at_unix DESC, e.id DESC
			 LIMIT 1`,
			triggerID,
		).Scan(&item.TriggerID, &item.OrderID, &item.TriggerType, &item.TriggerIDValue, &item.TriggerRole, &item.CreatedAtUnix, &item.Note, &payload)
		if err != nil {
			return businessTriggerItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListBusinessTriggersByOrderID 按 order_id 查询旧桥接记录列表。
func dbListBusinessTriggersByOrderID(ctx context.Context, store *clientDB, orderID string, limit int, offset int) (businessTriggerPage, error) {
	return dbListBusinessTriggers(ctx, store, businessTriggerFilter{
		Limit:   limit,
		Offset:  offset,
		OrderID: strings.TrimSpace(orderID),
	})
}

// dbListBusinessTriggers 查询旧桥接记录列表。
func dbListBusinessTriggers(ctx context.Context, store *clientDB, filter businessTriggerFilter) (businessTriggerPage, error) {
	if store == nil {
		return businessTriggerPage{}, fmt.Errorf("client db is nil")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	return clientDBValue(ctx, store, func(db sqlConn) (businessTriggerPage, error) {
		where := make([]string, 0, 5)
		args := make([]any, 0, 5)
		if filter.TriggerID != "" {
			where = append(where, "e.process_id=?")
			args = append(args, strings.TrimSpace(filter.TriggerID))
		}
		if filter.OrderID != "" {
			where = append(where, "(s.order_id=? OR e.settlement_id=?)")
			args = append(args, strings.TrimSpace(filter.OrderID), strings.TrimSpace(filter.OrderID))
		}
		if filter.TriggerType != "" {
			where = append(where, "e.source_type=?")
			args = append(args, strings.TrimSpace(filter.TriggerType))
		}
		if filter.TriggerIDValue != "" {
			where = append(where, "e.source_id=?")
			args = append(args, strings.TrimSpace(filter.TriggerIDValue))
		}
		if filter.TriggerRole != "" {
			where = append(where, "e.accounting_subtype=?")
			args = append(args, strings.TrimSpace(filter.TriggerRole))
		}

		countSQL := `
			SELECT COUNT(1)
			  FROM order_settlement_events e
			  LEFT JOIN order_settlements s ON s.settlement_id=e.settlement_id
			 WHERE e.event_type='bridge_trigger'`
		listSQL := `
			SELECT e.process_id, COALESCE(s.order_id,''), e.source_type, e.source_id, e.accounting_subtype, e.occurred_at_unix, e.note, COALESCE(e.payload_json,'{}')
			  FROM order_settlement_events e
			  LEFT JOIN order_settlements s ON s.settlement_id=e.settlement_id
			 WHERE e.event_type='bridge_trigger'`
		if len(where) > 0 {
			clause := " AND " + strings.Join(where, " AND ")
			countSQL += clause
			listSQL += clause
		}
		listSQL += " ORDER BY e.occurred_at_unix DESC, e.id DESC LIMIT ? OFFSET ?"

		var page businessTriggerPage
		if err := QueryRowContext(ctx, db, countSQL, args...).Scan(&page.Total); err != nil {
			return businessTriggerPage{}, err
		}
		rows, err := QueryContext(ctx, db, listSQL, append(args, filter.Limit, filter.Offset)...)
		if err != nil {
			return businessTriggerPage{}, err
		}
		defer rows.Close()

		items := make([]businessTriggerItem, 0, filter.Limit)
		for rows.Next() {
			var item businessTriggerItem
			var payload string
			if err := rows.Scan(&item.TriggerID, &item.OrderID, &item.TriggerType, &item.TriggerIDValue, &item.TriggerRole, &item.CreatedAtUnix, &item.Note, &payload); err != nil {
				return businessTriggerPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return businessTriggerPage{}, err
		}
		page.Items = items
		return page, nil
	})
}

// dbAppendFinBusiness 是共享财务业务写入口。
func dbAppendFinBusiness(ctx context.Context, store *clientDB, e finBusinessEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
	return clientDBValue(ctx, store, func(db sqlConn) (frontOrderPage, error) {
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
		if err := QueryRowContext(ctx, db, countSQL, args...).Scan(&page.Total); err != nil {
			return frontOrderPage{}, err
		}
		rows, err := QueryContext(ctx, db, listSQL, append(args, filter.Limit, filter.Offset)...)
		if err != nil {
			return frontOrderPage{}, err
		}
		defer rows.Close()

		items := make([]frontOrderItem, 0, filter.Limit)
		for rows.Next() {
			var item frontOrderItem
			var note string
			var payload string
			if err := rows.Scan(&item.FrontOrderID, &item.FrontType, &item.FrontSubtype, &item.OwnerPubkeyHex, &item.TargetObjectType, &item.TargetObjectID,
				&item.Status, &item.CreatedAtUnix, &item.UpdatedAtUnix, &note, &payload); err != nil {
				return frontOrderPage{}, err
			}
			item.Note = note
			item.Payload = json.RawMessage(payload)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return frontOrderPage{}, err
		}
		page.Items = items
		return page, nil
	})
}

// dbUpsertSettlementPaymentAttemptStore 写入结算支付尝试。
func dbUpsertSettlementPaymentAttemptStore(ctx context.Context, store *clientDB, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		return dbUpsertSettlementPaymentAttemptEntTx(ctx, tx, paymentAttemptID, sourceType, sourceID, state, grossSatoshi, gateFeeSatoshi, netSatoshi, paymentAttemptIndex, occurredAtUnix, note, payload)
	})
}

// dbGetSettlementPaymentAttemptSourceTxIDStore 兼容 store 入口。
func dbGetSettlementPaymentAttemptSourceTxIDStore(ctx context.Context, store *clientDB, settlementPaymentAttemptID int64) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (string, error) {
		return dbGetSettlementPaymentAttemptSourceTxIDCtx(ctx, db, settlementPaymentAttemptID)
	})
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

// dbAppendTokenConsumptionsForSettlementPaymentAttempt 写入 Token 消耗记录。
// 设计说明：
// - 先落结算记录，再按 lot 重算 used_quantity；
// - 这样重复回放同一批记录时不会重复累加。
func dbAppendTokenConsumptionsForSettlementPaymentAttemptCtx(ctx context.Context, db sqlConn, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	spentByTxid, err := dbGetSettlementPaymentAttemptSourceTxIDCtx(ctx, db, settlementPaymentAttemptID)
	if err != nil {
		return fmt.Errorf("resolve settlement payment attempt txid failed: %w", err)
	}
	if occurredAtUnix <= 0 {
		occurredAtUnix = time.Now().Unix()
	}

	updates := make(map[string]struct{})

	for _, fact := range utxoFacts {
		if strings.TrimSpace(fact.IOSide) != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		quantityText := strings.TrimSpace(fact.QuantityText)
		if quantityText == "" {
			return fmt.Errorf("quantity_text is required for token input utxo %s", utxoID)
		}

		var lotID string
		var ownerPubkeyHex string
		if err := QueryRowContext(ctx, db, `
			SELECT lot_id, owner_pubkey_hex
			  FROM fact_token_carrier_links
			 WHERE carrier_utxo_id=? AND link_state='active'
			 LIMIT 1`,
			utxoID,
		).Scan(&lotID, &ownerPubkeyHex); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("token carrier link not found for utxo %s", utxoID)
			}
			return fmt.Errorf("lookup token carrier link for utxo %s failed: %w", utxoID, err)
		}
		lotID = strings.TrimSpace(lotID)
		ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
		if lotID == "" || ownerPubkeyHex == "" {
			return fmt.Errorf("token carrier link for utxo %s is invalid", utxoID)
		}

		recordID := fmt.Sprintf("rec_token_%d_%s", settlementPaymentAttemptID, lotID)
		_, err = ExecContext(ctx, db, `
			INSERT INTO fact_settlement_records(
				record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex,
				source_utxo_id, source_lot_id, used_satoshi, used_quantity_text,
				state, occurred_at_unix, confirmed_at_unix, note, payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id) DO UPDATE SET
				owner_pubkey_hex=excluded.owner_pubkey_hex,
				used_quantity_text=excluded.used_quantity_text,
				state=excluded.state,
				occurred_at_unix=excluded.occurred_at_unix,
				confirmed_at_unix=excluded.confirmed_at_unix,
				note=excluded.note,
				payload_json=excluded.payload_json`,
			recordID, settlementPaymentAttemptID, "TOKEN", ownerPubkeyHex, utxoID, lotID, int64(0), quantityText,
			"confirmed", occurredAtUnix, occurredAtUnix, "Token consumed by settlement payment attempt", mustJSONString(fact.Payload),
		)
		if err != nil {
			return fmt.Errorf("append token settlement record for lot %s failed: %w", lotID, err)
		}
		updates[lotID] = struct{}{}
	}

	for lotID := range updates {
		var quantityText string
		var ownerPubkeyHex string
		var tokenID string
		var tokenStandard string
		var mintTxid string
		var lastSpendTxid string
		var note string
		var payloadJSON string
		var createdAtUnix int64
		var updatedAtUnix int64
		if err := QueryRowContext(ctx, db, `
			SELECT quantity_text, owner_pubkey_hex, token_id, token_standard, mint_txid, last_spend_txid, note, COALESCE(payload_json,'{}'), created_at_unix, updated_at_unix
			  FROM fact_token_lots
			 WHERE lot_id=?
			 LIMIT 1`,
			lotID,
		).Scan(&quantityText, &ownerPubkeyHex, &tokenID, &tokenStandard, &mintTxid, &lastSpendTxid, &note, &payloadJSON, &createdAtUnix, &updatedAtUnix); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("token lot not found: %s", lotID)
			}
			return fmt.Errorf("lookup token lot %s failed: %w", lotID, err)
		}

		rows, err := QueryContext(ctx, db, `
			SELECT used_quantity_text
			  FROM fact_settlement_records
			 WHERE asset_type='TOKEN' AND source_lot_id=? AND state='confirmed'
			 ORDER BY id ASC`,
			lotID,
		)
		if err != nil {
			return err
		}
		totalUsed := "0"
		for rows.Next() {
			var usedText string
			if err := rows.Scan(&usedText); err != nil {
				_ = rows.Close()
				return err
			}
			nextUsed, err := sumDecimalTexts(totalUsed + "," + usedText)
			if err != nil {
				_ = rows.Close()
				return err
			}
			totalUsed = nextUsed
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()

		lotState := "unspent"
		if compareDecimalText(totalUsed, quantityText) >= 0 {
			lotState = "spent"
		}
		now := time.Now().Unix()
		_, err = ExecContext(ctx, db, `
			UPDATE fact_token_lots
			   SET owner_pubkey_hex=?,
			       token_id=?,
			       token_standard=?,
			       quantity_text=?,
			       used_quantity_text=?,
			       lot_state=?,
			       mint_txid=?,
			       last_spend_txid=?,
			       updated_at_unix=?,
			       note=?,
			       payload_json=?
			 WHERE lot_id=?`,
			ownerPubkeyHex, tokenID, tokenStandard, quantityText, totalUsed, lotState, mintTxid, spentByTxid, now, note, payloadJSON, lotID,
		)
		if err != nil {
			return fmt.Errorf("update token lot %s failed: %w", lotID, err)
		}
	}
	return nil
}
