package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// newWalletAccountingTestDB 给测试提供统一的临时数据库。
// 设计说明：这里只是测试支架，不承载业务口径。
func newWalletAccountingTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}

// dbUpsertSettlementPaymentAttempt 是测试用的薄包装。
func dbUpsertSettlementPaymentAttempt(db *sql.DB, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) error {
	_, err := dbUpsertSettlementPaymentAttemptStore(context.Background(), newClientDB(db, nil), paymentAttemptID, sourceType, sourceID, state,
		grossSatoshi, gateFeeSatoshi, netSatoshi, paymentAttemptIndex, occurredAtUnix, note, payload)
	return err
}

// dbGetSettlementPaymentAttemptBySource 是测试用的薄包装。
func dbGetSettlementPaymentAttemptBySource(db *sql.DB, sourceType string, sourceID string) (int64, error) {
	return dbGetSettlementPaymentAttemptBySourceStore(context.Background(), newClientDB(db, nil), sourceType, sourceID)
}

// dbGetSettlementPaymentAttemptSourceTxID 是测试用的薄包装。
func dbGetSettlementPaymentAttemptSourceTxID(db *sql.DB, settlementPaymentAttemptID int64) (string, error) {
	return dbGetSettlementPaymentAttemptSourceTxIDStore(context.Background(), newClientDB(db, nil), settlementPaymentAttemptID)
}

// hasTable 是测试用的薄包装。
func hasTable(db *sql.DB, table string) (bool, error) {
	if db == nil {
		return false, nil
	}
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(name) != "", nil
}

// tableColumns 返回表字段名集合。
func tableColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// tableColumnNotNull 检查字段是否为 NOT NULL。
func tableColumnNotNull(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return notnull != 0, nil
		}
	}
	return false, rows.Err()
}

// tableHasUniqueIndexOnColumns 检查表是否存在匹配字段顺序的唯一索引。
func tableHasUniqueIndexOnColumns(db *sql.DB, table string, columns []string) (bool, error) {
	rows, err := db.Query(`PRAGMA index_list(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	want := strings.Join(columns, ",")
	for rows.Next() {
		var seq int
		var name string
		var unique, origin, partial any
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return false, err
		}
		isUnique := false
		switch v := unique.(type) {
		case int64:
			isUnique = v != 0
		case int:
			isUnique = v != 0
		}
		if !isUnique {
			continue
		}
		colsRows, err := db.Query(`PRAGMA index_info(` + name + `)`)
		if err != nil {
			return false, err
		}
		got := make([]string, 0, len(columns))
		for colsRows.Next() {
			var seqno, cid int
			var colName string
			if err := colsRows.Scan(&seqno, &cid, &colName); err != nil {
				_ = colsRows.Close()
				return false, err
			}
			got = append(got, colName)
		}
		_ = colsRows.Close()
		if err := colsRows.Err(); err != nil {
			return false, err
		}
		if strings.Join(got, ",") == want {
			return true, nil
		}
	}
	return false, rows.Err()
}

// dbAppendBSVConsumptionsForSettlementPaymentAttempt 是测试用的薄包装。
func dbAppendBSVConsumptionsForSettlementPaymentAttempt(db *sql.DB, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	return dbAppendBSVConsumptionsForSettlementPaymentAttemptCtx(context.Background(), db, settlementPaymentAttemptID, utxoFacts, occurredAtUnix)
}

// dbAppendTokenConsumptionsForSettlementPaymentAttempt 是测试用的薄包装。
func dbAppendTokenConsumptionsForSettlementPaymentAttempt(db *sql.DB, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	return dbAppendTokenConsumptionsForSettlementPaymentAttemptCtx(context.Background(), db, settlementPaymentAttemptID, utxoFacts, occurredAtUnix)
}

// tableHasForeignKey 是测试用的薄包装。
func tableHasForeignKey(db *sql.DB, table, fromColumn, parentTable, parentColumn string) (bool, error) {
	if db == nil {
		return false, nil
	}
	rows, err := db.Query(`PRAGMA foreign_key_list(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, seq, onUpdate, onDelete, match any
		var tbl, from, to string
		if err := rows.Scan(&id, &seq, &tbl, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return false, err
		}
		if from == fromColumn && tbl == parentTable && to == parentColumn {
			return true, nil
		}
	}
	return false, rows.Err()
}

// tableHasCreateSQLContains 是测试用的薄包装。
func tableHasCreateSQLContains(db *sql.DB, table, snippet string) (bool, error) {
	if db == nil {
		return false, nil
	}
	var sqlText string
	err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&sqlText)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return strings.Contains(sqlText, snippet), nil
}

// tableHasIndex 检查表是否存在指定索引。
func tableHasIndex(db *sql.DB, table, indexName string) (bool, error) {
	if db == nil {
		return false, nil
	}
	rows, err := db.Query(`PRAGMA index_list(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name string
		var unique, origin, partial any
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return false, err
		}
		if name == indexName {
			return true, nil
		}
	}
	return false, rows.Err()
}

// 为避免测试 helper 因未使用排序而被静态检查误判，保留一个轻量引用。
var _ = sort.Strings

// seedWalletUTXO 给钱包一致性测试种一条 wallet_utxo。
// 设计说明：这里只补测试需要的最小列，不触碰事实表。
func seedWalletUTXO(t *testing.T, db *sql.DB, utxoID, txid string, vout uint32, valueSatoshi int64) {
	t.Helper()
	if db == nil {
		t.Fatal("db is nil")
	}
	const seedAddress = "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(seedAddress)
	now := time.Now().Unix()
	_, err := db.Exec(
		`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(address,txid,vout) DO UPDATE SET
			utxo_id=excluded.utxo_id,
			wallet_id=excluded.wallet_id,
			value_satoshi=excluded.value_satoshi,
			state=excluded.state,
			allocation_class=excluded.allocation_class,
			allocation_reason=excluded.allocation_reason,
			created_txid=excluded.created_txid,
			spent_txid=excluded.spent_txid,
			updated_at_unix=excluded.updated_at_unix,
			spent_at_unix=excluded.spent_at_unix`,
		utxoID,
		walletID,
		seedAddress,
		txid,
		int64(vout),
		valueSatoshi,
		"unspent",
		walletUTXOAllocationPlainBSV,
		"",
		txid,
		"",
		now,
		now,
		0,
	)
	if err != nil {
		t.Fatalf("seed wallet utxo failed: %v", err)
	}
}

// seedDirectTransferPoolFacts 给直连池读模型测试种最小事实集。
// 设计说明：固定 seed 一组可复用的 open/pay/close 运行态事实，方便读模型直接挂 settlement_payment_attempt。
func seedDirectTransferPoolFacts(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	dealID := "deal_third_iter_1"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionId:        sessionID,
		DealId:           dealID,
		BuyerPubkeyHex:   buyerPubHex,
		ArbiterPubkeyHex: arbiterPubHex,
		ArbiterPubkey:    arbiterPubHex,
		PoolAmount:       990,
		SpendTxFee:       10,
		Sequence:         1,
		SellerAmount:     0,
		BuyerAmount:      990,
		BaseTxid:         "base_tx_third_iter_1",
		FeeRateSatByte:   0.5,
		LockBlocks:       6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("seed direct transfer open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, baseTxHex, 300, "paid"); err != nil {
		t.Fatalf("seed direct transfer pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed direct transfer close facts failed: %v", err)
	}
}

// mustSettlementPaymentAttemptIDBySource 让测试更容易写断言。
func mustSettlementPaymentAttemptIDBySource(t *testing.T, db *sql.DB, sourceType, sourceID string) int64 {
	t.Helper()
	id, err := dbGetSettlementPaymentAttemptBySource(db, sourceType, sourceID)
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	return id
}
