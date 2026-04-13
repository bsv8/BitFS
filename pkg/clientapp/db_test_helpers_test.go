package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// newWalletAccountingTestDB 给旧测试提供统一的临时数据库。
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
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

// initIndexDB 保留给现有测试调用，底层已经切到 contract 的 ent schema。
func initIndexDB(db *sql.DB) error {
	return initIndexDBCtx(context.Background(), db)
}

// ensureClientDBBaseSchema 保留给部分 schema 测试调用，实际执行同一套 ent 建表。
func ensureClientDBBaseSchema(db *sql.DB) error {
	return ensureClientDBBaseSchemaCtx(context.Background(), db)
}

// dbUpsertSettlementPaymentAttempt 是测试用的薄包装。
func dbUpsertSettlementPaymentAttempt(db *sql.DB, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) error {
	return dbUpsertSettlementPaymentAttemptCtx(context.Background(), db, paymentAttemptID, sourceType, sourceID, state,
		grossSatoshi, gateFeeSatoshi, netSatoshi, paymentAttemptIndex, occurredAtUnix, note, payload)
}

// dbGetSettlementPaymentAttemptBySource 是测试用的薄包装。
func dbGetSettlementPaymentAttemptBySource(db *sql.DB, sourceType string, sourceID string) (int64, error) {
	return dbGetSettlementPaymentAttemptBySourceCtx(context.Background(), db, sourceType, sourceID)
}

// dbGetSettlementPaymentAttemptSourceTxID 是测试用的薄包装。
func dbGetSettlementPaymentAttemptSourceTxID(db *sql.DB, settlementPaymentAttemptID int64) (string, error) {
	return dbGetSettlementPaymentAttemptSourceTxIDCtx(context.Background(), db, settlementPaymentAttemptID)
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
	return tableHasForeignKeyCtx(context.Background(), db, table, fromColumn, parentTable, parentColumn)
}

// tableHasCreateSQLContains 是测试用的薄包装。
func tableHasCreateSQLContains(db *sql.DB, table, snippet string) (bool, error) {
	return tableHasCreateSQLContainsCtx(context.Background(), db, table, snippet)
}

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
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, baseTxHex, 300); err != nil {
		t.Fatalf("seed direct transfer pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed direct transfer close facts failed: %v", err)
	}
}

// mustSettlementPaymentAttemptIDBySource 让旧测试更容易写断言。
func mustSettlementPaymentAttemptIDBySource(t *testing.T, db *sql.DB, sourceType, sourceID string) int64 {
	t.Helper()
	id, err := dbGetSettlementPaymentAttemptBySource(db, sourceType, sourceID)
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	return id
}
