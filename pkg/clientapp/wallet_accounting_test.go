package clientapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	te "github.com/bsv8/MultisigPool/pkg/triple_endpoint"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func recordWalletChainAccounting(db *sql.DB, in walletChainAccountingInput) error {
	return recordWalletChainAccountingCtx(context.Background(), db, in)
}

func recordWalletChainAccountingConn(db sqlConn, in walletChainAccountingInput) error {
	return recordWalletChainAccountingConnCtx(context.Background(), db, in)
}

func initIndexDB(db *sql.DB) error {
	return initIndexDBCtx(context.Background(), db)
}

func ensureClientDBBaseSchema(db *sql.DB) error {
	return ensureClientDBBaseSchemaCtx(context.Background(), db)
}

func dbUpsertSettlementCycle(db sqlConn, cycleID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64, cycleIndex int, occurredAtUnix int64, note string, payload any) error {
	return dbUpsertSettlementCycleCtx(context.Background(), db, cycleID, sourceType, sourceID, state, grossSatoshi, gateFeeSatoshi, netSatoshi, cycleIndex, occurredAtUnix, note, payload)
}

func dbGetSettlementCycleBySource(db sqlConn, sourceType string, sourceID string) (int64, error) {
	return dbGetSettlementCycleBySourceCtx(context.Background(), db, sourceType, sourceID)
}

func dbAppendBSVConsumptionsForSettlementCycle(db sqlConn, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	return dbAppendBSVConsumptionsForSettlementCycleCtx(context.Background(), db, settlementCycleID, utxoFacts, occurredAtUnix)
}

func dbAppendTokenConsumptionsForSettlementCycle(db sqlConn, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	return dbAppendTokenConsumptionsForSettlementCycleCtx(context.Background(), db, settlementCycleID, utxoFacts, occurredAtUnix)
}

func dbGetSettlementCycleSourceTxID(db sqlConn, settlementCycleID int64) (string, error) {
	return dbGetSettlementCycleSourceTxIDCtx(context.Background(), db, settlementCycleID)
}

func tableHasForeignKey(db *sql.DB, table, fromColumn, parentTable, parentColumn string) (bool, error) {
	return tableHasForeignKeyCtx(context.Background(), db, table, fromColumn, parentTable, parentColumn)
}

func tableHasCreateSQLContains(db *sql.DB, table, snippet string) (bool, error) {
	return tableHasCreateSQLContainsCtx(context.Background(), db, table, snippet)
}

func seedDirectTransferPoolFacts(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx := context.Background()
	store := newClientDB(db, nil)
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_third_iter_1"
	dealID := "deal_third_iter_1"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_third_iter_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, strings.ToLower(baseTxHex), baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, strings.ToLower(baseTxHex), 300); err != nil {
		t.Fatalf("seed pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, strings.ToLower(baseTxHex)); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}
}

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

func seedWalletUTXO(t *testing.T, db *sql.DB, utxoID string, txid string, vout int, value int64) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, "wallet1", "addr1", txid, vout, value, "confirmed", "plain_bsv", "",
		txid, "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("insert wallet_utxo %s failed: %v", utxoID, err)
	}
}

func walletChainBusinessID(sourceType, txid string) string {
	return "biz_wallet_chain_" + sourceType + "_" + txid
}

func mustSettlementCycleIDBySource(t *testing.T, db *sql.DB, sourceType, sourceID string) int64 {
	t.Helper()
	id, err := dbGetSettlementCycleBySource(db, sourceType, sourceID)
	if err != nil {
		t.Fatalf("lookup settlement cycle failed: %v", err)
	}
	return id
}

// TestPoolAccountingBoundary_ActionKinds 验证业务动作口径和 fact 口径的分界。
func TestPoolAccountingBoundary_ActionKinds(t *testing.T) {
	t.Parallel()

	if !IsPoolFactOpenCloseAction(PoolBusinessActionOpen) {
		t.Fatalf("open should be part of current fact path")
	}
	if !IsPoolFactOpenCloseAction(PoolBusinessActionClose) {
		t.Fatalf("close should be part of current fact path")
	}
	if IsPoolFactOpenCloseAction(PoolBusinessActionCycleFee) {
		t.Fatalf("cycle_fee should stay outside current fact path")
	}
	if NormalizePoolBusinessAction(PoolBusinessActionPayLegacy) != PoolBusinessActionServicePay {
		t.Fatalf("pay should normalize to service_pay")
	}
}

// TestDirectTransferPoolBizSnapshot_RuntimeWrite 验证 open/pay/close 会同步写新业务池表。
func TestDirectTransferPoolBizSnapshot_RuntimeWrite(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_biz_pool_runtime"
	dealID := "deal_biz_pool_runtime"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	openReq := directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_biz_pool_runtime",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, openReq, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("open write failed: %v", err)
	}
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, openReq, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("replay open write failed: %v", err)
	}

	var openCount int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_pool_allocations WHERE pool_session_id=?`, sessionID).Scan(&openCount); err != nil {
		t.Fatalf("query biz_pool_allocations after open replay failed: %v", err)
	}
	if openCount != 1 {
		t.Fatalf("open replay should not duplicate biz_pool_allocations, got %d", openCount)
	}

	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, baseTxHex, 300); err != nil {
		t.Fatalf("pay write failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, baseTxHex); err != nil {
		t.Fatalf("close write failed: %v", err)
	}

	var poolAmountSat, spendFeeSat, allocatedSat, cycleFeeSat, availableSat, nextSeq int64
	var status, openBaseTxID, openAllocationID, closeAllocationID string
	if err := db.QueryRow(`
		SELECT pool_amount_satoshi,spend_tx_fee_satoshi,allocated_satoshi,cycle_fee_satoshi,available_satoshi,next_sequence_num,
		       status,open_base_txid,open_allocation_id,close_allocation_id
		FROM biz_pool WHERE pool_session_id=?`,
		sessionID,
	).Scan(&poolAmountSat, &spendFeeSat, &allocatedSat, &cycleFeeSat, &availableSat, &nextSeq, &status, &openBaseTxID, &openAllocationID, &closeAllocationID); err != nil {
		t.Fatalf("query biz_pool failed: %v", err)
	}
	if poolAmountSat != 1000 || spendFeeSat != 10 {
		t.Fatalf("unexpected biz_pool gross amount: pool=%d fee=%d", poolAmountSat, spendFeeSat)
	}
	if allocatedSat != 700 || cycleFeeSat != 10 || availableSat != 290 || nextSeq != 4 {
		t.Fatalf("unexpected biz_pool snapshot: allocated=%d cycle_fee=%d available=%d next_seq=%d", allocatedSat, cycleFeeSat, availableSat, nextSeq)
	}
	if status != "closed" || openBaseTxID != "base_tx_biz_pool_runtime" {
		t.Fatalf("unexpected biz_pool status/base tx: status=%s open_base_txid=%s", status, openBaseTxID)
	}
	if openAllocationID != directTransferPoolAllocationID(sessionID, PoolBusinessActionOpen, 1) {
		t.Fatalf("unexpected open_allocation_id: %s", openAllocationID)
	}
	if closeAllocationID != directTransferPoolAllocationID(sessionID, PoolBusinessActionClose, 3) {
		t.Fatalf("unexpected close_allocation_id: %s", closeAllocationID)
	}

	var allocCount int64
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_pool_allocations WHERE pool_session_id=?`, sessionID).Scan(&allocCount); err != nil {
		t.Fatalf("query biz_pool_allocations failed: %v", err)
	}
	if allocCount != 3 {
		t.Fatalf("unexpected biz_pool_allocations count: %d", allocCount)
	}

	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 4, 100, 190, baseTxHex, 100); err == nil {
		t.Fatal("expected pay after close to fail")
	}
}

func TestDirectTransferPoolClose_ReplayAfterClosed(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_close_replay"
	dealID := "deal_close_replay"
	buyerPriv, err := ec.PrivateKeyFromHex("2796e78fad7d383fa5236607eba52d9a1904325daf9b4da3d77be5ad15ab1dae")
	if err != nil {
		t.Fatalf("load buyer key failed: %v", err)
	}
	sellerPriv, err := ec.PrivateKeyFromHex("e6d4d7685894d2644d1f4bf31c0b87f3f6aa8a3d7d4091eaa375e81d6c9f9091")
	if err != nil {
		t.Fatalf("load seller key failed: %v", err)
	}
	arbiterPriv, err := ec.PrivateKeyFromHex("a682814ac246ca65543197e593aa3b2633b891959c183416f54e2c63a8de1d8c")
	if err != nil {
		t.Fatalf("load arbiter key failed: %v", err)
	}
	buyerPubHex := strings.ToLower(hex.EncodeToString(buyerPriv.PubKey().Compressed()))
	sellerPubHex := strings.ToLower(hex.EncodeToString(sellerPriv.PubKey().Compressed()))
	arbiterPubHex := strings.ToLower(hex.EncodeToString(arbiterPriv.PubKey().Compressed()))

	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_close_replay",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, baseTxHex, 300); err != nil {
		t.Fatalf("seed pay facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}

	rawTx, err := tx.NewTransactionFromHex(baseTxHex)
	if err != nil {
		t.Fatalf("parse close tx failed: %v", err)
	}
	seq := uint32(3)
	if len(rawTx.Inputs) > 0 && rawTx.Inputs[0].SequenceNumber != 0 {
		seq = rawTx.Inputs[0].SequenceNumber
	}
	locktime := rawTx.LockTime
	parsedTx, err := te.TripleFeePoolLoadTx(baseTxHex, &locktime, seq, 700, arbiterPriv.PubKey(), buyerPriv.PubKey(), sellerPriv.PubKey(), 990)
	if err != nil {
		t.Fatalf("build close tx failed: %v", err)
	}
	buyerSig, err := te.ClientATripleFeePoolSpendTXUpdateSign(parsedTx, arbiterPriv.PubKey(), buyerPriv, sellerPriv.PubKey())
	if err != nil {
		t.Fatalf("sign buyer close tx failed: %v", err)
	}

	cfg := Config{}
	cfg.Keys.PrivkeyHex = sellerPriv.Hex()
	req := directTransferPoolCloseReq{
		SessionID:    sessionID,
		Sequence:     3,
		SellerAmount: 700,
		BuyerAmount:  290,
		CurrentTx:    mustDecodeHex(baseTxHex),
		BuyerSig:     append([]byte(nil), (*buyerSig)...),
	}

	resp1, err := handleDirectTransferPoolClose(nil, store, cfg, req)
	if err != nil {
		t.Fatalf("first close replay failed: %v", err)
	}
	if resp1.Status != "closed" || len(resp1.SellerSig) == 0 {
		t.Fatalf("unexpected first close replay resp: %+v", resp1)
	}

	resp2, err := handleDirectTransferPoolClose(nil, store, cfg, req)
	if err != nil {
		t.Fatalf("second close replay failed: %v", err)
	}
	if resp2.Status != "closed" || len(resp2.SellerSig) == 0 {
		t.Fatalf("unexpected second close replay resp: %+v", resp2)
	}
	if !bytes.Equal(resp1.SellerSig, resp2.SellerSig) {
		t.Fatalf("close replay should return same seller sig")
	}
}
func TestRecordDirectPoolCloseAccounting_AppendsUTXOLinks(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 一个最小可解析交易：1 输入，2 输出（seller=700, buyer=290）。
	finalTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      "sess_1",
		DealID:         "deal_sess_1",
		BuyerPeerID:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArbiterPeerID:  "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ArbiterPubKey:  "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_sess_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, "sess_1", "deal_sess_1", "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", finalTxHex, finalTxHex); err != nil {
		t.Fatalf("seed close facts open failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, "sess_1", 3, 700, 290, finalTxHex); err != nil {
		t.Fatalf("seed close facts close failed: %v", err)
	}
	dbRecordDirectPoolCloseAccounting(ctx, store, "sess_1", 3, "", finalTxHex, 700, 290, "seller_peer_1")

	// 第二阶段整改验证：close 生成过程型 settle_businesses，但不是正式收费对象
	var businessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&businessCount); err != nil {
		t.Fatalf("query settle_businesses failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 标记为过程型
	var accountingScene, accountingSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&accountingScene, &accountingSubtype); err != nil {
		t.Fatalf("query settle_businesses fields failed: %v", err)
	}
	if accountingScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：close 的 accounting_scene 应为 direct_transfer_process，got %s", accountingScene)
	}
	if accountingSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 的 accounting_subtype 应为 pool_close_settle，got %s", accountingSubtype)
	}

	// 第二阶段：tx_breakdown 和 utxo_links 挂正式的 business_id
	var txid string
	if err := db.QueryRow(`SELECT txid FROM settle_tx_breakdown WHERE business_id=?`, "biz_c2c_close_sess_1").Scan(&txid); err != nil {
		t.Fatalf("query settle_tx_breakdown failed: %v", err)
	}
	if txid == "" {
		t.Fatalf("settle_tx_breakdown txid should not be empty")
	}

	type linkCheck struct {
		txRole   string
		ioSide   string
		utxoRole string
		amount   int64
		count    int
	}
	checks := []linkCheck{
		{txRole: "close_final", ioSide: "input", utxoRole: "pool_input", count: 1},
		{txRole: "close_final", ioSide: "output", utxoRole: "settle_to_seller", amount: 700, count: 1},
		{txRole: "close_final", ioSide: "output", utxoRole: "settle_to_buyer", amount: 290, count: 1},
	}
	for _, c := range checks {
		var gotCount int
		var gotAmount int64
		if err := db.QueryRow(
			`SELECT COUNT(1),COALESCE(SUM(l.amount_satoshi),0)
			   FROM settle_tx_utxo_links l
			   JOIN settle_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE l.business_id=? AND l.txid=? AND b.tx_role=? AND l.io_side=? AND l.utxo_role=?`,
			"biz_c2c_close_sess_1", txid, c.txRole, c.ioSide, c.utxoRole,
		).Scan(&gotCount, &gotAmount); err != nil {
			t.Fatalf("query settle_tx_utxo_links tx_role=%s io_side=%s utxo_role=%s failed: %v", c.txRole, c.ioSide, c.utxoRole, err)
		}
		if gotCount != c.count {
			t.Fatalf("tx_role=%s io_side=%s utxo_role=%s count mismatch: got=%d want=%d", c.txRole, c.ioSide, c.utxoRole, gotCount, c.count)
		}
		if c.amount > 0 && gotAmount != c.amount {
			t.Fatalf("tx_role=%s io_side=%s utxo_role=%s amount mismatch: got=%d want=%d", c.txRole, c.ioSide, c.utxoRole, gotAmount, c.amount)
		}
	}
}

func TestDirectTransferPoolRuntimeWritesPoolFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_pool_fact_1"
	dealID := "deal_pool_fact_1"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	currentTxHex := strings.ToLower(baseTxHex)

	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_pool_fact_1",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, currentTxHex, baseTxHex); err != nil {
		t.Fatalf("open runtime write failed: %v", err)
	}

	if err := dbUpdateDirectTransferPoolPay(ctx, store, sessionID, 2, 300, 690, currentTxHex, 300); err != nil {
		t.Fatalf("pay runtime write failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 3, 700, 290, currentTxHex); err != nil {
		t.Fatalf("close runtime write failed: %v", err)
	}

	var status, scheme, counterparty, openBaseTxID string
	var amountSat, feeSat int64
	if err := db.QueryRow(
		`SELECT status,pool_scheme,counterparty_pubkey_hex,open_base_txid,pool_amount_satoshi,spend_tx_fee_satoshi
		   FROM fact_pool_sessions WHERE pool_session_id=?`,
		sessionID,
	).Scan(&status, &scheme, &counterparty, &openBaseTxID, &amountSat, &feeSat); err != nil {
		t.Fatalf("query pool session failed: %v", err)
	}
	if status != "closed" || scheme != "2of3" || strings.ToLower(counterparty) != sellerPubHex || openBaseTxID != strings.ToLower("base_tx_pool_fact_1") {
		t.Fatalf("unexpected pool session row: status=%s scheme=%s counterparty=%s open_base_txid=%s", status, scheme, counterparty, openBaseTxID)
	}
	if amountSat != 990 || feeSat != 10 {
		t.Fatalf("pool session amount mismatch: amount=%d fee=%d", amountSat, feeSat)
	}

	type allocRow struct {
		Kind  string
		Seq   int64
		Payee int64
		Payer int64
		TxID  string
		TxHex string
	}
	rows, err := db.Query(
		`SELECT allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex
		   FROM fact_pool_session_events WHERE pool_session_id=? ORDER BY allocation_no ASC`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("query pool allocations failed: %v", err)
	}
	defer rows.Close()
	got := make([]allocRow, 0, 3)
	for rows.Next() {
		var row allocRow
		if err := rows.Scan(&row.Kind, &row.Seq, &row.Payee, &row.Payer, &row.TxID, &row.TxHex); err != nil {
			t.Fatalf("scan pool allocation failed: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pool allocations failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("pool allocation count mismatch: got=%d want=3", len(got))
	}
	wantKinds := []string{"open", "pay", "close"}
	for i, want := range wantKinds {
		if got[i].Kind != want {
			t.Fatalf("allocation kind mismatch at %d: got=%s want=%s", i, got[i].Kind, want)
		}
	}
	if got[0].Seq != 1 || got[0].Payee != 0 || got[0].Payer != 990 {
		t.Fatalf("open allocation mismatch: %+v", got[0])
	}
	if got[1].Seq != 2 || got[1].Payee != 300 || got[1].Payer != 690 {
		t.Fatalf("pay allocation mismatch: %+v", got[1])
	}
	if got[2].Seq != 3 || got[2].Payee != 700 || got[2].Payer != 290 {
		t.Fatalf("close allocation mismatch: %+v", got[2])
	}
}

// TestDirectTransferAccounting_SourceIDUsesAutoIncrementID 验证结算锚点已经收口到自增主键
// source_id 应该是 fact_pool_session_events.id（整数），而不是 allocation_id（业务键）
// open/close 为过程型财务对象，只验证 pay 的 source_id 格式
func TestDirectTransferAccounting_SourceIDUsesAutoIncrementID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 这里验证 open/close 生成过程型 settle_businesses，不是正式收费对象
	// 设计意图：open/close 保留为过程型财务对象，正式收费主线只认 biz_download_pool_*
	var openBusinessCount, closeBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&openBusinessCount); err != nil {
		t.Fatalf("query settle_businesses open failed: %v", err)
	}
	if openBusinessCount != 1 {
		t.Fatalf("第二阶段：open 应生成 1 条过程型 settle_businesses 记录，got %d", openBusinessCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&closeBusinessCount); err != nil {
		t.Fatalf("query settle_businesses close failed: %v", err)
	}
	if closeBusinessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", closeBusinessCount)
	}

	// 验证：open/close 的 accounting_scene 标记为过程型
	var openScene, openSubtype, closeScene, closeSubtype string
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_open_"+sessionID).Scan(&openScene, &openSubtype); err != nil {
		t.Fatalf("query open settle_businesses fields failed: %v", err)
	}
	if openScene != "direct_transfer_process" || openSubtype != "pool_open_lock" {
		t.Fatalf("第二阶段：open 应标记为过程型: scene=%s subtype=%s", openScene, openSubtype)
	}
	if err := db.QueryRow(`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`, "biz_c2c_close_"+sessionID).Scan(&closeScene, &closeSubtype); err != nil {
		t.Fatalf("query close settle_businesses fields failed: %v", err)
	}
	if closeScene != "direct_transfer_process" || closeSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 应标记为过程型: scene=%s subtype=%s", closeScene, closeSubtype)
	}

	// 获取 pay allocation 的自增 id
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	paySettlementCycleID, err := dbGetSettlementCycleBySource(db, "pool_session", sessionID)
	if err != nil {
		t.Fatalf("lookup pay settlement cycle id failed: %v", err)
	}

	// 验证：pay 不再生成 biz_c2c_pay_* 正式收费对象，只保留 fin_process_event + settle_tx_breakdown
	var payBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2").Scan(&payBusinessCount); err != nil {
		t.Fatalf("query settle_businesses pay failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBusinessCount)
	}

	// 验证：fin_process_event 存在，process_id = proc_c2c_transfer_<sessionID>
	var procCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE process_id=? AND accounting_subtype='chunk_pay'`, "proc_c2c_transfer_"+sessionID).Scan(&procCount); err != nil {
		t.Fatalf("query settle_process_events pay failed: %v", err)
	}
	if procCount == 0 {
		t.Fatal("pay 应生成 fin_process_event 过程记录")
	}

	// 验证：settle_process_events 的 source_id 已统一落到 settlement_cycle.id
	var procSourceType, procSourceIDStr string
	if err := db.QueryRow(
		`SELECT source_type, source_id FROM settle_process_events WHERE process_id=? AND accounting_subtype='chunk_pay' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&procSourceType, &procSourceIDStr); err != nil {
		t.Fatalf("query settle_process_events pay source failed: %v", err)
	}
	if procSourceType != "settlement_cycle" {
		t.Fatalf("unexpected source_type for pay process: got=%s want=settlement_cycle", procSourceType)
	}
	wantSourceIDStr := fmt.Sprintf("%d", paySettlementCycleID)
	if procSourceIDStr != wantSourceIDStr {
		if procSourceIDStr == payAllocID {
			t.Fatalf("source_id is still allocation_id for pay: got=%s (expected integer id)", procSourceIDStr)
		}
		t.Fatalf("unexpected source_id for pay: got=%s want=%s", procSourceIDStr, wantSourceIDStr)
	}

	// 验证：settle_tx_breakdown 存在，且 business_id 指向 download business
	var breakdownCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_tx_breakdown WHERE business_id=? AND txid=?`, "biz_download_pool_test_"+sessionID, "pay_tx_third_iter_1").Scan(&breakdownCount); err != nil {
		t.Fatalf("query settle_tx_breakdown pay failed: %v", err)
	}
	if breakdownCount == 0 {
		t.Fatal("pay 应生成 settle_tx_breakdown 记录，挂到 download business")
	}
}

// TestDirectTransferAccounting_ProcessEventsSourceIDUsesAutoIncrementID 验证结算锚点已经收口到自增主键
// process events 的 source_id 应该是 fact_pool_session_events.id（整数）
func TestDirectTransferAccounting_ProcessEventsSourceIDUsesAutoIncrementID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 获取各 allocation 的自增 id
	openAllocID := directTransferPoolAllocationID(sessionID, "open", 1)
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	closeAllocID := directTransferPoolAllocationID(sessionID, "close", 3)

	openCycleID, err := dbGetSettlementCycleBySource(db, "pool_session", sessionID)
	if err != nil {
		t.Fatalf("lookup open settlement cycle id failed: %v", err)
	}
	payCycleID := openCycleID
	closeCycleID := openCycleID

	type wantRow struct {
		subtype        string
		expectedSource int64
	}
	checks := []wantRow{
		{subtype: "open", expectedSource: openCycleID},
		{subtype: "chunk_pay", expectedSource: payCycleID},
		{subtype: "close", expectedSource: closeCycleID},
	}
	for _, want := range checks {
		var gotSourceType, gotSourceIDStr, gotAccountingScene, gotAccountingSubtype string
		if err := db.QueryRow(
			`SELECT source_type,source_id,accounting_scene,accounting_subtype FROM settle_process_events WHERE process_id=? AND accounting_subtype=? ORDER BY id DESC LIMIT 1`,
			"proc_c2c_transfer_"+sessionID, want.subtype,
		).Scan(&gotSourceType, &gotSourceIDStr, &gotAccountingScene, &gotAccountingSubtype); err != nil {
			t.Fatalf("query settle_process_events %s failed: %v", want.subtype, err)
		}
		if gotSourceType != "settlement_cycle" {
			t.Fatalf("unexpected source_type for %s: got=%s want=settlement_cycle", want.subtype, gotSourceType)
		}
		// 这里的 source_id 应该是整数主键
		wantSourceIDStr := fmt.Sprintf("%d", want.expectedSource)
		if gotSourceIDStr != wantSourceIDStr {
			if gotSourceIDStr == openAllocID || gotSourceIDStr == payAllocID || gotSourceIDStr == closeAllocID {
				t.Fatalf("source_id is still allocation_id for %s: got=%s (expected integer id)", want.subtype, gotSourceIDStr)
			}
			t.Fatalf("unexpected source_id for %s: got=%s want=%s", want.subtype, gotSourceIDStr, wantSourceIDStr)
		}
		if gotAccountingScene == "" || gotAccountingSubtype == "" {
			t.Fatalf("missing accounting fields for %s", want.subtype)
		}
	}
}

func TestDirectTransferAccounting_ReadableByPrimaryQueries(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_third_iter_1",
		BaseTxID:          "base_tx_third_iter_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_third_iter_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_third_iter_1", baseTxHex, 700, 290, sellerPubHex)

	// 验证：pay 不再生成 biz_c2c_pay_* 正式收费对象
	var payBusinessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`, "biz_c2c_pay_sess_third_iter_1_2").Scan(&payBusinessCount); err != nil {
		t.Fatalf("query settle_businesses pay failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBusinessCount)
	}

	// 验证：process events 可读取
	page, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		ProcessID:         "proc_c2c_transfer_" + sessionID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "close",
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("process read failed: %v", err)
	}
	if page.Total == 0 || len(page.Items) == 0 {
		t.Fatalf("process read returned no rows")
	}
	// 第六次迭代：验证主口径字段
	if page.Items[0].SourceType != "settlement_cycle" || page.Items[0].SourceID == "" {
		t.Fatalf("process new fields incorrect: %+v", page.Items[0])
	}
}

func TestDirectTransferAccounting_CloseUsesExplicitSequence(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	sessionID := "sess_close_seq_7"
	dealID := "deal_close_seq_7"
	buyerPubHex := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPubHex := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      sessionID,
		DealID:         dealID,
		BuyerPeerID:    buyerPubHex,
		ArbiterPeerID:  arbiterPubHex,
		ArbiterPubKey:  arbiterPubHex,
		PoolAmount:     990,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    990,
		BaseTxID:       "base_tx_close_seq_7",
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex, baseTxHex, baseTxHex); err != nil {
		t.Fatalf("seed open facts failed: %v", err)
	}
	if err := dbUpdateDirectTransferPoolClosing(ctx, store, sessionID, 7, 700, 290, baseTxHex); err != nil {
		t.Fatalf("seed close facts failed: %v", err)
	}

	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 7, "close_tx_seq_7", baseTxHex, 700, 290, sellerPubHex)

	// 第二阶段整改：验证 close 生成过程型 settle_businesses，不是正式收费对象
	wantCycleID, err := dbGetSettlementCycleBySource(db, "pool_session", sessionID)
	if err != nil {
		t.Fatalf("lookup close settlement cycle id failed: %v", err)
	}
	wantIDStr := fmt.Sprintf("%d", wantCycleID)

	// 验证：settle_businesses 中存在 biz_c2c_close_*，但被标记为过程型
	var businessCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_close_"+sessionID,
	).Scan(&businessCount); err != nil {
		t.Fatalf("query settle_businesses failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("第二阶段：close 应生成 1 条过程型 settle_businesses 记录，got %d", businessCount)
	}

	// 验证：accounting_scene 和 accounting_subtype 标记为过程型
	var closeScene, closeSubtype string
	if err := db.QueryRow(
		`SELECT accounting_scene, accounting_subtype FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_close_"+sessionID,
	).Scan(&closeScene, &closeSubtype); err != nil {
		t.Fatalf("query close settle_businesses fields failed: %v", err)
	}
	if closeScene != "direct_transfer_process" {
		t.Fatalf("第二阶段：close 的 accounting_scene 应为 direct_transfer_process，got %s", closeScene)
	}
	if closeSubtype != "pool_close_settle" {
		t.Fatalf("第二阶段：close 的 accounting_subtype 应为 pool_close_settle，got %s", closeSubtype)
	}

	// 验证：settle_process_events 的 source_id 是 fact_pool_session_events.id
	var processSourceID string
	if err := db.QueryRow(
		`SELECT source_id FROM settle_process_events WHERE process_id=? AND accounting_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processSourceID); err != nil {
		t.Fatalf("query process source id failed: %v", err)
	}
	if processSourceID != wantIDStr {
		t.Fatalf("unexpected process source_id: got=%s want=%s", processSourceID, wantIDStr)
	}
}

// TestFinBusinessIdempotency 验证 settle_businesses 幂等写入
// 第六次迭代整改：验证 ON CONFLICT(idempotency_key) DO UPDATE 幂等性
func TestFinBusinessIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	businessID := "biz_idem_test_1"
	idempotencyKey := "idem_key_1"

	// 第一次写入
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "process", // 过程财务对象
		SourceType:        "settlement_cycle",
		SourceID:          "1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "open",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:abc",
		Status:            "pending",
		OccurredAtUnix:    1700000001,
		IdempotencyKey:    idempotencyKey,
		Note:              "first write",
		Payload:           map[string]any{"seq": 1},
	}); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// 第二次写入（相同 idempotency_key）- 应该更新而不是报错
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "process", // 过程财务对象
		SourceType:        "settlement_cycle",
		SourceID:          "1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "open",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:abc",
		Status:            "posted",   // 更新状态
		OccurredAtUnix:    1700000002, // 更新时间
		IdempotencyKey:    idempotencyKey,
		Note:              "second write",           // 更新 note
		Payload:           map[string]any{"seq": 2}, // 更新 payload
	}); err != nil {
		t.Fatalf("second write (idempotent update) failed: %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM settle_businesses WHERE idempotency_key=?`,
		idempotencyKey,
	).Scan(&status, &note, &payload); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "posted" {
		t.Fatalf("status not updated: got=%s want=posted", status)
	}
	if note != "second write" {
		t.Fatalf("note not updated: got=%s want='second write'", note)
	}
	if !strings.Contains(payload, `"seq":2`) {
		t.Fatalf("payload not updated: got=%s", payload)
	}

	_ = ctx
	_ = store
}

// TestFinProcessEventIdempotency 验证 settle_process_events 幂等写入
// 第六次迭代整改：验证 ON CONFLICT(idempotency_key) DO UPDATE 幂等性
func TestFinProcessEventIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	processID := "proc_idem_test_1"
	idempotencyKey := "idem_proc_1"

	// 第一次写入
	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         processID,
		SourceType:        "settlement_cycle",
		SourceID:          "1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "pay",
		EventType:         "accounting",
		Status:            "pending",
		OccurredAtUnix:    1700000001,
		IdempotencyKey:    idempotencyKey,
		Note:              "first event",
		Payload:           map[string]any{"amount": 100},
	}); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// 第二次写入（相同 idempotency_key）- 应该更新而不是报错
	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         processID,
		SourceType:        "settlement_cycle",
		SourceID:          "1",
		AccountingScene:   "c2c_transfer",
		AccountingSubType: "pay",
		EventType:         "accounting",
		Status:            "applied",  // 更新状态
		OccurredAtUnix:    1700000002, // 更新时间
		IdempotencyKey:    idempotencyKey,
		Note:              "second event",                // 更新 note
		Payload:           map[string]any{"amount": 200}, // 更新 payload
	}); err != nil {
		t.Fatalf("second write (idempotent update) failed: %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var status, note string
	var payload string
	if err := db.QueryRow(
		`SELECT status, note, payload_json FROM settle_process_events WHERE idempotency_key=?`,
		idempotencyKey,
	).Scan(&status, &note, &payload); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "applied" {
		t.Fatalf("status not updated: got=%s want=applied", status)
	}
	if note != "second event" {
		t.Fatalf("note not updated: got=%s want='second event'", note)
	}
	if !strings.Contains(payload, `"amount":200`) {
		t.Fatalf("payload not updated: got=%s", payload)
	}

	_ = ctx
	_ = store
}

// TestDirectTransferAccountingIdempotency 验证真实业务链路的幂等性
// 第六次迭代整改：用真实业务函数重复调用验证幂等性
func TestDirectTransferAccountingIdempotency(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	// 复用 seedDirectTransferPoolFacts 创建的 sessionID，避免重复创建 pool_allocation
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 先创建 open 记录
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_idem_real",
		BaseTxID:          "base_tx_idem_real",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})

	// 第一次 pay 记录
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_idem_real_1")

	// 获取第一次写入后的状态 - 验证 pay 不生成 settle_businesses
	var firstCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&firstCount); err != nil {
		t.Fatalf("first count failed: %v", err)
	}
	if firstCount != 0 {
		t.Fatalf("expected 0 settle_businesses for pay after first write, got %d (pay no longer creates business)", firstCount)
	}

	// 验证 fin_process_event 存在
	var firstProcCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`,
		"c2c_pay_event:"+sessionID+":2",
	).Scan(&firstProcCount); err != nil {
		t.Fatalf("first process count failed: %v", err)
	}
	if firstProcCount != 1 {
		t.Fatalf("expected 1 process event after first write, got %d", firstProcCount)
	}

	// 第二次 pay 记录（相同 idempotency_key）- 应该更新而不是报错
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_idem_real_2")

	// 验证 settle_businesses 仍然没有
	var secondCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_businesses WHERE business_id=?`,
		"biz_c2c_pay_"+sessionID+"_2",
	).Scan(&secondCount); err != nil {
		t.Fatalf("second count failed: %v", err)
	}
	if secondCount != 0 {
		t.Fatalf("expected 0 settle_businesses for pay after idempotent update, got %d", secondCount)
	}

	// 验证 process event 也只有一条（幂等更新）
	var procCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_process_events WHERE idempotency_key=?`,
		"c2c_pay_event:"+sessionID+":2",
	).Scan(&procCount); err != nil {
		t.Fatalf("process event count failed: %v", err)
	}
	if procCount != 1 {
		t.Fatalf("expected 1 process event record, got %d", procCount)
	}
}

// TestRecordWalletChainAccounting_WritesBreakdownWithTxRole 验证 wallet chain 链路写出的 breakdown 带 tx_role
// 硬切换后验证：MinerFeeSatoshi 从显式字段读取，net_out = counterparty_out + miner_fee
func TestRecordWalletChainAccounting_WritesBreakdownWithTxRole(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_role_1"
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 800,
		ExternalOutSat:  0,
		MinerFeeSat:     200,
		NetSat:          -200,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var txRole string
	if err := db.QueryRow(
		`SELECT tx_role FROM settle_tx_breakdown WHERE business_id=? AND txid=?`,
		walletChainBusinessID("chain_bsv", txid), txid,
	).Scan(&txRole); err != nil {
		t.Fatalf("query breakdown failed: %v", err)
	}
	if txRole != "internal_change" {
		t.Fatalf("expected tx_role=internal_change, got=%s", txRole)
	}

	// 硬切换验证：矿工费正确写入，net_out = 0 + miner_fee = 200
	var minerFee, netOut int64
	if err := db.QueryRow(
		`SELECT miner_fee_satoshi, net_out_satoshi FROM settle_tx_breakdown WHERE business_id=? AND txid=?`,
		walletChainBusinessID("chain_bsv", txid), txid,
	).Scan(&minerFee, &netOut); err != nil {
		t.Fatalf("query breakdown miner_fee/net_out failed: %v", err)
	}
	if minerFee != 200 {
		t.Fatalf("expected miner_fee=200, got=%d", minerFee)
	}
	if netOut != 200 {
		t.Fatalf("expected net_out=200 (0+200), got=%d", netOut)
	}
}

// TestRecordWalletChainAccounting_ExternalOutWithMinerFee 验证硬切换公式：
// net_out_satoshi = counterparty_out_satoshi + miner_fee_satoshi
// 场景：6 in / 3 external / 2 change / 1 fee => net_out = 3 + 1 = 4
func TestRecordWalletChainAccounting_ExternalOutWithMinerFee(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_external_out_1"

	// 6 sat 输入，2 sat 找零，3 sat 给第三方，1 sat 矿工费
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "THIRD_PARTY",
		WalletInputSat:  6,
		WalletOutputSat: 2,
		ExternalOutSat:  3,
		MinerFeeSat:     1,
		NetSat:          -4,
		Payload:         map[string]any{"test": true, "scenario": "6in_3ext_2chg_1fee"},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	// 硬断言 settle_tx_breakdown 各字段
	var grossInput, changeBack, externalIn, counterpartyOut, minerFee, netOut, netIn int64
	var txRole string
	if err := db.QueryRow(
		`SELECT gross_input_satoshi, change_back_satoshi, external_in_satoshi, counterparty_out_satoshi, miner_fee_satoshi, net_out_satoshi, net_in_satoshi, tx_role 
		 FROM settle_tx_breakdown WHERE business_id=? AND txid=?`,
		walletChainBusinessID("chain_bsv", txid), txid,
	).Scan(&grossInput, &changeBack, &externalIn, &counterpartyOut, &minerFee, &netOut, &netIn, &txRole); err != nil {
		t.Fatalf("query breakdown failed: %v", err)
	}

	if grossInput != 6 {
		t.Fatalf("expected gross_input=6, got=%d", grossInput)
	}
	if changeBack != 2 {
		t.Fatalf("expected change_back=2, got=%d", changeBack)
	}
	if externalIn != 0 {
		t.Fatalf("expected external_in=0, got=%d", externalIn)
	}
	if counterpartyOut != 3 {
		t.Fatalf("expected counterparty_out=3, got=%d", counterpartyOut)
	}
	if minerFee != 1 {
		t.Fatalf("expected miner_fee=1, got=%d", minerFee)
	}
	if netOut != 4 {
		t.Fatalf("expected net_out=4 (3+1), got=%d", netOut)
	}
	if netIn != 0 {
		t.Fatalf("expected net_in=0, got=%d", netIn)
	}
	if txRole != "external_out" {
		t.Fatalf("expected tx_role=external_out, got=%s", txRole)
	}

	// 验证 fact_settlement_cycles.net_amount_satoshi 与 net_in - net_out 一致 (-4)
	var cycleNetAmount int64
	if err := db.QueryRow(
		`SELECT net_amount_satoshi FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`,
		txid,
	).Scan(&cycleNetAmount); err != nil {
		t.Fatalf("query settlement cycle failed: %v", err)
	}
	if cycleNetAmount != -4 {
		t.Fatalf("expected cycle net_amount=-4, got=%d", cycleNetAmount)
	}
}

// TestDirectTransferAccounting_QueryByAllocationID 验证调试/正式查询都能按 allocation_id 命中统一来源。
func TestDirectTransferAccounting_QueryByAllocationID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"

	// 第二阶段：改用 pay 测试（pay 不再生成 settle_businesses，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_main_test")

	// 使用 pay allocation_id 查询
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)

	// 验证：pay 不再生成 settle_businesses（正式查询应返回空）
	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, payAllocID, "formal", 10, 0)
	if err != nil {
		t.Fatalf("formal query failed: %v", err)
	}
	if bizPage.Total != 0 {
		t.Fatalf("pay 不应生成 settle_businesses，got %d", bizPage.Total)
	}

	// 验证：process events 存在（pay 生成 process events）
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, payAllocID, 10, 0)
	if err != nil {
		t.Fatalf("process query failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatal("process query returned no results")
	}
}

// TestRecordWalletChainAccounting_UsesChainBSVCycle 验证钱包链财务已收口到 settlement_cycle
// wallet_chain 财务记录的 source_id 应该是 settlement_cycle.id
func TestRecordWalletChainAccounting_UsesChainBSVCycle(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_payment_id_test"
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  6000,
		WalletOutputSat: 5000,
		NetSat:          -1000,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	// 验证结算周期记录已创建
	var settlementCycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&settlementCycleID); err != nil {
		t.Fatalf("chain_bsv settlement cycle not found: %v", err)
	}
	if settlementCycleID <= 0 {
		t.Fatalf("expected positive settlement_cycle id, got %d", settlementCycleID)
	}

	// 验证 settle_businesses 的 source_id 已统一落到 settlement_cycle.id
	var sourceType, sourceID string
	if err := db.QueryRow(
		`SELECT source_type, source_id FROM settle_businesses WHERE business_id=?`,
		walletChainBusinessID("chain_bsv", txid),
	).Scan(&sourceType, &sourceID); err != nil {
		t.Fatalf("settle_businesses not found: %v", err)
	}

	if sourceType != "settlement_cycle" {
		t.Fatalf("unexpected source_type: got=%s want=settlement_cycle", sourceType)
	}
	expectedSourceID := fmt.Sprintf("%d", settlementCycleID)
	if sourceID != expectedSourceID {
		t.Fatalf("unexpected source_id: got=%s want=%s", sourceID, expectedSourceID)
	}
}

// TestRecordWalletChainAccounting_QueryByTxIDUsesChainBSVCycle
// 主口径查询：可以继续用 txid，但底层必须换算成 settlement_cycle.id
func TestRecordWalletChainAccounting_QueryByTxIDUsesChainBSVCycle(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	txid := "tx_chain_query_test"

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  6000,
		WalletOutputSat: 5000,
		NetSat:          -1000,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var settlementCycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&settlementCycleID); err != nil {
		t.Fatalf("chain_bsv settlement cycle lookup failed: %v", err)
	}
	wantSourceID := fmt.Sprintf("%d", settlementCycleID)

	// 第十一阶段：必须显式传 businessRole
	// wallet_chain 记录是过程型财务对象，所以传 process
	bizPage, err := dbListFinanceBusinessesByTxID(ctx, store, txid, "process", 10, 0)
	if err != nil {
		t.Fatalf("business lookup by txid failed: %v", err)
	}
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("business lookup by txid mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "settlement_cycle" || bizPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("business lookup by txid returned wrong source: %+v", bizPage.Items[0])
	}

	if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
		ProcessID:         "proc_wallet_chain_query_" + txid,
		SourceType:        "settlement_cycle",
		SourceID:          wantSourceID,
		AccountingScene:   "wallet_transfer",
		AccountingSubType: "query_probe",
		EventType:         "accounting",
		Status:            "applied",
		IdempotencyKey:    "wallet_chain_query:" + txid,
		Note:              "query probe",
		Payload:           map[string]any{"txid": txid},
	}); err != nil {
		t.Fatalf("append process event failed: %v", err)
	}

	procPage, err := dbListFinanceProcessEventsByTxID(ctx, store, txid, 10, 0)
	if err != nil {
		t.Fatalf("process lookup by txid failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("process lookup by txid mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}
	if procPage.Items[0].SourceType != "settlement_cycle" || procPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("process lookup by txid returned wrong source: %+v", procPage.Items[0])
	}

	// 直接按 settlement_cycle 查询，DB 层不再接受旧来源
	feePoolBizPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:           10,
		Offset:          0,
		BusinessRole:    "process",
		SourceType:      "settlement_cycle",
		SourceID:        wantSourceID,
		SettlementState: "confirmed",
	})
	if err != nil {
		t.Fatalf("business lookup by settlement_cycle failed: %v", err)
	}
	if feePoolBizPage.Total != 1 || len(feePoolBizPage.Items) != 1 {
		t.Fatalf("business lookup by settlement_cycle mismatch: total=%d items=%d", feePoolBizPage.Total, len(feePoolBizPage.Items))
	}
	if feePoolBizPage.Items[0].SourceType != "settlement_cycle" || feePoolBizPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("business lookup by settlement_cycle returned wrong source: %+v", feePoolBizPage.Items[0])
	}

	feePoolProcPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:           10,
		Offset:          0,
		SourceType:      "settlement_cycle",
		SourceID:        wantSourceID,
		SettlementState: "confirmed",
	})
	if err != nil {
		t.Fatalf("process lookup by settlement_cycle failed: %v", err)
	}
	if feePoolProcPage.Total != 1 || len(feePoolProcPage.Items) != 1 {
		t.Fatalf("process lookup by settlement_cycle mismatch: total=%d items=%d", feePoolProcPage.Total, len(feePoolProcPage.Items))
	}
	if feePoolProcPage.Items[0].SourceType != "settlement_cycle" || feePoolProcPage.Items[0].SourceID != wantSourceID {
		t.Fatalf("process lookup by settlement_cycle returned wrong source: %+v", feePoolProcPage.Items[0])
	}
}

// TestRecordWalletChainAccounting_RepaymentWithoutWalletInputSkipsSettlementCycle
// 验证：被动收款（无钱包输入）不会写 settlement_cycle。
func TestRecordWalletChainAccounting_RepaymentWithoutWalletInputSkipsSettlementCycle(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_receive_skip_cycle_1"
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "REPAYMENT",
		WalletInputSat:  0,
		WalletOutputSat: 5000,
		NetSat:          5000,
		Payload:         map[string]any{"test": true},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&cycleCount); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if cycleCount != 0 {
		t.Fatalf("expected 0 chain_bsv cycle for passive repayment, got %d", cycleCount)
	}
}

// TestRecordWalletChainAccounting_ChainPaymentUTXOLinks 验证 wallet_chain 链路补 utxo links
func TestRecordWalletChainAccounting_ChainPaymentUTXOLinks(t *testing.T) {
	t.Parallel()
	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_chain_utxo_link_test"
	inputUTXO := txid + ":0"
	changeUTXO := txid + ":1"
	counterpartyUTXO := txid + ":2"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 3000)
	seedWalletUTXO(t, db, changeUTXO, txid, 1, 800)
	seedWalletUTXO(t, db, counterpartyUTXO, txid, 2, 600)
	walletID := walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf")
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletID,
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   3000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed bsv utxo fact failed: %v", err)
	}

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  3000,
		WalletOutputSat: 800,
		NetSat:          -2200,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000, Note: "wallet input"},
			{UTXOID: changeUTXO, IOSide: "output", UTXORole: "wallet_change", AmountSatoshi: 800, Note: "wallet change"},
			{UTXOID: counterpartyUTXO, IOSide: "output", UTXORole: "counterparty_out", AmountSatoshi: 600, Note: "must skip"},
			{UTXOID: "missing:0", IOSide: "output", UTXORole: "external_in", AmountSatoshi: 1, Note: "missing must skip"},
		},
		ProcessEvents: []finProcessEventEntry{
			{
				ProcessID:         "proc_wallet_chain_" + txid,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "change_probe",
				EventType:         "accounting",
				Status:            "applied",
				IdempotencyKey:    "wallet_chain_event:" + txid,
				Note:              "wallet chain event",
				Payload:           map[string]any{"mark": "yes"},
			},
		},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var settlementCycleID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&settlementCycleID); err != nil {
		t.Fatalf("query chain_bsv settlement cycle failed: %v", err)
	}
	businessID := walletChainBusinessID("chain_bsv", txid)
	type wantLink struct {
		utxoID   string
		ioSide   string
		utxoRole string
		amount   int64
	}
	wants := []wantLink{
		{utxoID: inputUTXO, ioSide: "input", utxoRole: "wallet_input", amount: 3000},
		{utxoID: changeUTXO, ioSide: "output", utxoRole: "wallet_change", amount: 800},
	}
	for _, want := range wants {
		var count int
		var amount int64
		if err := db.QueryRow(
			`SELECT COUNT(1),COALESCE(SUM(amount_satoshi),0) FROM settle_tx_utxo_links
			  WHERE business_id=? AND txid=? AND utxo_id=? AND io_side=? AND utxo_role=?`,
			businessID, txid, want.utxoID, want.ioSide, want.utxoRole,
		).Scan(&count, &amount); err != nil {
			t.Fatalf("query link %s failed: %v", want.utxoID, err)
		}
		if count != 1 {
			t.Fatalf("expected 1 link for %s, got %d", want.utxoID, count)
		}
		if amount != want.amount {
			t.Fatalf("unexpected amount for %s: got=%d want=%d", want.utxoID, amount, want.amount)
		}
	}

	var skippedCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_tx_utxo_links WHERE business_id=? AND txid=? AND utxo_role IN ('counterparty_out','external_in')`,
		businessID, txid,
	).Scan(&skippedCount); err != nil {
		t.Fatalf("query skipped links failed: %v", err)
	}
	if skippedCount != 0 {
		t.Fatalf("expected skipped roles not to be written, got %d", skippedCount)
	}

	var eventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE process_id=?`, "proc_wallet_chain_"+txid).Scan(&eventCount); err != nil {
		t.Fatalf("query process event failed: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 process event, got %d", eventCount)
	}
}

func TestRecordWalletChainAccounting_Idempotent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	txid := "tx_chain_idem_1"
	inputUTXO := txid + ":0"
	changeUTXO := txid + ":1"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 3000)
	seedWalletUTXO(t, db, changeUTXO, txid, 1, 800)
	walletID := walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf")
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletID,
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   3000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed bsv utxo fact failed: %v", err)
	}

	in := walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  3000,
		WalletOutputSat: 800,
		NetSat:          -2200,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000},
			{UTXOID: changeUTXO, IOSide: "output", UTXORole: "wallet_change", AmountSatoshi: 800},
		},
	}
	for i := 0; i < 5; i++ {
		if err := recordWalletChainAccounting(db, in); err != nil {
			t.Fatalf("record round %d failed: %v", i+1, err)
		}
	}

	var cycleCount, bsvCount, linkCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&cycleCount); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 chain_bsv cycle, got %d", cycleCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='BSV' AND settlement_cycle_id IN (SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?)`, txid).Scan(&bsvCount); err != nil {
		t.Fatalf("count bsv settlement records failed: %v", err)
	}
	if bsvCount != 1 {
		t.Fatalf("expected 1 bsv settlement record, got %d", bsvCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_tx_utxo_links WHERE business_id=? AND txid=?`, walletChainBusinessID("chain_bsv", txid), txid).Scan(&linkCount); err != nil {
		t.Fatalf("count settle_tx_utxo_links failed: %v", err)
	}
	if linkCount != 2 {
		t.Fatalf("expected 2 settle_tx_utxo_links, got %d", linkCount)
	}
}

func TestWalletChainAccounting_DualLineReplay_NoDuplicateConsumption(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_chain_dual_replay_1"
	inputUTXO := txid + ":0"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 1)
	ctx := context.Background()
	store := newClientDB(db, nil)
	walletID := walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf")
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletID,
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed token carrier flow failed: %v", err)
	}
	// 写入 token lot 和 carrier link（使 collectTokenUTXOLinkFacts 能找到 token 输入）
	now := time.Now().Unix()
	lotID := "lot_dual_replay_" + txid
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   walletID,
		TokenID:          "token_dual_replay_001",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         txid,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_dual_replay_" + txid,
		LotID:          lotID,
		CarrierUTXOID:  inputUTXO,
		OwnerPubkeyHex: walletID,
		LinkState:      "active",
		BindTxid:       txid,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 900,
		NetSat:          -100,
		Payload:         map[string]any{"case": "dual"},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000},
		},
	}); err != nil {
		t.Fatalf("record bsv line failed: %v", err)
	}
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_token",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  0,
		WalletOutputSat: 0,
		NetSat:          0,
		Payload:         map[string]any{"case": "dual"},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1},
		},
	}); err != nil {
		t.Fatalf("record token line failed: %v", err)
	}

	for i := 0; i < 6; i++ {
		if err := recordWalletChainAccounting(db, walletChainAccountingInput{
			SourceType:      "chain_bsv",
			SourceID:        txid,
			TxID:            txid,
			Category:        "CHANGE",
			WalletInputSat:  1000,
			WalletOutputSat: 900,
			NetSat:          -100,
			Payload:         map[string]any{"case": "dual"},
			UTXOFacts: []chainPaymentUTXOFact{
				{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000},
			},
		}); err != nil {
			t.Fatalf("replay bsv line round %d failed: %v", i+1, err)
		}
		if err := recordWalletChainAccounting(db, walletChainAccountingInput{
			SourceType:      "chain_token",
			SourceID:        txid,
			TxID:            txid,
			Category:        "CHANGE",
			WalletInputSat:  0,
			WalletOutputSat: 0,
			NetSat:          0,
			Payload:         map[string]any{"case": "dual"},
			UTXOFacts: []chainPaymentUTXOFact{
				{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1},
			},
		}); err != nil {
			t.Fatalf("replay token line round %d failed: %v", i+1, err)
		}
	}

	consistency, err := CheckTokenTxDualLineConsistency(context.Background(), store, txid)
	if err != nil {
		t.Fatalf("check consistency failed: %v", err)
	}
	if len(consistency.MissingItems) != 0 {
		t.Fatalf("expected no missing items, got %+v", consistency.MissingItems)
	}
	var bsvCycleCount, tokenCycleCount, bsvConsumptionCount, tokenConsumptionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&bsvCycleCount); err != nil {
		t.Fatalf("count bsv cycle failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_token' AND source_id=?`, txid).Scan(&tokenCycleCount); err != nil {
		t.Fatalf("count token cycle failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='BSV' AND settlement_cycle_id IN (SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?)`, txid).Scan(&bsvConsumptionCount); err != nil {
		t.Fatalf("count bsv settlement records failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='TOKEN' AND settlement_cycle_id IN (SELECT id FROM fact_settlement_cycles WHERE source_type='chain_token' AND source_id=?)`, txid).Scan(&tokenConsumptionCount); err != nil {
		t.Fatalf("count token settlement records failed: %v", err)
	}
	if bsvCycleCount != 1 || tokenCycleCount != 1 || bsvConsumptionCount != 1 || tokenConsumptionCount != 1 {
		t.Fatalf("unexpected replay counts: bsv_cycle=%d token_cycle=%d bsv_cons=%d token_cons=%d", bsvCycleCount, tokenCycleCount, bsvConsumptionCount, tokenConsumptionCount)
	}
}

func TestWalletChainAccounting_ConcurrentReplay_NoDoubleWrite(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	txid := "tx_chain_dual_replay_concurrent_1"
	inputUTXO := txid + ":0"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 1)
	ctx := context.Background()
	store := newClientDB(db, nil)
	walletID := walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf")
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletID,
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed token carrier flow failed: %v", err)
	}
	// 写入 token lot 和 carrier link（使 collectTokenUTXOLinkFacts 能找到 token 输入）
	now := time.Now().Unix()
	lotID := "lot_dual_replay_concurrent_" + txid
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   walletID,
		TokenID:          "token_dual_replay_concurrent_001",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         txid,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_dual_replay_concurrent_" + txid,
		LotID:          lotID,
		CarrierUTXOID:  inputUTXO,
		OwnerPubkeyHex: walletID,
		LinkState:      "active",
		BindTxid:       txid,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	inBsv := walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 900,
		NetSat:          -100,
		Payload:         map[string]any{"case": "concurrent"},
		UTXOFacts:       []chainPaymentUTXOFact{{UTXOID: inputUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000}},
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- recordWalletChainAccounting(db, inBsv)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent replay failed: %v", err)
		}
	}

	consistency, err := CheckTokenTxDualLineConsistency(context.Background(), store, txid)
	if err != nil {
		t.Fatalf("check consistency failed: %v", err)
	}
	if len(consistency.MissingItems) != 0 {
		t.Fatalf("expected no missing items, got %+v", consistency.MissingItems)
	}
	var bsvCycleCount, bsvConsumptionCount, tokenConsumptionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&bsvCycleCount); err != nil {
		t.Fatalf("count bsv cycle failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='BSV' AND settlement_cycle_id IN (SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?)`, txid).Scan(&bsvConsumptionCount); err != nil {
		t.Fatalf("count bsv settlement records failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='TOKEN' AND settlement_cycle_id IN (SELECT id FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?)`, txid).Scan(&tokenConsumptionCount); err != nil {
		t.Fatalf("count token settlement records failed: %v", err)
	}
	if bsvCycleCount != 1 || bsvConsumptionCount != 1 || tokenConsumptionCount != 1 {
		t.Fatalf("unexpected concurrent counts: bsv_cycle=%d bsv_cons=%d token_cons=%d", bsvCycleCount, bsvConsumptionCount, tokenConsumptionCount)
	}
}

func TestRecordWalletChainAccounting_FeePoolSettleLink(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	txid := "tx_fee_pool_settle_1"
	settleUTXO := txid + ":0"
	seedWalletUTXO(t, db, settleUTXO, txid, 0, 1234)

	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "FEE_POOL",
		WalletInputSat:  1234,
		WalletOutputSat: 1234,
		NetSat:          0,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: settleUTXO, IOSide: "output", UTXORole: "fee_pool_settle", AmountSatoshi: 1234, Note: "fee settle"},
		},
	}); err != nil {
		t.Fatalf("record wallet chain accounting failed: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_tx_utxo_links WHERE business_id=? AND txid=? AND utxo_role='fee_pool_settle' AND utxo_id=?`, walletChainBusinessID("chain_bsv", txid), txid, settleUTXO).Scan(&count); err != nil {
		t.Fatalf("query fee_pool_settle link failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 fee_pool_settle link, got %d", count)
	}
}

func TestReconcileWalletUTXOSet_RecordsChainAccountingFromSyncEntry(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("1", 64)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(address)
	walletScriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}

	prevTxID := "tx_sync_prev_1"
	txid := "tx_sync_chain_accounting_1"
	seedWalletUTXO(t, db, prevTxID+":0", prevTxID, 0, 5000)
	if err := dbUpsertBSVUTXO(context.Background(), store, bsvUTXOEntry{
		UTXOID:         prevTxID + ":0",
		OwnerPubkeyHex: walletID,
		Address:        address,
		TxID:           prevTxID,
		Vout:           0,
		ValueSatoshi:   5000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed sync bsv utxo failed: %v", err)
	}

	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			txid + ":0": {TxID: txid, Vout: 0, Value: 1500},
		},
		ConfirmedLiveTxIDs: map[string]struct{}{txid: struct{}{}},
		Balance:            1500,
		Count:              1,
	}
	history := []walletHistoryTxRecord{
		{
			TxID:   txid,
			Height: 123,
			Tx: whatsonchain.TxDetail{
				TxID: txid,
				Vin: []whatsonchain.TxInput{
					{TxID: prevTxID, Vout: 0},
				},
				Vout: []whatsonchain.TxOutput{
					{N: 0, ValueSatoshi: 1500, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: walletScriptHex}},
					{N: 1, ValueSatoshi: 3300, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: "76a914111111111111111111111111111111111111111188ac"}},
				},
			},
		},
	}
	cursor := walletUTXOSyncCursor{
		WalletID:            walletID,
		Address:             address,
		NextConfirmedHeight: 1,
	}

	if err := SyncWalletAndApplyFacts(context.Background(), store, address, snapshot, history, cursor, "round-sync-1", "", "sync-test", time.Now().Unix(), 1); err != nil {
		t.Fatalf("SyncWalletAndApplyFacts failed: %v", err)
	}

	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_bsv' AND source_id=?`, txid).Scan(&cycleCount); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 chain_bsv cycle, got %d", cycleCount)
	}

	var linkCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_tx_utxo_links WHERE business_id=? AND txid=?`, walletChainBusinessID("chain_bsv", txid), txid).Scan(&linkCount); err != nil {
		t.Fatalf("count settle_tx_utxo_links failed: %v", err)
	}
	if linkCount != 1 {
		t.Fatalf("expected 1 settle_tx_utxo_links, got %d", linkCount)
	}

	var inputRole string
	if err := db.QueryRow(
		`SELECT utxo_role FROM settle_tx_utxo_links WHERE business_id=? AND txid=? AND io_side='input' LIMIT 1`,
		walletChainBusinessID("chain_bsv", txid), txid,
	).Scan(&inputRole); err != nil {
		t.Fatalf("query input role failed: %v", err)
	}
	if inputRole != "wallet_input" {
		t.Fatalf("unexpected input role: %s", inputRole)
	}

	// 这个同步样本只把前驱输入写进了钱包 UTXO 表，
	// 没有把 txid:0 的输出预先注册成可识别的钱包 UTXO，所以不应落出 output link。
	var outputCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM settle_tx_utxo_links WHERE business_id=? AND txid=? AND io_side='output'`,
		walletChainBusinessID("chain_bsv", txid), txid,
	).Scan(&outputCount); err != nil {
		t.Fatalf("query output role count failed: %v", err)
	}
	if outputCount != 0 {
		t.Fatalf("expected no output settle_tx_utxo_links, got %d", outputCount)
	}
}

// TestBuildWalletChainAccountingInputsFromTxDetail_DualLineWhenTokenCarrierExists 验证 token carrier 会明确拆成两条输入。
func TestBuildWalletChainAccountingInputsFromTxDetail_DualLineWhenTokenCarrierExists(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("2", 64)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletScriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}

	prevTxID := "tx_token_carrier_prev_1"
	txid := "tx_token_carrier_dual_1"
	inputUTXO := prevTxID + ":0"

	seedWalletUTXO(t, db, inputUTXO, prevTxID, 0, 1)

	now := time.Now().Unix()
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletIDByAddress(address),
		Address:        address,
		TxID:           prevTxID,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier flow failed: %v", err)
	}
	// 写入 token lot 和 carrier link（使 collectTokenUTXOLinkFacts 能找到 token 输入）
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            "lot_dual_" + prevTxID,
		OwnerPubkeyHex:   walletIDByAddress(address),
		TokenID:          "token_dual_001",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         prevTxID,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_dual_" + prevTxID,
		LotID:          "lot_dual_" + prevTxID,
		CarrierUTXOID:  inputUTXO,
		OwnerPubkeyHex: walletIDByAddress(address),
		LinkState:      "active",
		BindTxid:       prevTxID,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	inputs, err := buildWalletChainAccountingInputsFromTxDetail(context.Background(), db, address, whatsonchain.TxDetail{
		TxID: txid,
		Vin: []whatsonchain.TxInput{
			{TxID: prevTxID, Vout: 0},
		},
		Vout: []whatsonchain.TxOutput{
			{N: 0, ValueSatoshi: 1, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: walletScriptHex}},
		},
	})
	if err != nil {
		t.Fatalf("build wallet chain inputs failed: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("expected 1 explicit chain input, got %d", len(inputs))
	}
	if inputs[0].SourceType != "chain_bsv" {
		t.Fatalf("expected chain_bsv input, got %+v", inputs[0])
	}
}

// TestCheckTokenTxDualLineConsistency_AllPresent 验证 token tx 双线齐全时四个状态都为真。
func TestCheckTokenTxDualLineConsistency_AllPresent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("2", 64)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletScriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}

	prevTxID := "tx_token_carrier_prev_1"
	txid := "tx_token_carrier_dual_1"
	inputUTXO := prevTxID + ":0"

	seedWalletUTXO(t, db, inputUTXO, prevTxID, 0, 1)

	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         inputUTXO,
		OwnerPubkeyHex: walletIDByAddress(address),
		Address:        address,
		TxID:           prevTxID,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed token carrier flow failed: %v", err)
	}
	// 写入 token lot 和 carrier link（使 collectTokenUTXOLinkFacts 能找到 token 输入）
	now := time.Now().Unix()
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            "lot_all_present_" + prevTxID,
		OwnerPubkeyHex:   walletIDByAddress(address),
		TokenID:          "token_all_present_001",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         prevTxID,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_all_present_" + prevTxID,
		LotID:          "lot_all_present_" + prevTxID,
		CarrierUTXOID:  inputUTXO,
		OwnerPubkeyHex: walletIDByAddress(address),
		LinkState:      "active",
		BindTxid:       prevTxID,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	inputs, err := buildWalletChainAccountingInputsFromTxDetail(context.Background(), db, address, whatsonchain.TxDetail{
		TxID: txid,
		Vin: []whatsonchain.TxInput{
			{TxID: prevTxID, Vout: 0},
		},
		Vout: []whatsonchain.TxOutput{
			{N: 0, ValueSatoshi: 1, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: walletScriptHex}},
		},
	})
	if err != nil {
		t.Fatalf("build wallet chain inputs failed: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("expected 1 explicit chain input, got %d", len(inputs))
	}
	if inputs[0].SourceType != "chain_bsv" {
		t.Fatalf("expected chain_bsv input, got %+v", inputs[0])
	}

	if err := recordWalletChainAccounting(db, inputs[0]); err != nil {
		t.Fatalf("record chain_bsv failed: %v", err)
	}

	bsvCycleID, err := dbGetSettlementCycleBySource(db, "chain_bsv", txid)
	if err != nil {
		t.Fatalf("lookup chain_bsv cycle failed: %v", err)
	}

	var bsvCount, tokenCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='BSV' AND settlement_cycle_id=?`, bsvCycleID).Scan(&bsvCount); err != nil {
		t.Fatalf("count chain_bsv settlement records failed: %v", err)
	}
	if bsvCount != 1 {
		t.Fatalf("expected 1 BSV settlement record on chain_bsv, got %d", bsvCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE asset_type='TOKEN' AND settlement_cycle_id=?`, bsvCycleID).Scan(&tokenCount); err != nil {
		t.Fatalf("count chain_bsv token settlement records failed: %v", err)
	}
	if tokenCount != 1 {
		t.Fatalf("expected 1 token settlement record on chain_bsv, got %d", tokenCount)
	}

	consistency, err := CheckTokenTxDualLineConsistency(context.Background(), newClientDB(db, nil), txid)
	if err != nil {
		t.Fatalf("check token dual line consistency failed: %v", err)
	}
	if !consistency.HasChainBSVCycle || !consistency.HasCarrierBSVFact || !consistency.HasTokenQuantityFact {
		t.Fatalf("expected complete main-line consistency, got %+v", consistency)
	}
	if consistency.HasChainTokenCycle {
		t.Fatalf("expected no separate chain_token cycle, got %+v", consistency)
	}
	if len(consistency.MissingItems) != 0 {
		t.Fatalf("expected no missing items, got %+v", consistency.MissingItems)
	}
}

// TestBuildWalletChainAccountingInputsFromTxDetail_BSVOnlySingleLine 验证纯 BSV 交易只产出一条线。
func TestBuildWalletChainAccountingInputsFromTxDetail_BSVOnlySingleLine(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("3", 64)
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletScriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}

	txid := "tx_bsv_only_single_1"
	inputUTXO := txid + ":0"
	seedWalletUTXO(t, db, inputUTXO, txid, 0, 1200)

	inputs, err := buildWalletChainAccountingInputsFromTxDetail(context.Background(), db, address, whatsonchain.TxDetail{
		TxID: txid,
		Vin: []whatsonchain.TxInput{
			{TxID: txid, Vout: 0},
		},
		Vout: []whatsonchain.TxOutput{
			{N: 0, ValueSatoshi: 700, ScriptPubKey: whatsonchain.ScriptPubKey{Hex: walletScriptHex}},
		},
	})
	if err != nil {
		t.Fatalf("build wallet chain inputs failed: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("expected 1 chain input, got %d", len(inputs))
	}
	if inputs[0].SourceType != "chain_bsv" {
		t.Fatalf("expected chain_bsv input, got %+v", inputs[0])
	}
	if inputs[0].SourceID != txid {
		t.Fatalf("expected source_id %s, got %s", txid, inputs[0].SourceID)
	}
}

// TestCollectTokenUTXOLinkFacts_RequireQuantityText 验证 token carrier 缺少 quantity_text 时直接报错。
func TestCollectTokenUTXOLinkFacts_RequireQuantityText(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	address := "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"
	walletID := walletIDByAddress(address)
	utxoID := "tx_token_quantity_missing:0"

	seedWalletUTXO(t, db, utxoID, "tx_token_quantity_missing", 0, 1)
	lotID := "lot_token_quantity_missing"
	now := time.Now().Unix()
	if err := dbUpsertBSVUTXO(context.Background(), newClientDB(db, nil), bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: walletID,
		Address:        address,
		TxID:           "tx_token_quantity_missing",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier bsv utxo failed: %v", err)
	}
	// 插入 token lot（缺少 quantity_text）
	if _, err := db.Exec(`INSERT INTO fact_token_lots(
		lot_id,owner_pubkey_hex,token_id,token_standard,quantity_text,used_quantity_text,lot_state,mint_txid,last_spend_txid,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		lotID, walletID, "token_missing_qty", "BSV21", "", "0", "unspent", "tx_token_quantity_missing", "", now, now, "token lot", "{}",
	); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	// 插入 carrier link 绑定 lot 到 UTXO
	if _, err := db.Exec(`INSERT INTO fact_token_carrier_links(
		link_id,lot_id,carrier_utxo_id,owner_pubkey_hex,link_state,bind_txid,unbind_txid,created_at_unix,updated_at_unix,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"link_token_quantity_missing", lotID, utxoID, walletID, "active", "tx_token_quantity_missing", "", now, now, "carrier link", "{}",
	); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}

	_, err := collectTokenUTXOLinkFacts(context.Background(), db, []chainPaymentUTXOFact{
		{UTXOID: utxoID, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1},
	})
	if err == nil {
		t.Fatal("expected collectTokenUTXOLinkFacts to fail when quantity_text is empty")
	}
	if !strings.Contains(err.Error(), "token quantity is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRecordWalletChainAccountingSourceTypeRequired 验证来源类型为空时直接拒绝。
func TestRecordWalletChainAccountingSourceTypeRequired(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	err := recordWalletChainAccountingConn(db, walletChainAccountingInput{
		SourceID: "tx_source_required_1",
		TxID:     "tx_source_required_1",
	})
	if err == nil {
		t.Fatal("expected source_type to be required")
	}
	if !strings.Contains(err.Error(), "source_type and source_id are required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRecordWalletChainAccountingRejectUnknownSourceType 验证未知来源类型不允许写入。
func TestRecordWalletChainAccountingRejectUnknownSourceType(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	err := recordWalletChainAccountingConn(db, walletChainAccountingInput{
		SourceType: "chain_payment",
		SourceID:   "tx_unknown_source_1",
		TxID:       "tx_unknown_source_1",
	})
	if err == nil {
		t.Fatal("expected unknown source_type to be rejected")
	}
	if !strings.Contains(err.Error(), "source_type must be chain_bsv or chain_token") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRecordWalletChainAccounting_NoSourceInference 验证写入口不会替调用方猜来源。
func TestRecordWalletChainAccounting_NoSourceInference(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	err := recordWalletChainAccounting(db, walletChainAccountingInput{
		TxID:            "tx_no_source_inference_1",
		WalletInputSat:  1000,
		WalletOutputSat: 1000,
		NetSat:          0,
	})
	if err == nil {
		t.Fatal("expected missing source_type to fail")
	}
	if !strings.Contains(err.Error(), "source_type and source_id are required") {
		t.Fatalf("unexpected error: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles`).Scan(&count); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no settlement cycles to be written, got %d", count)
	}
}

// TestCheckTokenTxDualLineConsistency_MissingOneLine 验证少一条线时会明确暴露缺失项。
func TestCheckTokenTxDualLineConsistency_MissingOneLine(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()

	txid := "tx_chain_dual_missing_1"
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         txid + ":0",
		OwnerPubkeyHex: walletIDByAddress("mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf"),
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed chain_bsv fact utxo failed: %v", err)
	}
	if err := recordWalletChainAccounting(db, walletChainAccountingInput{
		SourceType:      "chain_bsv",
		SourceID:        txid,
		TxID:            txid,
		Category:        "CHANGE",
		WalletInputSat:  1000,
		WalletOutputSat: 900,
		NetSat:          -100,
		Payload:         map[string]any{"test": true},
		UTXOFacts: []chainPaymentUTXOFact{
			{UTXOID: txid + ":0", IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000, Note: "carrier"},
		},
	}); err != nil {
		t.Fatalf("record chain_bsv failed: %v", err)
	}

	consistency, err := CheckTokenTxDualLineConsistency(ctx, store, txid)
	if err != nil {
		t.Fatalf("check token dual line consistency failed: %v", err)
	}
	if consistency.HasChainTokenCycle || consistency.HasTokenQuantityFact {
		t.Fatalf("expected missing token line, got %+v", consistency)
	}
	if !consistency.HasChainBSVCycle || !consistency.HasCarrierBSVFact {
		t.Fatalf("expected bsv line intact, got %+v", consistency)
	}
	if !containsString(consistency.MissingItems, "chain_token_cycle") || !containsString(consistency.MissingItems, "token_quantity_fact") {
		t.Fatalf("expected missing token items, got %+v", consistency.MissingItems)
	}
}

func TestCheckTokenTxDualLineConsistency_MissingCarrierFact(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	txid := "tx_chain_missing_carrier_fact_1"
	tokenUTXOID := txid + ":0"

	if err := dbUpsertSettlementCycle(db, "cycle_chain_bsv_"+txid, "chain_bsv", txid, "confirmed", 1000, 0, -100, 0, time.Now().Unix(), "missing carrier fact", nil); err != nil {
		t.Fatalf("seed chain_bsv cycle failed: %v", err)
	}
	if err := dbUpsertSettlementCycle(db, "cycle_chain_token_"+txid, "chain_token", txid, "confirmed", 0, 0, 0, 0, time.Now().Unix(), "missing carrier fact", nil); err != nil {
		t.Fatalf("seed chain_token cycle failed: %v", err)
	}
	now := time.Now().Unix()
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         tokenUTXOID,
		OwnerPubkeyHex: "wallet_missing_carrier_fact",
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier bsv utxo failed: %v", err)
	}
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         tokenUTXOID,
		OwnerPubkeyHex: "wallet_missing_carrier_fact",
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token flow failed: %v", err)
	}
	// 写入 token lot 和 carrier link（使 token 消耗能关联到 lot）
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            "lot_missing_carrier_" + txid,
		OwnerPubkeyHex:   "wallet_missing_carrier_fact",
		TokenID:          "token_missing_001",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         txid,
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("seed token lot failed: %v", err)
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, tokenCarrierLinkEntry{
		LinkID:         "link_missing_carrier_" + txid,
		LotID:          "lot_missing_carrier_" + txid,
		CarrierUTXOID:  tokenUTXOID,
		OwnerPubkeyHex: "wallet_missing_carrier_fact",
		LinkState:      "active",
		BindTxid:       txid,
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed token carrier link failed: %v", err)
	}
	if err := dbAppendTokenConsumptionsForSettlementCycle(db, mustSettlementCycleIDBySource(t, db, "chain_token", txid), []chainPaymentUTXOLinkEntry{
		{
			UTXOID:       tokenUTXOID,
			IOSide:       "input",
			AssetKind:    "BSV21",
			TokenID:      "token_missing_001",
			QuantityText: "100",
		},
	}, now); err != nil {
		t.Fatalf("seed token consumption failed: %v", err)
	}

	consistency, err := CheckTokenTxDualLineConsistency(ctx, store, txid)
	if err != nil {
		t.Fatalf("check token dual line consistency failed: %v", err)
	}
	if consistency.HasChainBSVCycle && consistency.HasCarrierBSVFact {
		t.Fatalf("expected missing carrier fact, got %+v", consistency)
	}
	if !containsString(consistency.MissingItems, "carrier_bsv_fact") {
		t.Fatalf("expected carrier_bsv_fact missing, got %+v", consistency.MissingItems)
	}
	if containsString(consistency.MissingItems, "token_quantity_fact") {
		t.Fatalf("token quantity fact should exist, got %+v", consistency.MissingItems)
	}
}

func TestCheckTokenTxDualLineConsistency_MissingTokenQuantityFact(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	txid := "tx_chain_missing_token_qty_fact_1"

	if err := dbUpsertSettlementCycle(db, "cycle_chain_bsv_"+txid, "chain_bsv", txid, "confirmed", 1000, 0, -100, 0, time.Now().Unix(), "missing token qty fact", nil); err != nil {
		t.Fatalf("seed chain_bsv cycle failed: %v", err)
	}
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         txid + ":0",
		OwnerPubkeyHex: "wallet_missing_token_qty_fact",
		Address:        "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   1000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed bsv fact utxo failed: %v", err)
	}
	if err := dbAppendBSVConsumptionsForSettlementCycle(db, mustSettlementCycleIDBySource(t, db, "chain_bsv", txid), []chainPaymentUTXOLinkEntry{
		{UTXOID: txid + ":0", IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000, CreatedAtUnix: time.Now().Unix()},
	}, time.Now().Unix()); err != nil {
		t.Fatalf("seed bsv consumption failed: %v", err)
	}
	if err := dbUpsertSettlementCycle(db, "cycle_chain_token_"+txid, "chain_token", txid, "confirmed", 0, 0, 0, 0, time.Now().Unix(), "missing token qty fact", nil); err != nil {
		t.Fatalf("seed chain_token cycle failed: %v", err)
	}

	consistency, err := CheckTokenTxDualLineConsistency(ctx, store, txid)
	if err != nil {
		t.Fatalf("check token dual line consistency failed: %v", err)
	}
	if consistency.HasChainTokenCycle && consistency.HasTokenQuantityFact {
		t.Fatalf("expected missing token quantity fact, got %+v", consistency)
	}
	if !containsString(consistency.MissingItems, "token_quantity_fact") {
		t.Fatalf("expected token_quantity_fact missing, got %+v", consistency.MissingItems)
	}
	if containsString(consistency.MissingItems, "carrier_bsv_fact") {
		t.Fatalf("carrier bsv fact should exist, got %+v", consistency.MissingItems)
	}
}

// TestDirectTransferAccounting_RejectsMissingPoolAllocationFacts
// 第二阶段硬约束：查不到 fact_pool_session_events.id 时，直连池财务写入必须直接失败
func TestDirectTransferAccounting_RejectsMissingPoolAllocationFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	openErr := dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         "sess_missing_fact_1",
		DealID:            "deal_missing_fact_1",
		BaseTxID:          "base_tx_missing_fact_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
	if openErr == nil {
		t.Fatal("expected open accounting to fail without pool allocation fact")
	}

	payErr := dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_sess_missing_fact_1", "sess_missing_fact_1", 2, 300, "pay_tx_missing_fact_1")
	if payErr == nil {
		t.Fatal("expected pay accounting to fail without pool allocation fact")
	}

	closeErr := dbRecordDirectPoolCloseAccounting(ctx, store, "sess_missing_fact_1", 3, "close_tx_missing_fact_1", baseTxHex, 700, 290, "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if closeErr == nil {
		t.Fatal("expected close accounting to fail without pool allocation fact")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_businesses WHERE business_id IN (?,?,?)`,
		"biz_c2c_open_sess_missing_fact_1",
		"biz_c2c_pay_sess_missing_fact_1_2",
		"biz_c2c_close_sess_missing_fact_1",
	).Scan(&count); err != nil {
		t.Fatalf("count settle_businesses failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no finance rows to be written, got %d", count)
	}
}
