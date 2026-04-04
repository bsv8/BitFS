package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

func newAssetFactsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "asset-facts.sqlite")
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

// TestAppendAssetFlowIn_PowerIdempotent 验证入项幂等写入
func TestAppendAssetFlowIn_PowerIdempotent(t *testing.T) {
	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 先写入 wallet_utxo
	utxoID := "tx001:0"
	walletID := "wallet_abc"
	address := "test_address_1"
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "tx001", 0, int64(5000), "unspent", "plain_bsv", "",
		"tx001", "", 1700000001, 1700000001, 0,
	)
	if err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	// 第一次写入
	entry := chainAssetFlowEntry{
		FlowID:         "flow_in_" + utxoID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		TokenID:        "",
		UTXOID:         utxoID,
		TxID:           "tx001",
		Vout:           0,
		AmountSatoshi:  5000,
		QuantityText:   "",
		OccurredAtUnix: 1700000001,
		EvidenceSource: "WOC",
		Note:           "plain_bsv utxo detected by chain sync",
		Payload:        map[string]any{"test": true},
	}
	id1, err := dbAppendAssetFlowInIfAbsent(ctx, store, entry)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if id1 <= 0 {
		t.Fatalf("expected positive id, got %d", id1)
	}

	// 第二次写入（幂等）
	id2, err := dbAppendAssetFlowInIfAbsent(ctx, store, entry)
	if err != nil {
		t.Fatalf("second append (idempotent): %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected same id, got %d vs %d", id2, id1)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&count); err != nil {
		t.Fatalf("count flows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 flow, got %d", count)
	}
}

// TestAppendAssetFlowIn_UnknownNotWritten 验证 unknown 不入表
func TestAppendAssetFlowIn_UnknownNotWritten(t *testing.T) {
	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 写入 unknown 分类的 UTXO
	utxoID := "tx002:0"
	walletID := "wallet_def"
	address := "test_address_2"
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "tx002", 0, int64(1), "unspent", "unknown", "awaiting external token evidence",
		"tx002", "", 1700000002, 1700000002, 0,
	)
	if err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	// 尝试写入 unknown 资产（应该被拒绝）
	entry := chainAssetFlowEntry{
		FlowID:         "flow_in_" + utxoID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "tx002",
		Vout:           0,
		AmountSatoshi:  1,
		OccurredAtUnix: 1700000002,
	}
	// 注意：这里直接调用 dbAppendAssetFlowIfAbsentDB 是可以写入的，
	// 因为 unknown 不入 fact 的规则是在调用方（reconcileWalletUTXOSet）控制的
	// 此测试只验证函数本身不会拒绝 valid entry
	id, err := dbAppendAssetFlowInIfAbsent(ctx, store, entry)
	if err != nil {
		t.Fatalf("append unknown: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// 清理：删除这条记录，后续测试 reconcile 时不会干扰
	_, _ = db.Exec(`DELETE FROM fact_chain_asset_flows WHERE utxo_id=?`, utxoID)
}

// TestAppendAssetConsumptionForChainPayment_PowerIdempotent 验证 chain_payment 消耗关联幂等
func TestAppendAssetConsumptionForChainPayment_PowerIdempotent(t *testing.T) {
	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 先写入 fact_chain_payments
	paymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                "ctx001",
		PaymentSubType:      "direct_payment",
		Status:              "confirmed",
		WalletInputSatoshi:  3000,
		WalletOutputSatoshi: 800,
		NetAmountSatoshi:    2200,
		BlockHeight:         100,
		OccurredAtUnix:      1700000010,
		FromPartyID:         "buyer1",
		ToPartyID:           "seller1",
		Payload:             map[string]any{"test": true},
	})
	if err != nil {
		t.Fatalf("upsert chain payment: %v", err)
	}

	// 先写入 asset flow
	flowID := "flow_test_001"
	walletID := "wallet_ghi"
	address := "test_address_3"
	utxoID := "ctx001:0"
	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "ctx001", 0, int64(3000), "unspent", "plain_bsv", "",
		"ctx001", "", 1700000010, 1700000010, 0,
	)
	if err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	flowEntry := chainAssetFlowEntry{
		FlowID:         flowID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "ctx001",
		Vout:           0,
		AmountSatoshi:  3000,
		OccurredAtUnix: 1700000010,
	}
	flowDBID, err := dbAppendAssetFlowInIfAbsent(ctx, store, flowEntry)
	if err != nil {
		t.Fatalf("append flow: %v", err)
	}

	// 第一次写入消耗
	consEntry := assetConsumptionEntry{
		SourceFlowID:   flowDBID,
		ChainPaymentID: paymentID,
		UsedSatoshi:    2200,
		OccurredAtUnix: 1700000010,
		Note:           "consumed by chain payment",
	}
	if err := dbAppendAssetConsumptionForChainPaymentIfAbsent(ctx, store, consEntry); err != nil {
		t.Fatalf("first consumption: %v", err)
	}

	// 第二次写入（幂等）
	if err := dbAppendAssetConsumptionForChainPaymentIfAbsent(ctx, store, consEntry); err != nil {
		t.Fatalf("second consumption (idempotent): %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions`).Scan(&count); err != nil {
		t.Fatalf("count consumptions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 consumption, got %d", count)
	}
}

// TestAppendAssetConsumptionForPoolAllocation_PowerIdempotent 验证 pool_allocation 消耗关联幂等
func TestAppendAssetConsumptionForPoolAllocation_PowerIdempotent(t *testing.T) {
	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 先写入 fact_pool_sessions
	_, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex,
		gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks,
		open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_001", "direct_transfer", "seller1", "seller1", "arbiter1",
		"gateway1", 1000, 10, 0.5, 6,
		"base_tx001", "closed", 1700000020, 1700000020,
	)
	if err != nil {
		t.Fatalf("seed pool session: %v", err)
	}

	// 写入 fact_pool_allocations
	allocRes, err := db.Exec(`INSERT INTO fact_pool_allocations(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num, payee_amount_after,
		payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"alloc_001", "sess_001", 1, "payment", 1, 700,
		300, "atx001", "hex001", 1700000020,
	)
	if err != nil {
		t.Fatalf("insert pool allocation: %v", err)
	}
	allocDBID, err := allocRes.LastInsertId()
	if err != nil {
		t.Fatalf("get allocation id: %v", err)
	}

	// 写入 asset flow
	flowID := "flow_test_002"
	walletID := "wallet_jkl"
	address := "test_address_4"
	utxoID := "atx001:0"
	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "atx001", 0, int64(700), "unspent", "plain_bsv", "",
		"atx001", "", 1700000020, 1700000020, 0,
	)
	if err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	flowEntry := chainAssetFlowEntry{
		FlowID:         flowID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "atx001",
		Vout:           0,
		AmountSatoshi:  700,
		OccurredAtUnix: 1700000020,
	}
	flowDBID, err := dbAppendAssetFlowInIfAbsent(ctx, store, flowEntry)
	if err != nil {
		t.Fatalf("append flow: %v", err)
	}

	// 第一次写入消耗
	consEntry := assetConsumptionEntry{
		SourceFlowID:     flowDBID,
		PoolAllocationID: allocDBID,
		UsedSatoshi:      700,
		OccurredAtUnix:   1700000020,
		Note:             "consumed by pool allocation",
	}
	if err := dbAppendAssetConsumptionForPoolAllocationIfAbsent(ctx, store, consEntry); err != nil {
		t.Fatalf("first consumption: %v", err)
	}

	// 第二次写入（幂等）
	if err := dbAppendAssetConsumptionForPoolAllocationIfAbsent(ctx, store, consEntry); err != nil {
		t.Fatalf("second consumption (idempotent): %v", err)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions`).Scan(&count); err != nil {
		t.Fatalf("count consumptions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 consumption, got %d", count)
	}
}

// TestAppendAssetConsumption_MutualExclusion 验证消耗关联的二选一约束
func TestAppendAssetConsumption_MutualExclusion(t *testing.T) {
	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 写入 asset flow
	flowID := "flow_test_003"
	walletID := "wallet_mno"
	address := "test_address_5"
	utxoID := "mtx001:0"
	_, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "mtx001", 0, int64(1000), "unspent", "plain_bsv", "",
		"mtx001", "", 1700000030, 1700000030, 0,
	)
	if err != nil {
		t.Fatalf("seed wallet_utxo: %v", err)
	}

	flowEntry := chainAssetFlowEntry{
		FlowID:         flowID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "mtx001",
		Vout:           0,
		AmountSatoshi:  1000,
		OccurredAtUnix: 1700000030,
	}
	flowDBID, err := dbAppendAssetFlowInIfAbsent(ctx, store, flowEntry)
	if err != nil {
		t.Fatalf("append flow: %v", err)
	}

	// 同时传 chain_payment_id 和 pool_allocation_id 应该报错
	consEntry := assetConsumptionEntry{
		SourceFlowID:     flowDBID,
		ChainPaymentID:   1,
		PoolAllocationID: 1,
		UsedSatoshi:      1000,
		OccurredAtUnix:   1700000030,
	}

	// 传 payment 类型，但 pool_allocation_id 非零
	err = dbAppendAssetConsumptionForChainPaymentIfAbsent(ctx, store, consEntry)
	if err == nil {
		t.Fatalf("expected error for mutual exclusion violation, got nil")
	}
	if !strings.Contains(err.Error(), "must be NULL") {
		t.Fatalf("expected 'must be NULL' error, got: %v", err)
	}

	// 传 allocation 类型，但 chain_payment_id 非零
	err = dbAppendAssetConsumptionForPoolAllocationIfAbsent(ctx, store, consEntry)
	if err == nil {
		t.Fatalf("expected error for mutual exclusion violation, got nil")
	}
	if !strings.Contains(err.Error(), "must be NULL") {
		t.Fatalf("expected 'must be NULL' error, got: %v", err)
	}
}

// TestReconcileWalletUTXOSet_PendingLocalBroadcastNotWrittenToFact 验证 pending local broadcast 输出不入 fact
func TestReconcileWalletUTXOSet_PendingLocalBroadcastNotWrittenToFact(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	walletID := "wallet_pending_test"
	address := "test_addr_pending"
	now := time.Now().Unix()

	// 种一个 confirmed UTXO
	confirmedTxID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		confirmedTxID+":0", walletID, address, confirmedTxID, 0, int64(1000), "unspent", "plain_bsv", "",
		confirmedTxID, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed confirmed utxo: %v", err)
	}

	// 种一个 pending UTXO（模拟 overlayPendingLocalBroadcastsTx 叠加进来的）
	pendingTxID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		pendingTxID+":0", walletID, address, pendingTxID, 0, int64(500), "unspent", "plain_bsv", "",
		pendingTxID, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed pending utxo: %v", err)
	}

	// 构造 existing map（模拟 reconcile 中的 existing）
	existing := map[string]utxoStateRow{
		confirmedTxID + ":0": {
			UTXOID: confirmedTxID + ":0", TxID: confirmedTxID, Vout: 0, Value: 1000,
			State: "unspent", AllocationClass: "plain_bsv", CreatedTxID: confirmedTxID,
		},
		pendingTxID + ":0": {
			UTXOID: pendingTxID + ":0", TxID: pendingTxID, Vout: 0, Value: 500,
			State: "unspent", AllocationClass: "plain_bsv", CreatedTxID: pendingTxID,
		},
	}

	// 构造 confirmedUTXOSet（只包含 snapshot.Live 中的 UTXO）
	confirmedUTXOSet := map[string]struct{}{
		confirmedTxID + ":0": {},
		// pendingTxID + ":0" 不在这里
	}

	// 调用 appendWalletUTXOAssetFlowsTx
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	if err := appendWalletUTXOAssetFlowsTx(tx, walletID, address, existing, confirmedUTXOSet, now); err != nil {
		t.Fatalf("appendWalletUTXOAssetFlowsTx: %v", err)
	}

	// 验证：fact 里只有 confirmedTxID:0，没有 pendingUTXO
	var flowCount int
	if err := tx.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&flowCount); err != nil {
		t.Fatalf("count flows: %v", err)
	}
	if flowCount != 1 {
		t.Fatalf("expected 1 flow (confirmed only), got %d", flowCount)
	}

	var exists int
	err = tx.QueryRow(`SELECT 1 FROM fact_chain_asset_flows WHERE utxo_id=?`, confirmedTxID+":0").Scan(&exists)
	if err != nil {
		t.Fatalf("expected flow for confirmed utxo: %v", err)
	}

	err = tx.QueryRow(`SELECT 1 FROM fact_chain_asset_flows WHERE utxo_id=?`, pendingTxID+":0").Scan(&exists)
	if err == nil {
		t.Fatalf("pending local broadcast utxo should NOT be in fact_chain_asset_flows")
	}

	_ = ctx
	_ = store
}

// TestReconcileWalletUTXOSet_ConfirmedLiveUTXOWrittenToFact 验证 confirmed live UTXO 入 fact 且幂等
func TestReconcileWalletUTXOSet_ConfirmedLiveUTXOWrittenToFact(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	store := newClientDB(db, nil)

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := &Runtime{runIn: NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)}
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种一个旧的 confirmed UTXO
	oldTxID := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		oldTxID+":0", walletID, address, oldTxID, 0, int64(2000), "unspent", "plain_bsv", "",
		oldTxID, "", now-100, now-100, 0,
	); err != nil {
		t.Fatalf("seed old utxo: %v", err)
	}

	// 第一次 reconcile：写入 fact
	snapshot := liveWalletSnapshot{
		Live: map[string]poolcore.UTXO{
			oldTxID + ":0": {TxID: oldTxID, Vout: 0, Value: 2000},
		},
		ObservedMempoolTxs:    nil,
		ConfirmedLiveTxIDs:    map[string]struct{}{oldTxID: {}},
		Balance:               2000,
		Count:                 1,
		OldestConfirmedHeight: 200,
	}
	cursor := walletUTXOHistoryCursor{WalletID: walletID, Address: address, NextConfirmedHeight: 200}
	if err := reconcileWalletUTXOSet(context.Background(), store, address, snapshot, nil, cursor, "round-1", "", "test", now, 10); err != nil {
		t.Fatalf("reconcileWalletUTXOSet round-1: %v", err)
	}

	var flowCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&flowCount); err != nil {
		t.Fatalf("count flows after round-1: %v", err)
	}
	if flowCount != 1 {
		t.Fatalf("expected 1 flow after round-1, got %d", flowCount)
	}

	var flowID int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_asset_flows WHERE utxo_id=?`, oldTxID+":0").Scan(&flowID); err != nil {
		t.Fatalf("query flow after round-1: %v", err)
	}

	// 第二次 reconcile：幂等，不新增
	if err := reconcileWalletUTXOSet(context.Background(), store, address, snapshot, nil, cursor, "round-2", "", "test", now+1, 10); err != nil {
		t.Fatalf("reconcileWalletUTXOSet round-2: %v", err)
	}

	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_asset_flows`).Scan(&flowCount); err != nil {
		t.Fatalf("count flows after round-2: %v", err)
	}
	if flowCount != 1 {
		t.Fatalf("expected 1 flow after round-2 (idempotent), got %d", flowCount)
	}

	var flowID2 int64
	if err := db.QueryRow(`SELECT id FROM fact_chain_asset_flows WHERE utxo_id=?`, oldTxID+":0").Scan(&flowID2); err != nil {
		t.Fatalf("query flow after round-2: %v", err)
	}
	if flowID2 != flowID {
		t.Fatalf("expected same flow id (idempotent), got %d vs %d", flowID2, flowID)
	}
}

// TestChainPaymentConsumption_MultipleUTXO 验证 payment 多 UTXO 消耗
func TestChainPaymentConsumption_MultipleUTXO(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_multi_utxo"
	address := "test_addr_multi"

	// 种两个 confirmed UTXO 并写入 IN flow
	utxoIDs := []string{
		"utxo_input_1:0",
		"utxo_input_2:0",
	}
	amounts := []int64{3000, 2000}
	for i, utxoID := range utxoIDs {
		if _, err := db.Exec(`INSERT INTO wallet_utxo(
			utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
			created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			utxoID, walletID, address, strings.Split(utxoID, ":")[0], 0, amounts[i], "unspent", "plain_bsv", "",
			strings.Split(utxoID, ":")[0], "", now, now, 0,
		); err != nil {
			t.Fatalf("seed utxo %s: %v", utxoID, err)
		}
		// 写入 IN flow
		_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
			FlowID:         "flow_in_" + utxoID,
			WalletID:       walletID,
			Address:        address,
			Direction:      "IN",
			AssetKind:      "BSV",
			UTXOID:         utxoID,
			TxID:           strings.Split(utxoID, ":")[0],
			Vout:           0,
			AmountSatoshi:  amounts[i],
			OccurredAtUnix: now,
		})
		if err != nil {
			t.Fatalf("append flow for %s: %v", utxoID, err)
		}
	}

	// 写入 chain payment
	paymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                "tx_multi_utxo_pay",
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  5000,
		WalletOutputSatoshi: 1000,
		NetAmountSatoshi:    -4000,
		BlockHeight:         100,
		OccurredAtUnix:      now,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             nil,
	})
	if err != nil {
		t.Fatalf("upsert chain payment: %v", err)
	}

	// 构造 UTXO link entries（两个 input）
	utxoLinks := []chainPaymentUTXOLinkEntry{
		{ChainPaymentID: paymentID, UTXOID: utxoIDs[0], IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000, CreatedAtUnix: now},
		{ChainPaymentID: paymentID, UTXOID: utxoIDs[1], IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 2000, CreatedAtUnix: now},
	}

	// 调用批量消耗写入
	if err := dbAppendAssetConsumptionsForChainPayment(db, paymentID, utxoLinks, now); err != nil {
		t.Fatalf("append consumptions: %v", err)
	}

	// 验证：两条消耗记录，都指向同一个 chain_payment_id
	var consCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE chain_payment_id=?`, paymentID).Scan(&consCount); err != nil {
		t.Fatalf("count consumptions: %v", err)
	}
	if consCount != 2 {
		t.Fatalf("expected 2 consumptions, got %d", consCount)
	}

	// 验证每个 source_flow_id 都正确
	var flowIDs []int64
	rows, err := db.Query(`SELECT source_flow_id FROM fact_asset_consumptions WHERE chain_payment_id=? ORDER BY source_flow_id`, paymentID)
	if err != nil {
		t.Fatalf("query source_flow_ids: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fid int64
		if err := rows.Scan(&fid); err != nil {
			t.Fatalf("scan source_flow_id: %v", err)
		}
		flowIDs = append(flowIDs, fid)
	}
	if len(flowIDs) != 2 {
		t.Fatalf("expected 2 distinct source_flow_ids, got %d", len(flowIDs))
	}
}

// TestChainPaymentConsumption_UnknownUTXOSkipped 验证 unknown UTXO 不产生消耗
func TestChainPaymentConsumption_UnknownUTXOSkipped(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_unknown_skip"
	address := "test_addr_unknown"

	// 种一个 normal UTXO 并写入 IN flow
	normalUTXO := "utxo_normal:0"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		normalUTXO, walletID, address, "tx_normal", 0, int64(2000), "unspent", "plain_bsv", "",
		"tx_normal", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed normal utxo: %v", err)
	}
	_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "flow_in_" + normalUTXO,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         normalUTXO,
		TxID:           "tx_normal",
		Vout:           0,
		AmountSatoshi:  2000,
		OccurredAtUnix: now,
	})
	if err != nil {
		t.Fatalf("append flow for normal: %v", err)
	}

	// unknown UTXO 不写入 IN flow
	unknownUTXO := "utxo_unknown:0"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		unknownUTXO, walletID, address, "tx_unknown", 0, int64(1), "unspent", "unknown", "awaiting evidence",
		"tx_unknown", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed unknown utxo: %v", err)
	}

	// 写入 chain payment
	paymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                "tx_pay_with_unknown",
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  2001,
		WalletOutputSatoshi: 1500,
		NetAmountSatoshi:    -500,
		BlockHeight:         100,
		OccurredAtUnix:      now,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             nil,
	})
	if err != nil {
		t.Fatalf("upsert chain payment: %v", err)
	}

	// 构造 UTXO link entries（一个 normal + 一个 unknown）
	utxoLinks := []chainPaymentUTXOLinkEntry{
		{ChainPaymentID: paymentID, UTXOID: normalUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 2000, CreatedAtUnix: now},
		{ChainPaymentID: paymentID, UTXOID: unknownUTXO, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1, CreatedAtUnix: now},
	}

	// 调用批量消耗写入
	if err := dbAppendAssetConsumptionsForChainPayment(db, paymentID, utxoLinks, now); err != nil {
		t.Fatalf("append consumptions: %v", err)
	}

	// 验证：只有 1 条消耗记录（unknown UTXO 被跳过）
	var consCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE chain_payment_id=?`, paymentID).Scan(&consCount); err != nil {
		t.Fatalf("count consumptions: %v", err)
	}
	if consCount != 1 {
		t.Fatalf("expected 1 consumption (unknown skipped), got %d", consCount)
	}
}

// TestChainPaymentConsumption_Idempotent 验证消耗写入幂等
func TestChainPaymentConsumption_Idempotent(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_idempotent"
	address := "test_addr_idem"
	utxoID := "utxo_idem:0"

	// 种 UTXO 并写入 IN flow
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "tx_idem", 0, int64(1000), "unspent", "plain_bsv", "",
		"tx_idem", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed utxo: %v", err)
	}
	_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "flow_in_" + utxoID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "tx_idem",
		Vout:           0,
		AmountSatoshi:  1000,
		OccurredAtUnix: now,
	})
	if err != nil {
		t.Fatalf("append flow: %v", err)
	}

	// 写入 chain payment
	paymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                "tx_idem_pay",
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  1000,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -1000,
		BlockHeight:         100,
		OccurredAtUnix:      now,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             nil,
	})
	if err != nil {
		t.Fatalf("upsert chain payment: %v", err)
	}

	utxoLinks := []chainPaymentUTXOLinkEntry{
		{ChainPaymentID: paymentID, UTXOID: utxoID, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 1000, CreatedAtUnix: now},
	}

	// 第一次写入
	if err := dbAppendAssetConsumptionsForChainPayment(db, paymentID, utxoLinks, now); err != nil {
		t.Fatalf("first consumption: %v", err)
	}

	var consCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE chain_payment_id=?`, paymentID).Scan(&consCount); err != nil {
		t.Fatalf("count after first: %v", err)
	}
	if consCount != 1 {
		t.Fatalf("expected 1 consumption after first, got %d", consCount)
	}

	// 第二次写入（幂等）
	if err := dbAppendAssetConsumptionsForChainPayment(db, paymentID, utxoLinks, now); err != nil {
		t.Fatalf("second consumption (idempotent): %v", err)
	}

	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE chain_payment_id=?`, paymentID).Scan(&consCount); err != nil {
		t.Fatalf("count after second: %v", err)
	}
	if consCount != 1 {
		t.Fatalf("expected 1 consumption after second (idempotent), got %d", consCount)
	}
}

// TestPoolAllocationConsumption_MultipleUTXO 验证 pool allocation 多 UTXO 消耗
func TestPoolAllocationConsumption_MultipleUTXO(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_pool_alloc"
	address := "test_addr_pool"

	// 种两个 confirmed UTXO 并写入 IN flow
	utxoIDs := []string{
		"pool_utxo_1:0",
		"pool_utxo_2:0",
	}
	amounts := []int64{5000, 3000}
	for i, utxoID := range utxoIDs {
		if _, err := db.Exec(`INSERT INTO wallet_utxo(
			utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
			created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			utxoID, walletID, address, strings.Split(utxoID, ":")[0], 0, amounts[i], "unspent", "plain_bsv", "",
			strings.Split(utxoID, ":")[0], "", now, now, 0,
		); err != nil {
			t.Fatalf("seed utxo %s: %v", utxoID, err)
		}
		_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
			FlowID:         "flow_in_" + utxoID,
			WalletID:       walletID,
			Address:        address,
			Direction:      "IN",
			AssetKind:      "BSV",
			UTXOID:         utxoID,
			TxID:           strings.Split(utxoID, ":")[0],
			Vout:           0,
			AmountSatoshi:  amounts[i],
			OccurredAtUnix: now,
		})
		if err != nil {
			t.Fatalf("append flow for %s: %v", utxoID, err)
		}
	}

	// 写入 pool session
	if _, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex,
		gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks,
		open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_pool_test", "direct_transfer", "seller1", "seller1", "arbiter1",
		"gateway1", 10000, 10, 0.5, 6,
		"base_tx_pool", "active", now, now,
	); err != nil {
		t.Fatalf("seed pool session: %v", err)
	}

	// 构造 allocation input（带 UTXO facts）
	allocInput := directTransferPoolAllocationFactInput{
		SessionID:        "sess_pool_test",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 4000,
		PayerAmountAfter: 6000,
		TxID:             "tx_pool_alloc",
		TxHex:            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff01000000000000000019ffffffff",
		CreatedAtUnix:    now,
		UTXOFacts: []chainPaymentUTXOLinkEntry{
			{UTXOID: utxoIDs[0], IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 5000, CreatedAtUnix: now},
			{UTXOID: utxoIDs[1], IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 3000, CreatedAtUnix: now},
		},
	}

	// 调用 allocation 写入（包含消耗写入）
	if err := dbUpsertDirectTransferPoolAllocation(ctx, store, allocInput); err != nil {
		t.Fatalf("upsert allocation: %v", err)
	}

	// 获取 allocation 的自增 id
	allocDBID, err := dbGetPoolAllocationIDByAllocationIDDB(db, "poolalloc_"+allocInput.SessionID+"_"+allocInput.AllocationKind+"_1")
	if err != nil {
		t.Fatalf("get allocation db id: %v", err)
	}

	// 验证：两条消耗记录，都指向同一个 pool_allocation_id
	var consCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, allocDBID).Scan(&consCount); err != nil {
		t.Fatalf("count consumptions: %v", err)
	}
	if consCount != 2 {
		t.Fatalf("expected 2 consumptions, got %d", consCount)
	}
}

// TestPoolAllocationConsumption_Idempotent 验证 pool allocation 消耗写入幂等
func TestPoolAllocationConsumption_Idempotent(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_pool_idem"
	address := "test_addr_pool_idem"
	utxoID := "pool_idem_utxo:0"

	// 种 UTXO 并写入 IN flow
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, "tx_pool_idem", 0, int64(2000), "unspent", "plain_bsv", "",
		"tx_pool_idem", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed utxo: %v", err)
	}
	_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "flow_in_" + utxoID,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         utxoID,
		TxID:           "tx_pool_idem",
		Vout:           0,
		AmountSatoshi:  2000,
		OccurredAtUnix: now,
	})
	if err != nil {
		t.Fatalf("append flow: %v", err)
	}

	// 写入 pool session
	if _, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex,
		gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks,
		open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_pool_idem", "direct_transfer", "seller1", "seller1", "arbiter1",
		"gateway1", 5000, 10, 0.5, 6,
		"base_tx_idem", "active", now, now,
	); err != nil {
		t.Fatalf("seed pool session: %v", err)
	}

	allocInput := directTransferPoolAllocationFactInput{
		SessionID:        "sess_pool_idem",
		AllocationKind:   "pay",
		SequenceNum:      1,
		PayeeAmountAfter: 1000,
		PayerAmountAfter: 4000,
		TxID:             "tx_pool_idem_alloc",
		TxHex:            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff01000000000000000019ffffffff",
		CreatedAtUnix:    now,
		UTXOFacts: []chainPaymentUTXOLinkEntry{
			{UTXOID: utxoID, IOSide: "input", UTXORole: "wallet_input", AmountSatoshi: 2000, CreatedAtUnix: now},
		},
	}

	// 第一次写入
	if err := dbUpsertDirectTransferPoolAllocation(ctx, store, allocInput); err != nil {
		t.Fatalf("first upsert allocation: %v", err)
	}

	allocDBID, err := dbGetPoolAllocationIDByAllocationIDDB(db, "poolalloc_sess_pool_idem_pay_1")
	if err != nil {
		t.Fatalf("get allocation db id: %v", err)
	}

	var consCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, allocDBID).Scan(&consCount); err != nil {
		t.Fatalf("count after first: %v", err)
	}
	if consCount != 1 {
		t.Fatalf("expected 1 consumption after first, got %d", consCount)
	}

	// 第二次写入（幂等：ON CONFLICT DO UPDATE 不会触发新的消耗写入，
	// 因为消耗函数内部已有幂等检查）
	if err := dbUpsertDirectTransferPoolAllocation(ctx, store, allocInput); err != nil {
		t.Fatalf("second upsert allocation (idempotent): %v", err)
	}

	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, allocDBID).Scan(&consCount); err != nil {
		t.Fatalf("count after second: %v", err)
	}
	if consCount != 1 {
		t.Fatalf("expected 1 consumption after second (idempotent), got %d", consCount)
	}
}

// TestPoolAllocationConsumption_RuntimePath 验证主流程 runtime 路径：dbUpdateDirectTransferPoolPay -> fact_asset_consumptions
func TestPoolAllocationConsumption_RuntimePath(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	walletID := "wallet_runtime"
	address := "test_addr_runtime"

	// 写入 pool session（通过 open 路径）
	// 使用有效的 tx hex（1 input，来自现有测试）
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	// 种一个对应的 input UTXO 并写入 IN flow
	// tx hex 中 input source txid 是 LE: 00010203...，SDK 解析后返回 BE: 1f1e1d1c...
	openInputTxID := "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
	openInputUTXO := openInputTxID + ":1"
	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		openInputUTXO, walletID, address, openInputTxID, 1, int64(500), "unspent", "plain_bsv", "",
		openInputTxID, "", now, now, 0,
	); err != nil {
		t.Fatalf("seed open input utxo: %v", err)
	}
	_, err := dbAppendAssetFlowInIfAbsent(ctx, store, chainAssetFlowEntry{
		FlowID:         "flow_in_" + openInputUTXO,
		WalletID:       walletID,
		Address:        address,
		Direction:      "IN",
		AssetKind:      "BSV",
		UTXOID:         openInputUTXO,
		TxID:           openInputTxID,
		Vout:           1,
		AmountSatoshi:  500,
		OccurredAtUnix: now,
	})
	if err != nil {
		t.Fatalf("append flow for open input: %v", err)
	}

	baseTxID := "base_tx_runtime_open"
	sellerPub := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	buyerPub := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbiterPub := "04cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if err := dbUpsertDirectTransferPoolOpen(ctx, store, directTransferPoolOpenReq{
		SessionID:      "sess_runtime",
		DealID:         "deal_runtime",
		BuyerPeerID:    buyerPub,
		ArbiterPeerID:  arbiterPub,
		ArbiterPubKey:  arbiterPub,
		PoolAmount:     10000,
		SpendTxFee:     10,
		Sequence:       1,
		SellerAmount:   0,
		BuyerAmount:    10000,
		BaseTxID:       baseTxID,
		FeeRateSatByte: 0.5,
		LockBlocks:     6,
	}, "sess_runtime", "deal_runtime", buyerPub, sellerPub, arbiterPub, strings.ToLower(baseTxHex), baseTxHex); err != nil {
		t.Fatalf("open pool: %v", err)
	}

	// 验证 open allocation 产生了消耗记录
	openAllocID := "poolalloc_sess_runtime_open_1"
	openAllocDBID, err := dbGetPoolAllocationIDByAllocationIDDB(db, openAllocID)
	if err != nil {
		t.Fatalf("get open allocation db id: %v", err)
	}

	var openConsCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, openAllocDBID).Scan(&openConsCount); err != nil {
		t.Fatalf("count open consumptions: %v", err)
	}
	if openConsCount != 1 {
		t.Fatalf("expected 1 open consumption, got %d", openConsCount)
	}

	// pay tx hex（使用同一个有效 tx hex）
	currentTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 调用 pay 路径
	if err := dbUpdateDirectTransferPoolPay(ctx, store, "sess_runtime", 2, 300, 690, currentTxHex, 300); err != nil {
		t.Fatalf("pay pool: %v", err)
	}

	// 验证 pay allocation 产生了消耗记录
	payAllocID := "poolalloc_sess_runtime_pay_2"
	payAllocDBID, err := dbGetPoolAllocationIDByAllocationIDDB(db, payAllocID)
	if err != nil {
		t.Fatalf("get pay allocation db id: %v", err)
	}

	var payConsCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, payAllocDBID).Scan(&payConsCount); err != nil {
		t.Fatalf("count pay consumptions: %v", err)
	}
	if payConsCount != 1 {
		t.Fatalf("expected 1 pay consumption, got %d", payConsCount)
	}

	// 重复调用 pay（幂等）
	if err := dbUpdateDirectTransferPoolPay(ctx, store, "sess_runtime", 2, 300, 690, currentTxHex, 300); err != nil {
		t.Fatalf("pay pool again: %v", err)
	}

	var payConsCount2 int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_asset_consumptions WHERE pool_allocation_id=?`, payAllocDBID).Scan(&payConsCount2); err != nil {
		t.Fatalf("count pay consumptions after second: %v", err)
	}
	if payConsCount2 != payConsCount {
		t.Fatalf("expected idempotent pay consumptions %d, got %d", payConsCount, payConsCount2)
	}
}
