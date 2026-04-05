package clientapp

import (
	"context"
	"testing"
)

// TestDbUpsertChainPayment_Idempotent 验证 fact_chain_payments upsert 幂等性
// 第二步整改：同一个 txid 重复 upsert 不会产生多条记录
func TestDbUpsertChainPayment_Idempotent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_idem_test_123"

	// 第一次写入
	id1, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_in",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 1000,
		NetAmountSatoshi:    1000,
		BlockHeight:         100,
		OccurredAtUnix:      1700000001,
		FromPartyID:         "external:unknown",
		ToPartyID:           "wallet:self",
		Payload:             map[string]any{"test": 1},
	})
	if err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}
	if id1 <= 0 {
		t.Fatalf("expected positive id, got %d", id1)
	}

	// 第二次写入（相同 txid）- 应该返回相同 id
	id2, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_in",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 2000, // 修改金额
		NetAmountSatoshi:    2000,
		BlockHeight:         101, // 修改高度
		OccurredAtUnix:      1700000002,
		FromPartyID:         "external:unknown",
		ToPartyID:           "wallet:self",
		Payload:             map[string]any{"test": 2},
	})
	if err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected same id (%d), got %d", id1, id2)
	}

	// 验证只有一条记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_chain_payments WHERE txid=?`, txid).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	// 验证记录已被更新
	var walletOut int64
	if err := db.QueryRow(`SELECT wallet_output_satoshi FROM fact_chain_payments WHERE id=?`, id1).Scan(&walletOut); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if walletOut != 2000 {
		t.Fatalf("expected wallet_output_satoshi=2000, got %d", walletOut)
	}
}

// TestDbGetChainPaymentByTxID 验证按 txid 查询 chain_payment id
func TestDbGetChainPaymentByTxID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_lookup_test_456"

	// 先写入
	id, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  5000,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -5000,
		BlockHeight:         200,
		OccurredAtUnix:      1700000003,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             map[string]any{"test": 3},
	})
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}

	// 查询
	foundID, err := dbGetChainPaymentByTxID(ctx, store, txid)
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if foundID != id {
		t.Fatalf("expected id %d, got %d", id, foundID)
	}

	// 查询不存在的 txid
	_, err = dbGetChainPaymentByTxID(ctx, store, "nonexistent_txid")
	if err == nil {
		t.Fatal("expected error for nonexistent txid")
	}
}

// TestDbAppendChainPaymentUTXOLinkIfAbsent_Idempotent 验证 utxo link 幂等性
func TestDbAppendChainPaymentUTXOLinkIfAbsent_Idempotent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 先创建 chain_payment
	paymentID, err := dbUpsertChainPayment(ctx, store, chainPaymentEntry{
		TxID:                "tx_utxo_link_test",
		PaymentSubType:      "external_in",
		Status:              "confirmed",
		WalletInputSatoshi:  0,
		WalletOutputSatoshi: 3000,
		NetAmountSatoshi:    3000,
		BlockHeight:         300,
		OccurredAtUnix:      1700000004,
		FromPartyID:         "external:unknown",
		ToPartyID:           "wallet:self",
		Payload:             map[string]any{},
	})
	if err != nil {
		t.Fatalf("upsert chain_payment failed: %v", err)
	}

	// 需要先插入 wallet_utxo 记录（因为外键约束）
	_, err = db.Exec(`INSERT INTO wallet_utxo(
		utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
		created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"utxo_test_1:0", "wallet1", "addr1", "tx_utxo_link_test", 0, 3000, "confirmed", "plain_bsv", "",
		"tx_utxo_link_test", "", 1700000004, 1700000004, 0,
	)
	if err != nil {
		t.Fatalf("insert wallet_utxo failed: %v", err)
	}

	// 第一次写入 link
	if err := dbAppendChainPaymentUTXOLinkIfAbsent(ctx, store, chainPaymentUTXOLinkEntry{
		ChainPaymentID: paymentID,
		UTXOID:         "utxo_test_1:0",
		IOSide:         "output",
		UTXORole:       "external_in",
		AmountSatoshi:  3000,
		CreatedAtUnix:  1700000004,
		Note:           "test utxo link",
		Payload:        map[string]any{"idx": 1},
	}); err != nil {
		t.Fatalf("first link append failed: %v", err)
	}

	// 第二次写入（相同 link）- 应该无错误且不重复
	if err := dbAppendChainPaymentUTXOLinkIfAbsent(ctx, store, chainPaymentUTXOLinkEntry{
		ChainPaymentID: paymentID,
		UTXOID:         "utxo_test_1:0",
		IOSide:         "output",
		UTXORole:       "external_in",
		AmountSatoshi:  3000,
		CreatedAtUnix:  1700000005,
		Note:           "duplicate link",
		Payload:        map[string]any{"idx": 2},
	}); err != nil {
		t.Fatalf("second link append failed: %v", err)
	}

	// 验证只有一条 link 记录
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE chain_payment_id=? AND utxo_id=?`,
		paymentID, "utxo_test_1:0",
	).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 link record, got %d", count)
	}
}

// TestDbGetPoolAllocationIDByAllocationID 验证按 allocation_id 查自增 id
func TestDbGetPoolAllocationIDByAllocationID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	// 先创建 pool_session
	sessionID := "sess_alloc_lookup_1"
	_, err := db.Exec(`INSERT INTO fact_pool_sessions(
		pool_session_id, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex, gateway_pubkey_hex,
		pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sessionID, "2of3", "", "", "", "", 1000, 10, 0.5, 6, "base_tx_1", "active", 1700000001, 1700000001,
	)
	if err != nil {
		t.Fatalf("insert pool_session failed: %v", err)
	}

	// 创建 pool_allocation
	allocationID := "poolalloc_" + sessionID + "_open_1"
	_, err = db.Exec(`INSERT INTO fact_pool_session_events(
		allocation_id, pool_session_id, allocation_no, allocation_kind, sequence_num,
		payee_amount_after, payer_amount_after, txid, tx_hex, created_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		allocationID, sessionID, 1, "open", 1, 0, 1000, "tx_1", "hex_1", 1700000001,
	)
	if err != nil {
		t.Fatalf("insert pool_allocation failed: %v", err)
	}

	// 查询自增 id
	id, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// 查询不存在的 allocation_id
	_, err = dbGetPoolAllocationIDByAllocationID(ctx, store, "nonexistent_alloc")
	if err == nil {
		t.Fatal("expected error for nonexistent allocation_id")
	}
}
