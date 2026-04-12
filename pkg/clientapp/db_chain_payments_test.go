package clientapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// TestDbUpsertChainPayment_Idempotent 验证 fact_settlement_channel_chain_quote_pay upsert 幂等性
// 第二步整改：同一个 txid 重复 upsert 不会产生多条记录
func TestDbUpsertChainPayment_Idempotent(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_idem_test_123"

	// 第一次写入
	id1, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
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
	id2, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txid).Scan(&count); err != nil {
		t.Fatalf("count check failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=? AND state='confirmed'`, fmt.Sprintf("%d", id1)).Scan(&cycleCount); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 confirmed settlement cycle, got %d", cycleCount)
	}

	// 验证记录已被更新
	var walletOut int64
	if err := db.QueryRow(`SELECT wallet_output_satoshi FROM fact_settlement_channel_chain_quote_pay WHERE id=?`, id1).Scan(&walletOut); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if walletOut != 2000 {
		t.Fatalf("expected wallet_output_satoshi=2000, got %d", walletOut)
	}
}

// TestDbUpsertChainPayment_BackfillsMissingSettlementCycle 验证已有 chain_payment 但缺 cycle 时可自愈补齐。
func TestDbUpsertChainPayment_BackfillsMissingSettlementCycle(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_backfill_cycle_001"

	id1, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  1200,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -1200,
		BlockHeight:         200,
		OccurredAtUnix:      1700000100,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             map[string]any{"round": 1},
	})
	if err != nil {
		t.Fatalf("seed upsert failed: %v", err)
	}

	var restoredTxID string
	if err := db.QueryRow(`SELECT txid FROM fact_settlement_channel_chain_quote_pay WHERE id=?`, id1).Scan(&restoredTxID); err != nil {
		t.Fatalf("query chain payment txid failed: %v", err)
	}

	// 模拟异常老数据：cycle 仍在，但 source_id 被污染。
	if _, err := db.Exec(`UPDATE fact_settlement_cycles SET source_id='broken_source' WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", id1)); err != nil {
		t.Fatalf("corrupt settlement cycle source_id failed: %v", err)
	}

	id2, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      "external_out",
		Status:              "confirmed",
		WalletInputSatoshi:  1500,
		WalletOutputSatoshi: 0,
		NetAmountSatoshi:    -1500,
		BlockHeight:         201,
		OccurredAtUnix:      1700000200,
		FromPartyID:         "wallet:self",
		ToPartyID:           "external:unknown",
		Payload:             map[string]any{"round": 2},
	})
	if err != nil {
		t.Fatalf("re-upsert failed: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected same id %d, got %d", id1, id2)
	}

	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", id1)).Scan(&cycleCount); err != nil {
		t.Fatalf("count settlement cycles failed: %v", err)
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 settlement cycle after backfill, got %d", cycleCount)
	}

	var cycleID string
	if err := db.QueryRow(`SELECT cycle_id FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", id1)).Scan(&cycleID); err != nil {
		t.Fatalf("query settlement cycle failed: %v", err)
	}
	wantCycleID := fmt.Sprintf("cycle_chain_quote_pay_%s", txid)
	if cycleID != wantCycleID {
		t.Fatalf("expected %s, got %s", wantCycleID, cycleID)
	}
}

// TestDbUpsertChainPayment_PreservesSubmitAndObservedTime_QuotePay 验证真实支付事实的两时间字段只增不减。
func TestDbUpsertChainPayment_PreservesSubmitAndObservedTime_QuotePay(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	txid := "tx_time_guard_001"

	id1, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
		TxID:                 txid,
		PaymentSubType:       "quote_pay",
		Status:               "submitted",
		WalletInputSatoshi:   1200,
		WalletOutputSatoshi:  900,
		NetAmountSatoshi:     -300,
		BlockHeight:          0,
		OccurredAtUnix:       1700001000,
		SubmittedAtUnix:      1700001000,
		WalletObservedAtUnix: 0,
		FromPartyID:          "wallet:buyer",
		ToPartyID:            "wallet:seller",
		Payload:              map[string]any{"quote_id": "q_1", "pay_id": "p_1"},
	})
	if err != nil {
		t.Fatalf("first upsert failed: %v", err)
	}
	if id1 <= 0 {
		t.Fatalf("expected positive id, got %d", id1)
	}

	id2, err := dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
		TxID:                 txid,
		PaymentSubType:       "quote_pay",
		Status:               "observed",
		WalletInputSatoshi:   1200,
		WalletOutputSatoshi:  900,
		NetAmountSatoshi:     -300,
		BlockHeight:          0,
		OccurredAtUnix:       1700000500,
		SubmittedAtUnix:      1700000500,
		WalletObservedAtUnix: 1700002000,
		FromPartyID:          "wallet:buyer",
		ToPartyID:            "wallet:seller",
		Payload:              map[string]any{"quote_id": "q_1", "pay_id": "p_1", "round": 2},
	})
	if err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected same id (%d), got %d", id1, id2)
	}

	var submittedAt int64
	var observedAt int64
	if err := db.QueryRow(`SELECT submitted_at_unix,wallet_observed_at_unix FROM fact_settlement_channel_chain_quote_pay WHERE id=?`, id1).Scan(&submittedAt, &observedAt); err != nil {
		t.Fatalf("query fact_settlement_channel_chain_quote_pay failed: %v", err)
	}
	if submittedAt != 1700001000 {
		t.Fatalf("submitted_at_unix mismatch: got=%d want=1700001000", submittedAt)
	}
	if observedAt != 1700002000 {
		t.Fatalf("wallet_observed_at_unix mismatch: got=%d want=1700002000", observedAt)
	}

	var cycleState string
	if err := db.QueryRow(`SELECT state FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", id1)).Scan(&cycleState); err != nil {
		t.Fatalf("query settlement cycle state failed: %v", err)
	}
	if cycleState != "pending" {
		t.Fatalf("expected pending settlement cycle before confirmation, got %s", cycleState)
	}

	_, err = dbUpsertChainPaymentWithSettlementCycle(ctx, store, chainPaymentEntry{
		TxID:                 txid,
		PaymentSubType:       "quote_pay",
		Status:               "confirmed",
		WalletInputSatoshi:   1200,
		WalletOutputSatoshi:  900,
		NetAmountSatoshi:     -300,
		BlockHeight:          0,
		OccurredAtUnix:       1700003000,
		SubmittedAtUnix:      1700003000,
		WalletObservedAtUnix: 1700003000,
		FromPartyID:          "wallet:buyer",
		ToPartyID:            "wallet:seller",
		Payload:              map[string]any{"quote_id": "q_1", "pay_id": "p_1", "round": 3},
	})
	if err != nil {
		t.Fatalf("third upsert failed: %v", err)
	}
	if err := db.QueryRow(`SELECT state, confirmed_at_unix FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", id1)).Scan(&cycleState, &observedAt); err != nil {
		t.Fatalf("query confirmed settlement cycle failed: %v", err)
	}
	if cycleState != "confirmed" {
		t.Fatalf("expected confirmed settlement cycle after confirmation, got %s", cycleState)
	}
	if observedAt != 1700003000 {
		t.Fatalf("confirmed_at_unix mismatch: got=%d want=1700003000", observedAt)
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

// TestDbAppendBusinessUTXOFactIfAbsent_Noop 验证旧 UTXO 占位入口不再落旧表。
func TestDbAppendBusinessUTXOFactIfAbsent_Noop(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	if err := dbAppendBusinessUTXOFactIfAbsent(db, "external_in"); err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	if err := dbAppendBusinessUTXOFactIfAbsent(db, "external_in"); err != nil {
		t.Fatalf("second append failed: %v", err)
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
	if err := dbUpsertSettlementCycle(db, "cycle_pool_session_quote_pay_"+sessionID, "pool_session_quote_pay", "pending:pool_session_quote_pay:"+sessionID, "pending", 0, 0, 0, 0, 1700000001, "test", nil); err != nil {
		t.Fatalf("seed settlement cycle failed: %v", err)
	}
	settlementCycleID, err := dbGetSettlementCycleBySource(db, "pool_session_quote_pay", "pending:pool_session_quote_pay:"+sessionID)
	if err != nil {
		t.Fatalf("lookup settlement cycle failed: %v", err)
	}
	_, err = db.Exec(`INSERT INTO fact_settlement_channel_pool_session_quote_pay(
		settlement_cycle_id, pool_session_id, txid, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex, gateway_pubkey_hex,
		pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		settlementCycleID, sessionID, "tx_1", "2of3", "", "", "", "", 1000, 10, 0.5, 6, "base_tx_1", "active", 1700000001, 1700000001,
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

func TestRecordChainPaymentAccountingAfterBroadcast_WritesBusinessFacts(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: strings.Repeat("4", 64),
		},
	}
	rt.runIn.BSV.Network = "test"

	txHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"
	txID := "tx_chain_payment_facts_001"
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		t.Fatalf("parse tx hex failed: %v", err)
	}
	for _, input := range parsed.Inputs {
		if input == nil || input.SourceTXID == nil {
			continue
		}
		inputUTXO := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
		if _, err := db.Exec(`INSERT INTO wallet_utxo(
			utxo_id, wallet_id, address, txid, vout, value_satoshi, state, allocation_class, allocation_reason,
			created_txid, spent_txid, created_at_unix, updated_at_unix, spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			inputUTXO, "wallet1", "addr1", strings.ToLower(strings.TrimSpace(input.SourceTXID.String())), int64(input.SourceTxOutIndex), 1000, "confirmed", "plain_bsv", "",
			strings.ToLower(strings.TrimSpace(input.SourceTXID.String())), "", 1700000001, 1700000001, 0,
		); err != nil {
			t.Fatalf("seed wallet_utxo failed: %v", err)
		}
	}

	if err := recordChainPaymentAccountingAfterBroadcast(ctx, store, rt, txHex, txID, "quote_pay", "quote_pay", "wallet:self", "external:unknown"); err != nil {
		t.Fatalf("record chain payment accounting failed: %v", err)
	}

	var paymentCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txID).Scan(&paymentCount); err != nil {
		t.Fatalf("query fact_settlement_channel_chain_quote_pay failed: %v", err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 fact_settlement_channel_chain_quote_pay row, got %d", paymentCount)
	}

	var cycleCount int
	var channelID int64
	if err := db.QueryRow(`SELECT id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txID).Scan(&channelID); err != nil {
		t.Fatalf("query channel id failed: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_cycles WHERE source_type='chain_quote_pay' AND source_id=?`, fmt.Sprintf("%d", channelID)).Scan(&cycleCount); err != nil {
		t.Fatalf("query fact_settlement_cycles failed: %v", err)
	}
	if cycleCount != 1 {
		t.Fatalf("expected 1 settlement cycle, got %d", cycleCount)
	}

	var businessCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_records WHERE business_id=?`, "biz_chain_payment_"+txID).Scan(&businessCount); err != nil {
		t.Fatalf("query settle_records failed: %v", err)
	}
	if businessCount != 1 {
		t.Fatalf("expected 1 settle_records row, got %d", businessCount)
	}

	var processCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM settle_process_events WHERE process_id=?`, "proc_chain_payment_"+txID).Scan(&processCount); err != nil {
		t.Fatalf("query settle_process_events failed: %v", err)
	}
	if processCount != 1 {
		t.Fatalf("expected 1 settle_process_events row, got %d", processCount)
	}
}
