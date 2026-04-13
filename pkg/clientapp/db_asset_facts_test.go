package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestBSVUTXOBasicCRUD 验证 BSV UTXO 基本增删改查
func TestBSVUTXOBasicCRUD(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	utxoID := "tx001:0"

	// 测试写入
	entry := bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: ownerPubkey,
		Address:        "1TestAddress123",
		TxID:           "tx001",
		Vout:           0,
		ValueSatoshi:   5000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
		Note:           "test utxo",
		Payload:        map[string]any{"test": true},
	}
	if err := dbUpsertBSVUTXO(ctx, store, entry); err != nil {
		t.Fatalf("upsert bsv utxo: %v", err)
	}

	// 测试读取
	got, err := dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("get bsv utxo: %v", err)
	}
	if got == nil {
		t.Fatal("expected utxo, got nil")
	}
	if got.ValueSatoshi != 5000 {
		t.Fatalf("expected value 5000, got %d", got.ValueSatoshi)
	}
	if got.UTXOState != "unspent" {
		t.Fatalf("expected state unspent, got %s", got.UTXOState)
	}

	// 测试幂等更新（重复写入同一 utxo_id）
	entry.ValueSatoshi = 6000
	if err := dbUpsertBSVUTXO(ctx, store, entry); err != nil {
		t.Fatalf("upsert bsv utxo (idempotent): %v", err)
	}
	got, err = dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("get bsv utxo after update: %v", err)
	}
	if got.ValueSatoshi != 6000 {
		t.Fatalf("expected updated value 6000, got %d", got.ValueSatoshi)
	}
}

// TestBSVUTXOMarkSpent 验证标记 UTXO 为已花费
func TestBSVUTXOMarkSpent(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	utxoID := "tx002:1"
	spentByTxid := "tx_spent_001"

	// 先写入 unspent UTXO
	entry := bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: ownerPubkey,
		Address:        "1TestAddress456",
		TxID:           "tx002",
		Vout:           1,
		ValueSatoshi:   10000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}
	if err := dbUpsertBSVUTXO(ctx, store, entry); err != nil {
		t.Fatalf("upsert bsv utxo: %v", err)
	}

	// 标记为 spent
	if err := dbMarkBSVUTXOSpent(ctx, store, utxoID, spentByTxid); err != nil {
		t.Fatalf("mark utxo spent: %v", err)
	}

	// 验证状态
	got, err := dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("get bsv utxo: %v", err)
	}
	if got.UTXOState != "spent" {
		t.Fatalf("expected state spent, got %s", got.UTXOState)
	}
	if got.SpentByTxid != spentByTxid {
		t.Fatalf("expected spent_by_txid %s, got %s", spentByTxid, got.SpentByTxid)
	}
	if got.SpentAtUnix == 0 {
		t.Fatal("expected spent_at_unix to be set")
	}
}

// TestBSVConsumptionsFromSettlementPaymentAttempt_IsIdempotent 验证 BSV 扣账只走 settlement_payment_attempt，重复回放不二次扣。
func TestBSVConsumptionsFromSettlementPaymentAttempt_IsIdempotent(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	utxoID := "tx_cycle_spend_001:0"
	txid := "tx_cycle_spend_001"
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: ownerPubkey,
		Address:        "1CycleSpendAddress",
		TxID:           txid,
		Vout:           0,
		ValueSatoshi:   7000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed bsv utxo failed: %v", err)
	}
	if err := dbUpsertSettlementPaymentAttempt(db, "payment_attempt_chain_bsv_"+txid, "chain_direct_pay", txid, "confirmed", 7000, 0, -7000, 0, now, "payment attempt spend test", nil); err != nil {
		t.Fatalf("seed settlement payment attempt failed: %v", err)
	}
	paymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_direct_pay", txid)
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_settlement_channel_chain_direct_pay(settlement_payment_attempt_id, txid, payment_subtype, status, wallet_input_satoshi, wallet_output_satoshi, net_amount_satoshi, block_height, occurred_at_unix, submitted_at_unix, wallet_observed_at_unix, from_party_id, to_party_id, payload_json, updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		paymentAttemptID, txid, "external_out", "confirmed", 7000, 0, -7000, 0, now, now, 0, "wallet:self", "external:unknown", "{}", now,
	); err != nil {
		t.Fatalf("seed chain direct pay channel row failed: %v", err)
	}

	facts := []chainPaymentUTXOLinkEntry{{
		UTXOID:        utxoID,
		IOSide:        "input",
		UTXORole:      "wallet_input",
		AmountSatoshi: 7000,
		CreatedAtUnix: now,
	}}
	if err := dbAppendBSVConsumptionsForSettlementPaymentAttempt(db, paymentAttemptID, facts, now); err != nil {
		t.Fatalf("first append bsv consumption failed: %v", err)
	}
	if err := dbAppendBSVConsumptionsForSettlementPaymentAttempt(db, paymentAttemptID, facts, now+1); err != nil {
		t.Fatalf("second append bsv consumption failed: %v", err)
	}

	got, err := dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("get bsv utxo failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected utxo, got nil")
	}
	if got.UTXOState != "spent" {
		t.Fatalf("expected spent state, got %s", got.UTXOState)
	}
	if got.SpentByTxid != txid {
		t.Fatalf("expected spent_by_txid %s, got %s", txid, got.SpentByTxid)
	}

	var recordCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fact_settlement_records WHERE settlement_payment_attempt_id=? AND asset_type='BSV' AND source_utxo_id=?`, paymentAttemptID, utxoID).Scan(&recordCount); err != nil {
		t.Fatalf("count settlement records failed: %v", err)
	}
	if recordCount != 1 {
		t.Fatalf("expected 1 settlement record, got %d", recordCount)
	}
}

// TestGetSettlementPaymentAttemptSourceTxID_PoolSessionReturnsLatestPoolEventTxID 验证 pool_session 也能反查到 txid 锚点。
func TestGetSettlementPaymentAttemptSourceTxID_PoolSessionReturnsLatestPoolEventTxID(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	now := time.Now().Unix()
	sessionID := "sess_pool_source_txid_001"
	txid := "pool_source_txid_001"

	if err := dbUpsertSettlementPaymentAttempt(db, "payment_attempt_pool_session_"+sessionID, "pool_session_quote_pay", sessionID, "confirmed", 0, 0, 0, 0, now, "pool session source txid test", nil); err != nil {
		t.Fatalf("seed settlement payment attempt failed: %v", err)
	}
	paymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, "pool_session_quote_pay", sessionID)
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO fact_settlement_channel_pool_session_quote_pay(settlement_payment_attempt_id, pool_session_id, txid, pool_scheme, counterparty_pubkey_hex, seller_pubkey_hex, arbiter_pubkey_hex, gateway_pubkey_hex, pool_amount_satoshi, spend_tx_fee_satoshi, fee_rate_sat_byte, lock_blocks, open_base_txid, status, created_at_unix, updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		paymentAttemptID, sessionID, txid, "2of3", "", "", "", "", 0, 0, 0, 0, "", "confirmed", now, now,
	); err != nil {
		t.Fatalf("seed pool session channel row failed: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO fact_pool_session_events(
			allocation_id,pool_session_id,allocation_no,allocation_kind,event_kind,sequence_num,state,direction,amount_satoshi,purpose,note,msg_id,cycle_index,payee_amount_after,payer_amount_after,txid,tx_hex,gateway_pubkey_hex,created_at_unix,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"alloc_pool_source_txid_001",
		sessionID,
		1,
		"pay",
		PoolFactEventKindPoolEvent,
		2,
		"confirmed",
		"out",
		300,
		"pool pay",
		"pool session source txid test",
		"msg_pool_source_txid_001",
		0,
		300,
		690,
		txid,
		"",
		"02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		now,
		"{}",
	); err != nil {
		t.Fatalf("seed pool session event failed: %v", err)
	}

	paymentAttemptID, err = dbGetSettlementPaymentAttemptBySource(db, "pool_session_quote_pay", sessionID)
	if err != nil {
		t.Fatalf("lookup settlement payment attempt failed: %v", err)
	}
	gotTxID, err := dbGetSettlementPaymentAttemptSourceTxID(db, paymentAttemptID)
	if err != nil {
		t.Fatalf("resolve pool session txid failed: %v", err)
	}
	if gotTxID != txid {
		t.Fatalf("expected txid %s, got %s", txid, gotTxID)
	}
}

// TestGetSettlementPaymentAttemptSourceTxID_MissingChannelRowFailsFast 验证渠道行缺失时直接报错，不再回退到旧口径。
func TestGetSettlementPaymentAttemptSourceTxID_MissingChannelRowFailsFast(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	now := time.Now().Unix()

	cases := []struct {
		name       string
		sourceType string
		sourceID   string
	}{
		{name: "chain_quote_pay", sourceType: "chain_quote_pay", sourceID: "missing_quote_txid"},
		{name: "chain_direct_pay", sourceType: "chain_direct_pay", sourceID: "missing_direct_txid"},
		{name: "chain_asset_create", sourceType: "chain_asset_create", sourceID: "missing_asset_txid"},
		{name: "pool_session_quote_pay", sourceType: "pool_session_quote_pay", sourceID: "missing_pool_session_id"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			paymentAttemptKey := "payment_attempt_missing_" + tc.name
			if err := dbUpsertSettlementPaymentAttempt(db, paymentAttemptKey, tc.sourceType, tc.sourceID, "confirmed", 0, 0, 0, 0, now, "missing channel row test", nil); err != nil {
				t.Fatalf("seed settlement payment attempt failed: %v", err)
			}
			paymentAttemptID, err := dbGetSettlementPaymentAttemptBySource(db, tc.sourceType, tc.sourceID)
			if err != nil {
				t.Fatalf("lookup settlement payment attempt failed: %v", err)
			}
			gotTxID, err := dbGetSettlementPaymentAttemptSourceTxID(db, paymentAttemptID)
			if err == nil {
				t.Fatalf("expected error, got txid=%s", gotTxID)
			}
			if !strings.Contains(err.Error(), "channel row not found") {
				t.Fatalf("expected channel row missing error, got %v", err)
			}
		})
	}
}

// TestBSVUTXOListSpendable 验证可花费 UTXO 列表
func TestBSVUTXOListSpendable(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	// 写入多个 UTXO（不同状态）
	entries := []bsvUTXOEntry{
		{
			UTXOID:         "tx003:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr1",
			TxID:           "tx003",
			Vout:           0,
			ValueSatoshi:   3000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			UTXOID:         "tx004:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr2",
			TxID:           "tx004",
			Vout:           0,
			ValueSatoshi:   5000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			// token_carrier 类型不应该出现在可花费列表中
			UTXOID:         "tx005:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr3",
			TxID:           "tx005",
			Vout:           0,
			ValueSatoshi:   1,
			UTXOState:      "unspent",
			CarrierType:    "token_carrier",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			// spent 状态不应该出现在可花费列表中
			UTXOID:         "tx006:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr4",
			TxID:           "tx006",
			Vout:           0,
			ValueSatoshi:   2000,
			UTXOState:      "spent",
			CarrierType:    "plain_bsv",
			SpentByTxid:    "tx_spent",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
			SpentAtUnix:    now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertBSVUTXO(ctx, store, e); err != nil {
			t.Fatalf("upsert utxo %s: %v", e.UTXOID, err)
		}
	}

	// 查询可花费列表
	spendable, err := dbListSpendableBSVUTXOs(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("list spendable utxos: %v", err)
	}

	// 只应该有 2 个 plain_bsv + unspent
	if len(spendable) != 2 {
		t.Fatalf("expected 2 spendable utxos, got %d", len(spendable))
	}

	// 验证小额优先排序（3000 在 5000 前面）
	if spendable[0].ValueSatoshi != 3000 || spendable[1].ValueSatoshi != 5000 {
		t.Fatalf("expected ascending order by value, got %d, %d", spendable[0].ValueSatoshi, spendable[1].ValueSatoshi)
	}
}

// TestBSVBalanceCalculation 验证余额计算
func TestBSVBalanceCalculation(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	// 写入多个 UTXO
	entries := []bsvUTXOEntry{
		{
			UTXOID:         "tx007:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr1",
			TxID:           "tx007",
			Vout:           0,
			ValueSatoshi:   5000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			UTXOID:         "tx008:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr2",
			TxID:           "tx008",
			Vout:           0,
			ValueSatoshi:   3000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			// token_carrier 计入 total 但不计入 confirmed（plain_bsv 口径）
			UTXOID:         "tx009:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr3",
			TxID:           "tx009",
			Vout:           0,
			ValueSatoshi:   1,
			UTXOState:      "unspent",
			CarrierType:    "token_carrier",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			// spent 不计入
			UTXOID:         "tx010:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr4",
			TxID:           "tx010",
			Vout:           0,
			ValueSatoshi:   1000,
			UTXOState:      "spent",
			CarrierType:    "plain_bsv",
			SpentByTxid:    "tx_spent",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
			SpentAtUnix:    now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertBSVUTXO(ctx, store, e); err != nil {
			t.Fatalf("upsert utxo %s: %v", e.UTXOID, err)
		}
	}

	// 计算余额
	confirmed, total, err := dbCalcBSVBalance(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("calc bsv balance: %v", err)
	}

	// confirmed = plain_bsv + unspent = 5000 + 3000 = 8000
	if confirmed != 8000 {
		t.Fatalf("expected confirmed 8000, got %d", confirmed)
	}

	// total = unspent (all carrier types) = 5000 + 3000 + 1 = 8001
	if total != 8001 {
		t.Fatalf("expected total 8001, got %d", total)
	}
}

// TestTokenLotBasicCRUD 验证 Token Lot 基本增删改查
func TestTokenLotBasicCRUD(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	lotID := "lot_001"
	tokenID := "token_test_001"

	// 写入 Token Lot
	entry := tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   ownerPubkey,
		TokenID:          tokenID,
		TokenStandard:    "BSV21",
		QuantityText:     "10000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         "tx_mint_001",
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
		Note:             "test token lot",
		Payload:          map[string]any{"decimals": 0},
	}
	if err := dbUpsertTokenLot(ctx, store, entry); err != nil {
		t.Fatalf("upsert token lot: %v", err)
	}

	// 读取
	got, err := dbGetTokenLot(ctx, store, lotID)
	if err != nil {
		t.Fatalf("get token lot: %v", err)
	}
	if got == nil {
		t.Fatal("expected token lot, got nil")
	}
	if got.QuantityText != "10000" {
		t.Fatalf("expected quantity 10000, got %s", got.QuantityText)
	}
	if got.TokenStandard != "BSV21" {
		t.Fatalf("expected standard BSV21, got %s", got.TokenStandard)
	}

	// 幂等更新
	entry.UsedQuantityText = "3000"
	entry.LotState = "locked"
	if err := dbUpsertTokenLot(ctx, store, entry); err != nil {
		t.Fatalf("upsert token lot (idempotent): %v", err)
	}

	got, err = dbGetTokenLot(ctx, store, lotID)
	if err != nil {
		t.Fatalf("get token lot after update: %v", err)
	}
	if got.UsedQuantityText != "3000" {
		t.Fatalf("expected used_quantity 3000, got %s", got.UsedQuantityText)
	}
	if got.LotState != "locked" {
		t.Fatalf("expected state locked, got %s", got.LotState)
	}
}

// TestTokenLotListSpendable 验证可花费 Token Lot 列表
func TestTokenLotListSpendable(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "03ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	tokenID := "token_test_002"

	// 写入多个 Token Lot
	entries := []tokenLotEntry{
		{
			LotID:            "lot_002",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "5000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_002",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			LotID:            "lot_003",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "3000",
			UsedQuantityText: "1000",
			LotState:         "unspent",
			MintTxid:         "tx_mint_003",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			// spent 不应该出现在列表中
			LotID:            "lot_004",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "2000",
			UsedQuantityText: "2000",
			LotState:         "spent",
			MintTxid:         "tx_mint_004",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			// 不同 token_id 不应该出现在列表中
			LotID:            "lot_005",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "other_token",
			TokenStandard:    "BSV21",
			QuantityText:     "10000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_005",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertTokenLot(ctx, store, e); err != nil {
			t.Fatalf("upsert token lot %s: %v", e.LotID, err)
		}
	}

	// 查询可花费列表
	spendable, err := dbListSpendableTokenLots(ctx, store, ownerPubkey, "BSV21", tokenID)
	if err != nil {
		t.Fatalf("list spendable token lots: %v", err)
	}

	// 只应该有 2 个 unspent + 匹配 token_id
	if len(spendable) != 2 {
		t.Fatalf("expected 2 spendable lots, got %d", len(spendable))
	}
}

// TestTokenCarrierLinkBasicCRUD 验证 Token Carrier Link 基本增删改查
func TestTokenCarrierLinkBasicCRUD(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "031111111111111111111111111111111111111111111111111111111111111111"
	linkID := "link_001"
	lotID := "lot_carrier_001"
	carrierUTXOID := "tx_carrier_001:0"

	// 先写入关联的 Token Lot
	lot := tokenLotEntry{
		LotID:            lotID,
		OwnerPubkeyHex:   ownerPubkey,
		TokenID:          "token_carrier_test",
		TokenStandard:    "BSV21",
		QuantityText:     "5000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         "tx_mint_carrier",
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}
	if err := dbUpsertTokenLot(ctx, store, lot); err != nil {
		t.Fatalf("upsert token lot: %v", err)
	}
	if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
		UTXOID:         carrierUTXOID,
		OwnerPubkeyHex: ownerPubkey,
		Address:        "1CarrierAddress001",
		TxID:           "tx_carrier_001",
		Vout:           0,
		ValueSatoshi:   1,
		UTXOState:      "unspent",
		CarrierType:    "token_carrier",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed carrier bsv utxo failed: %v", err)
	}

	// 写入 Carrier Link
	link := tokenCarrierLinkEntry{
		LinkID:         linkID,
		LotID:          lotID,
		CarrierUTXOID:  carrierUTXOID,
		OwnerPubkeyHex: ownerPubkey,
		LinkState:      "active",
		BindTxid:       "tx_bind_001",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
		Note:           "test carrier link",
	}
	if err := dbUpsertTokenCarrierLink(ctx, store, link); err != nil {
		t.Fatalf("upsert carrier link: %v", err)
	}

	// 查询 active carrier
	got, err := dbGetActiveCarrierForLot(ctx, store, lotID)
	if err != nil {
		t.Fatalf("get active carrier: %v", err)
	}
	if got == nil {
		t.Fatal("expected active carrier, got nil")
	}
	if got.CarrierUTXOID != carrierUTXOID {
		t.Fatalf("expected carrier_utxo_id %s, got %s", carrierUTXOID, got.CarrierUTXOID)
	}

	// 更新为 released
	link.LinkState = "released"
	link.UnbindTxid = "tx_unbind_001"
	if err := dbUpsertTokenCarrierLink(ctx, store, link); err != nil {
		t.Fatalf("upsert carrier link (release): %v", err)
	}

	// 再次查询应该没有 active carrier
	got, err = dbGetActiveCarrierForLot(ctx, store, lotID)
	if err != nil {
		t.Fatalf("get active carrier after release: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil active carrier after release")
	}
}

// TestTokenCarrierLinkListByOwner 验证按用户查询 Carrier Link
func TestTokenCarrierLinkListByOwner(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "032222222222222222222222222222222222222222222222222222222222222222"

	// 先写入 Token Lots
	lots := []tokenLotEntry{
		{
			LotID:            "lot_link_001",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "token_link_test",
			TokenStandard:    "BSV21",
			QuantityText:     "1000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_link_001",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			LotID:            "lot_link_002",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "token_link_test",
			TokenStandard:    "BSV21",
			QuantityText:     "2000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_link_002",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
	}
	for _, lot := range lots {
		if err := dbUpsertTokenLot(ctx, store, lot); err != nil {
			t.Fatalf("upsert token lot: %v", err)
		}
	}
	if err := dbUpsertTokenLot(ctx, store, tokenLotEntry{
		LotID:            "lot_link_003",
		OwnerPubkeyHex:   ownerPubkey,
		TokenID:          "token_link_test",
		TokenStandard:    "BSV21",
		QuantityText:     "1000",
		UsedQuantityText: "0",
		LotState:         "unspent",
		MintTxid:         "tx_mint_link_003",
		CreatedAtUnix:    now,
		UpdatedAtUnix:    now,
	}); err != nil {
		t.Fatalf("upsert token lot lot_link_003 failed: %v", err)
	}
	for _, linkUTXO := range []string{"tx_carrier_002:0", "tx_carrier_003:0", "tx_carrier_004:0"} {
		if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
			UTXOID:         linkUTXO,
			OwnerPubkeyHex: ownerPubkey,
			Address:        "1CarrierAddress002",
			TxID:           strings.Split(linkUTXO, ":")[0],
			Vout:           0,
			ValueSatoshi:   1,
			UTXOState:      "unspent",
			CarrierType:    "token_carrier",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		}); err != nil {
			t.Fatalf("seed carrier bsv utxo %s failed: %v", linkUTXO, err)
		}
	}

	// 写入 Carrier Links
	links := []tokenCarrierLinkEntry{
		{
			LinkID:         "link_002",
			LotID:          "lot_link_001",
			CarrierUTXOID:  "tx_carrier_002:0",
			OwnerPubkeyHex: ownerPubkey,
			LinkState:      "active",
			BindTxid:       "tx_bind_002",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			LinkID:         "link_003",
			LotID:          "lot_link_002",
			CarrierUTXOID:  "tx_carrier_003:0",
			OwnerPubkeyHex: ownerPubkey,
			LinkState:      "active",
			BindTxid:       "tx_bind_003",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			// released 不应该出现在列表中
			LinkID:         "link_004",
			LotID:          "lot_link_003",
			CarrierUTXOID:  "tx_carrier_004:0",
			OwnerPubkeyHex: ownerPubkey,
			LinkState:      "released",
			BindTxid:       "tx_bind_004",
			UnbindTxid:     "tx_unbind_004",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
	}
	for _, link := range links {
		if err := dbUpsertTokenCarrierLink(ctx, store, link); err != nil {
			t.Fatalf("upsert carrier link: %v", err)
		}
	}

	// 查询 active links
	activeLinks, err := dbListActiveCarrierLinksByOwner(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("list active carrier links: %v", err)
	}

	// 只应该有 2 个 active
	if len(activeLinks) != 2 {
		t.Fatalf("expected 2 active links, got %d", len(activeLinks))
	}
}

// TestTokenBalanceCalculation 验证 Token 余额计算
func TestTokenBalanceCalculation(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "033333333333333333333333333333333333333333333333333333333333333333"
	tokenID := "token_balance_test"

	// 写入多个 Token Lots
	entries := []tokenLotEntry{
		{
			LotID:            "lot_bal_001",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "10000",
			UsedQuantityText: "3000",
			LotState:         "unspent",
			MintTxid:         "tx_mint_bal_001",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			LotID:            "lot_bal_002",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "5000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_bal_002",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			// spent 不计入余额
			LotID:            "lot_bal_003",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          tokenID,
			TokenStandard:    "BSV21",
			QuantityText:     "2000",
			UsedQuantityText: "2000",
			LotState:         "spent",
			MintTxid:         "tx_mint_bal_003",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertTokenLot(ctx, store, e); err != nil {
			t.Fatalf("upsert token lot %s: %v", e.LotID, err)
		}
	}

	// 计算余额 (10000 - 3000) + (5000 - 0) = 7000 + 5000 = 12000
	balance, err := dbCalcTokenBalance(ctx, store, ownerPubkey, "BSV21", tokenID)
	if err != nil {
		t.Fatalf("calc token balance: %v", err)
	}
	if balance != "12000" {
		t.Fatalf("expected balance 12000, got %s", balance)
	}
}

// TestSelectBSVUTXOsForTarget 验证选币函数
func TestSelectBSVUTXOsForTarget(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "034444444444444444444444444444444444444444444444444444444444444444"

	// 写入多个 UTXO（小额优先选币）
	entries := []bsvUTXOEntry{
		{
			UTXOID:         "tx_select_001:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr1",
			TxID:           "tx_select_001",
			Vout:           0,
			ValueSatoshi:   5000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			UTXOID:         "tx_select_002:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr2",
			TxID:           "tx_select_002",
			Vout:           0,
			ValueSatoshi:   3000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			UTXOID:         "tx_select_003:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr3",
			TxID:           "tx_select_003",
			Vout:           0,
			ValueSatoshi:   1000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertBSVUTXO(ctx, store, e); err != nil {
			t.Fatalf("upsert utxo %s: %v", e.UTXOID, err)
		}
	}

	// 选币：目标 3500，应该选 1000 + 3000（小额优先）
	selected, err := dbSelectBSVUTXOsForTarget(ctx, store, ownerPubkey, 3500)
	if err != nil {
		t.Fatalf("select utxos for target: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected utxos, got %d", len(selected))
	}

	// 验证小额优先顺序
	if selected[0].ValueSatoshi != 1000 || selected[1].ValueSatoshi != 3000 {
		t.Fatalf("expected ascending order, got %d, %d", selected[0].ValueSatoshi, selected[1].ValueSatoshi)
	}

	// 验证 UseAmount
	if selected[0].UseAmount != 1000 {
		t.Fatalf("expected use_amount 1000 for first utxo, got %d", selected[0].UseAmount)
	}
	if selected[1].UseAmount != 2500 { // 只需要 3500-1000=2500
		t.Fatalf("expected use_amount 2500 for second utxo, got %d", selected[1].UseAmount)
	}
}

// TestSelectBSVUTXOsForTargetInsufficient 验证选币余额不足
func TestSelectBSVUTXOsForTargetInsufficient(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "035555555555555555555555555555555555555555555555555555555555555555"

	// 写入一个小额 UTXO
	entry := bsvUTXOEntry{
		UTXOID:         "tx_insufficient:0",
		OwnerPubkeyHex: ownerPubkey,
		Address:        "addr1",
		TxID:           "tx_insufficient",
		Vout:           0,
		ValueSatoshi:   500,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}
	if err := dbUpsertBSVUTXO(ctx, store, entry); err != nil {
		t.Fatalf("upsert utxo: %v", err)
	}

	// 选币：目标 1000，但只有 500
	_, err := dbSelectBSVUTXOsForTarget(ctx, store, ownerPubkey, 1000)
	if err == nil {
		t.Fatal("expected insufficient balance error, got nil")
	}
}

// TestWalletBSVBalanceLoad 验证钱包余额加载
func TestWalletBSVBalanceLoad(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "036666666666666666666666666666666666666666666666666666666666666666"

	// 写入多个 UTXO
	entries := []bsvUTXOEntry{
		{
			UTXOID:         "tx_bal_load_001:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr1",
			TxID:           "tx_bal_load_001",
			Vout:           0,
			ValueSatoshi:   3000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
		{
			UTXOID:         "tx_bal_load_002:0",
			OwnerPubkeyHex: ownerPubkey,
			Address:        "addr2",
			TxID:           "tx_bal_load_002",
			Vout:           0,
			ValueSatoshi:   5000,
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix:  now,
			UpdatedAtUnix:  now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertBSVUTXO(ctx, store, e); err != nil {
			t.Fatalf("upsert utxo %s: %v", e.UTXOID, err)
		}
	}

	// 加载钱包余额
	balance, err := dbLoadWalletBSVBalance(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("load wallet bsv balance: %v", err)
	}

	if balance.ConfirmedSatoshi != 8000 {
		t.Fatalf("expected confirmed 8000, got %d", balance.ConfirmedSatoshi)
	}
	if balance.SpendableUTXOCount != 2 {
		t.Fatalf("expected spendable count 2, got %d", balance.SpendableUTXOCount)
	}
}

// TestWalletTokenBalancesLoad 验证钱包所有 Token 余额加载
func TestWalletTokenBalancesLoad(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	ownerPubkey := "037777777777777777777777777777777777777777777777777777777777777777"

	// 写入多个 Token Lots（不同 token）
	entries := []tokenLotEntry{
		{
			LotID:            "lot_multi_001",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "token_a",
			TokenStandard:    "BSV21",
			QuantityText:     "10000",
			UsedQuantityText: "3000",
			LotState:         "unspent",
			MintTxid:         "tx_mint_a",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			LotID:            "lot_multi_002",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "token_b",
			TokenStandard:    "BSV20",
			QuantityText:     "5000",
			UsedQuantityText: "0",
			LotState:         "unspent",
			MintTxid:         "tx_mint_b",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
		{
			// spent 不计入
			LotID:            "lot_multi_003",
			OwnerPubkeyHex:   ownerPubkey,
			TokenID:          "token_c",
			TokenStandard:    "BSV21",
			QuantityText:     "2000",
			UsedQuantityText: "2000",
			LotState:         "spent",
			MintTxid:         "tx_mint_c",
			CreatedAtUnix:    now,
			UpdatedAtUnix:    now,
		},
	}

	for _, e := range entries {
		if err := dbUpsertTokenLot(ctx, store, e); err != nil {
			t.Fatalf("upsert token lot %s: %v", e.LotID, err)
		}
	}

	// 加载所有 token 余额
	balances, err := dbLoadAllWalletTokenBalances(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("load all wallet token balances: %v", err)
	}

	// 只应该有 2 个 token 有余额（token_a 和 token_b）
	if len(balances) != 2 {
		t.Fatalf("expected 2 token balances, got %d", len(balances))
	}
}

// TestSettlementRecordBasic 验证结算记录基本写入和查询
func TestSettlementRecordBasic(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)
	now := time.Now().Unix()

	// 先创建 settlement payment attempt
	paymentAttemptID := "payment_attempt_test_001"
	if err := dbUpsertSettlementPaymentAttempt(db, paymentAttemptID, "chain_quote_pay", "tx_test_001", "confirmed", 10000, 0, 10000, 1, now, "test cycle", nil); err != nil {
		t.Fatalf("upsert settlement payment attempt: %v", err)
	}

	paymentAttemptDBID, err := dbGetSettlementPaymentAttemptBySource(db, "chain_quote_pay", "tx_test_001")
	if err != nil {
		t.Fatalf("get settlement payment attempt: %v", err)
	}

	ownerPubkey := "038888888888888888888888888888888888888888888888888888888888888888"

	// 写入 BSV 结算记录
	record := settlementRecordEntry{
		RecordID:                   "rec_bsv_001",
		SettlementPaymentAttemptID: paymentAttemptDBID,
		AssetType:                  "BSV",
		OwnerPubkeyHex:             ownerPubkey,
		SourceUTXOID:               "tx_input_001:0",
		UsedSatoshi:                5000,
		State:                      "confirmed",
		OccurredAtUnix:             now,
		Note:                       "test bsv record",
	}
	if err := dbAppendSettlementRecord(ctx, store, record); err != nil {
		t.Fatalf("append settlement record: %v", err)
	}

	// 查询记录
	records, err := dbListSettlementRecordsByCycle(ctx, store, paymentAttemptDBID)
	if err != nil {
		t.Fatalf("list settlement records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].UsedSatoshi != 5000 {
		t.Fatalf("expected used 5000, got %d", records[0].UsedSatoshi)
	}
}

// TestApplyVerifiedAssetFlowBSV 验证 ApplyVerifiedAssetFlow 写入 BSV
func TestApplyVerifiedAssetFlowBSV(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	ownerPubkey := "039999999999999999999999999999999999999999999999999999999999999999"
	utxoID := "tx_verified_001:0"

	// 使用兼容性函数写入 BSV UTXO
	params := verifiedAssetFlowParams{
		WalletID:      ownerPubkey,
		Address:       "1VerifiedAddr",
		UTXOID:        utxoID,
		TxID:          "tx_verified_001",
		Vout:          0,
		ValueSatoshi:  10000,
		AssetKind:     "BSV",
		CreatedAtUnix: time.Now().Unix(),
		Trigger:       "WOC",
	}
	if err := ApplyVerifiedAssetFlow(ctx, store, params); err != nil {
		t.Fatalf("apply verified asset flow: %v", err)
	}

	// 验证写入
	got, err := dbGetBSVUTXO(ctx, store, utxoID)
	if err != nil {
		t.Fatalf("get bsv utxo: %v", err)
	}
	if got == nil {
		t.Fatal("expected utxo, got nil")
	}
	if got.ValueSatoshi != 10000 {
		t.Fatalf("expected value 10000, got %d", got.ValueSatoshi)
	}
	if got.CarrierType != "plain_bsv" {
		t.Fatalf("expected carrier_type plain_bsv, got %s", got.CarrierType)
	}
}

// TestApplyVerifiedAssetFlowToken 验证 ApplyVerifiedAssetFlow 写入 Token
func TestApplyVerifiedAssetFlowToken(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)
	ctx := context.Background()
	store := newClientDB(db, nil)

	ownerPubkey := "03aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
	tokenID := "token_verified_001"

	// 使用兼容性函数写入 Token
	params := verifiedAssetFlowParams{
		WalletID:      ownerPubkey,
		Address:       "1VerifiedAddr",
		UTXOID:        "tx_verified_token:0",
		TxID:          "tx_verified_token",
		Vout:          0,
		ValueSatoshi:  1,
		AssetKind:     "BSV21",
		TokenID:       tokenID,
		QuantityText:  "5000",
		CreatedAtUnix: time.Now().Unix(),
		Trigger:       "WOC",
	}
	if err := ApplyVerifiedAssetFlow(ctx, store, params); err != nil {
		t.Fatalf("apply verified asset flow (token): %v", err)
	}

	// 验证 Token Lot 写入（lot_id 格式：lot_<token_id>_<txid>_<vout>）
	lotID := "lot_" + tokenID + "_tx_verified_token_0"
	got, err := dbGetTokenLot(ctx, store, lotID)
	if err != nil {
		t.Fatalf("get token lot: %v", err)
	}
	if got == nil {
		t.Fatal("expected token lot, got nil")
	}
	if got.QuantityText != "5000" {
		t.Fatalf("expected quantity 5000, got %s", got.QuantityText)
	}
	if got.TokenStandard != "BSV21" {
		t.Fatalf("expected standard BSV21, got %s", got.TokenStandard)
	}
}

// TestSchemaCreated 验证新表结构已正确创建
func TestSchemaCreated(t *testing.T) {
	t.Parallel()

	db := newAssetFactsTestDB(t)

	// 验证新表存在
	tables := []string{
		"fact_bsv_utxos",
		"fact_token_lots",
		"fact_token_carrier_links",
		"fact_settlement_records",
	}

	for _, table := range tables {
		var count int
		err := db.QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count)
		if err != nil {
			t.Fatalf("check table %s existence: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("expected table %s to exist", table)
		}
	}
}
