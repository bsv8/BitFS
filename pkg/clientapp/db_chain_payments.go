package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

type sqlConn interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// chainPaymentEntry fact_settlement_channel_chain_quote_pay 写入条目
// 设计说明：链支付事实只走显式业务提交入口，后续写入都靠事实表主键收口。
type chainPaymentEntry struct {
	TxID                 string
	PaymentSubType       string
	Status               string
	WalletInputSatoshi   int64
	WalletOutputSatoshi  int64
	NetAmountSatoshi     int64
	BlockHeight          int64
	OccurredAtUnix       int64
	SubmittedAtUnix      int64
	WalletObservedAtUnix int64
	FromPartyID          string
	ToPartyID            string
	Payload              any
}

// chainPaymentUTXOLinkEntry 统一结算链路的 UTXO 明细条目。
// 设计说明：这里是 chain_payment 事实的内部明细，不再依赖已下线的 settle tx 拆分表。
type chainPaymentUTXOLinkEntry struct {
	ChainPaymentID int64
	UTXOID         string
	IOSide         string
	UTXORole       string
	AssetKind      string
	TokenID        string
	TokenStandard  string
	AmountSatoshi  int64
	QuantityText   string
	CreatedAtUnix  int64
	Note           string
	Payload        any
}

// dbUpsertChainPayment 按 txid  upsert fact_settlement_channel_chain_quote_pay
// 同一个 txid 重复写入不会生成多条记录
func dbUpsertChainPayment(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainPaymentDB(ctx, db, e)
	})
}

// dbUpsertChainPaymentWithSettlementCycle 走同一个上下文包装，但会补 settlement_cycle。
func dbUpsertChainPaymentWithSettlementCycle(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainPaymentWithSettlementCycleDB(ctx, db, e)
	})
}

// dbUpsertChainPaymentDB 在已打开的 sql.DB 上执行 upsert，只落 fact。
func dbUpsertChainPaymentDB(ctx context.Context, db sqlConn, e chainPaymentEntry) (int64, error) {
	// 账务主线只认 settlement_cycle，chain_payment 落库后同步补 confirmed cycle。
	return dbUpsertChainPaymentDBWithSettlementCycle(ctx, db, e, true)
}

// dbUpsertChainPaymentWithSettlementCycleDB 先写 fact，再补 settlement_cycle。
// 设计说明：
// - 这里已经收口为同一口径：chain_payment 一旦写入，就必须补 confirmed settlement_cycle；
// - 提交、迁移、投影写入都不能再停在 payment 事实，不然后面会漏扣账。
func dbUpsertChainPaymentWithSettlementCycleDB(ctx context.Context, db sqlConn, e chainPaymentEntry) (int64, error) {
	return dbUpsertChainPaymentDBWithSettlementCycle(ctx, db, e, true)
}

// settlementCycleStateForChainPayment 把 payment 状态收口到 settlement cycle 状态。
// 设计说明：
// - confirmed 才直接落 confirmed；
// - submitted / observed / 空值 先落 pending，等后续确认再升级；
// - failed 直接记 failed，避免把失败单误算成已确认扣账。
func settlementCycleStateForChainPayment(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "confirmed":
		return "confirmed"
	case "failed":
		return "failed"
	default:
		return "pending"
	}
}

func dbUpsertChainPaymentDBWithSettlementCycle(ctx context.Context, db sqlConn, e chainPaymentEntry, writeSettlementCycle bool) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return 0, fmt.Errorf("txid is required")
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	submittedAt := e.SubmittedAtUnix
	walletObservedAt := e.WalletObservedAtUnix

	// 先尝试查询已存在的记录
	var existingID int64
	var existingSettlementCycleID int64
	var existingSubmittedAt int64
	var existingWalletObservedAt int64
	err := QueryRowContext(ctx, db, `SELECT id,settlement_cycle_id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txid).Scan(&existingID, &existingSettlementCycleID)
	if err == nil {
		if err := QueryRowContext(ctx, db, `SELECT submitted_at_unix,wallet_observed_at_unix FROM fact_settlement_channel_chain_quote_pay WHERE id=?`, existingID).Scan(&existingSubmittedAt, &existingWalletObservedAt); err != nil {
			return 0, err
		}
		if submittedAt < existingSubmittedAt {
			submittedAt = existingSubmittedAt
		}
		if walletObservedAt < existingWalletObservedAt {
			walletObservedAt = existingWalletObservedAt
		}
		// 存在则更新
		_, err = ExecContext(ctx, db,
			`UPDATE fact_settlement_channel_chain_quote_pay SET
				payment_subtype=?,
				status=?,
				wallet_input_satoshi=?,
				wallet_output_satoshi=?,
				net_amount_satoshi=?,
				block_height=?,
				occurred_at_unix=?,
				submitted_at_unix=?,
				wallet_observed_at_unix=?,
				from_party_id=?,
				to_party_id=?,
				payload_json=?,
				updated_at_unix=?
			WHERE id=?`,
			strings.TrimSpace(e.PaymentSubType),
			strings.TrimSpace(e.Status),
			e.WalletInputSatoshi,
			e.WalletOutputSatoshi,
			e.NetAmountSatoshi,
			e.BlockHeight,
			occurredAt,
			submittedAt,
			walletObservedAt,
			strings.TrimSpace(e.FromPartyID),
			strings.TrimSpace(e.ToPartyID),
			mustJSONString(e.Payload),
			now,
			existingID,
		)
		if err != nil {
			return 0, err
		}
		if writeSettlementCycle {
			settlementState := settlementCycleStateForChainPayment(e.Status)
			if _, err := ExecContext(ctx, db,
				`UPDATE fact_settlement_cycles
				    SET source_type='chain_quote_pay',
				        source_id=?,
				        state=?,
				        gross_amount_satoshi=?,
				        gate_fee_satoshi=0,
				        net_amount_satoshi=?,
				        occurred_at_unix=?,
				        confirmed_at_unix=CASE WHEN ?='confirmed' THEN ? ELSE confirmed_at_unix END,
				        note=?,
				        payload_json=?
				  WHERE id=?`,
				fmt.Sprintf("%d", existingID),
				settlementState,
				e.WalletInputSatoshi,
				e.NetAmountSatoshi,
				occurredAt,
				settlementState,
				occurredAt,
				"auto-created from chain quote pay",
				mustJSONString(e.Payload),
				existingSettlementCycleID,
			); err != nil {
				return 0, fmt.Errorf("update settlement cycle for existing chain quote pay %d: %w", existingID, err)
			}
		}
		return existingID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	settlementState := settlementCycleStateForChainPayment(e.Status)
	cycleToken := "pending:chain_quote_pay:" + txid
	if err := dbUpsertSettlementCycleCtx(ctx, db,
		fmt.Sprintf("cycle_chain_quote_pay_%s", txid), "chain_quote_pay", cycleToken, settlementState,
		e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
		0, occurredAt, "pre-bind chain quote pay channel", e.Payload,
	); err != nil {
		return 0, fmt.Errorf("create settlement cycle shell for chain quote pay: %w", err)
	}
	settlementCycleID, err := dbGetSettlementCycleBySourceCtx(ctx, db, "chain_quote_pay", cycleToken)
	if err != nil {
		return 0, fmt.Errorf("lookup settlement cycle shell for chain quote pay: %w", err)
	}

	// 不存在则插入
	res, err := ExecContext(ctx, db,
		`INSERT INTO fact_settlement_channel_chain_quote_pay(
			settlement_cycle_id,txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,
			block_height,occurred_at_unix,submitted_at_unix,wallet_observed_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		settlementCycleID,
		txid,
		strings.TrimSpace(e.PaymentSubType),
		strings.TrimSpace(e.Status),
		e.WalletInputSatoshi,
		e.WalletOutputSatoshi,
		e.NetAmountSatoshi,
		e.BlockHeight,
		occurredAt,
		submittedAt,
		walletObservedAt,
		strings.TrimSpace(e.FromPartyID),
		strings.TrimSpace(e.ToPartyID),
		mustJSONString(e.Payload),
		now,
	)
	if err != nil {
		return 0, err
	}
	paymentID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	if writeSettlementCycle {
		if _, err := ExecContext(ctx, db,
			`UPDATE fact_settlement_cycles
			    SET source_type='chain_quote_pay',
			        source_id=?,
			        note=?,
			        payload_json=?
			  WHERE id=?`,
			fmt.Sprintf("%d", paymentID),
			"bind chain quote pay channel id",
			mustJSONString(e.Payload),
			settlementCycleID,
		); err != nil {
			return 0, fmt.Errorf("bind settlement cycle source id for chain quote pay %d: %w", paymentID, err)
		}
	}

	return paymentID, nil
}

func dbUpsertChainChannelWithSettlementCycle(ctx context.Context, db sqlConn, e chainPaymentEntry, sourceType string, tableName string, cyclePrefix string, bindNote string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return 0, fmt.Errorf("txid is required")
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	submittedAt := e.SubmittedAtUnix
	walletObservedAt := e.WalletObservedAtUnix

	selectSQL := fmt.Sprintf("SELECT id,settlement_cycle_id,submitted_at_unix,wallet_observed_at_unix FROM %s WHERE txid=?", tableName)
	var existingID int64
	var existingSettlementCycleID int64
	var existingSubmittedAt int64
	var existingWalletObservedAt int64
	err := QueryRowContext(ctx, db, selectSQL, txid).Scan(&existingID, &existingSettlementCycleID, &existingSubmittedAt, &existingWalletObservedAt)
	if err == nil {
		if submittedAt < existingSubmittedAt {
			submittedAt = existingSubmittedAt
		}
		if walletObservedAt < existingWalletObservedAt {
			walletObservedAt = existingWalletObservedAt
		}
		updateSQL := fmt.Sprintf(`UPDATE %s SET
			payment_subtype=?,
			status=?,
			wallet_input_satoshi=?,
			wallet_output_satoshi=?,
			net_amount_satoshi=?,
			block_height=?,
			occurred_at_unix=?,
			submitted_at_unix=?,
			wallet_observed_at_unix=?,
			from_party_id=?,
			to_party_id=?,
			payload_json=?,
			updated_at_unix=?
		WHERE id=?`, tableName)
		if _, err := ExecContext(ctx, db, updateSQL,
			strings.TrimSpace(e.PaymentSubType),
			strings.TrimSpace(e.Status),
			e.WalletInputSatoshi,
			e.WalletOutputSatoshi,
			e.NetAmountSatoshi,
			e.BlockHeight,
			occurredAt,
			submittedAt,
			walletObservedAt,
			strings.TrimSpace(e.FromPartyID),
			strings.TrimSpace(e.ToPartyID),
			mustJSONString(e.Payload),
			now,
			existingID,
		); err != nil {
			return 0, err
		}
		state := settlementCycleStateForChainPayment(e.Status)
		if _, err := ExecContext(ctx, db,
			`UPDATE fact_settlement_cycles
			    SET source_type=?,source_id=?,state=?,gross_amount_satoshi=?,gate_fee_satoshi=0,net_amount_satoshi=?,occurred_at_unix=?,confirmed_at_unix=CASE WHEN ?='confirmed' THEN ? ELSE confirmed_at_unix END,note=?,payload_json=?
			  WHERE id=?`,
			sourceType, fmt.Sprintf("%d", existingID), state, e.WalletInputSatoshi, e.NetAmountSatoshi, occurredAt, state, occurredAt, bindNote, mustJSONString(e.Payload), existingSettlementCycleID,
		); err != nil {
			return 0, err
		}
		return existingID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	state := settlementCycleStateForChainPayment(e.Status)
	cycleToken := "pending:" + sourceType + ":" + txid
	if err := dbUpsertSettlementCycleCtx(ctx, db,
		fmt.Sprintf("%s_%s", cyclePrefix, txid), sourceType, cycleToken, state,
		e.WalletInputSatoshi, 0, e.NetAmountSatoshi, 0, occurredAt, "pre-bind channel row", e.Payload,
	); err != nil {
		return 0, err
	}
	settlementCycleID, err := dbGetSettlementCycleBySourceCtx(ctx, db, sourceType, cycleToken)
	if err != nil {
		return 0, err
	}
	insertSQL := fmt.Sprintf(`INSERT INTO %s(
		settlement_cycle_id,txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,
		block_height,occurred_at_unix,submitted_at_unix,wallet_observed_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, tableName)
	res, err := ExecContext(ctx, db, insertSQL,
		settlementCycleID,
		txid,
		strings.TrimSpace(e.PaymentSubType),
		strings.TrimSpace(e.Status),
		e.WalletInputSatoshi,
		e.WalletOutputSatoshi,
		e.NetAmountSatoshi,
		e.BlockHeight,
		occurredAt,
		submittedAt,
		walletObservedAt,
		strings.TrimSpace(e.FromPartyID),
		strings.TrimSpace(e.ToPartyID),
		mustJSONString(e.Payload),
		now,
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := ExecContext(ctx, db,
		`UPDATE fact_settlement_cycles SET source_type=?,source_id=?,note=?,payload_json=? WHERE id=?`,
		sourceType, fmt.Sprintf("%d", id), bindNote, mustJSONString(e.Payload), settlementCycleID,
	); err != nil {
		return 0, err
	}
	return id, nil
}

func dbUpsertChainDirectPayWithSettlementCycle(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainChannelWithSettlementCycle(ctx, db, e,
			"chain_direct_pay",
			"fact_settlement_channel_chain_direct_pay",
			"cycle_chain_direct_pay",
			"bind chain direct pay channel id",
		)
	})
}

func dbUpsertChainAssetCreateWithSettlementCycle(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainChannelWithSettlementCycle(ctx, db, e,
			"chain_asset_create",
			"fact_settlement_channel_chain_asset_create",
			"cycle_chain_asset_create",
			"bind chain asset create channel id",
		)
	})
}

// dbGetChainPaymentByTxID 按 txid 查 fact_settlement_channel_chain_quote_pay
func dbGetChainPaymentByTxID(ctx context.Context, store *clientDB, txid string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			return 0, fmt.Errorf("txid is required")
		}
		var id int64
		err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_channel_chain_quote_pay WHERE txid=?`, txid).Scan(&id)
		if err != nil {
			return 0, err
		}
		return id, nil
	})
}

// dbGetChainPaymentByID 按 id 查 fact_settlement_channel_chain_quote_pay（验证存在性）
func dbGetChainPaymentByID(ctx context.Context, store *clientDB, id int64) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (bool, error) {
		var exists int
		err := QueryRowContext(ctx, db, `SELECT 1 FROM fact_settlement_channel_chain_quote_pay WHERE id=?`, id).Scan(&exists)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

func dbWalletUTXOExistsConn(ctx context.Context, db sqlConn, utxoID string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return false, fmt.Errorf("utxo_id is required")
	}
	var one int
	err := QueryRowContext(ctx, db, `SELECT 1 FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func dbWalletUTXOValueConn(ctx context.Context, db sqlConn, utxoID string) (int64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return 0, false, fmt.Errorf("utxo_id is required")
	}
	var value int64
	err := QueryRowContext(ctx, db, `SELECT value_satoshi FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// recordChainPaymentAccountingAfterBroadcast 业务提交成功后，一次写完链支付已知财务内容。
// 设计说明：
// - 这里不等钱包同步，不走旧的事后回写；
// - 只接受已经广播成功的真实交易；
// - 输入金额来自 wallet_utxo，输出角色来自交易本身。
func recordChainPaymentAccountingAfterBroadcast(ctx context.Context, store *clientDB, rt *Runtime, txHex string, txID string, paymentSubType string, processSubType string, fromPartyID string, toPartyID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	txID = strings.ToLower(strings.TrimSpace(txID))
	if txHex == "" {
		return fmt.Errorf("tx_hex is required")
	}
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	walletAddr, err := clientWalletAddress(rt)
	if err != nil {
		return err
	}
	walletAddr = strings.TrimSpace(walletAddr)
	if walletAddr == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletScriptHex, err := walletAddressLockScriptHex(walletAddr)
	if err != nil {
		return err
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return fmt.Errorf("parse tx hex: %w", err)
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		inputFacts, grossInput, err := collectWalletInputFactsTx(ctx, tx, parsed, txID, "chain_payment input")
		if err != nil {
			return err
		}
		if len(inputFacts) == 0 {
			return fmt.Errorf("no wallet input facts found for txid %s", txID)
		}
		changeBack := int64(0)
		counterpartyOut := int64(0)
		for _, out := range parsed.Outputs {
			if out == nil {
				continue
			}
			amount := int64(out.Satoshis)
			if amount <= 0 {
				continue
			}
			outScriptHex := strings.ToLower(strings.TrimSpace(hex.EncodeToString(out.LockingScript.Bytes())))
			if outScriptHex != "" && walletScriptHexMatchesAddressControl(outScriptHex, walletScriptHex) {
				changeBack += amount
				continue
			}
			counterpartyOut += amount
		}
		minerFee := grossInput - changeBack - counterpartyOut
		if minerFee < 0 {
			minerFee = 0
		}
		netAmount := changeBack - grossInput
		channelID, err := dbUpsertChainPaymentDBWithSettlementCycle(ctx, tx, chainPaymentEntry{
			TxID:                 txID,
			PaymentSubType:       paymentSubType,
			Status:               "confirmed",
			WalletInputSatoshi:   grossInput,
			WalletOutputSatoshi:  changeBack,
			NetAmountSatoshi:     netAmount,
			OccurredAtUnix:       now,
			SubmittedAtUnix:      now,
			WalletObservedAtUnix: now,
			FromPartyID:          strings.TrimSpace(fromPartyID),
			ToPartyID:            strings.TrimSpace(toPartyID),
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}, true)
		if err != nil {
			return fmt.Errorf("upsert chain payment failed: %w", err)
		}
		settlementCycleID, err := dbGetSettlementCycleBySourceCtx(ctx, tx, "chain_quote_pay", fmt.Sprintf("%d", channelID))
		if err != nil {
			return fmt.Errorf("resolve settlement cycle for chain payment: %w", err)
		}
		businessID := "biz_chain_payment_" + txID
		if err := dbAppendSettlementCycleFinBusiness(tx, settlementCycleID, finBusinessEntry{
			BusinessID:        businessID,
			BusinessRole:      "process",
			AccountingScene:   "peer_call",
			AccountingSubType: strings.TrimSpace(processSubType),
			FromPartyID:       strings.TrimSpace(fromPartyID),
			ToPartyID:         strings.TrimSpace(toPartyID),
			Status:            "posted",
			OccurredAtUnix:    now,
			IdempotencyKey:    "chain_payment:" + txID,
			Note:              "chain payment broadcast accounting",
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}); err != nil {
			return fmt.Errorf("append settle record failed: %w", err)
		}
		// 旧 tx 拆解/UTXO 明细层已下线，这里只保留 settle_records、chain_payment 和流程事件。
		if err := dbAppendSettlementCycleFinProcessEvent(tx, settlementCycleID, finProcessEventEntry{
			ProcessID:         "proc_chain_payment_" + txID,
			AccountingScene:   "peer_call",
			AccountingSubType: strings.TrimSpace(processSubType),
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    now,
			IdempotencyKey:    "chain_payment_event:" + txID,
			Note:              "chain payment accounting event",
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}); err != nil {
			return fmt.Errorf("append settle_process_events failed: %w", err)
		}
		return nil
	})
}

func collectWalletInputFactsTx(ctx context.Context, db sqlConn, tx *txsdk.Transaction, txID string, note string) ([]string, int64, error) {
	if db == nil {
		return nil, 0, fmt.Errorf("db is nil")
	}
	if tx == nil {
		return nil, 0, fmt.Errorf("tx is nil")
	}
	out := make([]string, 0, len(tx.Inputs))
	var gross int64
	for _, inp := range tx.Inputs {
		if inp == nil || inp.SourceTXID == nil {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(inp.SourceTXID.String())) + ":" + fmt.Sprint(inp.SourceTxOutIndex)
		value, ok, err := dbWalletUTXOValueConn(ctx, db, utxoID)
		if err != nil {
			return nil, 0, fmt.Errorf("lookup wallet input value for %s failed: %w", utxoID, err)
		}
		if !ok {
			return nil, 0, fmt.Errorf("wallet input value missing for %s", utxoID)
		}
		gross += value
		out = append(out, utxoID)
	}
	return out, gross, nil
}
