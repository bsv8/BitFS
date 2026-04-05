package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type sqlConn interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// chainPaymentEntry fact_chain_payments 写入条目
// 第二步整改：为 wallet_chain 财务来源切换提供事实层支持
type chainPaymentEntry struct {
	TxID                string
	PaymentSubType      string
	Status              string
	WalletInputSatoshi  int64
	WalletOutputSatoshi int64
	NetAmountSatoshi    int64
	BlockHeight         int64
	OccurredAtUnix      int64
	FromPartyID         string
	ToPartyID           string
	Payload             any
}

// chainPaymentUTXOLinkEntry fact_chain_payment_utxo_links 写入条目
type chainPaymentUTXOLinkEntry struct {
	ChainPaymentID int64
	UTXOID         string
	IOSide         string
	UTXORole       string
	AmountSatoshi  int64
	CreatedAtUnix  int64
	Note           string
	Payload        any
}

// dbUpsertChainPayment 按 txid  upsert fact_chain_payments
// 同一个 txid 重复写入不会生成多条记录
func dbUpsertChainPayment(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainPaymentDB(db, e)
	})
}

// dbUpsertChainPaymentDB 在已打开的 sql.DB 上执行 upsert
func dbUpsertChainPaymentDB(db sqlConn, e chainPaymentEntry) (int64, error) {
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

	// 先尝试查询已存在的记录
	var existingID int64
	err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&existingID)
	if err == nil {
		// 存在则更新
		_, err = db.Exec(
			`UPDATE fact_chain_payments SET
				payment_subtype=?,
				status=?,
				wallet_input_satoshi=?,
				wallet_output_satoshi=?,
				net_amount_satoshi=?,
				block_height=?,
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
			strings.TrimSpace(e.FromPartyID),
			strings.TrimSpace(e.ToPartyID),
			mustJSONString(e.Payload),
			now,
			existingID,
		)
		if err != nil {
			return 0, err
		}
		if err := dbUpsertSettlementCycle(db,
			fmt.Sprintf("cycle_chain_%d", existingID), "chain", "confirmed",
			0, existingID,
			e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
			0, occurredAt, "auto-created from chain payment", e.Payload,
		); err != nil {
			return 0, fmt.Errorf("upsert settlement cycle for existing chain payment %d: %w", existingID, err)
		}
		return existingID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// 不存在则插入
	res, err := db.Exec(
		`INSERT INTO fact_chain_payments(
			txid,payment_subtype,status,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,
			block_height,occurred_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		txid,
		strings.TrimSpace(e.PaymentSubType),
		strings.TrimSpace(e.Status),
		e.WalletInputSatoshi,
		e.WalletOutputSatoshi,
		e.NetAmountSatoshi,
		e.BlockHeight,
		occurredAt,
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

	// Step 15: 同时写入 settlement_cycle 统一锚点
	if err := dbUpsertSettlementCycle(db,
		fmt.Sprintf("cycle_chain_%d", paymentID), "chain", "confirmed",
		0, paymentID,
		e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
		0, occurredAt, "auto-created from chain payment", e.Payload,
	); err != nil {
		return 0, fmt.Errorf("upsert settlement cycle for chain payment %d: %w", paymentID, err)
	}

	return paymentID, nil
}

// dbGetChainPaymentByTxID 按 txid 查 fact_chain_payments
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
		err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&id)
		if err != nil {
			return 0, err
		}
		return id, nil
	})
}

// dbGetChainPaymentByID 按 id 查 fact_chain_payments（验证存在性）
func dbGetChainPaymentByID(ctx context.Context, store *clientDB, id int64) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (bool, error) {
		var exists int
		err := db.QueryRow(`SELECT 1 FROM fact_chain_payments WHERE id=?`, id).Scan(&exists)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

// dbAppendChainPaymentUTXOLinkIfAbsent 幂等追加 fact_chain_payment_utxo_links
// 同一 payment-utxo link 重复写入不会生成多条记录
func dbAppendChainPaymentUTXOLinkIfAbsent(ctx context.Context, store *clientDB, e chainPaymentUTXOLinkEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		return dbAppendChainPaymentUTXOLinkIfAbsentDB(db, e)
	})
}

// dbAppendChainPaymentUTXOLinkIfAbsentDB 在已打开的 sql.DB 上执行幂等追加
func dbAppendChainPaymentUTXOLinkIfAbsentDB(db sqlConn, e chainPaymentUTXOLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.ChainPaymentID <= 0 {
		return fmt.Errorf("chain_payment_id is required")
	}
	utxoID := strings.ToLower(strings.TrimSpace(e.UTXOID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	ioSide := strings.TrimSpace(e.IOSide)
	if ioSide == "" {
		return fmt.Errorf("io_side is required")
	}
	utxoRole := strings.TrimSpace(e.UTXORole)
	if utxoRole == "" {
		return fmt.Errorf("utxo_role is required")
	}

	// 检查是否已存在
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fact_chain_payment_utxo_links WHERE chain_payment_id=? AND utxo_id=? AND io_side=? AND utxo_role=?`,
		e.ChainPaymentID, utxoID, ioSide, utxoRole,
	).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}

	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = time.Now().Unix()
	}

	_, err := db.Exec(
		`INSERT INTO fact_chain_payment_utxo_links(
			chain_payment_id,utxo_id,io_side,utxo_role,amount_satoshi,created_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?)`,
		e.ChainPaymentID,
		utxoID,
		ioSide,
		utxoRole,
		e.AmountSatoshi,
		createdAt,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func dbWalletUTXOExistsConn(db sqlConn, utxoID string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return false, fmt.Errorf("utxo_id is required")
	}
	var one int
	err := db.QueryRow(`SELECT 1 FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func dbWalletUTXOValueConn(db sqlConn, utxoID string) (int64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return 0, false, fmt.Errorf("utxo_id is required")
	}
	var value int64
	err := db.QueryRow(`SELECT value_satoshi FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}
