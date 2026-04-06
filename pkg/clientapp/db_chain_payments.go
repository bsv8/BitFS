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
// 设计说明：为 wallet_chain 财务来源切换提供事实层支持，后续写入都靠事实表主键收口。
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
// B组改造：增加 AssetKind/TokenID/TokenStandard 用于资产分流写入
type chainPaymentUTXOLinkEntry struct {
	ChainPaymentID int64
	UTXOID         string
	IOSide         string
	UTXORole       string
	AssetKind      string // BSV/BSV20/BSV21，B组新增
	TokenID        string // Token ID，当 AssetKind 不是 BSV 时使用，B组新增
	TokenStandard  string // BSV20/BSV21，B组新增
	AmountSatoshi  int64
	QuantityText   string // Token 数量（十进制字符串），B组新增
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

// dbUpsertChainPaymentWithSettlementCycle 走同一个上下文包装，但会补 settlement_cycle。
func dbUpsertChainPaymentWithSettlementCycle(ctx context.Context, store *clientDB, e chainPaymentEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbUpsertChainPaymentWithSettlementCycleDB(db, e)
	})
}

// dbUpsertChainPaymentDB 在已打开的 sql.DB 上执行 upsert，只落 fact。
func dbUpsertChainPaymentDB(db sqlConn, e chainPaymentEntry) (int64, error) {
	return dbUpsertChainPaymentDBWithSettlementCycle(db, e, false)
}

// dbUpsertChainPaymentWithSettlementCycleDB 先写 fact，再补 settlement_cycle。
// 设计说明：
// - 只有账务路径需要结算周期锚点；
// - 提交、迁移、投影写入只落 fact，不要把“已提交”误写成“已确认结算”。
func dbUpsertChainPaymentWithSettlementCycleDB(db sqlConn, e chainPaymentEntry) (int64, error) {
	return dbUpsertChainPaymentDBWithSettlementCycle(db, e, true)
}

func dbUpsertChainPaymentDBWithSettlementCycle(db sqlConn, e chainPaymentEntry, writeSettlementCycle bool) (int64, error) {
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
	var existingSubmittedAt int64
	var existingWalletObservedAt int64
	err := db.QueryRow(`SELECT id FROM fact_chain_payments WHERE txid=?`, txid).Scan(&existingID)
	if err == nil {
		if err := db.QueryRow(`SELECT submitted_at_unix,wallet_observed_at_unix FROM fact_chain_payments WHERE id=?`, existingID).Scan(&existingSubmittedAt, &existingWalletObservedAt); err != nil {
			return 0, err
		}
		if submittedAt < existingSubmittedAt {
			submittedAt = existingSubmittedAt
		}
		if walletObservedAt < existingWalletObservedAt {
			walletObservedAt = existingWalletObservedAt
		}
		// 存在则更新
		_, err = db.Exec(
			`UPDATE fact_chain_payments SET
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
			if err := dbUpsertSettlementCycle(db,
				fmt.Sprintf("cycle_chain_%d", existingID), "chain", "confirmed",
				0, existingID,
				e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
				0, occurredAt, "auto-created from chain payment", e.Payload,
			); err != nil {
				return 0, fmt.Errorf("upsert settlement cycle for existing chain payment %d: %w", existingID, err)
			}
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
			block_height,occurred_at_unix,submitted_at_unix,wallet_observed_at_unix,from_party_id,to_party_id,payload_json,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
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
		if err := dbUpsertSettlementCycle(db,
			fmt.Sprintf("cycle_chain_%d", paymentID), "chain", "confirmed",
			0, paymentID,
			e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
			0, occurredAt, "auto-created from chain payment", e.Payload,
		); err != nil {
			return 0, fmt.Errorf("upsert settlement cycle for chain payment %d: %w", paymentID, err)
		}
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
