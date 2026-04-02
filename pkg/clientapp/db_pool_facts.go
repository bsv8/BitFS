package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
)

type directTransferPoolSessionFactInput struct {
	SessionID          string
	PoolScheme         string
	CounterpartyPubHex string
	SellerPubHex       string
	ArbiterPubHex      string
	GatewayPubHex      string
	PoolAmountSat      uint64
	SpendTxFeeSat      uint64
	FeeRateSatByte     float64
	LockBlocks         uint32
	OpenBaseTxID       string
	Status             string
	CreatedAtUnix      int64
	UpdatedAtUnix      int64
}

type directTransferPoolAllocationFactInput struct {
	SessionID        string
	AllocationKind   string
	SequenceNum      uint32
	PayeeAmountAfter uint64
	PayerAmountAfter uint64
	TxID             string
	TxHex            string
	CreatedAtUnix    int64
}

func dbUpsertDirectTransferPoolSessionTx(tx *sql.Tx, in directTransferPoolSessionFactInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := in.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}
	poolScheme := strings.TrimSpace(in.PoolScheme)
	if poolScheme == "" {
		poolScheme = "2of3"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = "active"
	}
	_, err := tx.Exec(
		`INSERT INTO pool_sessions(
			pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
			pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(pool_session_id) DO UPDATE SET
			pool_scheme=excluded.pool_scheme,
			counterparty_pubkey_hex=excluded.counterparty_pubkey_hex,
			seller_pubkey_hex=excluded.seller_pubkey_hex,
			arbiter_pubkey_hex=excluded.arbiter_pubkey_hex,
			gateway_pubkey_hex=excluded.gateway_pubkey_hex,
			pool_amount_satoshi=excluded.pool_amount_satoshi,
			spend_tx_fee_satoshi=excluded.spend_tx_fee_satoshi,
			fee_rate_sat_byte=excluded.fee_rate_sat_byte,
			lock_blocks=excluded.lock_blocks,
			open_base_txid=excluded.open_base_txid,
			status=excluded.status,
			updated_at_unix=excluded.updated_at_unix`,
		sessionID,
		poolScheme,
		strings.ToLower(strings.TrimSpace(in.CounterpartyPubHex)),
		strings.ToLower(strings.TrimSpace(in.SellerPubHex)),
		strings.ToLower(strings.TrimSpace(in.ArbiterPubHex)),
		strings.ToLower(strings.TrimSpace(in.GatewayPubHex)),
		in.PoolAmountSat,
		in.SpendTxFeeSat,
		in.FeeRateSatByte,
		in.LockBlocks,
		strings.ToLower(strings.TrimSpace(in.OpenBaseTxID)),
		status,
		createdAt,
		updatedAt,
	)
	return err
}

func dbUpsertDirectTransferPoolAllocationTx(tx *sql.Tx, in directTransferPoolAllocationFactInput) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	kind := strings.TrimSpace(in.AllocationKind)
	if kind == "" {
		return fmt.Errorf("allocation_kind is required")
	}
	txID := strings.ToLower(strings.TrimSpace(in.TxID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	txHex := strings.ToLower(strings.TrimSpace(in.TxHex))
	if txHex == "" {
		return fmt.Errorf("tx_hex is required")
	}
	allocID := directTransferPoolAllocationID(sessionID, kind, in.SequenceNum)
	if allocID == "" {
		return fmt.Errorf("allocation_id is required")
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	var allocationNo int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(allocation_no),0)+1 FROM pool_allocations WHERE pool_session_id=?`,
		sessionID,
	).Scan(&allocationNo); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO pool_allocations(
			allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(allocation_id) DO UPDATE SET
			pool_session_id=excluded.pool_session_id,
			allocation_kind=excluded.allocation_kind,
			sequence_num=excluded.sequence_num,
			payee_amount_after=excluded.payee_amount_after,
			payer_amount_after=excluded.payer_amount_after,
			txid=excluded.txid,
			tx_hex=excluded.tx_hex`,
		allocID,
		sessionID,
		allocationNo,
		kind,
		in.SequenceNum,
		in.PayeeAmountAfter,
		in.PayerAmountAfter,
		txID,
		txHex,
		createdAt,
	)
	return err
}

func dbUpsertDirectTransferPoolSession(ctx context.Context, store *clientDB, in directTransferPoolSessionFactInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		return dbUpsertDirectTransferPoolSessionTx(tx, in)
	})
}

func dbUpsertDirectTransferPoolAllocation(ctx context.Context, store *clientDB, in directTransferPoolAllocationFactInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		return dbUpsertDirectTransferPoolAllocationTx(tx, in)
	})
}

func directTransferPoolAllocationID(sessionID string, allocationKind string, sequenceNum uint32) string {
	sessionID = strings.TrimSpace(sessionID)
	allocationKind = strings.TrimSpace(allocationKind)
	if sessionID == "" || allocationKind == "" {
		return ""
	}
	return "poolalloc_" + sessionID + "_" + allocationKind + "_" + fmt.Sprint(sequenceNum)
}

func directTransferPoolTxIDFromHex(txHex string) (string, error) {
	if strings.TrimSpace(txHex) == "" {
		return "", fmt.Errorf("tx hex is required")
	}
	parsed, err := transaction.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(parsed.TxID().String())), nil
}

// dbGetPoolAllocationIDByAllocationID 按 allocation_id 查自增 id
// 第二步整改：财务来源从业务键切换到事实表自增主键
func dbGetPoolAllocationIDByAllocationID(ctx context.Context, store *clientDB, allocationID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		var id int64
		err := db.QueryRow(
			`SELECT id FROM pool_allocations WHERE allocation_id=?`,
			strings.TrimSpace(allocationID),
		).Scan(&id)
		if err != nil {
			return 0, err
		}
		return id, nil
	})
}

// dbGetPoolAllocationIDByAllocationIDTx 在事务内按 allocation_id 查自增 id
// 用于财务写入时在同一事务内获取主键
func dbGetPoolAllocationIDByAllocationIDTx(tx *sql.Tx, allocationID string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	var id int64
	err := tx.QueryRow(
		`SELECT id FROM pool_allocations WHERE allocation_id=?`,
		strings.TrimSpace(allocationID),
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}
