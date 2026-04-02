package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// 设计说明：
// - 直连池和 seed 读取是运行期高频路径；
// - SQL 留在这里，协议处理只保留校验和签名逻辑；
// - 文件读取虽然最终走文件系统，但 seed 路径解析也统一收在 db 层。

func dbLoadDirectDealParties(ctx context.Context, store *clientDB, dealID string) (string, string, string, error) {
	// 运行期辅助查询：只给 direct transfer 串 buyer / seller / arbiter 上下文。
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (struct {
		buyer   string
		seller  string
		arbiter string
	}, error) {
		var out struct {
			buyer   string
			seller  string
			arbiter string
		}
		err := db.QueryRow(`SELECT buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex FROM direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).
			Scan(&out.buyer, &out.seller, &out.arbiter)
		return out, err
	})
	return out.buyer, out.seller, out.arbiter, err
}

func dbLoadDirectSessionDealID(ctx context.Context, store *clientDB, sessionID string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var dealID string
		err := db.QueryRow(`SELECT deal_id FROM direct_transfer_pools WHERE session_id=?`, strings.TrimSpace(sessionID)).Scan(&dealID)
		return strings.TrimSpace(dealID), err
	})
}

func dbLoadDirectDealSeedHash(ctx context.Context, store *clientDB, dealID string) (string, error) {
	// 运行期辅助查询：只用于恢复直连链路的 seed 关联。
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedHash string
		err := db.QueryRow(`SELECT seed_hash FROM direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).Scan(&seedHash)
		return strings.ToLower(strings.TrimSpace(seedHash)), err
	})
}

func dbLoadDirectTransferPoolRow(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (directTransferPoolRow, error) {
		return loadDirectTransferPoolRowDB(db, sessionID)
	})
}

func dbLoadDirectTransferPoolRowTx(tx *sql.Tx, sessionID string) (directTransferPoolRow, error) {
	if tx == nil {
		return directTransferPoolRow{}, fmt.Errorf("tx is nil")
	}
	return loadDirectTransferPoolRowDB(tx, sessionID)
}

func loadDirectTransferPoolRowDB(queryer interface {
	QueryRow(query string, args ...any) *sql.Row
}, sessionID string) (directTransferPoolRow, error) {
	var row directTransferPoolRow
	err := queryer.QueryRow(
		`SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			buyer_pubkey_hex AS buyer_pubkey_hex_alias,
			seller_pubkey_hex AS seller_pubkey_hex_alias,
			arbiter_pubkey_hex AS arbiter_pubkey_hex_alias,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
		 FROM direct_transfer_pools WHERE session_id=?`,
		strings.TrimSpace(sessionID),
	).Scan(
		&row.SessionID, &row.DealID, &row.BuyerPubHex, &row.SellerPubHex, &row.ArbiterPubHex,
		&row.BuyerPubKeyHex, &row.SellerPubKeyHex, &row.ArbiterPubKeyHex,
		&row.PoolAmount, &row.SpendTxFee, &row.SequenceNum, &row.SellerAmount, &row.BuyerAmount,
		&row.CurrentTxHex, &row.BaseTxHex, &row.BaseTxID, &row.Status, &row.FeeRateSatByte, &row.LockBlocks, &row.CreatedAtUnix, &row.UpdatedAtUnix,
	)
	return row, err
}

func dbUpsertDirectTransferPoolOpen(ctx context.Context, store *clientDB, req directTransferPoolOpenReq, sessionID string, dealID string, buyerPubHex string, sellerPubHex string, arbiterPubHex string, currentTxHex string, baseTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO direct_transfer_pools(
				session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
				pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(session_id) DO UPDATE SET
				deal_id=excluded.deal_id,
				buyer_pubkey_hex=excluded.buyer_pubkey_hex,
				seller_pubkey_hex=excluded.seller_pubkey_hex,
				arbiter_pubkey_hex=excluded.arbiter_pubkey_hex,
				pool_amount=excluded.pool_amount,
				spend_tx_fee=excluded.spend_tx_fee,
				sequence_num=excluded.sequence_num,
				seller_amount=excluded.seller_amount,
				buyer_amount=excluded.buyer_amount,
				current_tx_hex=excluded.current_tx_hex,
				base_tx_hex=excluded.base_tx_hex,
				base_txid=excluded.base_txid,
				status=excluded.status,
				fee_rate_sat_byte=excluded.fee_rate_sat_byte,
				lock_blocks=excluded.lock_blocks,
				updated_at_unix=excluded.updated_at_unix`,
			sessionID, dealID, buyerPubHex, sellerPubHex, arbiterPubHex,
			req.PoolAmount, req.SpendTxFee, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex, baseTxHex, strings.TrimSpace(req.BaseTxID), "active", req.FeeRateSatByte, req.LockBlocks, now, now,
		); err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(tx, directTransferPoolSessionFactInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: sellerPubHex,
			SellerPubHex:       sellerPubHex,
			ArbiterPubHex:      arbiterPubHex,
			PoolAmountSat:      req.PoolAmount,
			SpendTxFeeSat:      req.SpendTxFee,
			FeeRateSatByte:     req.FeeRateSatByte,
			LockBlocks:         req.LockBlocks,
			OpenBaseTxID:       strings.TrimSpace(req.BaseTxID),
			Status:             "active",
			CreatedAtUnix:      now,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		return dbUpsertDirectTransferPoolAllocationTx(tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "open",
			SequenceNum:      req.Sequence,
			PayeeAmountAfter: 0,
			PayerAmountAfter: req.PoolAmount,
			TxID:             strings.TrimSpace(req.BaseTxID),
			TxHex:            baseTxHex,
			CreatedAtUnix:    now,
		})
	})
}

func dbUpdateDirectTransferPoolPay(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string, delta uint64) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		row, err := loadDirectTransferPoolRowDB(tx, sessionID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE direct_transfer_pools SET sequence_num=?,seller_amount=?,buyer_amount=?,current_tx_hex=?,updated_at_unix=? WHERE session_id=?`,
			sequence, sellerAmount, buyerAmount, currentTxHex, now, sessionID,
		); err != nil {
			return err
		}
		txid, err := directTransferPoolTxIDFromHex(currentTxHex)
		if err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(tx, directTransferPoolSessionFactInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: row.SellerPubHex,
			SellerPubHex:       row.SellerPubHex,
			ArbiterPubHex:      row.ArbiterPubHex,
			PoolAmountSat:      row.PoolAmount,
			SpendTxFeeSat:      row.SpendTxFee,
			FeeRateSatByte:     row.FeeRateSatByte,
			LockBlocks:         row.LockBlocks,
			OpenBaseTxID:       row.BaseTxID,
			Status:             "active",
			CreatedAtUnix:      row.CreatedAtUnix,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		return dbUpsertDirectTransferPoolAllocationTx(tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "pay",
			SequenceNum:      sequence,
			PayeeAmountAfter: sellerAmount,
			PayerAmountAfter: buyerAmount,
			TxID:             txid,
			TxHex:            currentTxHex,
			CreatedAtUnix:    now,
		})
	})
}

func dbUpdateDirectTransferPoolClosing(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		row, err := loadDirectTransferPoolRowDB(tx, sessionID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE direct_transfer_pools SET status='closing',sequence_num=?,seller_amount=?,buyer_amount=?,current_tx_hex=?,updated_at_unix=? WHERE session_id=?`,
			sequence, sellerAmount, buyerAmount, currentTxHex, now, sessionID); err != nil {
			return err
		}
		txid, err := directTransferPoolTxIDFromHex(currentTxHex)
		if err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(tx, directTransferPoolSessionFactInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: row.SellerPubHex,
			SellerPubHex:       row.SellerPubHex,
			ArbiterPubHex:      row.ArbiterPubHex,
			PoolAmountSat:      row.PoolAmount,
			SpendTxFeeSat:      row.SpendTxFee,
			FeeRateSatByte:     row.FeeRateSatByte,
			LockBlocks:         row.LockBlocks,
			OpenBaseTxID:       row.BaseTxID,
			Status:             "closing",
			CreatedAtUnix:      row.CreatedAtUnix,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		return dbUpsertDirectTransferPoolAllocationTx(tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "close",
			SequenceNum:      sequence,
			PayeeAmountAfter: sellerAmount,
			PayerAmountAfter: buyerAmount,
			TxID:             txid,
			TxHex:            currentTxHex,
			CreatedAtUnix:    now,
		})
	})
}

func dbLoadSeedBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string) ([]byte, error) {
	seedPath, err := clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedPath string
		err := db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&seedPath)
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("seed not found")
		}
		return seedPath, err
	})
	if err != nil {
		return nil, err
	}
	return os.ReadFile(seedPath)
}

func dbLoadChunkBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32) ([]byte, error) {
	meta, err := clientDBValue(ctx, store, func(db *sql.DB) (struct {
		workspacePath string
		filePath      string
		chunkCount    uint32
	}, error) {
		var out struct {
			workspacePath string
			filePath      string
			chunkCount    uint32
		}
		if err := db.QueryRow(
			`SELECT s.chunk_count FROM seeds s WHERE s.seed_hash=?`,
			strings.ToLower(strings.TrimSpace(seedHash)),
		).Scan(&out.chunkCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return out, fmt.Errorf("seed not found")
			}
			return out, err
		}
		_ = db.QueryRow(`SELECT workspace_path,file_path FROM workspace_files WHERE seed_hash=? ORDER BY workspace_path ASC,file_path ASC LIMIT 1`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&out.workspacePath, &out.filePath)
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	if chunkIndex >= meta.chunkCount {
		return nil, fmt.Errorf("chunk_index out of range")
	}
	have, err := dbIsSeedChunkAvailable(ctx, store, seedHash, chunkIndex)
	if err != nil {
		return nil, err
	}
	if !have {
		return nil, fmt.Errorf("chunk not available")
	}
	filePath := workspacePathJoin(meta.workspacePath, meta.filePath)
	if filePath == "" {
		return nil, fmt.Errorf("workspace file not found")
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	offset := int64(chunkIndex) * seedBlockSize
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	out := make([]byte, seedBlockSize)
	n, err := io.ReadFull(f, out)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n < seedBlockSize {
		for i := n; i < seedBlockSize; i++ {
			out[i] = 0
		}
	}
	return out, nil
}
