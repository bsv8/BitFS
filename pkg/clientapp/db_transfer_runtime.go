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
//
// ⚠️ 第五步分层护栏（2026-04）：
// - 本文件所有函数【仅允许协议运行层使用】，禁止在 handler/业务查询/财务查询中复用
// - proc_direct_transfer_pools / proc_direct_deals 已降级为【协议运行态表】
// - 这些表只保留协议运行期的上下文（deal/parties/session/seed_hash 等），用于协议流恢复/校验
// - 业务完成状态统一以 order_settlements 为准，禁止根据 proc_direct_transfer_pools.status 判断业务是否完成
// - 如需查询业务状态，请使用 GetFrontOrderSettlementSummary

// dbLoadDirectDealParties 【协议运行层专用】恢复 deal 参与方上下文
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
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
		err := QueryRowContext(ctx, db, `SELECT buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex FROM proc_direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).
			Scan(&out.buyer, &out.seller, &out.arbiter)
		return out, err
	})
	return out.buyer, out.seller, out.arbiter, err
}

// dbLoadDirectSessionDealID 【协议运行层专用】按 session_id 查 deal_id
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
func dbLoadDirectSessionDealID(ctx context.Context, store *clientDB, sessionID string) (string, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var dealID string
		err := QueryRowContext(ctx, db, `SELECT deal_id FROM proc_direct_transfer_pools WHERE session_id=?`, strings.TrimSpace(sessionID)).Scan(&dealID)
		return strings.TrimSpace(dealID), err
	})
}

// dbLoadDirectDealSeedHash 【协议运行层专用】恢复 deal 关联的 seed
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
func dbLoadDirectDealSeedHash(ctx context.Context, store *clientDB, dealID string) (string, error) {
	// 运行期辅助查询：只用于恢复直连链路的 seed 关联。
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedHash string
		err := QueryRowContext(ctx, db, `SELECT seed_hash FROM proc_direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).Scan(&seedHash)
		return strings.ToLower(strings.TrimSpace(seedHash)), err
	})
}

// dbLoadDirectTransferPoolRow 【协议运行层专用】加载池运行时状态
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
// - 返回值中的 Status 是协议运行时状态，不代表业务结算状态
// - 业务状态请查 order_settlements
func dbLoadDirectTransferPoolRow(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolRow, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (directTransferPoolRow, error) {
		return loadDirectTransferPoolRowDB(ctx, db, sessionID)
	})
}

func loadDirectTransferPoolRowDB(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, sessionID string) (directTransferPoolRow, error) {
	var row directTransferPoolRow
	err := QueryRowContext(ctx, queryer,
		`SELECT
			session_id,deal_id,
			buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,
			buyer_pubkey_hex AS buyer_pubkey_hex_alias,
			seller_pubkey_hex AS seller_pubkey_hex_alias,
			arbiter_pubkey_hex AS arbiter_pubkey_hex_alias,
			pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
		 FROM proc_direct_transfer_pools WHERE session_id=?`,
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
		if row, err := loadDirectTransferPoolRowDB(ctx, tx, sessionID); err == nil {
			if strings.EqualFold(strings.TrimSpace(row.Status), "closed") {
				return fmt.Errorf("transfer pool is closed")
			}
		}
		if _, err := ExecContext(ctx, tx,
			`INSERT INTO proc_direct_transfer_pools(
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
			req.PoolAmount, req.SpendTxFee, req.Sequence, req.SellerAmount, req.BuyerAmount, currentTxHex, baseTxHex, strings.TrimSpace(req.BaseTxid), "active", req.FeeRateSatByte, req.LockBlocks, now, now,
		); err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(ctx, tx, directTransferPoolSessionFactInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: sellerPubHex,
			SellerPubHex:       sellerPubHex,
			ArbiterPubHex:      arbiterPubHex,
			PoolAmountSat:      req.PoolAmount,
			SpendTxFeeSat:      req.SpendTxFee,
			FeeRateSatByte:     req.FeeRateSatByte,
			LockBlocks:         req.LockBlocks,
			OpenBaseTxID:       strings.TrimSpace(req.BaseTxid),
			Status:             "active",
			CreatedAtUnix:      now,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		if err := dbUpsertDirectTransferBizPoolSnapshotTx(ctx, tx, directTransferBizPoolSnapshotInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: sellerPubHex,
			SellerPubHex:       sellerPubHex,
			ArbiterPubHex:      arbiterPubHex,
			PoolAmountSat:      req.PoolAmount + req.SpendTxFee,
			SpendTxFeeSat:      req.SpendTxFee,
			AllocatedSat:       0,
			CycleFeeSat:        req.SpendTxFee,
			AvailableSat:       req.PoolAmount,
			NextSequenceNum:    req.Sequence + 1,
			Status:             "active",
			OpenBaseTxID:       strings.TrimSpace(req.BaseTxid),
			OpenAllocationID:   directTransferPoolAllocationID(sessionID, PoolBusinessActionOpen, req.Sequence),
			CreatedAtUnix:      now,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		// Step 4 出项关联：从交易 hex 提取 input UTXO 明细
		utxoFacts, err := dbExtractUTXOFactsFromTxHex(baseTxHex, now)
		if err != nil {
			return fmt.Errorf("extract utxo facts from open tx: %w", err)
		}
		return dbUpsertDirectTransferPoolAllocationTx(ctx, tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "open",
			SequenceNum:      req.Sequence,
			PayeeAmountAfter: 0,
			PayerAmountAfter: req.PoolAmount,
			TxID:             strings.TrimSpace(req.BaseTxid),
			TxHex:            baseTxHex,
			CreatedAtUnix:    now,
			UTXOFacts:        utxoFacts,
		})
	})
}

func dbUpdateDirectTransferPoolPay(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string, delta uint64) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		row, err := loadDirectTransferPoolRowDB(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(row.Status), "closed") {
			return fmt.Errorf("transfer pool is closed")
		}
		if _, err := ExecContext(ctx, tx,
			`UPDATE proc_direct_transfer_pools SET sequence_num=?,seller_amount=?,buyer_amount=?,current_tx_hex=?,updated_at_unix=? WHERE session_id=?`,
			sequence, sellerAmount, buyerAmount, currentTxHex, now, sessionID,
		); err != nil {
			return err
		}
		txid, err := directTransferPoolTxIDFromHex(currentTxHex)
		if err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(ctx, tx, directTransferPoolSessionFactInput{
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
		openAllocationID, err := dbGetPoolAllocationIDByKindTx(ctx, tx, sessionID, PoolBusinessActionOpen)
		if err != nil {
			return fmt.Errorf("load open allocation id: %w", err)
		}
		if err := dbUpsertDirectTransferBizPoolSnapshotTx(ctx, tx, directTransferBizPoolSnapshotInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: row.SellerPubHex,
			SellerPubHex:       row.SellerPubHex,
			ArbiterPubHex:      row.ArbiterPubHex,
			PoolAmountSat:      row.PoolAmount + row.SpendTxFee,
			SpendTxFeeSat:      row.SpendTxFee,
			AllocatedSat:       sellerAmount,
			CycleFeeSat:        row.SpendTxFee,
			AvailableSat:       buyerAmount,
			NextSequenceNum:    sequence + 1,
			Status:             "active",
			OpenBaseTxID:       row.BaseTxID,
			OpenAllocationID:   openAllocationID,
			CreatedAtUnix:      row.CreatedAtUnix,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		// Step 4 出项关联：从交易 hex 提取 input UTXO 明细
		utxoFacts, err := dbExtractUTXOFactsFromTxHex(currentTxHex, now)
		if err != nil {
			return fmt.Errorf("extract utxo facts from pay tx: %w", err)
		}
		return dbUpsertDirectTransferPoolAllocationTx(ctx, tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "pay",
			SequenceNum:      sequence,
			PayeeAmountAfter: sellerAmount,
			PayerAmountAfter: buyerAmount,
			TxID:             txid,
			TxHex:            currentTxHex,
			CreatedAtUnix:    now,
			UTXOFacts:        utxoFacts,
		})
	})
}

func dbUpdateDirectTransferPoolClosing(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return store.Tx(ctx, func(tx *sql.Tx) error {
		row, err := loadDirectTransferPoolRowDB(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if _, err := ExecContext(ctx, tx, `UPDATE proc_direct_transfer_pools SET status='closed',sequence_num=?,seller_amount=?,buyer_amount=?,current_tx_hex=?,updated_at_unix=? WHERE session_id=?`,
			sequence, sellerAmount, buyerAmount, currentTxHex, now, sessionID); err != nil {
			return err
		}
		txid, err := directTransferPoolTxIDFromHex(currentTxHex)
		if err != nil {
			return err
		}
		if err := dbUpsertDirectTransferPoolSessionTx(ctx, tx, directTransferPoolSessionFactInput{
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
			Status:             "closed",
			CreatedAtUnix:      row.CreatedAtUnix,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		openAllocationID, err := dbGetPoolAllocationIDByKindTx(ctx, tx, sessionID, PoolBusinessActionOpen)
		if err != nil {
			return fmt.Errorf("load open allocation id: %w", err)
		}
		if err := dbUpsertDirectTransferBizPoolSnapshotTx(ctx, tx, directTransferBizPoolSnapshotInput{
			SessionID:          sessionID,
			PoolScheme:         "2of3",
			CounterpartyPubHex: row.SellerPubHex,
			SellerPubHex:       row.SellerPubHex,
			ArbiterPubHex:      row.ArbiterPubHex,
			PoolAmountSat:      row.PoolAmount + row.SpendTxFee,
			SpendTxFeeSat:      row.SpendTxFee,
			AllocatedSat:       sellerAmount,
			CycleFeeSat:        row.SpendTxFee,
			AvailableSat:       buyerAmount,
			NextSequenceNum:    sequence + 1,
			Status:             "closed",
			OpenBaseTxID:       row.BaseTxID,
			OpenAllocationID:   openAllocationID,
			CloseAllocationID:  directTransferPoolAllocationID(sessionID, PoolBusinessActionClose, sequence),
			CreatedAtUnix:      row.CreatedAtUnix,
			UpdatedAtUnix:      now,
		}); err != nil {
			return err
		}
		// Step 4 出项关联：从交易 hex 提取 input UTXO 明细
		utxoFacts, err := dbExtractUTXOFactsFromTxHex(currentTxHex, now)
		if err != nil {
			return fmt.Errorf("extract utxo facts from closing tx: %w", err)
		}
		return dbUpsertDirectTransferPoolAllocationTx(ctx, tx, directTransferPoolAllocationFactInput{
			SessionID:        sessionID,
			AllocationKind:   "close",
			SequenceNum:      sequence,
			PayeeAmountAfter: sellerAmount,
			PayerAmountAfter: buyerAmount,
			TxID:             txid,
			TxHex:            currentTxHex,
			CreatedAtUnix:    now,
			UTXOFacts:        utxoFacts,
		})
	})
}

func dbLoadSeedBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string) ([]byte, error) {
	seedPath, err := clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedPath string
		err := QueryRowContext(ctx, db, `SELECT seed_file_path FROM biz_seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&seedPath)
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
		if err := QueryRowContext(ctx, db,
			`SELECT s.chunk_count FROM biz_seeds s WHERE s.seed_hash=?`,
			strings.ToLower(strings.TrimSpace(seedHash)),
		).Scan(&out.chunkCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return out, fmt.Errorf("seed not found")
			}
			return out, err
		}
		_ = QueryRowContext(ctx, db, `SELECT workspace_path,file_path FROM biz_workspace_files WHERE seed_hash=? ORDER BY workspace_path ASC,file_path ASC LIMIT 1`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&out.workspacePath, &out.filePath)
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
