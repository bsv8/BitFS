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

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procdirectdeals"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procdirecttransferpools"
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
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (struct {
		buyer   string
		seller  string
		arbiter string
	}, error) {
		var out struct {
			buyer   string
			seller  string
			arbiter string
		}
		row, err := tx.ProcDirectDeals.Query().
			Where(procdirectdeals.DealIDEQ(strings.TrimSpace(dealID))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return out, sql.ErrNoRows
			}
			return out, err
		}
		out.buyer = row.BuyerPubkeyHex
		out.seller = row.SellerPubkeyHex
		out.arbiter = row.ArbiterPubkeyHex
		return out, nil
	})
	return out.buyer, out.seller, out.arbiter, err
}

// dbLoadDirectSessionDealID 【协议运行层专用】按 session_id 查 deal_id
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断

// dbLoadDirectDealSeedHash 【协议运行层专用】恢复 deal 关联的 seed
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
func dbLoadDirectDealSeedHash(ctx context.Context, store *clientDB, dealID string) (string, error) {
	// 运行期辅助查询：只用于恢复直连链路的 seed 关联。
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		row, err := tx.ProcDirectDeals.Query().
			Where(procdirectdeals.DealIDEQ(strings.TrimSpace(dealID))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", sql.ErrNoRows
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(row.SeedHash)), nil
	})
}

// dbLoadDirectTransferPoolRow 【协议运行层专用】加载池运行时状态
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
// - 返回值中的 Status 是协议运行时状态，不代表业务结算状态
// - 业务状态请查 order_settlements
func dbLoadDirectTransferPoolRow(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolRow, error) {
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (directTransferPoolRow, error) {
		return loadDirectTransferPoolRowEntTx(ctx, tx, sessionID)
	})
}

func loadDirectTransferPoolRowEntTx(ctx context.Context, tx *gen.Tx, sessionID string) (directTransferPoolRow, error) {
	var row directTransferPoolRow
	if tx == nil {
		return row, fmt.Errorf("tx is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return row, fmt.Errorf("session_id is required")
	}
	node, err := tx.ProcDirectTransferPools.Query().
		Where(procdirecttransferpools.SessionIDEQ(sessionID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return row, sql.ErrNoRows
		}
		return row, err
	}
	row.SessionID = strings.TrimSpace(node.SessionID)
	row.DealID = strings.TrimSpace(node.DealID)
	row.BuyerPubHex = strings.TrimSpace(node.BuyerPubkeyHex)
	row.SellerPubHex = strings.TrimSpace(node.SellerPubkeyHex)
	row.ArbiterPubHex = strings.TrimSpace(node.ArbiterPubkeyHex)
	row.BuyerPubKeyHex = row.BuyerPubHex
	row.SellerPubKeyHex = row.SellerPubHex
	row.ArbiterPubKeyHex = row.ArbiterPubHex
	row.PoolAmount = uint64(node.PoolAmount)
	row.SpendTxFee = uint64(node.SpendTxFee)
	row.SequenceNum = uint32(node.SequenceNum)
	row.SellerAmount = uint64(node.SellerAmount)
	row.BuyerAmount = uint64(node.BuyerAmount)
	row.CurrentTxHex = strings.TrimSpace(node.CurrentTxHex)
	row.BaseTxHex = strings.TrimSpace(node.BaseTxHex)
	row.BaseTxID = strings.TrimSpace(node.BaseTxid)
	row.Status = strings.TrimSpace(node.Status)
	row.FeeRateSatByte = node.FeeRateSatByte
	row.LockBlocks = uint32(node.LockBlocks)
	row.CreatedAtUnix = node.CreatedAtUnix
	row.UpdatedAtUnix = node.UpdatedAtUnix
	return row, nil
}

func dbUpsertDirectTransferPoolOpen(ctx context.Context, store *clientDB, req directTransferPoolOpenReq, sessionID string, dealID string, buyerPubHex string, sellerPubHex string, arbiterPubHex string, currentTxHex string, baseTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		if row, err := loadDirectTransferPoolRowEntTx(ctx, tx, sessionID); err == nil {
			if strings.EqualFold(strings.TrimSpace(row.Status), "closed") {
				return fmt.Errorf("transfer pool is closed")
			}
		}
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(strings.TrimSpace(sessionID))).
			Only(ctx)
		if err == nil {
			_, err = tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
				SetDealID(strings.TrimSpace(dealID)).
				SetBuyerPubkeyHex(strings.TrimSpace(buyerPubHex)).
				SetSellerPubkeyHex(strings.TrimSpace(sellerPubHex)).
				SetArbiterPubkeyHex(strings.TrimSpace(arbiterPubHex)).
				SetPoolAmount(int64(req.PoolAmount)).
				SetSpendTxFee(int64(req.SpendTxFee)).
				SetSequenceNum(int64(req.Sequence)).
				SetSellerAmount(int64(req.SellerAmount)).
				SetBuyerAmount(int64(req.BuyerAmount)).
				SetCurrentTxHex(strings.TrimSpace(currentTxHex)).
				SetBaseTxHex(strings.TrimSpace(baseTxHex)).
				SetBaseTxid(strings.TrimSpace(req.BaseTxid)).
				SetStatus("active").
				SetFeeRateSatByte(req.FeeRateSatByte).
				SetLockBlocks(int64(req.LockBlocks)).
				SetUpdatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return err
			}
		} else if err != nil && !gen.IsNotFound(err) {
			return err
		} else {
			_, err = tx.ProcDirectTransferPools.Create().
				SetSessionID(strings.TrimSpace(sessionID)).
				SetDealID(strings.TrimSpace(dealID)).
				SetBuyerPubkeyHex(strings.TrimSpace(buyerPubHex)).
				SetSellerPubkeyHex(strings.TrimSpace(sellerPubHex)).
				SetArbiterPubkeyHex(strings.TrimSpace(arbiterPubHex)).
				SetPoolAmount(int64(req.PoolAmount)).
				SetSpendTxFee(int64(req.SpendTxFee)).
				SetSequenceNum(int64(req.Sequence)).
				SetSellerAmount(int64(req.SellerAmount)).
				SetBuyerAmount(int64(req.BuyerAmount)).
				SetCurrentTxHex(strings.TrimSpace(currentTxHex)).
				SetBaseTxHex(strings.TrimSpace(baseTxHex)).
				SetBaseTxid(strings.TrimSpace(req.BaseTxid)).
				SetStatus("active").
				SetFeeRateSatByte(req.FeeRateSatByte).
				SetLockBlocks(int64(req.LockBlocks)).
				SetCreatedAtUnix(now).
				SetUpdatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return err
			}
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

func dbUpdateDirectTransferPoolPay(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string, delta uint64, nextStatus string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	nextStatus = strings.TrimSpace(nextStatus)
	if nextStatus == "" {
		nextStatus = "paid"
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		row, err := loadDirectTransferPoolRowEntTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(row.Status), "closed") {
			return fmt.Errorf("transfer pool is closed")
		}
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(strings.TrimSpace(sessionID))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("transfer pool not found")
			}
			return err
		}
		if _, err := tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
			SetStatus(nextStatus).
			SetSequenceNum(int64(sequence)).
			SetSellerAmount(int64(sellerAmount)).
			SetBuyerAmount(int64(buyerAmount)).
			SetCurrentTxHex(strings.TrimSpace(currentTxHex)).
			SetUpdatedAtUnix(now).
			Save(ctx); err != nil {
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
			Status:             nextStatus,
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
			Status:             nextStatus,
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

func dbUpdateDirectTransferPoolStatus(ctx context.Context, store *clientDB, sessionID string, status string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	status = strings.TrimSpace(status)
	if sessionID == "" || status == "" {
		return fmt.Errorf("session_id and status are required")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(sessionID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("transfer pool not found")
			}
			return err
		}
		if _, err := tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
			SetStatus(status).
			SetUpdatedAtUnix(now).
			Save(ctx); err != nil {
			return err
		}
		return nil
	})
}

func dbUpsertDirectArbitrationState(ctx context.Context, store *clientDB, sessionID string, evidenceKey string, status string, txid string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	evidenceKey = strings.TrimSpace(evidenceKey)
	status = strings.TrimSpace(status)
	if sessionID == "" || evidenceKey == "" || status == "" {
		return fmt.Errorf("session_id/evidence_key/status are required")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		row, err := loadDirectTransferPoolRowEntTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		// 状态机幂等：同状态重复请求直接视为成功。
		if strings.EqualFold(strings.TrimSpace(row.Status), status) {
			return nil
		}
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(sessionID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("transfer pool not found")
			}
			return err
		}
		_ = row
		_ = evidenceKey
		_ = txid
		_, err = tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
			SetStatus(status).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

// dbApplyDirectTransferPoolArbitrated 在仲裁交易上链后写入最终会话快照。
// 说明：
// - 这里不做“close 记账事实”的复用，避免把仲裁路径伪装成正常 close；
// - 只同步会话主状态和金额快照，保证状态机与链上花费一致。
func dbApplyDirectTransferPoolArbitrated(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	currentTxHex = strings.TrimSpace(currentTxHex)
	if sessionID == "" || currentTxHex == "" {
		return fmt.Errorf("session_id/current_tx_hex are required")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(sessionID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("transfer pool not found")
			}
			return err
		}
		_, err = tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
			SetStatus("arbitrated").
			SetSequenceNum(int64(sequence)).
			SetSellerAmount(int64(sellerAmount)).
			SetBuyerAmount(int64(buyerAmount)).
			SetCurrentTxHex(currentTxHex).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

func dbUpdateDirectTransferPoolClosing(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		row, err := loadDirectTransferPoolRowEntTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		existing, err := tx.ProcDirectTransferPools.Query().
			Where(procdirecttransferpools.SessionIDEQ(strings.TrimSpace(sessionID))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("transfer pool not found")
			}
			return err
		}
		if _, err := tx.ProcDirectTransferPools.UpdateOneID(existing.ID).
			SetStatus("closed").
			SetSequenceNum(int64(sequence)).
			SetSellerAmount(int64(sellerAmount)).
			SetBuyerAmount(int64(buyerAmount)).
			SetCurrentTxHex(strings.TrimSpace(currentTxHex)).
			SetUpdatedAtUnix(now).
			Save(ctx); err != nil {
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
	seedPath, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		row, err := tx.BizSeeds.Query().
			Where(bizseeds.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", fmt.Errorf("seed not found")
			}
			return "", err
		}
		return row.SeedFilePath, nil
	})
	if err != nil {
		return nil, err
	}
	return os.ReadFile(seedPath)
}

func dbLoadChunkBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32) ([]byte, error) {
	meta, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (struct {
		workspacePath string
		filePath      string
		chunkCount    uint32
	}, error) {
		var out struct {
			workspacePath string
			filePath      string
			chunkCount    uint32
		}
		seedRow, err := tx.BizSeeds.Query().
			Where(bizseeds.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return out, fmt.Errorf("seed not found")
			}
			return out, err
		}
		out.chunkCount = uint32(seedRow.ChunkCount)
		fileRow, err := tx.BizWorkspaceFiles.Query().
			Where(bizworkspacefiles.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).
			First(ctx)
		if err == nil {
			out.workspacePath = fileRow.WorkspacePath
			out.filePath = fileRow.FilePath
		}
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
