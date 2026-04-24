package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseeds"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procdirectdeals"
)

// 设计说明：
// - 直连池和 seed 读取是运行期高频路径；
// - SQL 留在这里，协议处理只保留校验和签名逻辑；
// - 文件读取虽然最终走文件系统，但 seed 路径解析也统一收在 db 层。
//
// ⚠️ 第九阶段整改（2026-04）：
// - proc_direct_transfer_pools 已删除，相关函数改为返回错误
// - proc_direct_deals 保留，用于协议运行期上下文恢复
// - 业务完成状态统一以 order_settlements 为准，禁止根据 proc_direct_transfer_pools.status 判断业务是否完成
// - 如需查询业务状态，请使用 GetFrontOrderSettlementSummary

// dbLoadDirectDealParties 【协议运行层专用】恢复 deal 参与方上下文
// ⚠️ 第五步：仅用于协议运行期上下文恢复，禁止用于业务状态判断
func dbLoadDirectDealParties(ctx context.Context, store *clientDB, dealID string) (string, string, string, error) {
	// 运行期辅助查询：只给 direct transfer 串 buyer / seller / arbiter 上下文。
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (struct {
		buyer   string
		seller  string
		arbiter string
	}, error) {
		var out struct {
			buyer   string
			seller  string
			arbiter string
		}
		row, err := root.ProcDirectDeals.Query().
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
	return readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		row, err := root.ProcDirectDeals.Query().
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
// ⚠️ 第九阶段：proc_direct_transfer_pools 已删除，此函数返回错误
func dbLoadDirectTransferPoolRow(ctx context.Context, store *clientDB, sessionID string) (directTransferPoolRow, error) {
	return directTransferPoolRow{}, fmt.Errorf("dbLoadDirectTransferPoolRow is no longer available: proc_direct_transfer_pools schema has been removed")
}

// loadDirectTransferPoolRowEntRoot 已废弃
func loadDirectTransferPoolRowEntRoot(ctx context.Context, root EntReadRoot, sessionID string) (directTransferPoolRow, error) {
	var row directTransferPoolRow
	if root == nil {
		return row, fmt.Errorf("tx is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return row, fmt.Errorf("session_id is required")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return row, fmt.Errorf("loadDirectTransferPoolRowEntRoot is no longer available: proc_direct_transfer_pools schema has been removed")
}

func loadDirectTransferPoolRowEntTx(ctx context.Context, tx EntWriteRoot, sessionID string) (directTransferPoolRow, error) {
	if tx == nil {
		return directTransferPoolRow{}, fmt.Errorf("tx is nil")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return directTransferPoolRow{}, fmt.Errorf("loadDirectTransferPoolRowEntTx is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbUpsertDirectTransferPoolOpen 已废弃
func dbUpsertDirectTransferPoolOpen(ctx context.Context, store *clientDB, req directTransferPoolOpenReq, sessionID string, dealID string, buyerPubHex string, sellerPubHex string, arbiterPubHex string, currentTxHex string, baseTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbUpsertDirectTransferPoolOpen is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbUpdateDirectTransferPoolPay 已废弃
func dbUpdateDirectTransferPoolPay(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string, delta uint64, nextStatus string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbUpdateDirectTransferPoolPay is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbUpdateDirectTransferPoolStatus 已废弃
func dbUpdateDirectTransferPoolStatus(ctx context.Context, store *clientDB, sessionID string, status string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	status = strings.TrimSpace(status)
	if sessionID == "" || status == "" {
		return fmt.Errorf("session_id and status are required")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbUpdateDirectTransferPoolStatus is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbUpsertDirectArbitrationState 已废弃
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
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbUpsertDirectArbitrationState is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbApplyDirectTransferPoolArbitrated 在仲裁交易上链后写入最终会话快照。
// 说明：
// - 这里不做"close 记账事实"的复用，避免把仲裁路径伪装成正常 close；
// - 只同步会话主状态和金额快照，保证状态机与链上花费一致。
// 第九阶段：proc_direct_transfer_pools 已删除
func dbApplyDirectTransferPoolArbitrated(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	currentTxHex = strings.TrimSpace(currentTxHex)
	if sessionID == "" || currentTxHex == "" {
		return fmt.Errorf("session_id/current_tx_hex are required")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbApplyDirectTransferPoolArbitrated is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbUpdateDirectTransferPoolClosing 已废弃
func dbUpdateDirectTransferPoolClosing(ctx context.Context, store *clientDB, sessionID string, sequence uint32, sellerAmount uint64, buyerAmount uint64, currentTxHex string) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	// 第九阶段：proc_direct_transfer_pools 已删除
	return fmt.Errorf("dbUpdateDirectTransferPoolClosing is no longer available: proc_direct_transfer_pools schema has been removed")
}

// dbLoadSeedBytesBySeedHash 【协议运行层专用】按 seed_hash 读取 seed 文件内容
// seed 文件存储在本地文件系统，db 层只负责路径解析
func dbLoadSeedBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string) ([]byte, error) {
	seedPath, err := readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		row, err := root.BizSeeds.Query().
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

// dbLoadChunkBytesBySeedHash 【协议运行层专用】按 seed_hash 和 chunk_index 读取分块数据
func dbLoadChunkBytesBySeedHash(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32) ([]byte, error) {
	var meta struct {
		workspacePath string
		filePath      string
		chunkCount    uint32
	}
	seedCount, err := readEntValue(ctx, store, func(root EntReadRoot) (uint32, error) {
		seedRow, err := root.BizSeeds.Query().
			Where(bizseeds.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, fmt.Errorf("seed not found")
			}
			return 0, err
		}
		return uint32(seedRow.ChunkCount), nil
	})
	if err != nil {
		return nil, err
	}
	absPath, _, err := store.FindLatestWorkspaceFileBySeedHash(ctx, seedHash)
	if err != nil {
		return nil, err
	}
	meta = struct {
		workspacePath string
		filePath      string
		chunkCount    uint32
	}{
		workspacePath: filepath.Dir(absPath),
		filePath:      filepath.Base(absPath),
		chunkCount:    seedCount,
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
	offset := int64(chunkIndex) * int64(seedBlockSize)
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
