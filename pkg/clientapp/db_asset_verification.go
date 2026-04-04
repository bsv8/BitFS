package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// ==================== Unknown 资产确认流程 ====================
// 设计说明：
// - 对 wallet_utxo 中 allocation_class='unknown' 且 value_satoshi=1 的 UTXO
// - 通过 WOC 查询确认其真实资产类型（BSV20/BSV21/BSV）
// - 确认后更新 allocation_class 并写入 fact_chain_asset_flows
// - 保持幂等：同一 UTXO 不会重复确认，同一 flow 不会重复写入

const (
	assetVerificationTimeout = 30 * time.Second
	assetVerificationMaxBatch = 50
)

// unknownAssetVerificationTask 控制并发确认任务的执行
type unknownAssetVerificationTask struct {
	mu        sync.Mutex
	inFlight  bool
	lastRunAt int64
}

var assetVerificationTask = &unknownAssetVerificationTask{}

// unknownUTXORow 待确认的 unknown UTXO 行
type unknownUTXORow struct {
	UTXOID          string
	WalletID        string
	Address         string
	TxID            string
	Vout            uint32
	ValueSatoshi    uint64
	CreatedAtUnix   int64
}

// wocTokenEvidence WOC 返回的 token 证据
type wocTokenEvidence struct {
	IsToken      bool
	TokenStandard string // "bsv20" | "bsv21" | ""
	TokenID      string
	Symbol       string
	QuantityText string
}

// dbListUnknownOneSatUTXOs 列出待确认的 unknown 1-sat UTXO
// 输入条件：state='unspent' AND allocation_class='unknown' AND value_satoshi=1
func dbListUnknownOneSatUTXOs(ctx context.Context, store *clientDB, address string, limit int) ([]unknownUTXORow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if limit <= 0 {
		limit = assetVerificationMaxBatch
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]unknownUTXORow, error) {
		address = strings.TrimSpace(address)
		if address == "" {
			return nil, fmt.Errorf("wallet address is empty")
		}
		walletID := walletIDByAddress(address)
		rows, err := db.Query(
			`SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,created_at_unix
			 FROM wallet_utxo
			 WHERE wallet_id=? AND address=? AND state='unspent' 
			   AND allocation_class=? AND value_satoshi=1
			 ORDER BY created_at_unix ASC, txid ASC, vout ASC
			 LIMIT ?`,
			walletID, address, walletUTXOAllocationUnknown, limit,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]unknownUTXORow, 0, limit)
		for rows.Next() {
			var row unknownUTXORow
			if err := rows.Scan(&row.UTXOID, &row.WalletID, &row.Address, 
				&row.TxID, &row.Vout, &row.ValueSatoshi, &row.CreatedAtUnix); err != nil {
				return nil, err
			}
			row.UTXOID = strings.ToLower(strings.TrimSpace(row.UTXOID))
			row.TxID = strings.ToLower(strings.TrimSpace(row.TxID))
			out = append(out, row)
		}
		return out, rows.Err()
	})
}

// dbUpdateUTXOAllocationClass 更新 UTXO 的 allocation_class
func dbUpdateUTXOAllocationClass(ctx context.Context, store *clientDB, utxoID string, newClass string, reason string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		utxoID = strings.ToLower(strings.TrimSpace(utxoID))
		newClass = normalizeWalletUTXOAllocationClass(newClass)
		if utxoID == "" {
			return fmt.Errorf("utxo_id is required")
		}
		_, err := db.Exec(
			`UPDATE wallet_utxo 
			 SET allocation_class=?, allocation_reason=?, updated_at_unix=?
			 WHERE utxo_id=?`,
			newClass, strings.TrimSpace(reason), time.Now().Unix(), utxoID,
		)
		return err
	})
}

// runUnknownAssetVerification 执行 unknown 资产确认流程
// 在链同步成功后调用，受并发保护
func runUnknownAssetVerification(ctx context.Context, rt *Runtime, store *clientDB, address string, trigger string) error {
	if rt == nil || store == nil {
		return fmt.Errorf("runtime or store is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	
	// 并发保护：同一时刻只执行一个确认任务
	assetVerificationTask.mu.Lock()
	if assetVerificationTask.inFlight {
		assetVerificationTask.mu.Unlock()
		obs.Info("bitcast-client", "asset_verification_skipped", map[string]any{
			"reason": "already_in_flight",
			"address": address,
		})
		return nil
	}
	// 简单节流：距离上次执行至少 5 秒
	now := time.Now().Unix()
	if now-assetVerificationTask.lastRunAt < 5 {
		assetVerificationTask.mu.Unlock()
		return nil
	}
	assetVerificationTask.inFlight = true
	assetVerificationTask.lastRunAt = now
	assetVerificationTask.mu.Unlock()
	
	defer func() {
		assetVerificationTask.mu.Lock()
		assetVerificationTask.inFlight = false
		assetVerificationTask.mu.Unlock()
	}()
	
	taskCtx, cancel := context.WithTimeout(ctx, assetVerificationTimeout)
	defer cancel()
	
	// 1. 列出待确认的 unknown UTXO
	rows, err := dbListUnknownOneSatUTXOs(taskCtx, store, address, assetVerificationMaxBatch)
	if err != nil {
		obs.Error("bitcast-client", "asset_verification_list_failed", map[string]any{
			"error": err.Error(),
			"address": address,
		})
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	
	obs.Info("bitcast-client", "asset_verification_started", map[string]any{
		"address": address,
		"unknown_count": len(rows),
		"trigger": trigger,
	})
	
	// 2. 批量查询 WOC 获取 token 证据
	verifiedCount := 0
	failedCount := 0
	skippedCount := 0
	
	for _, row := range rows {
		select {
		case <-taskCtx.Done():
			obs.Info("bitcast-client", "asset_verification_cancelled", map[string]any{
				"address": address,
			})
			return taskCtx.Err()
		default:
		}
		
		evidence, err := queryWOCTokenEvidence(taskCtx, rt, row.Address, row.TxID, row.Vout)
		if err != nil {
			// WOC 查询失败：保持 unknown，不入账
			obs.Error("bitcast-client", "asset_verification_woc_failed", map[string]any{
				"utxo_id": row.UTXOID,
				"error": err.Error(),
			})
			failedCount++
			continue
		}
		
		// 3. 根据证据更新分类并写入 fact
		if err := processAssetVerificationResult(ctx, store, row, evidence, trigger); err != nil {
			obs.Error("bitcast-client", "asset_verification_process_failed", map[string]any{
				"utxo_id": row.UTXOID,
				"error": err.Error(),
			})
			failedCount++
			continue
		}
		verifiedCount++
	}
	
	obs.Info("bitcast-client", "asset_verification_completed", map[string]any{
		"address": address,
		"verified_count": verifiedCount,
		"failed_count": failedCount,
		"skipped_count": skippedCount,
		"total_count": len(rows),
	})
	return nil
}

// processAssetVerificationResult 处理单个 UTXO 的确认结果
func processAssetVerificationResult(ctx context.Context, store *clientDB, row unknownUTXORow, evidence *wocTokenEvidence, trigger string) error {
	if evidence == nil {
		return fmt.Errorf("evidence is nil")
	}
	
	// 判定新的 allocation_class
	var newClass, assetKind, note string
	var tokenID, symbol, quantityText string
	
	switch {
	case evidence.IsToken && evidence.TokenStandard == "bsv21":
		newClass = walletUTXOAllocationProtectedAsset
		assetKind = "BSV21"
		tokenID = evidence.TokenID
		symbol = evidence.Symbol
		quantityText = evidence.QuantityText
		note = "verified as BSV21 token by WOC"
		
	case evidence.IsToken && evidence.TokenStandard == "bsv20":
		newClass = walletUTXOAllocationProtectedAsset
		assetKind = "BSV20"
		tokenID = evidence.TokenID
		symbol = evidence.Symbol
		quantityText = evidence.QuantityText
		note = "verified as BSV20 token by WOC"
		
	default:
		// 非 token，回落为 BSV
		newClass = walletUTXOAllocationPlainBSV
		assetKind = "BSV"
		note = "verified as plain BSV by WOC"
	}
	
	// 更新 wallet_utxo 分类
	if err := dbUpdateUTXOAllocationClass(ctx, store, row.UTXOID, newClass, note); err != nil {
		return fmt.Errorf("update allocation class: %w", err)
	}
	
	// 幂等写入 fact_chain_asset_flows
	// 设计说明：
	// - 只对已确认的 token 或 plain_bsv 写入 IN 事实
	// - 使用幂等写入防止重复
	flowEntry := chainAssetFlowEntry{
		FlowID:         fmt.Sprintf("flow_in_%s", row.UTXOID),
		WalletID:       row.WalletID,
		Address:        row.Address,
		Direction:      "IN",
		AssetKind:      assetKind,
		TokenID:        tokenID,
		UTXOID:         row.UTXOID,
		TxID:           row.TxID,
		Vout:           row.Vout,
		AmountSatoshi:  int64(row.ValueSatoshi),
		QuantityText:   quantityText,
		OccurredAtUnix: row.CreatedAtUnix,
		EvidenceSource: "WOC",
		Note:           note + " (trigger: " + trigger + ")",
		Payload: map[string]any{
			"verification_trigger": trigger,
			"token_symbol": symbol,
		},
	}
	
	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, flowEntry); err != nil {
		return fmt.Errorf("append asset flow: %w", err)
	}
	
	return nil
}

// queryWOCTokenEvidence 查询 WOC 获取指定 outpoint 的 token 证据
// 按优先级：BSV21 > BSV20 > 非 token
func queryWOCTokenEvidence(ctx context.Context, rt *Runtime, address string, txid string, vout uint32) (*wocTokenEvidence, error) {
	if rt == nil || rt.WalletChain == nil {
		return nil, fmt.Errorf("wallet chain not initialized")
	}
	
	// 首先查询 BSV21
	bsv21Evidence, err := queryBSV21TokenEvidence(ctx, rt, address, txid, vout)
	if err != nil {
		return nil, fmt.Errorf("query BSV21 evidence: %w", err)
	}
	if bsv21Evidence != nil {
		return bsv21Evidence, nil
	}
	
	// 然后查询 BSV20
	bsv20Evidence, err := queryBSV20TokenEvidence(ctx, rt, address, txid, vout)
	if err != nil {
		return nil, fmt.Errorf("query BSV20 evidence: %w", err)
	}
	if bsv20Evidence != nil {
		return bsv20Evidence, nil
	}
	
	// 非 token
	return &wocTokenEvidence{IsToken: false}, nil
}

// queryBSV21TokenEvidence 查询 BSV21 token 证据
func queryBSV21TokenEvidence(ctx context.Context, rt *Runtime, address string, txid string, vout uint32) (*wocTokenEvidence, error) {
	// 复用 wallet_bsv21_woc.go 中的查询
	candidates, err := queryWalletBSV21WOCUnspent(ctx, rt, address)
	if err != nil {
		return nil, err
	}
	
	targetTxID := strings.ToLower(strings.TrimSpace(txid))
	for _, c := range candidates {
		if strings.ToLower(strings.TrimSpace(c.Current.TxID)) != targetTxID {
			continue
		}
		// 通过 scriptHash 匹配 vout
		txHex, err := rt.WalletChain.GetTxHex(ctx, txid)
		if err != nil {
			continue
		}
		matched, err := matchBSV21VoutByScriptHash(ctx, txHex, c.ScriptHash, vout)
		if err != nil || !matched {
			continue
		}
		
		return &wocTokenEvidence{
			IsToken:       true,
			TokenStandard: "bsv21",
			TokenID:       strings.ToLower(strings.TrimSpace(c.Data.BSV20.ID)),
			Symbol:        strings.TrimSpace(c.Data.BSV20.Symbol),
			QuantityText:  c.Data.BSV20.Amount.String(),
		}, nil
	}
	return nil, nil
}

// queryBSV20TokenEvidence 查询 BSV20 token 证据  
func queryBSV20TokenEvidence(ctx context.Context, rt *Runtime, address string, txid string, vout uint32) (*wocTokenEvidence, error) {
	// 复用 wallet_bsv20_woc.go 中的查询
	candidates, err := queryWalletBSV20WOCUnspent(ctx, rt, address)
	if err != nil {
		return nil, err
	}
	
	targetTxID := strings.ToLower(strings.TrimSpace(txid))
	for _, c := range candidates {
		if strings.ToLower(strings.TrimSpace(c.Current.TxID)) != targetTxID {
			continue
		}
		// 通过 scriptHash 匹配 vout
		txHex, err := rt.WalletChain.GetTxHex(ctx, txid)
		if err != nil {
			continue
		}
		matched, err := matchBSV20VoutByScriptHash(ctx, txHex, c.ScriptHash, vout)
		if err != nil || !matched {
			continue
		}
		
		return &wocTokenEvidence{
			IsToken:       true,
			TokenStandard: "bsv20",
			TokenID:       strings.ToLower(strings.TrimSpace(c.Data.BSV20.ID)),
			Symbol:        strings.TrimSpace(c.Data.BSV20.Symbol),
			QuantityText:  c.Data.BSV20.Amount.String(),
		}, nil
	}
	return nil, nil
}
