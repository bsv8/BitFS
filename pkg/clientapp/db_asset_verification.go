package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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
	assetVerificationTimeout   = 30 * time.Second
	assetVerificationMaxBatch  = 50
	assetVerificationBaseDelay = 60    // 1 minute
	assetVerificationMaxDelay  = 86400 // 1 day
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
	UTXOID        string
	WalletID      string
	Address       string
	TxID          string
	Vout          uint32
	ValueSatoshi  uint64
	CreatedAtUnix int64
}

// enqueueUnknownUTXOToVerification 将 unknown UTXO 加入待确认队列
// 设计说明：
// - 在 reconcileWalletUTXOSet 发现新 UTXO 且 classification 为 unknown 时调用
// - 幂等：已存在的记录不会重复插入
func enqueueUnknownUTXOToVerification(ctx context.Context, store *clientDB, walletID string, address string, utxoID string, txid string, vout uint32, value uint64) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		_, err := db.Exec(
			`INSERT OR IGNORE INTO wallet_utxo_token_verification(
				utxo_id,wallet_id,address,txid,vout,value_satoshi,status,woc_response_json,
				last_check_at_unix,next_retry_at_unix,retry_count,error_message,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			utxoID, walletID, address, txid, vout, value, "pending", "{}",
			0, now, 0, "", now,
		)
		return err
	})
}

// wocTokenEvidence WOC 返回的 token 证据
type wocTokenEvidence struct {
	IsToken       bool
	TokenStandard string // "bsv20" | "bsv21" | ""
	TokenID       string
	Symbol        string
	QuantityText  string
}

// dbListPendingVerificationItems 从队列表查询待处理项（支持指数退避）
func dbListPendingVerificationItems(ctx context.Context, store *clientDB, limit int) ([]unknownUTXORow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if limit <= 0 {
		limit = assetVerificationMaxBatch
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]unknownUTXORow, error) {
		now := time.Now().Unix()
		rows, err := db.Query(
			`SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,0
			 FROM wallet_utxo_token_verification
			 WHERE status='pending' AND next_retry_at_unix <= ?
			 ORDER BY next_retry_at_unix ASC, retry_count ASC
			 LIMIT ?`,
			now, limit,
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
			"reason":  "already_in_flight",
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

	// 1. 从队列表获取待处理项
	rows, err := dbListPendingVerificationItems(taskCtx, store, assetVerificationMaxBatch)
	if err != nil {
		obs.Error("bitcast-client", "asset_verification_list_failed", map[string]any{
			"error":   err.Error(),
			"address": address,
		})
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	obs.Info("bitcast-client", "asset_verification_started", map[string]any{
		"address":       address,
		"unknown_count": len(rows),
		"trigger":       trigger,
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
			// WOC 查询失败：指数退避
			_ = updateVerificationBackoff(ctx, store, row.UTXOID, err.Error())
			obs.Error("bitcast-client", "asset_verification_woc_failed", map[string]any{
				"utxo_id": row.UTXOID,
				"error":   err.Error(),
			})
			failedCount++
			continue
		}

		// 3. 根据证据更新分类并写入 fact
		if err := processAssetVerificationResult(ctx, store, row, evidence, trigger); err != nil {
			obs.Error("bitcast-client", "asset_verification_process_failed", map[string]any{
				"utxo_id": row.UTXOID,
				"error":   err.Error(),
			})
			failedCount++
			continue
		}
		verifiedCount++
	}

	obs.Info("bitcast-client", "asset_verification_completed", map[string]any{
		"address":        address,
		"verified_count": verifiedCount,
		"failed_count":   failedCount,
		"skipped_count":  skippedCount,
		"total_count":    len(rows),
	})
	return nil
}

// processAssetVerificationResult 处理单个 UTXO 的确认结果
func processAssetVerificationResult(ctx context.Context, store *clientDB, row unknownUTXORow, evidence *wocTokenEvidence, trigger string) error {
	if evidence == nil {
		return fmt.Errorf("evidence is nil")
	}

	// 判定新的 allocation_class 和 verification 状态
	var newClass, assetKind, note, verifyStatus string
	var tokenID, symbol, quantityText string

	switch {
	case evidence.IsToken && evidence.TokenStandard == "bsv21":
		newClass = walletUTXOAllocationProtectedAsset
		assetKind = "BSV21"
		verifyStatus = "confirmed_bsv21"
		tokenID = evidence.TokenID
		symbol = evidence.Symbol
		quantityText = evidence.QuantityText
		note = "verified as BSV21 token by WOC"

	case evidence.IsToken && evidence.TokenStandard == "bsv20":
		newClass = walletUTXOAllocationProtectedAsset
		assetKind = "BSV20"
		verifyStatus = "confirmed_bsv20"
		tokenID = evidence.TokenID
		symbol = evidence.Symbol
		quantityText = evidence.QuantityText
		note = "verified as BSV20 token by WOC"

	default:
		// 非 token，回落为 BSV
		newClass = walletUTXOAllocationPlainBSV
		assetKind = "BSV"
		verifyStatus = "confirmed_plain_bsv"
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
			"token_symbol":         symbol,
		},
	}

	if _, err := dbAppendAssetFlowInIfAbsent(ctx, store, flowEntry); err != nil {
		return fmt.Errorf("append asset flow: %w", err)
	}

	// 更新 verification 队列表状态（闭环关键）
	// 设计说明：
	// - 成功后必须更新 status 为 confirmed_*，否则会被反复重试
	// - 记录 woc_response_json 和 last_check_at_unix 用于审计
	if err := updateVerificationQueueSuccess(ctx, store, row.UTXOID, verifyStatus, evidence); err != nil {
		// 队列状态更新失败不阻塞主流程，仅记录日志
		obs.Error("bitcast-client", "verification_queue_update_failed", map[string]any{
			"utxo_id": row.UTXOID,
			"error":   err.Error(),
		})
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

// updateVerificationQueueSuccess 更新 verification 队列表为成功状态
// 设计说明：
// - 必须调用，否则 pending 项会被无限重试
// - 记录证据快照和检查时间
func updateVerificationQueueSuccess(ctx context.Context, store *clientDB, utxoID string, status string, evidence *wocTokenEvidence) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		wocJSON := "{}"
		if evidence != nil {
			if b, err := json.Marshal(evidence); err == nil {
				wocJSON = string(b)
			}
		}
		_, err := db.Exec(
			`UPDATE wallet_utxo_token_verification
			 SET status=?, woc_response_json=?, last_check_at_unix=?, next_retry_at_unix=?, retry_count=?, error_message=?, updated_at_unix=?
			 WHERE utxo_id=?`,
			status, wocJSON, now, now+86400*30, 0, "", now, utxoID,
		)
		return err
	})
}

// updateVerificationBackoff 更新重试时间（指数退避）
func updateVerificationBackoff(ctx context.Context, store *clientDB, utxoID string, errMsg string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		var retryCount int
		err := db.QueryRow(`SELECT retry_count FROM wallet_utxo_token_verification WHERE utxo_id=?`, utxoID).Scan(&retryCount)
		if err != nil {
			return err
		}
		retryCount++
		nextRetry := time.Now().Unix() + int64(math.Min(float64(assetVerificationBaseDelay*int(math.Pow(2, float64(retryCount)))), float64(assetVerificationMaxDelay)))
		_, err = db.Exec(
			`UPDATE wallet_utxo_token_verification SET retry_count=?, next_retry_at_unix=?, last_check_at_unix=?, error_message=?, updated_at_unix=? WHERE utxo_id=?`,
			retryCount, nextRetry, time.Now().Unix(), errMsg, time.Now().Unix(), utxoID,
		)
		return err
	})
}
