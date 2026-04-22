package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// ==================== Unknown 资产确认流程 ====================
// 设计说明：
// - 对 wallet_utxo 中 allocation_class='unknown' 且 value_satoshi=1 的 UTXO
// - 通过 WOC 查询确认其真实资产类型（BSV20/BSV21/BSV）
// - 确认后更新 allocation_class 并写入相应 fact 表：
//   * BSV UTXO -> fact_bsv_utxos
//   * Token UTXO -> fact_token_lots + fact_token_carrier_links
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
func enqueueUnknownUTXOToVerification(ctx context.Context, store *clientDB, walletID, address, utxoID, txID string, vout uint32, valueSatoshi uint64) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		return enqueueUnknownUTXOToVerificationTx(ctx, wtx, walletID, address, utxoID, txID, vout, valueSatoshi)
	})
}

// enqueueUnknownUTXOToVerificationTx 在已有事务内入队 unknown UTXO。
// 设计说明：
// - 事务闭包中禁止再次走 store.Do/store.Tx，避免 actor/事务重入导致超时。
func enqueueUnknownUTXOToVerificationTx(ctx context.Context, wtx moduleapi.WriteTx, walletID, address, utxoID, txID string, vout uint32, valueSatoshi uint64) error {
	if wtx == nil {
		return fmt.Errorf("wtx is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	walletID = strings.ToLower(strings.TrimSpace(walletID))
	if walletID == "" {
		return fmt.Errorf("wallet_id is required")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("address is required")
	}
	txID = strings.ToLower(strings.TrimSpace(txID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}

	now := time.Now().Unix()
	_, err := wtx.ExecContext(ctx, `
		INSERT INTO wallet_utxo_token_verification(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,
			status,woc_response_json,last_check_at_unix,next_retry_at_unix,retry_count,error_message,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(utxo_id) DO NOTHING
	`, utxoID, walletID, address, txID, int64(vout), int64(valueSatoshi), "pending", "{}", int64(0), now, int64(0), "", now)
	return err
}

type verificationSQLExec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
func dbListPendingVerificationItems(ctx context.Context, store *clientDB, limit int) ([]VerificationQueueItem, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if limit <= 0 {
		limit = assetVerificationMaxBatch
	}
	now := time.Now().Unix()
	var out []VerificationQueueItem
	if err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,
			       next_retry_at_unix,last_check_at_unix,woc_response_json,updated_at_unix
			  FROM wallet_utxo_token_verification
			 WHERE status='pending' AND next_retry_at_unix<=?
			 ORDER BY next_retry_at_unix ASC, updated_at_unix ASC
			 LIMIT ?`,
			now, limit,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		out = make([]VerificationQueueItem, 0, limit)
		for rows.Next() {
			var item VerificationQueueItem
			var vout int64
			var valueSatoshi int64
			var retryCount int64
			if err := rows.Scan(
				&item.UTXOID,
				&item.WalletID,
				&item.Address,
				&item.TxID,
				&vout,
				&valueSatoshi,
				&item.Status,
				&item.ErrorMessage,
				&retryCount,
				&item.NextRetryAtUnix,
				&item.LastCheckAtUnix,
				&item.WOCResponseJSON,
				&item.UpdatedAtUnix,
			); err != nil {
				return err
			}
			item.Vout = uint32(vout)
			item.ValueSatoshi = uint64(valueSatoshi)
			item.RetryCount = int(retryCount)
			out = append(out, item)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// dbUpdateUTXOAllocationClass 更新 UTXO 的 allocation_class
func dbUpdateUTXOAllocationClass(ctx context.Context, store *clientDB, utxoID, allocationClass, reason string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	allocationClass = strings.ToLower(strings.TrimSpace(allocationClass))
	switch allocationClass {
	case walletUTXOAllocationPlainBSV, walletUTXOAllocationProtectedAsset, walletUTXOAllocationUnknown:
	default:
		return fmt.Errorf("allocation_class is invalid: %s", allocationClass)
	}
	reason = strings.TrimSpace(reason)
	now := time.Now().Unix()
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		result, err := wtx.ExecContext(ctx,
			`UPDATE wallet_utxo
			    SET allocation_class=?, allocation_reason=?, updated_at_unix=?
			  WHERE utxo_id=?`,
			allocationClass, reason, now, utxoID,
		)
		if err != nil {
			return err
		}
		if result == nil {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		return nil
	})
}

// runUnknownAssetVerification 执行 unknown 资产确认流程
// 在链同步成功后调用，受并发保护
func runUnknownAssetVerification(ctx context.Context, store *clientDB, source walletScriptEvidenceSource, trigger string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}

	assetVerificationTask.mu.Lock()
	if assetVerificationTask.inFlight {
		assetVerificationTask.mu.Unlock()
		return nil
	}
	if assetVerificationTask.lastRunAt > 0 && time.Since(time.Unix(assetVerificationTask.lastRunAt, 0)) < 5*time.Second {
		assetVerificationTask.mu.Unlock()
		return nil
	}
	assetVerificationTask.inFlight = true
	assetVerificationTask.mu.Unlock()
	defer func() {
		assetVerificationTask.mu.Lock()
		assetVerificationTask.inFlight = false
		assetVerificationTask.lastRunAt = time.Now().Unix()
		assetVerificationTask.mu.Unlock()
	}()

	if _, err := dbAutoFailExhaustedRetries(ctx, store); err != nil {
		return err
	}

	rows, err := dbListPendingVerificationItems(ctx, store, assetVerificationMaxBatch)
	if err != nil {
		return err
	}
	for _, row := range rows {
		evidence, err := queryWOCTokenEvidence(ctx, source, row.Address, row.TxID, row.Vout)
		if err != nil {
			if backoffErr := updateVerificationBackoff(ctx, store, row.UTXOID, err.Error()); backoffErr != nil {
				return backoffErr
			}
			continue
		}
		if evidence == nil {
			evidence = &wocTokenEvidence{IsToken: false}
		}
		if err := processAssetVerificationResult(ctx, store, unknownUTXORow{
			UTXOID:        row.UTXOID,
			WalletID:      row.WalletID,
			Address:       row.Address,
			TxID:          row.TxID,
			Vout:          row.Vout,
			ValueSatoshi:  row.ValueSatoshi,
			CreatedAtUnix: row.UpdatedAtUnix,
		}, evidence, trigger); err != nil {
			if backoffErr := updateVerificationBackoff(ctx, store, row.UTXOID, err.Error()); backoffErr != nil {
				return backoffErr
			}
		}
	}
	return nil
}

// 并发保护：同一时刻只执行一个确认任务

// 简单节流：距离上次执行至少 5 秒

// 0. 自动将超过最大重试次数的 pending 项转 failed

// 1. 从队列表获取待处理项

// 2. 批量查询 WOC 获取 token 证据

// WOC 查询失败：指数退避

// 3. 根据证据更新分类并写入 fact

// processAssetVerificationResult 处理单个 UTXO 的确认结果

// 判定新的 allocation_class 和 verification 状态

// 非 token，回落为 BSV

// 更新 wallet_utxo 分类

// 调用统一 fact 入口写入 verified 资产事实
// 设计说明：
// - 通过 ApplyVerifiedAssetFlow 封装 fact 写入细节
// - 幂等写入，同一 UTXO 不会重复写入

// 更新 verification 队列表状态（闭环关键）
// 设计说明：
// - 成功后必须更新 status 为 confirmed_*，否则会被反复重试
// - 记录 woc_response_json 和 last_check_at_unix 用于审计

// 队列状态更新失败不阻塞主流程，仅记录日志

// queryWOCTokenEvidence 查询 WOC 获取指定 outpoint 的 token 证据
// 按优先级：BSV21 > BSV20 > 非 token
func queryWOCTokenEvidence(ctx context.Context, source walletScriptEvidenceSource, address string, txID string, vout uint32) (*wocTokenEvidence, error) {
	if source == nil {
		return nil, fmt.Errorf("wallet script evidence source is nil")
	}
	address = strings.TrimSpace(address)
	txID = strings.ToLower(strings.TrimSpace(txID))
	if address == "" {
		return nil, fmt.Errorf("address is required")
	}
	if txID == "" {
		return nil, fmt.Errorf("txid is required")
	}

	if evidence, err := queryBSV21TokenEvidence(ctx, source, address, txID, vout); err != nil {
		return nil, err
	} else if evidence != nil {
		return evidence, nil
	}
	if evidence, err := queryBSV20TokenEvidence(ctx, source, address, txID, vout); err != nil {
		return nil, err
	} else if evidence != nil {
		return evidence, nil
	}
	return &wocTokenEvidence{IsToken: false}, nil
}

// 首先查询 BSV21
func queryBSV21TokenEvidence(ctx context.Context, source walletScriptEvidenceSource, address string, txID string, vout uint32) (*wocTokenEvidence, error) {
	items, err := source.GetAddressBSV21TokenUnspent(ctx, address)
	if err != nil {
		return nil, err
	}
	txHex, err := source.GetTxHex(ctx, txID)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if !strings.EqualFold(strings.TrimSpace(item.Current.TxID), txID) {
			continue
		}
		ok, err := matchBSV21VoutByScriptHash(ctx, txHex, strings.TrimSpace(item.ScriptHash), vout)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		return &wocTokenEvidence{
			IsToken:       true,
			TokenStandard: strings.ToLower(strings.TrimSpace("bsv21")),
			TokenID:       strings.TrimSpace(item.Data.BSV20.ID),
			Symbol:        strings.TrimSpace(item.Data.BSV20.Symbol),
			QuantityText:  item.Data.BSV20.Amount.String(),
		}, nil
	}
	return nil, nil
}

// 然后查询 BSV20
func queryBSV20TokenEvidence(ctx context.Context, source walletScriptEvidenceSource, address string, txID string, vout uint32) (*wocTokenEvidence, error) {
	items, err := source.GetAddressBSV20TokenUnspent(ctx, address)
	if err != nil {
		return nil, err
	}
	txHex, err := source.GetTxHex(ctx, txID)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if !strings.EqualFold(strings.TrimSpace(item.Current.TxID), txID) {
			continue
		}
		ok, err := matchBSV20VoutByScriptHash(ctx, txHex, strings.TrimSpace(item.ScriptHash), vout)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		return &wocTokenEvidence{
			IsToken:       true,
			TokenStandard: strings.ToLower(strings.TrimSpace("bsv20")),
			TokenID:       strings.TrimSpace(item.Data.BSV20.ID),
			Symbol:        strings.TrimSpace(item.Data.BSV20.Symbol),
			QuantityText:  item.Data.BSV20.Amount.String(),
		}, nil
	}
	return nil, nil
}

// 非 token

// queryBSV21TokenEvidence 查询 BSV21 token 证据

// 复用 wallet_bsv21_woc.go 中的查询

// 通过 scriptHash 匹配 vout

// queryBSV20TokenEvidence 查询 BSV20 token 证据

// 复用 wallet_bsv20_woc.go 中的查询

// 通过 scriptHash 匹配 vout

// updateVerificationQueueSuccess 更新 verification 队列表为成功状态
// 设计说明：
// - 必须调用，否则 pending 项会被无限重试
// - 记录证据快照和检查时间
func updateVerificationQueueSuccess(ctx context.Context, store *clientDB, utxoID string, status string, evidence *wocTokenEvidence) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if !strings.HasPrefix(status, "confirmed_") {
		return fmt.Errorf("status must be confirmed_*")
	}
	now := time.Now().Unix()
	wocResponseJSON := "{}"
	if evidence != nil {
		wocResponseJSON = mustJSONString(map[string]any{
			"is_token":       evidence.IsToken,
			"token_standard": strings.ToLower(strings.TrimSpace(evidence.TokenStandard)),
			"token_id":       strings.TrimSpace(evidence.TokenID),
			"symbol":         strings.TrimSpace(evidence.Symbol),
			"quantity_text":  strings.TrimSpace(evidence.QuantityText),
		})
	}
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		result, err := wtx.ExecContext(ctx, `
			UPDATE wallet_utxo_token_verification
			   SET status=?,
			       woc_response_json=?,
			       last_check_at_unix=?,
			       next_retry_at_unix=0,
			       retry_count=0,
			       error_message='',
			       updated_at_unix=?
			 WHERE utxo_id=?`,
			status, wocResponseJSON, now, now, utxoID,
		)
		if err != nil {
			return err
		}
		if result == nil {
			return nil
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return nil
		}
		return nil
	})
}

// updateVerificationBackoff 更新重试时间（指数退避）
func updateVerificationBackoff(ctx context.Context, store *clientDB, utxoID string, reason string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	reason = strings.TrimSpace(reason)
	now := time.Now().Unix()
	return store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		var retryCount int64
		if err := wtx.QueryRowContext(ctx, `SELECT retry_count FROM wallet_utxo_token_verification WHERE utxo_id=?`, utxoID).Scan(&retryCount); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("utxo_id %s not found", utxoID)
			}
			return err
		}
		retryCount++
		delay := int64(assetVerificationBaseDelay)
		for i := int64(1); i < retryCount && delay < assetVerificationMaxDelay; i++ {
			delay *= 2
			if delay > assetVerificationMaxDelay {
				delay = assetVerificationMaxDelay
				break
			}
		}
		nextRetryAt := now + delay
		result, err := wtx.ExecContext(ctx, `
			UPDATE wallet_utxo_token_verification
			   SET status='pending',
			       retry_count=?,
			       next_retry_at_unix=?,
			       last_check_at_unix=?,
			       error_message=?,
			       updated_at_unix=?
			 WHERE utxo_id=?`,
			retryCount, nextRetryAt, now, reason, now, utxoID,
		)
		if err != nil {
			return err
		}
		if result == nil {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("utxo_id %s not found", utxoID)
		}
		return nil
	})
}

func dbAutoFailExhaustedRetries(ctx context.Context, store *clientDB) (int, error) {
	if store == nil {
		return 0, fmt.Errorf("store is nil")
	}
	now := time.Now().Unix()
	var affectedCount int
	if err := store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		result, err := wtx.ExecContext(ctx, `
			UPDATE wallet_utxo_token_verification
			   SET status='failed',
			       error_message=CASE WHEN error_message='' THEN 'max retries exceeded' ELSE error_message END,
			       last_check_at_unix=?,
			       updated_at_unix=?
			 WHERE status='pending' AND retry_count>=?`,
			now, now, assetVerificationMaxRetries,
		)
		if err != nil {
			return err
		}
		if result == nil {
			affectedCount = 0
			return nil
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		affectedCount = int(affected)
		return nil
	}); err != nil {
		return 0, err
	}
	return affectedCount, nil
}

func processAssetVerificationResult(ctx context.Context, store *clientDB, row unknownUTXORow, evidence *wocTokenEvidence, trigger string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	if evidence == nil {
		evidence = &wocTokenEvidence{IsToken: false}
	}
	walletID := strings.ToLower(strings.TrimSpace(row.WalletID))
	if walletID == "" {
		return fmt.Errorf("wallet_id is required")
	}
	address := strings.TrimSpace(row.Address)
	if address == "" {
		return fmt.Errorf("address is required")
	}
	txID := strings.ToLower(strings.TrimSpace(row.TxID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}

	trigger = strings.TrimSpace(trigger)
	allocationReason := "verified as plain BSV"
	flowAssetKind := "BSV"
	verificationStatus := "confirmed_plain_bsv"
	updateClass := walletUTXOAllocationPlainBSV
	if evidence.IsToken {
		standard := strings.ToUpper(strings.TrimSpace(evidence.TokenStandard))
		switch standard {
		case "BSV20":
			flowAssetKind = "BSV20"
			verificationStatus = "confirmed_bsv20"
		case "BSV21":
			flowAssetKind = "BSV21"
			verificationStatus = "confirmed_bsv21"
		default:
			return fmt.Errorf("token standard is required")
		}
		allocationReason = "verified as protected asset"
		updateClass = walletUTXOAllocationProtectedAsset
	}

	if err := ApplyVerifiedAssetFlow(ctx, store, verifiedAssetFlowParams{
		UTXOID:        row.UTXOID,
		WalletID:      walletID,
		Address:       address,
		TxID:          txID,
		Vout:          row.Vout,
		ValueSatoshi:  row.ValueSatoshi,
		AssetKind:     flowAssetKind,
		TokenID:       strings.TrimSpace(evidence.TokenID),
		Symbol:        strings.TrimSpace(evidence.Symbol),
		QuantityText:  strings.TrimSpace(evidence.QuantityText),
		CreatedAtUnix: row.CreatedAtUnix,
		Trigger:       trigger,
	}); err != nil {
		return err
	}
	if err := dbUpdateUTXOAllocationClass(ctx, store, row.UTXOID, updateClass, allocationReason); err != nil {
		return err
	}
	if err := updateVerificationQueueSuccess(ctx, store, row.UTXOID, verificationStatus, evidence); err != nil {
		return err
	}
	return nil
}

// wocTokenEvidence 只在资产确认链路内流转，避免把 WOC 原始结构散到业务层。
// 设计说明：这里只保存确认结果需要的最小字段。
func (e *wocTokenEvidence) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	return json.Marshal(map[string]any{
		"is_token":       e.IsToken,
		"token_standard": strings.ToLower(strings.TrimSpace(e.TokenStandard)),
		"token_id":       strings.TrimSpace(e.TokenID),
		"symbol":         strings.TrimSpace(e.Symbol),
		"quantity_text":  strings.TrimSpace(e.QuantityText),
	})
}
