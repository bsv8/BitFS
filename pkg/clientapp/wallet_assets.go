package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/obs"
)

const (
	walletAssetGroupToken   = "token"
	walletAssetGroupOrdinal = "ordinal"
)

type walletUTXOAssetBinding struct {
	UTXOID        string
	AssetGroup    string
	AssetStandard string
	AssetKey      string
	AssetSymbol   string
	QuantityText  string
	SourceName    string
	Payload       any
}

type walletUTXOAssetClassification struct {
	AllocationClass  string
	AllocationReason string
	Assets           []walletUTXOAssetBinding
}

// walletUTXOAssetDetector 负责把“当前钱包未花费集合”映射成资产识别结果。
// 设计说明：
// - 探测器只做识别，不直接写库；
// - 同步层统一把识别结果投影到 wallet_utxo / wallet_utxo_assets，避免 CLI/HTTP/Electron 各自再问一次第三方索引；
// - 这里故意保留最小抽象，后续无论接哪家索引服务，都只需要实现这个接口。
type walletUTXOAssetDetector interface {
	DetectUTXOAssets(ctx context.Context, address string, utxos []poolcore.UTXO) (map[string]walletUTXOAssetClassification, error)
}

type walletUTXONoopAssetDetector struct{}

func (walletUTXONoopAssetDetector) DetectUTXOAssets(context.Context, string, []poolcore.UTXO) (map[string]walletUTXOAssetClassification, error) {
	return map[string]walletUTXOAssetClassification{}, nil
}

type walletUTXOAssetRow struct {
	UTXOID        string `json:"utxo_id"`
	WalletID      string `json:"wallet_id"`
	Address       string `json:"address"`
	AssetGroup    string `json:"asset_group"`
	AssetStandard string `json:"asset_standard"`
	AssetKey      string `json:"asset_key"`
	AssetSymbol   string `json:"asset_symbol"`
	QuantityText  string `json:"quantity_text"`
	SourceName    string `json:"source_name"`
	PayloadJSON   string `json:"payload_json"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// defaultWalletUTXOProtectionForValue 按当前 1sat 资产模型给新 UTXO 一个默认保护分类。
// 设计说明：
// - 当前要保护的是“承载 1sat 资产语义的 1 聪输出”，不是所有未花费；
// - 因此只有 value=1 的输出，才先进入 unknown 等待索引明确放行；
// - 非 1 聪输出默认仍按 plain_bsv 处理，避免把保护范围错误放大。
func defaultWalletUTXOProtectionForValue(value uint64) (string, string) {
	if value == 1 {
		return walletUTXOAllocationUnknown, "awaiting asset detector confirmation"
	}
	return walletUTXOAllocationPlainBSV, ""
}

func walletAssetDetectorOfRuntime(rt *Runtime) walletUTXOAssetDetector {
	if rt == nil {
		return nil
	}
	if rt.WalletAssetDetector != nil {
		return rt.WalletAssetDetector
	}
	if rt.runIn.WalletAssetDetector != nil {
		return rt.runIn.WalletAssetDetector
	}
	return nil
}

func loadCurrentWalletUTXOStateRows(db *sql.DB, address string) (map[string]utxoStateRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	walletID := walletIDByAddress(address)
	rows, err := db.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, strings.TrimSpace(address))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if err := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.AllocationClass, &r.AllocationReason, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix); err != nil {
			return nil, err
		}
		r.AllocationClass = normalizeWalletUTXOAllocationClass(r.AllocationClass)
		r.AllocationReason = strings.TrimSpace(r.AllocationReason)
		out[strings.ToLower(strings.TrimSpace(r.UTXOID))] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func listWalletUTXOAssetRows(db *sql.DB, utxoID string) ([]walletUTXOAssetRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(
		`SELECT utxo_id,wallet_id,address,asset_group,asset_standard,asset_key,asset_symbol,quantity_text,source_name,payload_json,updated_at_unix
		 FROM wallet_utxo_assets
		 WHERE utxo_id=?
		 ORDER BY asset_group ASC,asset_standard ASC,asset_key ASC`,
		strings.ToLower(strings.TrimSpace(utxoID)),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]walletUTXOAssetRow, 0, 4)
	for rows.Next() {
		var item walletUTXOAssetRow
		if err := rows.Scan(&item.UTXOID, &item.WalletID, &item.Address, &item.AssetGroup, &item.AssetStandard, &item.AssetKey, &item.AssetSymbol, &item.QuantityText, &item.SourceName, &item.PayloadJSON, &item.UpdatedAtUnix); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// refreshWalletAssetProjection 在链同步后把“当前未花费集合”的资产识别结果回填到本地投影。
// 设计说明：
// - 资产探测依赖外部索引，不能混进 wallet_utxo 原始链同步事务里；
// - 这里单独做一轮投影，把探测结果沉淀到 wallet_utxo_assets，同时反向更新 allocation_class；
// - 当前按 1sat 资产模型，只对 1 聪输出默认保护；
// - 新的 1 聪 UTXO 先进入 unknown，等外部索引明确确认“不是 1sat 资产输出”后才允许降级成 plain_bsv；
// - 非 1 聪输出默认仍按 plain_bsv 处理，避免把保护范围错误扩大到整个钱包；
// - 若探测失败、超时、索引临时下线，1 聪输出必须继续保留 unknown，绝不能因为“没查到”就放行。
func refreshWalletAssetProjection(ctx context.Context, rt *Runtime, address string, trigger string) error {
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	detector := walletAssetDetectorOfRuntime(rt)
	loadCurrent := func(db *sql.DB) (map[string]utxoStateRow, error) {
		return loadCurrentWalletUTXOStateRows(db, address)
	}
	var current map[string]utxoStateRow
	var err error
	if rt.DBActor != nil {
		current, err = runtimeDBValue(rt, ctx, loadCurrent)
	} else if rt.DB != nil {
		current, err = loadCurrent(rt.DB)
	} else {
		return fmt.Errorf("runtime not initialized")
	}
	if err != nil {
		return err
	}
	unspent := make([]poolcore.UTXO, 0, len(current))
	for _, row := range current {
		if strings.TrimSpace(strings.ToLower(row.State)) != "unspent" {
			continue
		}
		unspent = append(unspent, poolcore.UTXO{TxID: row.TxID, Vout: row.Vout, Value: row.Value})
	}
	// 设计说明：
	// - 即使当前没装外部资产探测器，也必须继续跑这轮投影；
	// - 原因不是为了识别新资产，而是为了把“已经花掉的旧资产行”从 wallet_utxo_assets 清干净；
	// - 否则本地广播后会残留过期 ordinal / token 影子，查询面会和 wallet_utxo 主视图打架。
	classifications := map[string]walletUTXOAssetClassification{}
	var detectErr error
	if detector != nil {
		classifications, detectErr = detector.DetectUTXOAssets(ctx, address, unspent)
	}
	if detectErr != nil {
		classifications = make(map[string]walletUTXOAssetClassification, len(unspent))
		for _, u := range unspent {
			if u.Value != 1 {
				continue
			}
			utxoID := strings.ToLower(strings.TrimSpace(u.TxID)) + ":" + fmt.Sprint(u.Vout)
			classifications[utxoID] = walletUTXOAssetClassification{
				AllocationClass:  walletUTXOAllocationUnknown,
				AllocationReason: "asset detector unavailable",
			}
		}
	}
	apply := func(db *sql.DB) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		walletID := walletIDByAddress(address)
		if _, err := tx.Exec(`DELETE FROM wallet_utxo_assets WHERE wallet_id=? AND address=?`, walletID, address); err != nil {
			return err
		}
		updatedAt := time.Now().Unix()
		for utxoID, row := range current {
			if strings.TrimSpace(strings.ToLower(row.State)) != "unspent" {
				continue
			}
			defaultClass, defaultReason := defaultWalletUTXOProtectionForValue(row.Value)
			classification := walletUTXOAssetClassification{
				AllocationClass:  defaultClass,
				AllocationReason: defaultReason,
			}
			if detector != nil && detectErr == nil {
				classification = walletUTXOAssetClassification{
					AllocationClass: walletUTXOAllocationPlainBSV,
				}
				if item, ok := classifications[utxoID]; ok {
					classification = item
				}
			} else if item, ok := classifications[utxoID]; ok {
				classification = item
			}
			allocationClass := normalizeWalletUTXOAllocationClass(classification.AllocationClass)
			allocationReason := strings.TrimSpace(classification.AllocationReason)
			if allocationReason == "" && allocationClass != walletUTXOAllocationPlainBSV {
				allocationReason = "detected by asset detector"
			}
			if _, err := tx.Exec(`UPDATE wallet_utxo SET allocation_class=?,allocation_reason=? WHERE utxo_id=?`, allocationClass, allocationReason, utxoID); err != nil {
				return err
			}
			for _, asset := range classification.Assets {
				payloadJSON := "{}"
				if asset.Payload != nil {
					b, err := json.Marshal(asset.Payload)
					if err != nil {
						return err
					}
					payloadJSON = string(b)
				}
				if _, err := tx.Exec(
					`INSERT INTO wallet_utxo_assets(utxo_id,wallet_id,address,asset_group,asset_standard,asset_key,asset_symbol,quantity_text,source_name,payload_json,updated_at_unix)
					 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
					utxoID,
					walletID,
					address,
					strings.TrimSpace(asset.AssetGroup),
					strings.TrimSpace(asset.AssetStandard),
					strings.TrimSpace(asset.AssetKey),
					strings.TrimSpace(asset.AssetSymbol),
					strings.TrimSpace(asset.QuantityText),
					strings.TrimSpace(asset.SourceName),
					payloadJSON,
					updatedAt,
				); err != nil {
					return err
				}
			}
		}
		stats, err := loadWalletUTXOAggregateTx(tx, address)
		if err != nil {
			return err
		}
		if err := updateWalletUTXOSyncStateStatsTx(tx, address, stats, updatedAt); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	if rt.DBActor != nil {
		err = runtimeDBDo(rt, ctx, apply)
	} else if rt.DB != nil {
		err = apply(rt.DB)
	} else {
		return fmt.Errorf("runtime not initialized")
	}
	if err != nil {
		return err
	}
	payload := map[string]any{
		"address":              address,
		"trigger":              strings.TrimSpace(trigger),
		"current_utxo_count":   len(unspent),
		"classification_count": len(classifications),
		"detector_configured":  detector != nil,
	}
	if detectErr != nil {
		payload["degraded"] = true
		payload["error"] = detectErr.Error()
		obs.Error("bitcast-client", "wallet_asset_projection_degraded", payload)
		return nil
	}
	obs.Info("bitcast-client", "wallet_asset_projection_refreshed", payload)
	return nil
}

func loadWalletUTXOAggregateTx(tx *sql.Tx, address string) (walletUTXOAggregateStats, error) {
	if tx == nil {
		return walletUTXOAggregateStats{}, fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOAggregateStats{}, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	rows, err := tx.Query(
		`SELECT allocation_class,COUNT(1),COALESCE(SUM(value_satoshi),0)
		 FROM wallet_utxo
		 WHERE wallet_id=? AND address=? AND state='unspent'
		 GROUP BY allocation_class`,
		walletID,
		address,
	)
	if err != nil {
		return walletUTXOAggregateStats{}, err
	}
	defer rows.Close()
	var stats walletUTXOAggregateStats
	for rows.Next() {
		var class string
		var count int
		var balance uint64
		if err := rows.Scan(&class, &count, &balance); err != nil {
			return walletUTXOAggregateStats{}, err
		}
		class = normalizeWalletUTXOAllocationClass(class)
		stats.UTXOCount += count
		stats.BalanceSatoshi += balance
		switch class {
		case walletUTXOAllocationPlainBSV:
			stats.PlainBSVUTXOCount += count
			stats.PlainBSVBalanceSatoshi += balance
		case walletUTXOAllocationProtectedAsset:
			stats.ProtectedUTXOCount += count
			stats.ProtectedBalanceSatoshi += balance
		default:
			stats.UnknownUTXOCount += count
			stats.UnknownBalanceSatoshi += balance
		}
	}
	if err := rows.Err(); err != nil {
		return walletUTXOAggregateStats{}, err
	}
	return stats, nil
}

func updateWalletUTXOSyncStateStatsTx(tx *sql.Tx, address string, stats walletUTXOAggregateStats, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	res, err := tx.Exec(
		`UPDATE wallet_utxo_sync_state
		 SET wallet_id=?,
		     utxo_count=?,
		     balance_satoshi=?,
		     plain_bsv_utxo_count=?,
		     plain_bsv_balance_satoshi=?,
		     protected_utxo_count=?,
		     protected_balance_satoshi=?,
		     unknown_utxo_count=?,
		     unknown_balance_satoshi=?,
		     updated_at_unix=?
		 WHERE address=?`,
		walletID,
		stats.UTXOCount,
		stats.BalanceSatoshi,
		stats.PlainBSVUTXOCount,
		stats.PlainBSVBalanceSatoshi,
		stats.ProtectedUTXOCount,
		stats.ProtectedBalanceSatoshi,
		stats.UnknownUTXOCount,
		stats.UnknownBalanceSatoshi,
		updatedAt,
		address,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	_, err = tx.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		address,
		walletID,
		stats.UTXOCount,
		stats.BalanceSatoshi,
		stats.PlainBSVUTXOCount,
		stats.PlainBSVBalanceSatoshi,
		stats.ProtectedUTXOCount,
		stats.ProtectedBalanceSatoshi,
		stats.UnknownUTXOCount,
		stats.UnknownBalanceSatoshi,
		updatedAt,
		"",
		"wallet_asset_projection",
		"",
		0,
		"",
		"",
		"",
		0,
	)
	return err
}
