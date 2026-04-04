package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// chainAssetFlowEntry fact_chain_asset_flows 写入条目
// 设计说明：
// - 记录链上资产流入流出的事实，作为余额计算的可信来源
// - flow_id 由调用方生成，保证幂等性
// - direction: 'IN' 或 'OUT'
// - asset_kind: 'BSV' / 'BSV20' / 'BSV21'
type chainAssetFlowEntry struct {
	FlowID         string
	WalletID       string
	Address        string
	Direction      string
	AssetKind      string
	TokenID        string
	UTXOID         string
	TxID           string
	Vout           uint32
	AmountSatoshi  int64
	QuantityText   string
	OccurredAtUnix int64
	EvidenceSource string
	Note           string
	Payload        any
}

// assetConsumptionEntry fact_asset_consumptions 写入条目
// 设计说明：
// - 把资产消耗关联到 chain_payment 或 pool_allocation
// - 二选一：ChainPaymentID 与 PoolAllocationID 必须且只能有一个非零
type assetConsumptionEntry struct {
	SourceFlowID     int64
	ChainPaymentID   int64
	PoolAllocationID int64
	UsedSatoshi      int64
	UsedQuantityText string
	OccurredAtUnix   int64
	Note             string
	Payload          any
}

// dbAppendAssetFlowInIfAbsent 幂等追加资产流入事实
// 同一 wallet_id + utxo_id + direction 不会重复写入
func dbAppendAssetFlowInIfAbsent(ctx context.Context, store *clientDB, e chainAssetFlowEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbAppendAssetFlowIfAbsentDB(db, e, "IN")
	})
}

// dbAppendAssetFlowOutIfAbsent 幂等追加资产流出事实
// 同一 wallet_id + utxo_id + direction 不会重复写入
func dbAppendAssetFlowOutIfAbsent(ctx context.Context, store *clientDB, e chainAssetFlowEntry) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return dbAppendAssetFlowIfAbsentDB(db, e, "OUT")
	})
}

// dbAppendAssetFlowIfAbsentDB 在已打开的 sql.DB 上执行幂等追加
// 设计说明：
// - 使用 UNIQUE(wallet_id, utxo_id, direction) 约束做幂等检查
// - 同一 UTXO 在同一钱包里，IN 或 OUT 只会记录一次
func dbAppendAssetFlowIfAbsentDB(db sqlConn, e chainAssetFlowEntry, directionOverride string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	flowID := strings.TrimSpace(e.FlowID)
	if flowID == "" {
		return 0, fmt.Errorf("flow_id is required")
	}
	walletID := strings.TrimSpace(e.WalletID)
	if walletID == "" {
		return 0, fmt.Errorf("wallet_id is required")
	}
	utxoID := strings.ToLower(strings.TrimSpace(e.UTXOID))
	if utxoID == "" {
		return 0, fmt.Errorf("utxo_id is required")
	}
	direction := directionOverride
	if direction == "" {
		direction = strings.ToUpper(strings.TrimSpace(e.Direction))
	}
	if direction != "IN" && direction != "OUT" {
		return 0, fmt.Errorf("direction must be IN or OUT, got %s", direction)
	}
	assetKind := strings.ToUpper(strings.TrimSpace(e.AssetKind))
	if assetKind == "" {
		return 0, fmt.Errorf("asset_kind is required")
	}
	if assetKind != "BSV" && assetKind != "BSV20" && assetKind != "BSV21" {
		return 0, fmt.Errorf("asset_kind must be BSV/BSV20/BSV21, got %s", assetKind)
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return 0, fmt.Errorf("txid is required")
	}

	// 检查是否已存在（幂等）
	var existingID int64
	err := db.QueryRow(
		`SELECT id FROM fact_chain_asset_flows WHERE wallet_id=? AND utxo_id=? AND direction=?`,
		walletID, utxoID, direction,
	).Scan(&existingID)
	if err == nil {
		// 已存在，直接返回
		return existingID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	// 不存在则插入
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	evidenceSource := strings.TrimSpace(e.EvidenceSource)
	if evidenceSource == "" {
		evidenceSource = "WOC"
	}

	res, err := db.Exec(
		`INSERT INTO fact_chain_asset_flows(
			flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,
			amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		flowID,
		walletID,
		strings.TrimSpace(e.Address),
		direction,
		assetKind,
		strings.TrimSpace(e.TokenID),
		utxoID,
		txid,
		e.Vout,
		e.AmountSatoshi,
		strings.TrimSpace(e.QuantityText),
		occurredAt,
		now,
		evidenceSource,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// dbAppendAssetConsumptionForChainPaymentIfAbsent 幂等追加资产消耗（关联 chain_payment）
// 设计说明：
// - 使用部分唯一索引 uq_fact_asset_consumptions_flow_payment 做幂等
// - 同一 source_flow_id + chain_payment_id 只记录一次
func dbAppendAssetConsumptionForChainPaymentIfAbsent(ctx context.Context, store *clientDB, e assetConsumptionEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		return dbAppendAssetConsumptionIfAbsentDB(db, e, "payment")
	})
}

// dbAppendAssetConsumptionForPoolAllocationIfAbsent 幂等追加资产消耗（关联 pool_allocation）
// 设计说明：
// - 使用部分唯一索引 uq_fact_asset_consumptions_flow_allocation 做幂等
// - 同一 source_flow_id + pool_allocation_id 只记录一次
func dbAppendAssetConsumptionForPoolAllocationIfAbsent(ctx context.Context, store *clientDB, e assetConsumptionEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		return dbAppendAssetConsumptionIfAbsentDB(db, e, "allocation")
	})
}

// dbAppendAssetConsumptionIfAbsentDB 在已打开的 sql.DB 上执行幂等追加
func dbAppendAssetConsumptionIfAbsentDB(db sqlConn, e assetConsumptionEntry, linkType string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.SourceFlowID <= 0 {
		return fmt.Errorf("source_flow_id is required")
	}

	// 校验二选一约束
	if linkType == "payment" {
		if e.ChainPaymentID <= 0 {
			return fmt.Errorf("chain_payment_id is required for payment link type")
		}
		if e.PoolAllocationID != 0 {
			return fmt.Errorf("pool_allocation_id must be NULL for payment link type")
		}
	} else if linkType == "allocation" {
		if e.PoolAllocationID <= 0 {
			return fmt.Errorf("pool_allocation_id is required for allocation link type")
		}
		if e.ChainPaymentID != 0 {
			return fmt.Errorf("chain_payment_id must be NULL for allocation link type")
		}
	} else {
		return fmt.Errorf("link_type must be 'payment' or 'allocation', got %s", linkType)
	}

	// 检查是否已存在（幂等）
	var n int
	if linkType == "payment" {
		err := db.QueryRow(
			`SELECT COUNT(1) FROM fact_asset_consumptions WHERE source_flow_id=? AND chain_payment_id=?`,
			e.SourceFlowID, e.ChainPaymentID,
		).Scan(&n)
		if err != nil {
			return err
		}
	} else {
		err := db.QueryRow(
			`SELECT COUNT(1) FROM fact_asset_consumptions WHERE source_flow_id=? AND pool_allocation_id=?`,
			e.SourceFlowID, e.PoolAllocationID,
		).Scan(&n)
		if err != nil {
			return err
		}
	}
	if n > 0 {
		return nil
	}

	// 不存在则插入
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}

	_, err := db.Exec(
		`INSERT INTO fact_asset_consumptions(
			source_flow_id,chain_payment_id,pool_allocation_id,used_satoshi,used_quantity_text,
			occurred_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?)`,
		e.SourceFlowID,
		nilIfZero(e.ChainPaymentID),
		nilIfZero(e.PoolAllocationID),
		e.UsedSatoshi,
		strings.TrimSpace(e.UsedQuantityText),
		occurredAt,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// nilIfZero 把 0 转成 nil，用于 SQLite NULL 插入
func nilIfZero(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// dbListAssetFlowsByWallet 按钱包查询资产流入流出记录（调试用）
func dbListAssetFlowsByWallet(ctx context.Context, store *clientDB, walletID string, direction string, limit int) ([]map[string]any, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]map[string]any, error) {
		if limit <= 0 {
			limit = 50
		}
		walletID = strings.TrimSpace(walletID)
		if walletID == "" {
			return nil, fmt.Errorf("wallet_id is required")
		}

		query := `SELECT id,flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,
			amount_satoshi,quantity_text,occurred_at_unix,evidence_source,note,payload_json
			FROM fact_chain_asset_flows WHERE wallet_id=?`
		args := []any{walletID}

		if direction != "" {
			direction = strings.ToUpper(strings.TrimSpace(direction))
			if direction != "IN" && direction != "OUT" {
				return nil, fmt.Errorf("direction must be IN or OUT, got %s", direction)
			}
			query += ` AND direction=?`
			args = append(args, direction)
		}
		query += ` ORDER BY occurred_at_unix DESC, id DESC LIMIT ?`
		args = append(args, limit)

		rows, err := db.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		out := make([]map[string]any, 0, limit)
		for rows.Next() {
			var id int64
			var flowID, walletID, address, dir, assetKind, tokenID, utxoID, txid, quantityText, evidenceSource, note, payloadJSON string
			var vout uint32
			var amountSatoshi, occurredAtUnix int64
			if err := rows.Scan(&id, &flowID, &walletID, &address, &dir, &assetKind, &tokenID, &utxoID, &txid, &vout,
				&amountSatoshi, &quantityText, &occurredAtUnix, &evidenceSource, &note, &payloadJSON); err != nil {
				return nil, err
			}
			out = append(out, map[string]any{
				"id":               id,
				"flow_id":          flowID,
				"wallet_id":        walletID,
				"address":          address,
				"direction":        dir,
				"asset_kind":       assetKind,
				"token_id":         tokenID,
				"utxo_id":          utxoID,
				"txid":             txid,
				"vout":             vout,
				"amount_satoshi":   amountSatoshi,
				"quantity_text":    quantityText,
				"occurred_at_unix": occurredAtUnix,
				"evidence_source":  evidenceSource,
				"note":             note,
				"payload_json":     payloadJSON,
			})
		}
		return out, rows.Err()
	})
}

// dbGetAssetFlowByUTXO 按 UTXO 查询资产流入记录
func dbGetAssetFlowByUTXO(ctx context.Context, store *clientDB, walletID string, utxoID string, direction string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		walletID = strings.TrimSpace(walletID)
		utxoID = strings.ToLower(strings.TrimSpace(utxoID))
		if walletID == "" || utxoID == "" {
			return 0, fmt.Errorf("wallet_id and utxo_id are required")
		}
		direction = strings.ToUpper(strings.TrimSpace(direction))
		if direction != "IN" && direction != "OUT" {
			return 0, fmt.Errorf("direction must be IN or OUT")
		}

		var id int64
		err := db.QueryRow(
			`SELECT id FROM fact_chain_asset_flows WHERE wallet_id=? AND utxo_id=? AND direction=?`,
			walletID, utxoID, direction,
		).Scan(&id)
		if err != nil {
			return 0, err
		}
		return id, nil
	})
}

// dbGetAssetFlowInByUTXO 按 utxo_id 查 IN 方向的 source_flow_id
// 设计说明：
// - 用于出项关联时查找源流入记录
// - UNIQUE(wallet_id, utxo_id, direction) 保证同一 UTXO 在同一钱包里只有一条 IN
func dbGetAssetFlowInByUTXO(db sqlConn, utxoID string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return 0, fmt.Errorf("utxo_id is required")
	}
	var id int64
	err := db.QueryRow(
		`SELECT id FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN' LIMIT 1`,
		utxoID,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// dbAppendAssetConsumptionsForChainPayment 批量写入资产消耗（关联 chain_payment）
// 设计说明：
// - 对每个 input UTXO，查 fact_chain_asset_flows 找到 source_flow_id
// - 找不到 source_flow_id 的 UTXO（如 unknown）跳过，不产生消耗记录
// - 幂等：同一 source_flow_id + chain_payment_id 不会重复写
func dbAppendAssetConsumptionsForChainPayment(db sqlConn, chainPaymentID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if chainPaymentID <= 0 {
		return fmt.Errorf("chain_payment_id is required")
	}
	for _, fact := range utxoFacts {
		// 只处理 input 方向的 UTXO
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		// 查找源流入记录
		sourceFlowID, err := dbGetAssetFlowInByUTXO(db, utxoID)
		if err != nil {
			if err == sql.ErrNoRows {
				// 没有 source flow（unknown UTXO），跳过
				continue
			}
			return fmt.Errorf("lookup source flow for utxo %s: %w", utxoID, err)
		}
		// 写入消耗记录
		if err := dbAppendAssetConsumptionIfAbsentDB(db, assetConsumptionEntry{
			SourceFlowID:     sourceFlowID,
			ChainPaymentID:   chainPaymentID,
			UsedSatoshi:      fact.AmountSatoshi,
			UsedQuantityText: "",
			OccurredAtUnix:   occurredAtUnix,
			Note:             "consumed by chain payment",
			Payload:          fact.Payload,
		}, "payment"); err != nil {
			return fmt.Errorf("append consumption for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}

// dbAppendAssetConsumptionsForPoolAllocation 批量写入资产消耗（关联 pool_allocation）
// 设计说明：
// - 对每个 input UTXO，查 fact_chain_asset_flows 找到 source_flow_id
// - 找不到 source_flow_id 的 UTXO（如 unknown）跳过，不产生消耗记录
// - 幂等：同一 source_flow_id + pool_allocation_id 不会重复写
func dbAppendAssetConsumptionsForPoolAllocation(db sqlConn, poolAllocationID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if poolAllocationID <= 0 {
		return fmt.Errorf("pool_allocation_id is required")
	}
	for _, fact := range utxoFacts {
		// 只处理 input 方向的 UTXO
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		// 查找源流入记录
		sourceFlowID, err := dbGetAssetFlowInByUTXO(db, utxoID)
		if err != nil {
			if err == sql.ErrNoRows {
				// 没有 source flow（unknown UTXO），跳过
				continue
			}
			return fmt.Errorf("lookup source flow for utxo %s: %w", utxoID, err)
		}
		// 写入消耗记录
		if err := dbAppendAssetConsumptionIfAbsentDB(db, assetConsumptionEntry{
			SourceFlowID:     sourceFlowID,
			PoolAllocationID: poolAllocationID,
			UsedSatoshi:      fact.AmountSatoshi,
			UsedQuantityText: "",
			OccurredAtUnix:   occurredAtUnix,
			Note:             "consumed by pool allocation",
			Payload:          fact.Payload,
		}, "allocation"); err != nil {
			return fmt.Errorf("append consumption for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}
