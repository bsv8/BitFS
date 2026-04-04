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

// walletAssetBalance fact 口径资产余额
type walletAssetBalance struct {
	WalletID       string `json:"wallet_id"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	TotalInSatoshi int64  `json:"total_in_satoshi"`
	TotalUsed      int64  `json:"total_used"`
	Remaining      int64  `json:"remaining"`
}

// spendableSourceFlow 可花费源流（剩余额度 > 0 的 IN flow）
type spendableSourceFlow struct {
	FlowID         int64  `json:"flow_id"`
	WalletID       string `json:"wallet_id"`
	Address        string `json:"address"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	UTXOID         string `json:"utxo_id"`
	TxID           string `json:"txid"`
	Vout           uint32 `json:"vout"`
	TotalInSatoshi int64  `json:"total_in_satoshi"`
	TotalUsed      int64  `json:"total_used"`
	Remaining      int64  `json:"remaining"`
	OccurredAtUnix int64  `json:"occurred_at_unix"`
}

// dbLoadWalletAssetBalanceFact 按 wallet_id + asset_kind + token_id 聚合 fact 余额
// 设计说明：
// - 余额 = fact_chain_asset_flows(IN) - fact_asset_consumptions(used)
// - BSV 用 amount_satoshi/used_satoshi，BSV20/BSV21 当前只返回总和
func dbLoadWalletAssetBalanceFact(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	if store == nil {
		return walletAssetBalance{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletAssetBalance, error) {
		return dbLoadWalletAssetBalanceFactDB(db, walletID, assetKind, tokenID)
	})
}

func dbLoadWalletAssetBalanceFactDB(db *sql.DB, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	walletID = strings.TrimSpace(walletID)
	if walletID == "" {
		return walletAssetBalance{}, fmt.Errorf("wallet_id is required")
	}
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind == "" {
		assetKind = "BSV"
	}
	tokenID = strings.TrimSpace(tokenID)

	var totalIn, totalUsed int64
	err := db.QueryRow(
		`SELECT COALESCE(SUM(amount_satoshi),0) FROM fact_chain_asset_flows WHERE wallet_id=? AND asset_kind=? AND token_id=? AND direction='IN'`,
		walletID, assetKind, tokenID,
	).Scan(&totalIn)
	if err != nil {
		return walletAssetBalance{}, err
	}

	err = db.QueryRow(
		`SELECT COALESCE(SUM(c.used_satoshi),0)
		 FROM fact_asset_consumptions c
		 JOIN fact_chain_asset_flows f ON c.source_flow_id=f.id
		 WHERE f.wallet_id=? AND f.asset_kind=? AND f.token_id=? AND f.direction='IN'`,
		walletID, assetKind, tokenID,
	).Scan(&totalUsed)
	if err != nil {
		return walletAssetBalance{}, err
	}

	return walletAssetBalance{
		WalletID:       walletID,
		AssetKind:      assetKind,
		TokenID:        tokenID,
		TotalInSatoshi: totalIn,
		TotalUsed:      totalUsed,
		Remaining:      totalIn - totalUsed,
	}, nil
}

// dbLoadAllWalletAssetBalancesFact 按 wallet_id 聚合所有 asset_kind+token_id 的余额
func dbLoadAllWalletAssetBalancesFact(ctx context.Context, store *clientDB, walletID string) ([]walletAssetBalance, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]walletAssetBalance, error) {
		return dbLoadAllWalletAssetBalancesFactDB(db, walletID)
	})
}

func dbLoadAllWalletAssetBalancesFactDB(db *sql.DB, walletID string) ([]walletAssetBalance, error) {
	walletID = strings.TrimSpace(walletID)
	if walletID == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}

	// 先汇总每个 (asset_kind, token_id) 的 IN 总额
	rows, err := db.Query(
		`SELECT asset_kind, token_id, COALESCE(SUM(amount_satoshi),0)
		 FROM fact_chain_asset_flows WHERE wallet_id=? AND direction='IN'
		 GROUP BY asset_kind, token_id`,
		walletID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type groupKey struct {
		AssetKind string
		TokenID   string
	}
	inMap := map[groupKey]int64{}
	for rows.Next() {
		var k groupKey
		var totalIn int64
		if err := rows.Scan(&k.AssetKind, &k.TokenID, &totalIn); err != nil {
			return nil, err
		}
		inMap[k] = totalIn
	}

	// 再汇总每个 (asset_kind, token_id) 的 used 总额
	rows2, err := db.Query(
		`SELECT f.asset_kind, f.token_id, COALESCE(SUM(c.used_satoshi),0)
		 FROM fact_asset_consumptions c
		 JOIN fact_chain_asset_flows f ON c.source_flow_id=f.id
		 WHERE f.wallet_id=? AND f.direction='IN'
		 GROUP BY f.asset_kind, f.token_id`,
		walletID,
	)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()

	usedMap := map[groupKey]int64{}
	for rows2.Next() {
		var k groupKey
		var totalUsed int64
		if err := rows2.Scan(&k.AssetKind, &k.TokenID, &totalUsed); err != nil {
			return nil, err
		}
		usedMap[k] = totalUsed
	}

	out := make([]walletAssetBalance, 0, len(inMap))
	for k, totalIn := range inMap {
		totalUsed := usedMap[k]
		out = append(out, walletAssetBalance{
			WalletID:       walletID,
			AssetKind:      k.AssetKind,
			TokenID:        k.TokenID,
			TotalInSatoshi: totalIn,
			TotalUsed:      totalUsed,
			Remaining:      totalIn - totalUsed,
		})
	}
	return out, nil
}

// dbListSpendableSourceFlows 返回仍有剩余额度的 source flow（用于选币/选 token）
// 设计说明：
// - 剩余 <= 0 的不再返回
// - 同一 source flow 被多个 payment/allocation 消耗时，累计扣减
func dbListSpendableSourceFlows(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]spendableSourceFlow, error) {
		return dbListSpendableSourceFlowsDB(db, walletID, assetKind, tokenID)
	})
}

func dbListSpendableSourceFlowsDB(db *sql.DB, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	walletID = strings.TrimSpace(walletID)
	if walletID == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind == "" {
		assetKind = "BSV"
	}
	tokenID = strings.TrimSpace(tokenID)

	// 查询每个 source flow 的 IN 总额 - 累计 used 总额
	// 设计说明：
	// - JOIN wallet_utxo 确保 UTXO 仍处于 unspent 状态
	// - 过滤 allocation_class='plain_bsv' 排除 protected/unknown
	// - 包含有 fact 记录的（已确认）和无 fact 记录的（pending local broadcast）
	rows, err := db.Query(
		`SELECT COALESCE(f.id,0),w.wallet_id,w.address,'BSV','',w.utxo_id,w.txid,w.vout,
				w.value_satoshi,
				COALESCE(used_agg.total_used,0),
				w.value_satoshi - COALESCE(used_agg.total_used,0),
				w.created_at_unix
		 FROM wallet_utxo w
		 LEFT JOIN fact_chain_asset_flows f ON w.utxo_id=f.utxo_id AND f.direction='IN'
		 LEFT JOIN (
			SELECT c.source_flow_id, SUM(c.used_satoshi) AS total_used
			FROM fact_asset_consumptions c
			GROUP BY c.source_flow_id
		 ) used_agg ON f.id=used_agg.source_flow_id
		 WHERE w.wallet_id=? AND w.state='unspent' AND w.allocation_class='plain_bsv'
		   AND w.value_satoshi > COALESCE(used_agg.total_used,0)
		 ORDER BY w.created_at_unix ASC, w.utxo_id ASC`,
		walletID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]spendableSourceFlow, 0, 16)
	for rows.Next() {
		var s spendableSourceFlow
		if err := rows.Scan(&s.FlowID, &s.WalletID, &s.Address, &s.AssetKind, &s.TokenID,
			&s.UTXOID, &s.TxID, &s.Vout, &s.TotalInSatoshi, &s.TotalUsed, &s.Remaining, &s.OccurredAtUnix); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// selectedSourceFlow 选源结果（含本次扣减金额）
type selectedSourceFlow struct {
	spendableSourceFlow
	UseAmount int64 `json:"use_amount"`
}

// dbSelectSourceFlowsForTarget 按目标金额从可花费 source flow 中选源
// 设计说明：
// - 按 remaining 升序（小额优先），时间升序
// - 累计金额 >= target 即停止
// - 余额不足返回稳定错误
// - 同一 source flow 不会重复选
func dbSelectSourceFlowsForTarget(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]selectedSourceFlow, error) {
		return dbSelectSourceFlowsForTargetDB(db, walletID, assetKind, tokenID, target)
	})
}

func dbSelectSourceFlowsForTargetDB(db *sql.DB, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	flows, err := dbListSpendableSourceFlowsDB(db, walletID, assetKind, tokenID)
	if err != nil {
		return nil, fmt.Errorf("list spendable flows: %w", err)
	}
	if len(flows) == 0 {
		return nil, fmt.Errorf("no spendable source flows available")
	}

	// 选源：小额优先，时间升序（已由 SQL ORDER BY 保证）
	remaining := int64(target)
	out := make([]selectedSourceFlow, 0, len(flows))
	for _, f := range flows {
		if remaining <= 0 {
			break
		}
		use := f.Remaining
		if use > remaining {
			use = remaining
		}
		out = append(out, selectedSourceFlow{
			spendableSourceFlow: f,
			UseAmount:           use,
		})
		remaining -= use
	}

	if remaining > 0 {
		// 计算可用总额用于错误提示
		var totalAvailable int64
		for _, f := range flows {
			totalAvailable += f.Remaining
		}
		return nil, fmt.Errorf("insufficient balance: target=%d, available=%d, missing=%d", target, totalAvailable, remaining)
	}

	return out, nil
}
