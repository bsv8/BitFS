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

// verifiedAssetFlowParams verification 确认后写入 fact 的参数
// 设计说明：
// - verification 流程确认 unknown UTXO 的真实资产类型后调用此入口
// - 封装了 flowEntry 的构造细节，避免在 verification 逻辑里手写
type verifiedAssetFlowParams struct {
	WalletID      string
	Address       string
	UTXOID        string
	TxID          string
	Vout          uint32
	ValueSatoshi  uint64
	AssetKind     string
	TokenID       string
	QuantityText  string
	CreatedAtUnix int64
	Trigger       string
	Symbol        string
}

// ApplyVerifiedAssetFlow 写入 verification 确认后的资产事实
// 设计说明：
// - 封装幂等写入逻辑，调用方只需提供资产参数
// - 这是 verification 流程写入 fact 的唯一入口
func ApplyVerifiedAssetFlow(ctx context.Context, store *clientDB, p verifiedAssetFlowParams) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	entry := chainAssetFlowEntry{
		FlowID:        fmt.Sprintf("flow_in_%s", p.UTXOID),
		WalletID:      p.WalletID,
		Address:       p.Address,
		Direction:     "IN",
		AssetKind:     p.AssetKind,
		TokenID:       p.TokenID,
		UTXOID:        p.UTXOID,
		TxID:          p.TxID,
		Vout:          p.Vout,
		AmountSatoshi: int64(p.ValueSatoshi),
		QuantityText:  p.QuantityText,
		OccurredAtUnix: func() int64 {
			if p.CreatedAtUnix > 0 {
				return p.CreatedAtUnix
			}
			return time.Now().Unix()
		}(),
		EvidenceSource: "WOC",
		Note:           fmt.Sprintf("verified %s by WOC (trigger: %s)", strings.ToLower(p.AssetKind), p.Trigger),
		Payload: map[string]any{
			"verification_trigger": p.Trigger,
			"token_symbol":         p.Symbol,
		},
	}
	_, err := dbAppendAssetFlowInIfAbsent(ctx, store, entry)
	return err
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

// ==================== 新消费表写入 ====================
// 设计说明：
// - Token 两层语义要拆开看：
//   1. carrier 这一层仍然是普通 BSV 消耗，写 fact_bsv_consumptions；
//   2. token 数量这一层单独写 fact_token_consumptions / fact_token_utxo_links；
// - 任何一层都不替另一层补账。

// dbAppendBSVConsumptionsForSettlementCycle 写入 BSV 消耗（关联 settlement_cycle）
// 设计说明：
// - 这里只管 carrier 这一层的本币消耗；
// - token 数量不在这里写，避免跨层补账。
func dbAppendBSVConsumptionsForSettlementCycle(db sqlConn, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		sourceFlowID, err := dbGetAssetFlowInByUTXO(db, utxoID)
		state := "confirmed"
		if err != nil {
			if err == sql.ErrNoRows {
				sourceFlowID = 0
				state = "pending"
			} else {
				return fmt.Errorf("lookup source flow for utxo %s: %w", utxoID, err)
			}
		}
		if err := dbAppendBSVConsumptionIfAbsentDB(db, bsvConsumptionEntry{
			ConsumptionID:     fmt.Sprintf("bsv_cons_%d_%s", settlementCycleID, utxoID),
			SourceFlowID:      sourceFlowID,
			SourceUTXOID:      utxoID,
			SettlementCycleID: settlementCycleID,
			State:             state,
			UsedSatoshi:       fact.AmountSatoshi,
			OccurredAtUnix:    occurredAtUnix,
			Note:              "BSV consumed by settlement cycle",
			Payload:           fact.Payload,
		}); err != nil {
			return fmt.Errorf("append BSV consumption for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}

// dbAppendBSVConsumptionsForPoolSession 写入 BSV 消耗（关联 pool_session）
func dbAppendBSVConsumptionsForPoolSession(db sqlConn, poolSessionID string, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	if settlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		sourceFlowID, err := dbGetAssetFlowInByUTXO(db, utxoID)
		state := "confirmed"
		if err != nil {
			if err == sql.ErrNoRows {
				sourceFlowID = 0
				state = "pending"
			} else {
				return fmt.Errorf("lookup source flow for utxo %s: %w", utxoID, err)
			}
		}
		amountSatoshi := fact.AmountSatoshi
		if amountSatoshi == 0 && sourceFlowID > 0 {
			var dbAmount int64
			if err := db.QueryRow(`SELECT amount_satoshi FROM fact_chain_asset_flows WHERE id=?`, sourceFlowID).Scan(&dbAmount); err == nil {
				amountSatoshi = dbAmount
			}
		}
		if amountSatoshi <= 0 {
			amountSatoshi = 1
		}
		if err := dbAppendBSVConsumptionIfAbsentDB(db, bsvConsumptionEntry{
			ConsumptionID:     fmt.Sprintf("bsv_cons_pool_%s_%s", poolSessionID, utxoID),
			SourceFlowID:      sourceFlowID,
			SourceUTXOID:      utxoID,
			SettlementCycleID: settlementCycleID,
			State:             state,
			UsedSatoshi:       amountSatoshi,
			OccurredAtUnix:    occurredAtUnix,
			Note:              "BSV consumed by pool session",
			Payload:           fact.Payload,
		}); err != nil {
			return fmt.Errorf("append BSV consumption for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}

// dbAppendTokenConsumptionsForSettlementCycle 写入 Token 消耗（关联 settlement_cycle）
// 设计说明：
// - 这里只写 token 数量这一层；
// - carrier 的 1 sat 本币消耗已由 chain_bsv 线单独承担。
func dbAppendTokenConsumptionsForSettlementCycle(db sqlConn, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		assetKind := strings.TrimSpace(fact.AssetKind)
		if assetKind == "" || assetKind == "BSV" {
			continue
		}
		tokenID := strings.TrimSpace(fact.TokenID)
		if tokenID == "" {
			return fmt.Errorf("token_id is required for token consumption, utxo %s", utxoID)
		}
		tokenStandard := strings.TrimSpace(fact.TokenStandard)
		if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
			return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s for utxo %s", tokenStandard, utxoID)
		}
		quantityText := strings.TrimSpace(fact.QuantityText)
		if quantityText == "" {
			return fmt.Errorf("quantity_text is required for token consumption, utxo %s", utxoID)
		}
		sourceFlowID, state, err := dbGetTokenFlowInByUTXO(db, utxoID, tokenID)
		if err != nil {
			if err == sql.ErrNoRows {
				sourceFlowID = 0
				state = "pending"
			} else {
				return fmt.Errorf("lookup token source flow for utxo %s: %w", utxoID, err)
			}
		}
		if err := dbAppendTokenConsumptionIfAbsentDB(db, tokenConsumptionEntry{
			ConsumptionID: func() string {
				if sourceFlowID > 0 {
					return fmt.Sprintf("tok_cons_flow_%d_%s_%s", sourceFlowID, tokenID, tokenStandard)
				}
				return fmt.Sprintf("tok_cons_utxo_%s_%s_%s", utxoID, tokenID, tokenStandard)
			}(),
			SourceFlowID:      sourceFlowID,
			SourceUTXOID:      utxoID,
			TokenID:           tokenID,
			TokenStandard:     tokenStandard,
			SettlementCycleID: settlementCycleID,
			State:             state,
			UsedQuantityText:  quantityText,
			OccurredAtUnix:    occurredAtUnix,
			Note:              "Token consumed by settlement cycle",
			Payload:           fact.Payload,
		}); err != nil {
			return fmt.Errorf("append token consumption for utxo %s: %w", utxoID, err)
		}
		if err := dbAppendTokenUTXOLinkIfAbsentForConsumption(db, sourceFlowID, utxoID, tokenID, tokenStandard, quantityText, "OUT", occurredAtUnix); err != nil {
			return fmt.Errorf("append token utxo link for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}

// dbAppendTokenConsumptionsForPoolSession 写入 Token 消耗（关联 pool_session）
func dbAppendTokenConsumptionsForPoolSession(db sqlConn, poolSessionID string, settlementCycleID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	poolSessionID = strings.TrimSpace(poolSessionID)
	if poolSessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	if settlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		assetKind := strings.TrimSpace(fact.AssetKind)
		if assetKind == "" || assetKind == "BSV" {
			continue
		}
		tokenID := strings.TrimSpace(fact.TokenID)
		if tokenID == "" {
			return fmt.Errorf("token_id is required for token consumption, utxo %s", utxoID)
		}
		tokenStandard := strings.TrimSpace(fact.TokenStandard)
		if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
			return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s for utxo %s", tokenStandard, utxoID)
		}
		quantityText := strings.TrimSpace(fact.QuantityText)
		if quantityText == "" {
			return fmt.Errorf("quantity_text is required for token consumption, utxo %s", utxoID)
		}
		sourceFlowID, state, err := dbGetTokenFlowInByUTXO(db, utxoID, tokenID)
		if err != nil {
			if err == sql.ErrNoRows {
				sourceFlowID = 0
				state = "pending"
			} else {
				return fmt.Errorf("lookup token source flow for utxo %s: %w", utxoID, err)
			}
		}
		if err := dbAppendTokenConsumptionIfAbsentDB(db, tokenConsumptionEntry{
			ConsumptionID:     fmt.Sprintf("tok_cons_pool_%s_%s", poolSessionID, utxoID),
			SourceFlowID:      sourceFlowID,
			SourceUTXOID:      utxoID,
			TokenID:           tokenID,
			TokenStandard:     tokenStandard,
			SettlementCycleID: settlementCycleID,
			State:             state,
			UsedQuantityText:  quantityText,
			OccurredAtUnix:    occurredAtUnix,
			Note:              "Token consumed by pool session",
			Payload:           fact.Payload,
		}); err != nil {
			return fmt.Errorf("append token consumption for utxo %s: %w", utxoID, err)
		}
		if err := dbAppendTokenUTXOLinkIfAbsentForConsumption(db, sourceFlowID, utxoID, tokenID, tokenStandard, quantityText, "OUT", occurredAtUnix); err != nil {
			return fmt.Errorf("append token utxo link for utxo %s: %w", utxoID, err)
		}
	}
	return nil
}

// ==================== B组改造：新消费表底层写入函数 ====================

// bsvConsumptionEntry fact_bsv_consumptions 写入条目
type bsvConsumptionEntry struct {
	ConsumptionID     string
	SourceFlowID      int64
	SourceUTXOID      string
	SettlementCycleID int64
	State             string
	UsedSatoshi       int64
	OccurredAtUnix    int64
	Note              string
	Payload           any
}

// tokenConsumptionEntry fact_token_consumptions 写入条目
type tokenConsumptionEntry struct {
	ConsumptionID     string
	SourceFlowID      int64
	SourceUTXOID      string
	TokenID           string
	TokenStandard     string
	SettlementCycleID int64
	State             string
	UsedQuantityText  string
	OccurredAtUnix    int64
	Note              string
	Payload           any
}

// dbAppendBSVConsumptionIfAbsentDB 写入 fact_bsv_consumptions（幂等）
func dbAppendBSVConsumptionIfAbsentDB(db sqlConn, e bsvConsumptionEntry, _ ...string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	sourceUTXOID := strings.ToLower(strings.TrimSpace(e.SourceUTXOID))
	state := strings.ToLower(strings.TrimSpace(e.State))
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "failed" {
		return fmt.Errorf("state must be pending, confirmed or failed, got %s", state)
	}
	if e.SettlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	if sourceUTXOID == "" && e.SourceFlowID <= 0 {
		return fmt.Errorf("source_utxo_id or source_flow_id is required")
	}
	consumptionID := strings.TrimSpace(e.ConsumptionID)
	if consumptionID == "" {
		if sourceUTXOID == "" {
			sourceUTXOID = fmt.Sprintf("flow_%d", e.SourceFlowID)
		}
		consumptionID = fmt.Sprintf("bsv_cons_%d_%s", e.SettlementCycleID, sourceUTXOID)
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	_, err := db.Exec(
		`INSERT INTO fact_bsv_consumptions(
			consumption_id,source_flow_id,source_utxo_id,settlement_cycle_id,state,used_satoshi,
			occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(consumption_id) DO UPDATE SET
			source_flow_id=COALESCE(excluded.source_flow_id, fact_bsv_consumptions.source_flow_id),
			source_utxo_id=CASE WHEN excluded.source_utxo_id!='' THEN excluded.source_utxo_id ELSE fact_bsv_consumptions.source_utxo_id END,
			settlement_cycle_id=COALESCE(excluded.settlement_cycle_id, fact_bsv_consumptions.settlement_cycle_id),
			state=CASE
				WHEN fact_bsv_consumptions.state='confirmed' AND excluded.state='pending' THEN fact_bsv_consumptions.state
				ELSE excluded.state
			END,
			used_satoshi=excluded.used_satoshi,
			occurred_at_unix=excluded.occurred_at_unix,
			confirmed_at_unix=CASE
				WHEN excluded.state='confirmed' THEN excluded.occurred_at_unix
				ELSE fact_bsv_consumptions.confirmed_at_unix
			END,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		consumptionID,
		nilIfZero(e.SourceFlowID),
		sourceUTXOID,
		e.SettlementCycleID,
		state,
		e.UsedSatoshi,
		occurredAt,
		func() any {
			if state == "confirmed" {
				return occurredAt
			}
			return 0
		}(),
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbAppendTokenConsumptionIfAbsentDB 写入 fact_token_consumptions（幂等）
func dbAppendTokenConsumptionIfAbsentDB(db sqlConn, e tokenConsumptionEntry, _ ...string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	sourceUTXOID := strings.ToLower(strings.TrimSpace(e.SourceUTXOID))
	state := strings.ToLower(strings.TrimSpace(e.State))
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "failed" {
		return fmt.Errorf("state must be pending, confirmed or failed, got %s", state)
	}
	if e.SettlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	tokenID := strings.TrimSpace(e.TokenID)
	if tokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	tokenStandard := strings.TrimSpace(e.TokenStandard)
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s", tokenStandard)
	}
	if sourceUTXOID == "" && e.SourceFlowID <= 0 {
		return fmt.Errorf("source_utxo_id or source_flow_id is required")
	}
	consumptionID := strings.TrimSpace(e.ConsumptionID)
	if consumptionID == "" {
		if e.SourceFlowID > 0 {
			consumptionID = fmt.Sprintf("tok_cons_flow_%d_%s_%s", e.SourceFlowID, tokenID, tokenStandard)
		} else {
			if sourceUTXOID == "" {
				sourceUTXOID = fmt.Sprintf("flow_%d", e.SourceFlowID)
			}
			consumptionID = fmt.Sprintf("tok_cons_utxo_%s_%s_%s", sourceUTXOID, tokenID, tokenStandard)
		}
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	quantityText := strings.TrimSpace(e.UsedQuantityText)
	_, err := db.Exec(
		`INSERT INTO fact_token_consumptions(
			consumption_id,source_flow_id,source_utxo_id,token_id,token_standard,settlement_cycle_id,state,used_quantity_text,
			occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(consumption_id) DO UPDATE SET
			source_flow_id=COALESCE(excluded.source_flow_id, fact_token_consumptions.source_flow_id),
			source_utxo_id=CASE WHEN excluded.source_utxo_id!='' THEN excluded.source_utxo_id ELSE fact_token_consumptions.source_utxo_id END,
			token_id=COALESCE(excluded.token_id, fact_token_consumptions.token_id),
			token_standard=COALESCE(excluded.token_standard, fact_token_consumptions.token_standard),
			settlement_cycle_id=COALESCE(excluded.settlement_cycle_id, fact_token_consumptions.settlement_cycle_id),
			state=CASE
				WHEN fact_token_consumptions.state='confirmed' AND excluded.state='pending' THEN fact_token_consumptions.state
				ELSE excluded.state
			END,
			used_quantity_text=excluded.used_quantity_text,
			occurred_at_unix=excluded.occurred_at_unix,
			confirmed_at_unix=CASE
				WHEN excluded.state='confirmed' THEN excluded.occurred_at_unix
				ELSE fact_token_consumptions.confirmed_at_unix
			END,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		consumptionID,
		nilIfZero(e.SourceFlowID),
		sourceUTXOID,
		tokenID,
		tokenStandard,
		e.SettlementCycleID,
		state,
		quantityText,
		occurredAt,
		func() any {
			if state == "confirmed" {
				return occurredAt
			}
			return 0
		}(),
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbAppendTokenUTXOLinkIfAbsentForConsumption 写入 fact_token_utxo_links（幂等）
// B组改造：Token 消耗时同步建立载体映射，校验 carrier_flow_id 对应的 token_id/token_standard 是否和入参一致
func dbAppendTokenUTXOLinkIfAbsentForConsumption(db sqlConn, carrierFlowID int64, carrierUTXOID, tokenID, tokenStandard, quantityText, direction string, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if carrierFlowID <= 0 {
		return fmt.Errorf("carrier_flow_id is required for token utxo link")
	}
	carrierUTXOID = strings.ToLower(strings.TrimSpace(carrierUTXOID))
	if carrierUTXOID == "" {
		return fmt.Errorf("carrier_utxo_id is required")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	tokenStandard = strings.TrimSpace(tokenStandard)
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s", tokenStandard)
	}
	quantityText = strings.TrimSpace(quantityText)
	if quantityText == "" {
		return fmt.Errorf("quantity_text is required")
	}
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction != "IN" && direction != "OUT" {
		return fmt.Errorf("direction must be IN or OUT, got %s", direction)
	}
	now := time.Now().Unix()
	occurredAt := occurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	var walletID, flowAssetKind, flowTokenID string
	if err := db.QueryRow(`SELECT wallet_id, asset_kind, token_id FROM fact_chain_asset_flows WHERE id=?`, carrierFlowID).Scan(&walletID, &flowAssetKind, &flowTokenID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("carrier_flow_id %d not found in fact_chain_asset_flows", carrierFlowID)
		}
		return fmt.Errorf("lookup wallet_id for carrier_flow_id %d: %w", carrierFlowID, err)
	}
	if flowAssetKind != tokenStandard {
		return fmt.Errorf("token_standard mismatch: carrier_flow_id %d has asset_kind %s, but input token_standard is %s", carrierFlowID, flowAssetKind, tokenStandard)
	}
	if flowTokenID != tokenID {
		return fmt.Errorf("token_id mismatch: carrier_flow_id %d has token_id %s, but input token_id is %s", carrierFlowID, flowTokenID, tokenID)
	}
	var txid, voutStr string
	var vout uint32
	parts := strings.Split(carrierUTXOID, ":")
	if len(parts) >= 2 {
		txid = parts[0]
		voutStr = parts[1]
		fmt.Sscanf(voutStr, "%d", &vout)
	} else {
		return fmt.Errorf("invalid carrier_utxo_id format: %s, expected txid:vout", carrierUTXOID)
	}
	linkID := fmt.Sprintf("toklink_cons_%d_%s_%s", carrierFlowID, tokenID, direction)
	_, err := db.Exec(
		`INSERT INTO fact_token_utxo_links(
			link_id,wallet_id,token_id,token_standard,carrier_flow_id,carrier_utxo_id,quantity_text,direction,txid,vout,
			occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(link_id) DO UPDATE SET
			token_id=COALESCE(excluded.token_id, fact_token_utxo_links.token_id),
			token_standard=COALESCE(excluded.token_standard, fact_token_utxo_links.token_standard),
			quantity_text=excluded.quantity_text,
			direction=excluded.direction,
			updated_at_unix=excluded.updated_at_unix,
			payload_json=excluded.payload_json`,
		linkID,
		walletID,
		tokenID,
		tokenStandard,
		carrierFlowID,
		carrierUTXOID,
		quantityText,
		direction,
		txid,
		vout,
		occurredAt,
		now,
		"consumption_link",
		"",
		"{}",
	)
	return err
}

// dbGetTokenFlowInByUTXO 查询 Token 流入记录
func dbGetTokenFlowInByUTXO(db sqlConn, utxoID, tokenID string) (int64, string, error) {
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	tokenID = strings.TrimSpace(tokenID)
	if utxoID == "" || tokenID == "" {
		return 0, "", fmt.Errorf("utxo_id and token_id are required")
	}
	var id int64
	var quantityText string
	err := db.QueryRow(
		`SELECT id, quantity_text FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN' AND token_id=? LIMIT 1`,
		utxoID, tokenID,
	).Scan(&id, &quantityText)
	if err != nil {
		return 0, "", err
	}
	return id, "confirmed", nil
}

// ==================== Step 15: 结算周期辅助函数 ====================

// dbGetSettlementCycleBySource 通过 source_type/source_id 查找 settlement_cycle_id
// 设计说明：
// - 这里是写路径锚点，不允许查不到还继续写
// - source_type/source_id 才是当前唯一来源口径
func dbGetSettlementCycleBySource(db sqlConn, sourceType string, sourceID string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	var id int64
	err := db.QueryRow(`SELECT id FROM fact_settlement_cycles WHERE source_type=? AND source_id=?`, sourceType, sourceID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("settlement cycle not found for %s:%s", sourceType, sourceID)
	}
	return id, err
}

// dbUpsertSettlementCycle 幂等写入结算周期
// 设计说明：
// - source_type/source_id 才是唯一锚点
// - cycle_id 只是可读业务键，必须由调用方按同一来源稳定生成
// - 状态 promotion：confirmed 不能被降级为 pending
func dbUpsertSettlementCycle(db sqlConn, cycleID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	cycleIndex int, occurredAtUnix int64, note string, payload any) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if cycleID == "" {
		return fmt.Errorf("cycle_id is required")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return fmt.Errorf("source_type and source_id are required")
	}
	switch sourceType {
	case "chain_payment", "pool_session", "chain_bsv", "chain_token":
	default:
		return fmt.Errorf("source_type must be chain_payment, pool_session, chain_bsv or chain_token, got %s", sourceType)
	}
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "failed" {
		return fmt.Errorf("state must be pending/confirmed/failed, got %s", state)
	}
	now := time.Now().Unix()
	occurredAt := occurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	confirmedAt := func() int64 {
		if state == "confirmed" {
			return occurredAt
		}
		return 0
	}()

	_, err := db.Exec(
		`INSERT INTO fact_settlement_cycles(
			cycle_id,source_type,source_id,state,
			gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,
			cycle_index,occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(source_type, source_id) DO UPDATE SET
			cycle_id=excluded.cycle_id,
			state=CASE
				WHEN fact_settlement_cycles.state='confirmed' AND excluded.state='pending' THEN fact_settlement_cycles.state
				ELSE excluded.state
			END,
			confirmed_at_unix=CASE
				WHEN excluded.state='confirmed' THEN excluded.occurred_at_unix
				ELSE fact_settlement_cycles.confirmed_at_unix
			END,
			gross_amount_satoshi=excluded.gross_amount_satoshi,
			gate_fee_satoshi=excluded.gate_fee_satoshi,
			net_amount_satoshi=excluded.net_amount_satoshi,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		cycleID, sourceType, sourceID, state,
		grossSatoshi, gateFeeSatoshi, netSatoshi,
		cycleIndex, occurredAt, confirmedAt,
		strings.TrimSpace(note), mustJSONString(payload),
	)
	return err
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
// - 余额 = fact_chain_asset_flows(IN) - fact_bsv_consumptions(used)
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
		 FROM fact_bsv_consumptions c
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
		 FROM fact_bsv_consumptions c
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
			FROM fact_bsv_consumptions c
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

// tokenSourceFlow token 方向的 source flow
type tokenSourceFlow struct {
	FlowID         int64  `json:"flow_id"`
	WalletID       string `json:"wallet_id"`
	Address        string `json:"address"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	UTXOID         string `json:"utxo_id"`
	TxID           string `json:"txid"`
	Vout           uint32 `json:"vout"`
	QuantityText   string `json:"quantity_text"`
	TotalUsedText  string `json:"total_used_text"`
	OccurredAtUnix int64  `json:"occurred_at_unix"`
}

// dbListTokenSpendableSourceFlows 按 token_id 返回仍有剩余额度的 source flow
// 设计说明：
// - 只返回 asset_kind='BSV20' 或 'BSV21' 的记录
// - remaining 由 quantity_text - used_quantity_text 计算
// - 因为 quantity_text 是字符串，返回原始值由调用方做小数运算
func dbListTokenSpendableSourceFlows(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) ([]tokenSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]tokenSourceFlow, error) {
		return dbListTokenSpendableSourceFlowsDB(db, walletID, assetKind, tokenID)
	})
}

func dbListTokenSpendableSourceFlowsDB(db *sql.DB, walletID string, assetKind string, tokenID string) ([]tokenSourceFlow, error) {
	walletID = strings.TrimSpace(walletID)
	if walletID == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind != "BSV20" && assetKind != "BSV21" {
		return nil, fmt.Errorf("asset_kind must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("token_id is required")
	}

	rows, err := db.Query(
		`SELECT f.id,f.wallet_id,f.address,f.asset_kind,f.token_id,f.utxo_id,f.txid,f.vout,
				f.quantity_text,
				COALESCE(used_agg.total_used_text,''),
				f.occurred_at_unix
		 FROM fact_chain_asset_flows f
		 JOIN wallet_utxo w ON f.utxo_id=w.utxo_id
		 LEFT JOIN (
			SELECT c.source_flow_id, COALESCE(GROUP_CONCAT(c.used_quantity_text,','),'') AS total_used_text
			FROM fact_token_consumptions c
			WHERE c.used_quantity_text != ''
			GROUP BY c.source_flow_id
		 ) used_agg ON f.id=used_agg.source_flow_id
		 WHERE f.wallet_id=? AND f.asset_kind=? AND f.token_id=? AND f.direction='IN'
		   AND w.state='unspent'
		   AND w.allocation_class != 'unknown'
		 ORDER BY f.occurred_at_unix ASC, f.id ASC`,
		walletID, assetKind, tokenID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tokenSourceFlow, 0, 16)
	for rows.Next() {
		var s tokenSourceFlow
		if err := rows.Scan(&s.FlowID, &s.WalletID, &s.Address, &s.AssetKind, &s.TokenID,
			&s.UTXOID, &s.TxID, &s.Vout, &s.QuantityText, &s.TotalUsedText, &s.OccurredAtUnix); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// tokenBalanceResult token 余额结果
type tokenBalanceResult struct {
	WalletID      string `json:"wallet_id"`
	AssetKind     string `json:"asset_kind"`
	TokenID       string `json:"token_id"`
	TotalInText   string `json:"total_in_text"`
	TotalUsedText string `json:"total_used_text"`
}

// dbLoadTokenBalanceFact 按 wallet_id + asset_kind + token_id 聚合 token 余额
// 设计说明：
// - 返回 quantity_text 的累加和（逗号分隔字符串），由调用方做小数运算
// - 如果无 fact 数据，返回空字符串，调用方应优雅降级
func dbLoadTokenBalanceFact(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (tokenBalanceResult, error) {
	if store == nil {
		return tokenBalanceResult{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (tokenBalanceResult, error) {
		return dbLoadTokenBalanceFactDB(db, walletID, assetKind, tokenID)
	})
}

func dbLoadTokenBalanceFactDB(db *sql.DB, walletID string, assetKind string, tokenID string) (tokenBalanceResult, error) {
	walletID = strings.TrimSpace(walletID)
	if walletID == "" {
		return tokenBalanceResult{}, fmt.Errorf("wallet_id is required")
	}
	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	tokenID = strings.TrimSpace(tokenID)

	var totalInText, totalUsedText string
	err := db.QueryRow(
		`SELECT COALESCE(GROUP_CONCAT(quantity_text,','),'') FROM fact_chain_asset_flows WHERE wallet_id=? AND asset_kind=? AND token_id=? AND direction='IN'`,
		walletID, assetKind, tokenID,
	).Scan(&totalInText)
	if err != nil {
		return tokenBalanceResult{}, err
	}

	err = db.QueryRow(
		`SELECT COALESCE(GROUP_CONCAT(c.used_quantity_text,','),'')
		 FROM fact_token_consumptions c
		 JOIN fact_chain_asset_flows f ON c.source_flow_id=f.id
		 WHERE f.wallet_id=? AND f.asset_kind=? AND f.token_id=? AND f.direction='IN'`,
		walletID, assetKind, tokenID,
	).Scan(&totalUsedText)
	if err != nil {
		return tokenBalanceResult{}, err
	}

	return tokenBalanceResult{
		WalletID:      walletID,
		AssetKind:     assetKind,
		TokenID:       tokenID,
		TotalInText:   totalInText,
		TotalUsedText: totalUsedText,
	}, nil
}

// dbAppendTokenConsumptionForSettlementCycle 写入 token 消耗（含 used_quantity_text）
func dbAppendTokenConsumptionForSettlementCycle(ctx context.Context, store *clientDB, settlementCycleID int64, utxoID string, usedQuantityText string, occurredAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		return dbAppendTokenConsumptionForSettlementCycleByUTXO(db, settlementCycleID, utxoID, usedQuantityText, occurredAtUnix)
	})
}

func dbAppendTokenConsumptionForSettlementCycleByUTXO(db sqlConn, settlementCycleID int64, utxoID string, usedQuantityText string, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementCycleID <= 0 {
		return fmt.Errorf("settlement_cycle_id is required")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}

	var assetKind, tokenID, quantityText string
	var sourceFlowID int64
	err := db.QueryRow(
		`SELECT id, wallet_id, asset_kind, token_id, quantity_text FROM fact_chain_asset_flows WHERE utxo_id=? AND direction='IN' LIMIT 1`,
		utxoID,
	).Scan(&sourceFlowID, new(string), &assetKind, &tokenID, &quantityText)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("lookup source flow for utxo %s: %w", utxoID, err)
	}
	if sourceFlowID <= 0 {
		return fmt.Errorf("source_flow_id is invalid for utxo %s", utxoID)
	}

	tokenStandard := strings.TrimSpace(assetKind)
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return fmt.Errorf("asset_kind must be BSV20 or BSV21 for token consumption, got %s for utxo %s", assetKind, utxoID)
	}
	usedQty := strings.TrimSpace(usedQuantityText)
	if usedQty == "" {
		usedQty = quantityText
	}
	if usedQty == "" {
		return fmt.Errorf("used_quantity_text is required for token consumption, utxo %s", utxoID)
	}

	now := time.Now().Unix()
	occurredAt := occurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}

	consumptionID := func() string {
		if sourceFlowID > 0 {
			return fmt.Sprintf("tok_cons_flow_%d_%s_%s", sourceFlowID, tokenID, tokenStandard)
		}
		return fmt.Sprintf("tok_cons_utxo_%s_%s_%s", utxoID, tokenID, tokenStandard)
	}()
	_, err = db.Exec(
		`INSERT INTO fact_token_consumptions(
			consumption_id,source_flow_id,source_utxo_id,token_id,token_standard,settlement_cycle_id,state,used_quantity_text,
			occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(consumption_id) DO UPDATE SET
			used_quantity_text=excluded.used_quantity_text,
			state=CASE
				WHEN fact_token_consumptions.state='confirmed' AND excluded.state='pending' THEN fact_token_consumptions.state
				ELSE excluded.state
			END`,
		consumptionID,
		sourceFlowID,
		utxoID,
		tokenID,
		tokenStandard,
		settlementCycleID,
		"confirmed",
		usedQty,
		occurredAt,
		occurredAt,
		"token consumed by settlement cycle",
		"{}",
	)
	if err != nil {
		return fmt.Errorf("append token consumption for utxo %s: %w", utxoID, err)
	}

	if err := dbAppendTokenUTXOLinkIfAbsentForConsumption(db, sourceFlowID, utxoID, tokenID, tokenStandard, usedQty, "OUT", occurredAt); err != nil {
		return fmt.Errorf("append token utxo link for utxo %s: %w", utxoID, err)
	}
	return nil
}
