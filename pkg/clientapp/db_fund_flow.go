package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// walletFundFlowFilter 资金流水过滤条件（历史兼容层）
// 设计说明：底层已迁移到 fact_* 事实表组装
// - visit_id 保留字段，不参与过滤（fact 表未保留该上下文）
// - 查询使用 UNION ALL 统一视图，保证全局排序和分页正确
type walletFundFlowFilter struct {
	Limit       int
	Offset      int
	FlowID      string
	FlowType    string
	RefID       string
	Stage       string
	Directions  []string // 多值 IN 匹配
	Purpose     string
	RelatedTxID string
	VisitID     string
	Query       string
}

type walletFundFlowPage struct {
	Total int
	Items []walletFundFlowItem
}

type walletFundFlowItem struct {
	ID              int64           `json:"id"`
	CreatedAtUnix   int64           `json:"created_at_unix"`
	VisitID         string          `json:"visit_id"`
	VisitLocator    string          `json:"visit_locator"`
	FlowID          string          `json:"flow_id"`
	FlowType        string          `json:"flow_type"`
	RefID           string          `json:"ref_id"`
	Stage           string          `json:"stage"`
	Direction       string          `json:"direction"`
	Purpose         string          `json:"purpose"`
	AssetKind       string          `json:"asset_kind"`
	TokenID         string          `json:"token_id"`
	TokenStandard   string          `json:"token_standard"`
	AmountSatoshi   int64           `json:"amount_satoshi"`
	UsedSatoshi     int64           `json:"used_satoshi"`
	ReturnedSatoshi int64           `json:"returned_satoshi"`
	RelatedTxID     string          `json:"related_txid"`
	Note            string          `json:"note"`
	Payload         json.RawMessage `json:"payload"`
}

// unionSource 统一事实视图的行结构
// 设计说明：三张 fact 表列名不同，这里定义统一投影列，确保 UNION ALL 列对齐
type unionSource struct {
	ID              int64
	CreatedAtUnix   int64
	FlowID          string
	FlowType        string
	RefID           string
	Stage           string
	Direction       string
	Purpose         string
	AssetKind       string
	TokenID         string
	TokenStandard   string
	AmountSatoshi   int64
	UsedSatoshi     int64
	ReturnedSatoshi int64
	RelatedTxID     string
	Note            string
	Payload         string
	VisitID         string // 兼容字段，当前固定空值
}

// dbListWalletFundFlows 从 fact_* 事实表组装资金流水列表
// 设计说明：
// - 使用 UNION ALL 合并三类事实，先过滤再全局排序，最后 LIMIT/OFFSET
// - visit_id 字段仅为兼容保留，当前不参与过滤，值固定空
// - direction 过滤对所有事实源生效（chain_payment 按 net_amount_satoshi 正负推断）
func dbListWalletFundFlows(ctx context.Context, store *clientDB, f walletFundFlowFilter) (walletFundFlowPage, error) {
	if store == nil {
		return walletFundFlowPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletFundFlowPage, error) {
		args := make([]any, 0, 12)
		where := ""

		// 构建统一 WHERE 条件
		if f.FlowID != "" {
			where += " AND flow_id=?"
			args = append(args, f.FlowID)
		}
		if f.FlowType != "" {
			where += " AND flow_type=?"
			args = append(args, f.FlowType)
		}
		if f.RefID != "" {
			where += " AND ref_id=?"
			args = append(args, f.RefID)
		}
		if f.Stage != "" {
			where += " AND stage=?"
			args = append(args, f.Stage)
		}
		if len(f.Directions) > 0 {
			placeholders := make([]string, len(f.Directions))
			for i, d := range f.Directions {
				placeholders[i] = "?"
				args = append(args, strings.ToUpper(d))
			}
			where += " AND UPPER(direction) IN (" + strings.Join(placeholders, ",") + ")"
		}
		if f.Purpose != "" {
			where += " AND purpose=?"
			args = append(args, f.Purpose)
		}
		if f.RelatedTxID != "" {
			where += " AND related_txid=?"
			args = append(args, f.RelatedTxID)
		}
		if f.Query != "" {
			where += " AND (flow_id LIKE ? OR ref_id LIKE ? OR note LIKE ? OR related_txid LIKE ?)"
			like := "%" + f.Query + "%"
			args = append(args, like, like, like, like)
		}

		var out walletFundFlowPage

		// 计数查询
		countSQL := `SELECT COUNT(1) FROM (` + unionAllQuery() + `) AS unified WHERE 1=1` + where
		if err := QueryRowContext(ctx, db, countSQL, args...).Scan(&out.Total); err != nil {
			return walletFundFlowPage{}, err
		}

		// 明细查询：全局排序 + 分页
		detailSQL := `SELECT id, created_at_unix, flow_id, flow_type, ref_id, stage, direction, purpose, asset_kind, token_id, token_standard, amount_satoshi, used_satoshi, returned_satoshi, related_txid, note, payload, visit_id FROM (` + unionAllQuery() + `) AS unified WHERE 1=1` + where + ` ORDER BY created_at_unix DESC, flow_type, id DESC LIMIT ? OFFSET ?`
		rows, err := QueryContext(ctx, db, detailSQL, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return walletFundFlowPage{}, err
		}
		defer rows.Close()

		out.Items = make([]walletFundFlowItem, 0, f.Limit)
		for rows.Next() {
			var src unionSource
			if err := rows.Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage, &src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi, &src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID); err != nil {
				return walletFundFlowPage{}, err
			}
			out.Items = append(out.Items, walletFundFlowItem{
				ID:              src.ID,
				CreatedAtUnix:   src.CreatedAtUnix,
				VisitID:         src.VisitID,
				VisitLocator:    "", // 旧表无源
				FlowID:          src.FlowID,
				FlowType:        src.FlowType,
				RefID:           src.RefID,
				Stage:           src.Stage,
				Direction:       src.Direction,
				Purpose:         src.Purpose,
				AssetKind:       src.AssetKind,
				TokenID:         src.TokenID,
				TokenStandard:   src.TokenStandard,
				AmountSatoshi:   src.AmountSatoshi,
				UsedSatoshi:     src.UsedSatoshi,
				ReturnedSatoshi: src.ReturnedSatoshi,
				RelatedTxID:     src.RelatedTxID,
				Note:            src.Note,
				Payload:         json.RawMessage(src.Payload),
			})
		}
		if err := rows.Err(); err != nil {
			return walletFundFlowPage{}, err
		}
		return out, nil
	})
}

// unionAllQuery 构建三张 fact 表的 UNION ALL 统一视图
// 设计说明（硬切版）：
// - 列顺序必须一致：id, created_at_unix, flow_id, flow_type, ref_id, stage, direction, purpose, asset_kind, token_id, token_standard, amount_satoshi, used_satoshi, returned_satoshi, related_txid, note, payload, visit_id
// - 只从新 schema 表查询：fact_bsv_utxos, fact_token_lots
func unionAllQuery() string {
	return `
		-- BSV UTXO 流入（unspent 状态的入账）
		SELECT 
			rowid as id, 
			created_at_unix, 
			utxo_id AS flow_id, 
			'chain_bsv_in' AS flow_type,
			utxo_id AS ref_id, 
			'confirmed' AS stage, 
			'IN' AS direction, 
			carrier_type AS purpose,
			'BSV' AS asset_kind, 
			'' AS token_id, 
			'' AS token_standard,
			value_satoshi AS amount_satoshi, 
			0 AS used_satoshi, 
			0 AS returned_satoshi,
			txid AS related_txid, 
			COALESCE(note, '') AS note,
			COALESCE(payload_json, '{}') AS payload, 
			'' AS visit_id
		FROM fact_bsv_utxos
		WHERE utxo_state = 'unspent'
		UNION ALL
		-- BSV UTXO 流出（spent 状态的花费）
		SELECT 
			rowid as id, 
			spent_at_unix AS created_at_unix, 
			utxo_id AS flow_id, 
			'chain_bsv_out' AS flow_type,
			utxo_id AS ref_id, 
			'confirmed' AS stage, 
			'OUT' AS direction, 
			carrier_type AS purpose,
			'BSV' AS asset_kind, 
			'' AS token_id, 
			'' AS token_standard,
			value_satoshi AS amount_satoshi, 
			value_satoshi AS used_satoshi, 
			0 AS returned_satoshi,
			spent_by_txid AS related_txid, 
			COALESCE(note, '') AS note,
			COALESCE(payload_json, '{}') AS payload, 
			'' AS visit_id
		FROM fact_bsv_utxos
		WHERE utxo_state = 'spent' AND spent_at_unix > 0
		UNION ALL
		-- Token Lot 流入
		SELECT 
			rowid as id, 
			created_at_unix, 
			lot_id AS flow_id, 
			'chain_token_in' AS flow_type,
			lot_id AS ref_id, 
			'confirmed' AS stage, 
			'IN' AS direction, 
			token_standard AS purpose,
			token_standard AS asset_kind, 
			token_id, 
			token_standard,
			0 AS amount_satoshi, 
			0 AS used_satoshi, 
			0 AS returned_satoshi,
			mint_txid AS related_txid, 
			COALESCE(note, '') AS note,
			COALESCE(payload_json, '{}') AS payload, 
			'' AS visit_id
		FROM fact_token_lots
		WHERE lot_state IN ('unspent', 'spent')
	`
}

// dbGetWalletFundFlowItem 从 fact_* 事实表获取单条资金流水详情
// 设计说明：flow_type 必填，用于精确定位；使用 refID（文本）替代 id（int64）
func dbGetWalletFundFlowItem(ctx context.Context, store *clientDB, refID string, flowType string) (walletFundFlowItem, error) {
	if store == nil {
		return walletFundFlowItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletFundFlowItem, error) {
		var out walletFundFlowItem
		var src unionSource

		// flow_type 已由 API 层校验，直接查询
		err := queryUnionSourceByRefID(db, refID, flowType, &src)
		if err != nil {
			return walletFundFlowItem{}, err
		}

		out = walletFundFlowItem{
			ID:              src.ID,
			CreatedAtUnix:   src.CreatedAtUnix,
			VisitID:         src.VisitID,
			VisitLocator:    "",
			FlowID:          src.FlowID,
			FlowType:        src.FlowType,
			RefID:           src.RefID,
			Stage:           src.Stage,
			Direction:       src.Direction,
			Purpose:         src.Purpose,
			AmountSatoshi:   src.AmountSatoshi,
			UsedSatoshi:     src.UsedSatoshi,
			ReturnedSatoshi: src.ReturnedSatoshi,
			RelatedTxID:     src.RelatedTxID,
			Note:            src.Note,
			Payload:         json.RawMessage(src.Payload),
		}
		return out, nil
	})
}

// queryUnionSourceByRefID 按 flow_type 和 ref_id 路由查询单条记录
// 设计说明（硬切版）：
// - 只支持新 schema 的 flow_type：chain_bsv_in, chain_bsv_out, chain_token_in
// - 从 fact_bsv_utxos 和 fact_token_lots 查询
// - 使用 ref_id（文本）替代 id（int64），因为新表使用 utxo_id/lot_id 作为主键
func queryUnionSourceByRefID(db *sql.DB, refID string, flowType string, src *unionSource) error {
	var err error
	switch flowType {
	case "chain_bsv_in":
		err = QueryRowContext(ctx, db, `
			SELECT rowid, created_at_unix, utxo_id, 'chain_bsv_in', utxo_id, 'confirmed', 'IN', carrier_type,
				'BSV', '', '', value_satoshi, 0, 0, txid, COALESCE(note,''), COALESCE(payload_json,'{}'), ''
			FROM fact_bsv_utxos WHERE utxo_id=? AND utxo_state='unspent'`, refID).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	case "chain_bsv_out":
		err = QueryRowContext(ctx, db, `
			SELECT rowid, spent_at_unix, utxo_id, 'chain_bsv_out', utxo_id, 'confirmed', 'OUT', carrier_type,
				'BSV', '', '', value_satoshi, value_satoshi, 0, spent_by_txid, COALESCE(note,''), COALESCE(payload_json,'{}'), ''
			FROM fact_bsv_utxos WHERE utxo_id=? AND utxo_state='spent'`, refID).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	case "chain_token_in":
		err = QueryRowContext(ctx, db, `
			SELECT rowid, created_at_unix, lot_id, 'chain_token_in', lot_id, 'confirmed', 'IN', token_standard,
				token_standard, token_id, token_standard, 0, 0, 0, mint_txid, COALESCE(note,''), COALESCE(payload_json,'{}'), ''
			FROM fact_token_lots WHERE lot_id=?`, refID).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	default:
		return fmt.Errorf("unknown flow_type: %s", flowType)
	}
	return err
}
