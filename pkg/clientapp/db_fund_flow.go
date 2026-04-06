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
		if err := db.QueryRow(countSQL, args...).Scan(&out.Total); err != nil {
			return walletFundFlowPage{}, err
		}

		// 明细查询：全局排序 + 分页
		detailSQL := `SELECT id, created_at_unix, flow_id, flow_type, ref_id, stage, direction, purpose, asset_kind, token_id, token_standard, amount_satoshi, used_satoshi, returned_satoshi, related_txid, note, payload, visit_id FROM (` + unionAllQuery() + `) AS unified WHERE 1=1` + where + ` ORDER BY created_at_unix DESC, flow_type, id DESC LIMIT ? OFFSET ?`
		rows, err := db.Query(detailSQL, append(args, f.Limit, f.Offset)...)
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
// 设计说明：
// - 列顺序必须一致：id, created_at_unix, flow_id, flow_type, ref_id, stage, direction, purpose, asset_kind, token_id, token_standard, amount_satoshi, used_satoshi, returned_satoshi, related_txid, note, payload, visit_id
// - visit_id 保留字段，不参与过滤（fact 表未保留该上下文）
func unionAllQuery() string {
	return `
		SELECT id, occurred_at_unix AS created_at_unix, flow_id, 'chain_asset' AS flow_type,
			utxo_id AS ref_id, 'confirmed' AS stage, direction, asset_kind AS purpose,
			COALESCE(asset_kind, '') AS asset_kind, COALESCE(token_id, '') AS token_id,
			CASE WHEN asset_kind IN ('BSV20', 'BSV21') THEN asset_kind ELSE '' END AS token_standard,
			amount_satoshi, 0 AS used_satoshi, 0 AS returned_satoshi,
			txid AS related_txid, COALESCE(note, '') AS note,
			COALESCE(payload_json, '{}') AS payload, '' AS visit_id
		FROM fact_chain_asset_flows
		UNION ALL
		SELECT id, occurred_at_unix AS created_at_unix, txid AS flow_id, 'chain_payment' AS flow_type,
			txid AS ref_id, status AS stage,
			CASE WHEN net_amount_satoshi > 0 THEN 'IN' ELSE 'OUT' END AS direction,
			payment_subtype AS purpose,
			'BSV' AS asset_kind, '' AS token_id, '' AS token_standard,
			net_amount_satoshi AS amount_satoshi,
			CASE WHEN net_amount_satoshi < 0 THEN -net_amount_satoshi ELSE 0 END AS used_satoshi,
			0 AS returned_satoshi,
			txid AS related_txid, '' AS note,
			COALESCE(payload_json, '{}') AS payload, '' AS visit_id
		FROM fact_chain_payments
		UNION ALL
		SELECT id, created_at_unix, allocation_id AS flow_id, 'pool_event' AS flow_type,
			pool_session_id AS ref_id, event_kind AS stage, direction, purpose,
			'BSV' AS asset_kind, '' AS token_id, '' AS token_standard,
			amount_satoshi, 0 AS used_satoshi, 0 AS returned_satoshi,
			COALESCE(txid, '') AS related_txid, COALESCE(note, '') AS note,
			COALESCE(payload_json, '{}') AS payload, '' AS visit_id
		FROM fact_pool_session_events
		WHERE event_kind = 'tx_history'`
}

// dbGetWalletFundFlowItem 从 fact_* 事实表获取单条资金流水详情
// 设计说明：flow_type 必填，用于精确定位，避免 id 跨表冲突
func dbGetWalletFundFlowItem(ctx context.Context, store *clientDB, id int64, flowType string) (walletFundFlowItem, error) {
	if store == nil {
		return walletFundFlowItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletFundFlowItem, error) {
		var out walletFundFlowItem
		var src unionSource

		// flow_type 已由 API 层校验，直接查询
		err := queryUnionSourceByID(db, id, flowType, &src)
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

// queryUnionSourceByID 按 flow_type 路由查询单条记录
func queryUnionSourceByID(db *sql.DB, id int64, flowType string, src *unionSource) error {
	var err error
	switch flowType {
	case "chain_asset":
		err = db.QueryRow(`
			SELECT id, occurred_at_unix, flow_id, 'chain_asset', utxo_id, 'confirmed', direction, asset_kind,
				COALESCE(asset_kind, ''), COALESCE(token_id, ''),
				CASE WHEN asset_kind IN ('BSV20', 'BSV21') THEN asset_kind ELSE '' END,
				amount_satoshi, 0, 0, txid, COALESCE(note,''), COALESCE(payload_json,'{}'), ''
			FROM fact_chain_asset_flows WHERE id=?`, id).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	case "chain_payment":
		err = db.QueryRow(`
			SELECT id, occurred_at_unix, txid, 'chain_payment', txid, status,
				CASE WHEN net_amount_satoshi > 0 THEN 'IN' ELSE 'OUT' END,
				payment_subtype, 'BSV', '', '',
				net_amount_satoshi,
				CASE WHEN net_amount_satoshi < 0 THEN -net_amount_satoshi ELSE 0 END,
				0, txid, '', COALESCE(payload_json,'{}'), ''
			FROM fact_chain_payments WHERE id=?`, id).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	case "pool_event":
		err = db.QueryRow(`
			SELECT id, created_at_unix, allocation_id, 'pool_event', pool_session_id, event_kind,
				direction, purpose, 'BSV', '', '', amount_satoshi, 0, 0,
				COALESCE(txid,''), COALESCE(note,''), COALESCE(payload_json,'{}'), ''
			FROM fact_pool_session_events WHERE id=? AND event_kind='tx_history'`, id).
			Scan(&src.ID, &src.CreatedAtUnix, &src.FlowID, &src.FlowType, &src.RefID, &src.Stage,
				&src.Direction, &src.Purpose, &src.AssetKind, &src.TokenID, &src.TokenStandard, &src.AmountSatoshi, &src.UsedSatoshi, &src.ReturnedSatoshi,
				&src.RelatedTxID, &src.Note, &src.Payload, &src.VisitID)
	default:
		return fmt.Errorf("unknown flow_type: %s", flowType)
	}
	return err
}
