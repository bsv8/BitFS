package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type walletFundFlowFilter struct {
	Limit       int
	Offset      int
	FlowID      string
	FlowType    string
	RefID       string
	Stage       string
	Direction   string
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
	AmountSatoshi   int64           `json:"amount_satoshi"`
	UsedSatoshi     int64           `json:"used_satoshi"`
	ReturnedSatoshi int64           `json:"returned_satoshi"`
	RelatedTxID     string          `json:"related_txid"`
	Note            string          `json:"note"`
	Payload         json.RawMessage `json:"payload"`
}

func dbListWalletFundFlows(ctx context.Context, store *clientDB, f walletFundFlowFilter) (walletFundFlowPage, error) {
	if store == nil {
		return walletFundFlowPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletFundFlowPage, error) {
		args := make([]any, 0, 10)
		where := ""
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
		if f.Direction != "" {
			where += " AND direction=?"
			args = append(args, f.Direction)
		}
		if f.Purpose != "" {
			where += " AND purpose=?"
			args = append(args, f.Purpose)
		}
		if f.RelatedTxID != "" {
			where += " AND related_txid=?"
			args = append(args, f.RelatedTxID)
		}
		if f.VisitID != "" {
			where += " AND visit_id=?"
			args = append(args, f.VisitID)
		}
		if f.Query != "" {
			where += " AND (flow_id LIKE ? OR ref_id LIKE ? OR note LIKE ? OR related_txid LIKE ? OR visit_locator LIKE ?)"
			like := "%" + f.Query + "%"
			args = append(args, like, like, like, like, like)
		}
		var out walletFundFlowPage
		if err := db.QueryRow("SELECT COUNT(1) FROM wallet_fund_flows WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return walletFundFlowPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,visit_id,visit_locator,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json FROM wallet_fund_flows WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return walletFundFlowPage{}, err
		}
		defer rows.Close()
		out.Items = make([]walletFundFlowItem, 0, f.Limit)
		for rows.Next() {
			var it walletFundFlowItem
			var payload string
			if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.VisitID, &it.VisitLocator, &it.FlowID, &it.FlowType, &it.RefID, &it.Stage, &it.Direction, &it.Purpose, &it.AmountSatoshi, &it.UsedSatoshi, &it.ReturnedSatoshi, &it.RelatedTxID, &it.Note, &payload); err != nil {
				return walletFundFlowPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return walletFundFlowPage{}, err
		}
		return out, nil
	})
}

func dbGetWalletFundFlowItem(ctx context.Context, store *clientDB, id int64) (walletFundFlowItem, error) {
	if store == nil {
		return walletFundFlowItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletFundFlowItem, error) {
		var out walletFundFlowItem
		var payload string
		err := db.QueryRow(`SELECT id,created_at_unix,visit_id,visit_locator,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json FROM wallet_fund_flows WHERE id=?`, id).
			Scan(&out.ID, &out.CreatedAtUnix, &out.VisitID, &out.VisitLocator, &out.FlowID, &out.FlowType, &out.RefID, &out.Stage, &out.Direction, &out.Purpose, &out.AmountSatoshi, &out.UsedSatoshi, &out.ReturnedSatoshi, &out.RelatedTxID, &out.Note, &payload)
		if err != nil {
			return walletFundFlowItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}
