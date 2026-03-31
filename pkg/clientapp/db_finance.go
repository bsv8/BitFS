package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type financeBusinessFilter struct {
	Limit        int
	Offset       int
	BusinessID   string
	SceneType    string
	SceneSubType string
	Status       string
	FromPartyID  string
	ToPartyID    string
	RefID        string
	Query        string
}

type financeBusinessPage struct {
	Total int
	Items []financeBusinessItem
}

type financeBusinessItem struct {
	BusinessID     string          `json:"business_id"`
	SceneType      string          `json:"scene_type"`
	SceneSubType   string          `json:"scene_subtype"`
	FromPartyID    string          `json:"from_party_id"`
	ToPartyID      string          `json:"to_party_id"`
	RefID          string          `json:"ref_id"`
	Status         string          `json:"status"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type financeProcessEventFilter struct {
	Limit        int
	Offset       int
	ProcessID    string
	SceneType    string
	SceneSubType string
	EventType    string
	Status       string
	RefID        string
	Query        string
}

type financeProcessEventPage struct {
	Total int
	Items []financeProcessEventItem
}

type financeProcessEventItem struct {
	ID             int64           `json:"id"`
	ProcessID      string          `json:"process_id"`
	SceneType      string          `json:"scene_type"`
	SceneSubType   string          `json:"scene_subtype"`
	EventType      string          `json:"event_type"`
	Status         string          `json:"status"`
	RefID          string          `json:"ref_id"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type financeBreakdownFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	Query      string
}

type financeBreakdownPage struct {
	Total int
	Items []financeBreakdownItem
}

type financeBreakdownItem struct {
	ID                 int64           `json:"id"`
	BusinessID         string          `json:"business_id"`
	TxID               string          `json:"txid"`
	GrossInputSatoshi  int64           `json:"gross_input_satoshi"`
	ChangeBackSatoshi  int64           `json:"change_back_satoshi"`
	ExternalInSatoshi  int64           `json:"external_in_satoshi"`
	CounterpartyOutSat int64           `json:"counterparty_out_satoshi"`
	MinerFeeSatoshi    int64           `json:"miner_fee_satoshi"`
	NetOutSatoshi      int64           `json:"net_out_satoshi"`
	NetInSatoshi       int64           `json:"net_in_satoshi"`
	CreatedAtUnix      int64           `json:"created_at_unix"`
	Note               string          `json:"note"`
	Payload            json.RawMessage `json:"payload"`
}

type financeUTXOLinkFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	UTXOID     string
	TxRole     string
	IOSide     string
	UTXORole   string
	Query      string
}

type financeUTXOLinkPage struct {
	Total int
	Items []financeUTXOLinkItem
}

type financeUTXOLinkItem struct {
	ID            int64           `json:"id"`
	BusinessID    string          `json:"business_id"`
	TxID          string          `json:"txid"`
	UTXOID        string          `json:"utxo_id"`
	TxRole        string          `json:"tx_role"`
	IOSide        string          `json:"io_side"`
	UTXORole      string          `json:"utxo_role"`
	AmountSatoshi int64           `json:"amount_satoshi"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	Note          string          `json:"note"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListFinanceBusinesses(ctx context.Context, store *clientDB, f financeBusinessFilter) (financeBusinessPage, error) {
	if store == nil {
		return financeBusinessPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.SceneType != "" {
			where += " AND scene_type=?"
			args = append(args, f.SceneType)
		}
		if f.SceneSubType != "" {
			where += " AND scene_subtype=?"
			args = append(args, f.SceneSubType)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.FromPartyID != "" {
			where += " AND from_party_id=?"
			args = append(args, f.FromPartyID)
		}
		if f.ToPartyID != "" {
			where += " AND to_party_id=?"
			args = append(args, f.ToPartyID)
		}
		if f.RefID != "" {
			where += " AND ref_id=?"
			args = append(args, f.RefID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (business_id LIKE ? OR note LIKE ? OR ref_id LIKE ? OR idempotency_key LIKE ?)"
			args = append(args, like, like, like, like)
		}
		var out financeBusinessPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_business WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeBusinessPage{}, err
		}
		rows, err := db.Query(
			`SELECT business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_business WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,business_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBusinessPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBusinessItem, 0, f.Limit)
		for rows.Next() {
			var it financeBusinessItem
			var payload string
			if err := rows.Scan(&it.BusinessID, &it.SceneType, &it.SceneSubType, &it.FromPartyID, &it.ToPartyID, &it.RefID, &it.Status, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeBusinessPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBusinessPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBusiness(ctx context.Context, store *clientDB, businessID string) (financeBusinessItem, error) {
	if store == nil {
		return financeBusinessItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessItem, error) {
		var out financeBusinessItem
		var payload string
		err := db.QueryRow(
			`SELECT business_id,scene_type,scene_subtype,from_party_id,to_party_id,ref_id,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_business WHERE business_id=?`,
			businessID,
		).Scan(&out.BusinessID, &out.SceneType, &out.SceneSubType, &out.FromPartyID, &out.ToPartyID, &out.RefID, &out.Status, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeBusinessItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceProcessEvents(ctx context.Context, store *clientDB, f financeProcessEventFilter) (financeProcessEventPage, error) {
	if store == nil {
		return financeProcessEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.ProcessID != "" {
			where += " AND process_id=?"
			args = append(args, f.ProcessID)
		}
		if f.SceneType != "" {
			where += " AND scene_type=?"
			args = append(args, f.SceneType)
		}
		if f.SceneSubType != "" {
			where += " AND scene_subtype=?"
			args = append(args, f.SceneSubType)
		}
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.RefID != "" {
			where += " AND ref_id=?"
			args = append(args, f.RefID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (process_id LIKE ? OR note LIKE ? OR ref_id LIKE ? OR idempotency_key LIKE ?)"
			args = append(args, like, like, like, like)
		}
		var out financeProcessEventPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_process_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeProcessEventPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,process_id,scene_type,scene_subtype,event_type,status,ref_id,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_process_events WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeProcessEventItem, 0, f.Limit)
		for rows.Next() {
			var it financeProcessEventItem
			var payload string
			if err := rows.Scan(&it.ID, &it.ProcessID, &it.SceneType, &it.SceneSubType, &it.EventType, &it.Status, &it.RefID, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeProcessEventPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeProcessEventPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceProcessEvent(ctx context.Context, store *clientDB, id int64) (financeProcessEventItem, error) {
	if store == nil {
		return financeProcessEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventItem, error) {
		var out financeProcessEventItem
		var payload string
		err := db.QueryRow(
			`SELECT id,process_id,scene_type,scene_subtype,event_type,status,ref_id,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_process_events WHERE id=?`,
			id,
		).Scan(&out.ID, &out.ProcessID, &out.SceneType, &out.SceneSubType, &out.EventType, &out.Status, &out.RefID, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeProcessEventItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceBreakdowns(ctx context.Context, store *clientDB, f financeBreakdownFilter) (financeBreakdownPage, error) {
	if store == nil {
		return financeBreakdownPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownPage, error) {
		where := ""
		args := make([]any, 0, 12)
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND txid=?"
			args = append(args, f.TxID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (business_id LIKE ? OR txid LIKE ? OR note LIKE ?)"
			args = append(args, like, like, like)
		}
		var out financeBreakdownPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_tx_breakdown WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeBreakdownPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,business_id,txid,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
			 FROM fin_tx_breakdown WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBreakdownPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBreakdownItem, 0, f.Limit)
		for rows.Next() {
			var it financeBreakdownItem
			var payload string
			if err := rows.Scan(
				&it.ID, &it.BusinessID, &it.TxID, &it.GrossInputSatoshi, &it.ChangeBackSatoshi, &it.ExternalInSatoshi, &it.CounterpartyOutSat,
				&it.MinerFeeSatoshi, &it.NetOutSatoshi, &it.NetInSatoshi, &it.CreatedAtUnix, &it.Note, &payload,
			); err != nil {
				return financeBreakdownPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBreakdownPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBreakdown(ctx context.Context, store *clientDB, id int64) (financeBreakdownItem, error) {
	if store == nil {
		return financeBreakdownItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownItem, error) {
		var out financeBreakdownItem
		var payload string
		err := db.QueryRow(
			`SELECT id,business_id,txid,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
			 FROM fin_tx_breakdown WHERE id=?`,
			id,
		).Scan(
			&out.ID, &out.BusinessID, &out.TxID, &out.GrossInputSatoshi, &out.ChangeBackSatoshi, &out.ExternalInSatoshi, &out.CounterpartyOutSat,
			&out.MinerFeeSatoshi, &out.NetOutSatoshi, &out.NetInSatoshi, &out.CreatedAtUnix, &out.Note, &payload,
		)
		if err != nil {
			return financeBreakdownItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceUTXOLinks(ctx context.Context, store *clientDB, f financeUTXOLinkFilter) (financeUTXOLinkPage, error) {
	if store == nil {
		return financeUTXOLinkPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkPage, error) {
		where := ""
		args := make([]any, 0, 12)
		if f.BusinessID != "" {
			where += " AND l.business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND l.txid=?"
			args = append(args, f.TxID)
		}
		if f.UTXOID != "" {
			where += " AND l.utxo_id=?"
			args = append(args, f.UTXOID)
		}
		if f.TxRole != "" {
			where += " AND bt.tx_role=?"
			args = append(args, f.TxRole)
		}
		if f.IOSide != "" {
			where += " AND l.io_side=?"
			args = append(args, f.IOSide)
		}
		if f.UTXORole != "" {
			where += " AND l.utxo_role=?"
			args = append(args, f.UTXORole)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (l.business_id LIKE ? OR l.txid LIKE ? OR l.utxo_id LIKE ? OR l.note LIKE ? OR bt.tx_role LIKE ? OR l.utxo_role LIKE ?)"
			args = append(args, like, like, like, like, like, like)
		}
		var out financeUTXOLinkPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM fin_tx_utxo_links l
			   JOIN fin_business_txs bt ON bt.business_id=l.business_id AND bt.txid=l.txid
			  WHERE 1=1`+where,
			args...,
		).Scan(&out.Total); err != nil {
			return financeUTXOLinkPage{}, err
		}
		rows, err := db.Query(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,bt.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM fin_tx_utxo_links l
			   JOIN fin_business_txs bt ON bt.business_id=l.business_id AND bt.txid=l.txid
			  WHERE 1=1`+where+` ORDER BY l.id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeUTXOLinkPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeUTXOLinkItem, 0, f.Limit)
		for rows.Next() {
			var it financeUTXOLinkItem
			var payload string
			if err := rows.Scan(&it.ID, &it.BusinessID, &it.TxID, &it.UTXOID, &it.TxRole, &it.IOSide, &it.UTXORole, &it.AmountSatoshi, &it.CreatedAtUnix, &it.Note, &payload); err != nil {
				return financeUTXOLinkPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeUTXOLinkPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceUTXOLink(ctx context.Context, store *clientDB, id int64) (financeUTXOLinkItem, error) {
	if store == nil {
		return financeUTXOLinkItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkItem, error) {
		var out financeUTXOLinkItem
		var payload string
		err := db.QueryRow(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,bt.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM fin_tx_utxo_links l
			   JOIN fin_business_txs bt ON bt.business_id=l.business_id AND bt.txid=l.txid
			  WHERE l.id=?`,
			id,
		).Scan(&out.ID, &out.BusinessID, &out.TxID, &out.UTXOID, &out.TxRole, &out.IOSide, &out.UTXORole, &out.AmountSatoshi, &out.CreatedAtUnix, &out.Note, &payload)
		if err != nil {
			return financeUTXOLinkItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}
