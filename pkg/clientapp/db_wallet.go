package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type walletSummaryCounters struct {
	FlowCount         int64
	TotalIn           int64
	TotalOut          int64
	TotalUsed         int64
	TotalReturned     int64
	LedgerCount       int64
	LedgerIn          int64
	LedgerOut         int64
	TxCount           int64
	PurchaseCount     int64
	GatewayEventCount int64
}

type walletLedgerFilter struct {
	Limit     int
	Offset    int
	Direction string
	Category  string
	Status    string
	TxID      string
	Query     string
}

type walletLedgerPage struct {
	Total int
	Items []walletLedgerItem
}

type walletLedgerItem struct {
	ID                int64           `json:"id"`
	CreatedAtUnix     int64           `json:"created_at_unix"`
	TxID              string          `json:"txid"`
	Direction         string          `json:"direction"`
	Category          string          `json:"category"`
	AmountSatoshi     int64           `json:"amount_satoshi"`
	CounterpartyLabel string          `json:"counterparty_label"`
	Status            string          `json:"status"`
	BlockHeight       int64           `json:"block_height"`
	OccurredAtUnix    int64           `json:"occurred_at_unix"`
	RawRefID          string          `json:"raw_ref_id"`
	Note              string          `json:"note"`
	Payload           json.RawMessage `json:"payload"`
}

func dbLoadWalletSummaryCounters(ctx context.Context, store *clientDB) (walletSummaryCounters, error) {
	if store == nil {
		return walletSummaryCounters{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletSummaryCounters, error) {
		var out walletSummaryCounters
		if err := db.QueryRow(`SELECT COUNT(1),COALESCE(SUM(CASE WHEN amount_satoshi>0 THEN amount_satoshi ELSE 0 END),0),COALESCE(SUM(CASE WHEN amount_satoshi<0 THEN -amount_satoshi ELSE 0 END),0),COALESCE(SUM(used_satoshi),0),COALESCE(SUM(returned_satoshi),0) FROM wallet_fund_flows`).Scan(&out.FlowCount, &out.TotalIn, &out.TotalOut, &out.TotalUsed, &out.TotalReturned); err != nil {
			return walletSummaryCounters{}, err
		}
		if err := db.QueryRow(`SELECT COUNT(1),COALESCE(SUM(CASE WHEN direction='IN' THEN amount_satoshi ELSE 0 END),0),COALESCE(SUM(CASE WHEN direction='OUT' THEN amount_satoshi ELSE 0 END),0) FROM wallet_ledger_entries`).Scan(&out.LedgerCount, &out.LedgerIn, &out.LedgerOut); err != nil {
			return walletSummaryCounters{}, err
		}
		if err := db.QueryRow(`SELECT COUNT(1) FROM fact_pool_session_events WHERE event_kind='tx_history'`).Scan(&out.TxCount); err != nil {
			return walletSummaryCounters{}, err
		}
		if err := db.QueryRow(`SELECT COUNT(1) FROM biz_purchases WHERE status='done'`).Scan(&out.PurchaseCount); err != nil {
			return walletSummaryCounters{}, err
		}
		if err := db.QueryRow(`SELECT COUNT(1) FROM proc_gateway_events`).Scan(&out.GatewayEventCount); err != nil {
			return walletSummaryCounters{}, err
		}
		return out, nil
	})
}

func dbListWalletLedger(ctx context.Context, store *clientDB, f walletLedgerFilter) (walletLedgerPage, error) {
	if store == nil {
		return walletLedgerPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletLedgerPage, error) {
		buildArgs := make([]any, 0, 8)
		where := ""
		if f.Direction != "" {
			where += " AND direction=?"
			buildArgs = append(buildArgs, f.Direction)
		}
		if f.Category != "" {
			where += " AND category=?"
			buildArgs = append(buildArgs, f.Category)
		}
		if f.Status != "" {
			where += " AND status=?"
			buildArgs = append(buildArgs, f.Status)
		}
		if f.TxID != "" {
			where += " AND txid=?"
			buildArgs = append(buildArgs, f.TxID)
		}
		if f.Query != "" {
			where += " AND (txid LIKE ? OR counterparty_label LIKE ? OR note LIKE ? OR raw_ref_id LIKE ?)"
			like := "%" + f.Query + "%"
			buildArgs = append(buildArgs, like, like, like, like)
		}
		var out walletLedgerPage
		if err := db.QueryRow("SELECT COUNT(1) FROM wallet_ledger_entries WHERE 1=1"+where, buildArgs...).Scan(&out.Total); err != nil {
			return walletLedgerPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
			FROM wallet_ledger_entries WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
			append(buildArgs, f.Limit, f.Offset)...,
		)
		if err != nil {
			return walletLedgerPage{}, err
		}
		defer rows.Close()
		out.Items = make([]walletLedgerItem, 0, f.Limit)
		for rows.Next() {
			var it walletLedgerItem
			var payload string
			if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.TxID, &it.Direction, &it.Category, &it.AmountSatoshi, &it.CounterpartyLabel, &it.Status, &it.BlockHeight, &it.OccurredAtUnix, &it.RawRefID, &it.Note, &payload); err != nil {
				return walletLedgerPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return walletLedgerPage{}, err
		}
		return out, nil
	})
}

func dbGetWalletLedgerItem(ctx context.Context, store *clientDB, id int64) (walletLedgerItem, error) {
	if store == nil {
		return walletLedgerItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletLedgerItem, error) {
		var out walletLedgerItem
		var payload string
		err := db.QueryRow(
			`SELECT id,created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
			FROM wallet_ledger_entries WHERE id=?`, id,
		).Scan(&out.ID, &out.CreatedAtUnix, &out.TxID, &out.Direction, &out.Category, &out.AmountSatoshi, &out.CounterpartyLabel, &out.Status, &out.BlockHeight, &out.OccurredAtUnix, &out.RawRefID, &out.Note, &payload)
		if err != nil {
			return walletLedgerItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}
