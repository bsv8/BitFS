package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

type walletSummaryCounters struct {
	FlowCount         int64
	TotalIn           int64
	TotalOut          int64
	TotalUsed         int64
	TotalReturned     int64
	TxCount           int64
	PurchaseCount     int64
	GatewayEventCount int64
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
