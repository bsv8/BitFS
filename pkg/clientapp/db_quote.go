package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// dbListLiveQuotes 只负责读取 biz_live_quotes，不做业务过滤。
func dbListLiveQuotes(ctx context.Context, store *clientDB, demandID string) ([]LiveQuoteItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	demandID = strings.TrimSpace(demandID)
	if demandID == "" {
		return nil, fmt.Errorf("demand_id required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]LiveQuoteItem, error) {
		rows, err := db.Query(`SELECT demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix FROM biz_live_quotes WHERE demand_id=? ORDER BY created_at_unix ASC`, demandID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]LiveQuoteItem, 0, 4)
		for rows.Next() {
			var it LiveQuoteItem
			var recentJSON string
			if err := rows.Scan(&it.DemandID, &it.SellerPubHex, &it.StreamID, &it.LatestSegmentIndex, &recentJSON, &it.ExpiresAtUnix); err != nil {
				return nil, err
			}
			if strings.TrimSpace(recentJSON) != "" {
				_ = json.Unmarshal([]byte(recentJSON), &it.RecentSegments)
			}
			out = append(out, it)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

// dbUpsertLiveQuote 只负责写入 biz_live_quotes，不带任何业务判断。
func dbUpsertLiveQuote(ctx context.Context, store *clientDB, item LiveQuoteItem) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if strings.TrimSpace(item.DemandID) == "" || strings.TrimSpace(item.SellerPubHex) == "" || strings.TrimSpace(item.StreamID) == "" {
		return fmt.Errorf("live quote keys are required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		recentJSON, err := json.Marshal(item.RecentSegments)
		if err != nil {
			return err
		}
		_, err = db.Exec(`INSERT INTO biz_live_quotes(demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix,created_at_unix)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(demand_id,seller_pubkey_hex) DO UPDATE SET
				stream_id=excluded.stream_id,
				latest_segment_index=excluded.latest_segment_index,
				recent_segments_json=excluded.recent_segments_json,
				expires_at_unix=excluded.expires_at_unix,
				created_at_unix=excluded.created_at_unix`,
			strings.TrimSpace(item.DemandID),
			strings.ToLower(strings.TrimSpace(item.SellerPubHex)),
			strings.ToLower(strings.TrimSpace(item.StreamID)),
			item.LatestSegmentIndex,
			string(recentJSON),
			item.ExpiresAtUnix,
			time.Now().Unix(),
		)
		return err
	})
}
