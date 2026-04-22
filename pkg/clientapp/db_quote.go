package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizlivequotes"
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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]LiveQuoteItem, error) {
		rows, err := root.BizLiveQuotes.Query().
			Where(bizlivequotes.DemandIDEQ(demandID)).
			Order(bizlivequotes.ByCreatedAtUnix()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]LiveQuoteItem, 0, len(rows))
		for _, row := range rows {
			var it LiveQuoteItem
			it.DemandID = row.DemandID
			it.SellerPubHex = row.SellerPubkeyHex
			it.StreamID = row.StreamID
			it.LatestSegmentIndex = uint64(row.LatestSegmentIndex)
			it.ExpiresAtUnix = row.ExpiresAtUnix
			if strings.TrimSpace(row.RecentSegmentsJSON) != "" {
				_ = json.Unmarshal([]byte(row.RecentSegmentsJSON), &it.RecentSegments)
			}
			out = append(out, it)
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		recentJSON, err := json.Marshal(item.RecentSegments)
		if err != nil {
			return err
		}
		existing, err := tx.BizLiveQuotes.Query().
			Where(
				bizlivequotes.DemandIDEQ(strings.TrimSpace(item.DemandID)),
				bizlivequotes.SellerPubkeyHexEQ(strings.ToLower(strings.TrimSpace(item.SellerPubHex))),
			).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetStreamID(strings.ToLower(strings.TrimSpace(item.StreamID))).
				SetLatestSegmentIndex(int64(item.LatestSegmentIndex)).
				SetRecentSegmentsJSON(string(recentJSON)).
				SetExpiresAtUnix(item.ExpiresAtUnix).
				SetCreatedAtUnix(time.Now().Unix()).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.BizLiveQuotes.Create().
			SetDemandID(strings.TrimSpace(item.DemandID)).
			SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(item.SellerPubHex))).
			SetStreamID(strings.ToLower(strings.TrimSpace(item.StreamID))).
			SetLatestSegmentIndex(int64(item.LatestSegmentIndex)).
			SetRecentSegmentsJSON(string(recentJSON)).
			SetExpiresAtUnix(item.ExpiresAtUnix).
			SetCreatedAtUnix(time.Now().Unix()).
			Save(ctx)
		return err
	})
}
