package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// 设计说明：
// - 直连交易相关的写库动作统一收口到这里；
// - run.go 只保留协议校验、签名和流程编排，不再直接写 SQL。
func dbUpsertDirectQuote(ctx context.Context, store *clientDB, req directQuoteSubmitReq, sellerPubHex string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	demandID := strings.TrimSpace(req.DemandId)
	sellerPubHex = strings.ToLower(strings.TrimSpace(sellerPubHex))
	if demandID == "" || sellerPubHex == "" {
		return fmt.Errorf("demand_id and seller_pub_hex are required")
	}
	now := time.Now().Unix()
	availableChunkBitmapHex := normalizeChunkBitmapBytes(req.AvailableChunkBitmap)
	recommendedName := sanitizeRecommendedFileName(req.RecommendedFileName)
	mimeType := sanitizeMIMEHint(req.MimeHint)
	arbiterPubHexes, err := normalizePubHexList(req.ArbiterPubkeyHexes)
	if err != nil {
		return err
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		_, err := ExecContext(ctx, tx,
			`INSERT INTO biz_demand_quotes(
				demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(demand_id,seller_pub_hex) DO UPDATE SET
				seed_price_satoshi=excluded.seed_price_satoshi,
				chunk_price_satoshi=excluded.chunk_price_satoshi,
				chunk_count=excluded.chunk_count,
				file_size_bytes=excluded.file_size_bytes,
				recommended_file_name=excluded.recommended_file_name,
				mime_type=excluded.mime_type,
				available_chunk_bitmap_hex=excluded.available_chunk_bitmap_hex,
				expires_at_unix=excluded.expires_at_unix,
				created_at_unix=excluded.created_at_unix`,
			demandID,
			sellerPubHex,
			req.SeedPrice,
			req.ChunkPrice,
			req.ChunkCount,
			req.FileSize,
			recommendedName,
			mimeType,
			availableChunkBitmapHex,
			req.ExpiresAtUnix,
			now,
		)
		if err != nil {
			return err
		}
		var quoteID int64
		if err := QueryRowContext(ctx, tx, `SELECT id FROM biz_demand_quotes WHERE demand_id=? AND seller_pub_hex=?`, demandID, sellerPubHex).Scan(&quoteID); err != nil {
			return err
		}
		if _, err := ExecContext(ctx, tx, `DELETE FROM biz_demand_quote_arbiters WHERE quote_id=?`, quoteID); err != nil {
			return err
		}
		for _, arbiterPubHex := range arbiterPubHexes {
			if _, err := ExecContext(ctx, tx, `INSERT INTO biz_demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?)
				ON CONFLICT(quote_id,arbiter_pub_hex) DO NOTHING`, quoteID, arbiterPubHex); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbRecordDemand(ctx context.Context, store ClientStore, demandID string, seedHash string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db,
			`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)
			 ON CONFLICT(demand_id) DO NOTHING`,
			strings.TrimSpace(demandID),
			strings.ToLower(strings.TrimSpace(seedHash)),
			time.Now().Unix(),
		)
		return err
	})
}

func dbInsertDirectDeal(ctx context.Context, store *clientDB, dealID string, req directDealAcceptReq, buyerPubHex string, sellerPubHex string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	// 第五步定性：proc_direct_deals 是【协议过程对象】
	// - 只保存协议协商上下文（buyer/seller/seed_hash/price 等）
	// - 不承载支付事实语义，不决定业务是否完成
	// - 业务完成状态统一看 order_settlements
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db,
			`INSERT INTO proc_direct_deals(deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?)`,
			strings.TrimSpace(dealID),
			strings.TrimSpace(req.DemandId),
			strings.ToLower(strings.TrimSpace(buyerPubHex)),
			strings.ToLower(strings.TrimSpace(sellerPubHex)),
			strings.ToLower(strings.TrimSpace(req.SeedHash)),
			req.SeedPrice,
			req.ChunkPrice,
			strings.TrimSpace(req.ArbiterPubkeyHex),
			"accepted",
			time.Now().Unix(),
		)
		return err
	})
}
