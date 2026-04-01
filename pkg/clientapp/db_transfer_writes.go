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
	demandID := strings.TrimSpace(req.DemandID)
	sellerPubHex = strings.ToLower(strings.TrimSpace(sellerPubHex))
	if demandID == "" || sellerPubHex == "" {
		return fmt.Errorf("demand_id and seller_pub_hex are required")
	}
	now := time.Now().Unix()
	availableChunkBitmapHex := normalizeChunkBitmapBytes(req.AvailableChunkBitmap)
	recommendedName := sanitizeRecommendedFileName(req.RecommendedFileName)
	mimeType := sanitizeMIMEHint(req.MIMEHint)
	arbiterPubHexes, err := normalizePubHexList(req.ArbiterPeerIDs)
	if err != nil {
		return err
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO demand_quotes(
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
		if err := tx.QueryRow(`SELECT id FROM demand_quotes WHERE demand_id=? AND seller_pub_hex=?`, demandID, sellerPubHex).Scan(&quoteID); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM demand_quote_arbiters WHERE quote_id=?`, quoteID); err != nil {
			return err
		}
		for _, arbiterPubHex := range arbiterPubHexes {
			if _, err := tx.Exec(`INSERT INTO demand_quote_arbiters(quote_id,arbiter_pub_hex) VALUES(?,?)
				ON CONFLICT(quote_id,arbiter_pub_hex) DO NOTHING`, quoteID, arbiterPubHex); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbRecordDemand(ctx context.Context, store *clientDB, demandID string, seedHash string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO direct_deals(deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?)`,
			strings.TrimSpace(dealID),
			strings.TrimSpace(req.DemandID),
			strings.ToLower(strings.TrimSpace(buyerPubHex)),
			strings.ToLower(strings.TrimSpace(sellerPubHex)),
			strings.ToLower(strings.TrimSpace(req.SeedHash)),
			req.SeedPrice,
			req.ChunkPrice,
			strings.TrimSpace(req.ArbiterPeerID),
			"accepted",
			time.Now().Unix(),
		)
		return err
	})
}

func dbOpenDirectSession(ctx context.Context, store *clientDB, dealID string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	var sessionID string
	err := store.Tx(ctx, func(tx *sql.Tx) error {
		var chunkPrice uint64
		if err := tx.QueryRow(`SELECT chunk_price FROM direct_deals WHERE deal_id=?`, strings.TrimSpace(dealID)).Scan(&chunkPrice); err != nil {
			return err
		}
		sessionID = "dsess_" + randHex(8)
		now := time.Now().Unix()
		_, err := tx.Exec(`INSERT INTO direct_sessions(session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			sessionID, strings.TrimSpace(dealID), chunkPrice, 0, 0, 0, 0, "active", now, now)
		return err
	})
	if err != nil {
		return "", err
	}
	return sessionID, nil
}

func dbFinalizeDirectSession(ctx context.Context, store *clientDB, sessionID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`UPDATE direct_sessions SET status='finalized',updated_at_unix=? WHERE session_id=?`, time.Now().Unix(), strings.TrimSpace(sessionID))
		return err
	})
}
