package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
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
	now := time.Now().Unix()
	arbIDsJSON, err := json.Marshal(normalizePeerIDList(req.ArbiterPeerIDs))
	if err != nil {
		return err
	}
	availableChunkBitmapHex := normalizeChunkBitmapBytes(req.AvailableChunkBitmap)
	recommendedName := sanitizeRecommendedFileName(req.RecommendedFileName)
	mimeHint := sanitizeMIMEHint(req.MIMEHint)
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO direct_quotes(demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(demand_id,seller_pubkey_hex) DO UPDATE SET
			 seed_price=excluded.seed_price,
			 chunk_price=excluded.chunk_price,
			 chunk_count=excluded.chunk_count,
			 file_size=excluded.file_size,
			 expires_at_unix=excluded.expires_at_unix,
			 recommended_file_name=excluded.recommended_file_name,
			 mime_hint=excluded.mime_hint,
			 available_chunk_bitmap_hex=excluded.available_chunk_bitmap_hex,
			 seller_arbiter_pubkey_hexes_json=excluded.seller_arbiter_pubkey_hexes_json,
			 created_at_unix=excluded.created_at_unix`,
			strings.TrimSpace(req.DemandID),
			strings.ToLower(strings.TrimSpace(sellerPubHex)),
			req.SeedPrice,
			req.ChunkPrice,
			req.ChunkCount,
			req.FileSize,
			req.ExpiresAtUnix,
			recommendedName,
			mimeHint,
			availableChunkBitmapHex,
			string(arbIDsJSON),
			now,
		)
		return err
	})
}

func dbRecordDemandDedup(ctx context.Context, store *clientDB, demandID string, seedHash string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO demand_dedup(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)
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
