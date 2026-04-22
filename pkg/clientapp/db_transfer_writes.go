package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizdemandquotearbiters"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizdemandquotes"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizdemands"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procdirectdeals"
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		existing, err := tx.BizDemandQuotes.Query().
			Where(bizdemandquotes.DemandIDEQ(demandID), bizdemandquotes.SellerPubHexEQ(sellerPubHex)).
			Only(ctx)
		if err != nil {
			if !gen.IsNotFound(err) {
				return err
			}
			existing, err = tx.BizDemandQuotes.Create().
				SetDemandID(demandID).
				SetSellerPubHex(sellerPubHex).
				SetSeedPriceSatoshi(int64(req.SeedPrice)).
				SetChunkPriceSatoshi(int64(req.ChunkPrice)).
				SetChunkCount(int64(req.ChunkCount)).
				SetFileSizeBytes(int64(req.FileSize)).
				SetRecommendedFileName(recommendedName).
				SetMimeType(mimeType).
				SetAvailableChunkBitmapHex(availableChunkBitmapHex).
				SetExpiresAtUnix(req.ExpiresAtUnix).
				SetCreatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return err
			}
		} else {
			if _, err := existing.Update().
				SetSeedPriceSatoshi(int64(req.SeedPrice)).
				SetChunkPriceSatoshi(int64(req.ChunkPrice)).
				SetChunkCount(int64(req.ChunkCount)).
				SetFileSizeBytes(int64(req.FileSize)).
				SetRecommendedFileName(recommendedName).
				SetMimeType(mimeType).
				SetAvailableChunkBitmapHex(availableChunkBitmapHex).
				SetExpiresAtUnix(req.ExpiresAtUnix).
				SetCreatedAtUnix(now).
				Save(ctx); err != nil {
				return err
			}
		}
		quoteID := int64(existing.ID)
		if _, err := tx.BizDemandQuoteArbiters.Delete().Where(bizdemandquotearbiters.QuoteIDEQ(quoteID)).Exec(ctx); err != nil {
			return err
		}
		for _, arbiterPubHex := range arbiterPubHexes {
			if _, err := tx.BizDemandQuoteArbiters.Create().
				SetQuoteID(quoteID).
				SetArbiterPubHex(arbiterPubHex).
				Save(ctx); err != nil {
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
	db, ok := store.(*clientDB)
	if !ok {
		return fmt.Errorf("client store type unsupported")
	}
	return db.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		demandID = strings.TrimSpace(demandID)
		seedHash = strings.ToLower(strings.TrimSpace(seedHash))
		if demandID == "" || seedHash == "" {
			return fmt.Errorf("demand_id and seed_hash are required")
		}
		if _, err := tx.BizDemands.Query().Where(bizdemands.DemandIDEQ(demandID)).Only(ctx); err == nil {
			return nil
		} else if !gen.IsNotFound(err) {
			return err
		}
		_, err := tx.BizDemands.Create().
			SetDemandID(demandID).
			SetSeedHash(seedHash).
			SetCreatedAtUnix(time.Now().Unix()).
			Save(ctx)
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		dealID = strings.TrimSpace(dealID)
		if dealID == "" {
			return fmt.Errorf("deal_id is required")
		}
		existing, err := tx.ProcDirectDeals.Query().Where(procdirectdeals.DealIDEQ(dealID)).Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetDemandID(strings.TrimSpace(req.DemandId)).
				SetBuyerPubkeyHex(strings.ToLower(strings.TrimSpace(buyerPubHex))).
				SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(sellerPubHex))).
				SetSeedHash(strings.ToLower(strings.TrimSpace(req.SeedHash))).
				SetSeedPrice(int64(req.SeedPrice)).
				SetChunkPrice(int64(req.ChunkPrice)).
				SetArbiterPubkeyHex(strings.TrimSpace(req.ArbiterPubkeyHex)).
				SetStatus("accepted").
				SetCreatedAtUnix(time.Now().Unix()).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcDirectDeals.Create().
			SetDealID(dealID).
			SetDemandID(strings.TrimSpace(req.DemandId)).
			SetBuyerPubkeyHex(strings.ToLower(strings.TrimSpace(buyerPubHex))).
			SetSellerPubkeyHex(strings.ToLower(strings.TrimSpace(sellerPubHex))).
			SetSeedHash(strings.ToLower(strings.TrimSpace(req.SeedHash))).
			SetSeedPrice(int64(req.SeedPrice)).
			SetChunkPrice(int64(req.ChunkPrice)).
			SetArbiterPubkeyHex(strings.TrimSpace(req.ArbiterPubkeyHex)).
			SetStatus("accepted").
			SetCreatedAtUnix(time.Now().Unix()).
			Save(ctx)
		return err
	})
}
