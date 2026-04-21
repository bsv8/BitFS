package clientapp

import (
	"context"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileQuoteAdapter struct {
	store *clientDB
}

func newDownloadFileQuoteAdapter(store *clientDB) *downloadFileQuoteAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileQuoteAdapter{store: store}
}

func (a *downloadFileQuoteAdapter) ListQuotes(ctx context.Context, demandID string) ([]filedownload.QuoteReport, error) {
	if a == nil || a.store == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "quote reader is not available")
	}
	demandID = strings.TrimSpace(demandID)
	if demandID == "" {
		return nil, filedownload.NewError(filedownload.CodeBadRequest, "demand_id is required")
	}
	items, err := TriggerClientListDirectQuotes(ctx, a.store, demandID)
	if err != nil {
		return nil, err
	}
	out := make([]filedownload.QuoteReport, 0, len(items))
	now := time.Now().Unix()
	for _, item := range items {
		out = append(out, filedownload.QuoteReport{
			SellerPubkey:        item.SellerPubHex,
			SeedPriceSat:        item.SeedPrice,
			ChunkPriceSat:       item.ChunkPrice,
			ChunkCount:          item.ChunkCount,
			AvailableChunks:     append([]uint32(nil), item.AvailableChunkIndexes...),
			RecommendedFileName: item.RecommendedFileName,
			MimeType:            item.MimeType,
			FileSizeBytes:       item.FileSizeBytes,
			QuoteTimestamp:      now,
			ExpiresAtUnix:       item.ExpiresAtUnix,
		})
	}
	return out, nil
}

var _ filedownload.QuoteReader = (*downloadFileQuoteAdapter)(nil)
