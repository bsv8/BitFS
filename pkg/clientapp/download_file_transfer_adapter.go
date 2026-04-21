package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileTransferAdapter struct {
	env   transferRuntimeCaps
	store *clientDB
}

func newDownloadFileTransferAdapter(env transferRuntimeCaps, store *clientDB) *downloadFileTransferAdapter {
	if env == nil || store == nil {
		return nil
	}
	return &downloadFileTransferAdapter{
		env:   env,
		store: store,
	}
}

func (a *downloadFileTransferAdapter) RunChunkTransfer(ctx context.Context, req filedownload.ChunkTransferRequest) (filedownload.ChunkTransferResult, error) {
	var result filedownload.ChunkTransferResult
	if a == nil || a.env == nil || a.store == nil {
		return result, filedownload.NewError(filedownload.CodeModuleDisabled, "transfer runner is not available")
	}
	if ctx == nil {
		return result, filedownload.NewError(filedownload.CodeBadRequest, "ctx is required")
	}
	if err := ctx.Err(); err != nil {
		return result, filedownload.NewError(filedownload.CodeRequestCanceled, err.Error())
	}
	demandID := strings.TrimSpace(req.DemandID)
	seedHash := normalizeSeedHashHex(req.SeedHash)
	chunkHash := strings.ToLower(strings.TrimSpace(req.ChunkHash))
	sellerPubHex := strings.ToLower(strings.TrimSpace(req.SellerPubkey))
	if demandID == "" || seedHash == "" || chunkHash == "" || sellerPubHex == "" {
		return result, filedownload.NewError(filedownload.CodeBadRequest, "demand_id/seed_hash/chunk_hash/seller_pubkey are required")
	}

	quotes, err := TriggerClientListDirectQuotes(ctx, a.store, demandID)
	if err != nil {
		return result, err
	}
	var selectedQuote DirectQuoteItem
	foundQuote := false
	for _, quote := range quotes {
		if !strings.EqualFold(strings.TrimSpace(quote.SellerPubHex), sellerPubHex) {
			continue
		}
		if len(quote.AvailableChunkIndexes) > 0 {
			allowed := false
			for _, idx := range quote.AvailableChunkIndexes {
				if idx == req.ChunkIndex {
					allowed = true
					break
				}
			}
			if !allowed {
				continue
			}
		}
		selectedQuote = quote
		foundQuote = true
		break
	}
	if !foundQuote {
		return result, filedownload.NewError(filedownload.CodeTransferFailed, "quote not found for seller")
	}
	if selectedQuote.ChunkPrice == 0 {
		return result, filedownload.NewError(filedownload.CodeTransferFailed, "chunk price is required")
	}

	shortChunkHash := chunkHash
	if len(shortChunkHash) > 8 {
		shortChunkHash = shortChunkHash[:8]
	}
	worker := &transferSellerWorker{
		buyer:           a.env,
		store:           a.store,
		frontOrderID:    fmt.Sprintf("fo_download_chunk_%d_%s", time.Now().UnixNano(), shortChunkHash),
		quote:           selectedQuote,
		arbiterPubHex:   strings.TrimSpace(firstNonEmpty(selectedQuote.SellerArbiterPubHexes...)),
		seedHash:        seedHash,
		poolAmount:      chunkTransferPoolAmount(selectedQuote.ChunkPrice),
		availableChunks: map[uint32]struct{}{req.ChunkIndex: {}},
	}
	if strings.TrimSpace(worker.arbiterPubHex) == "" {
		return result, filedownload.NewError(filedownload.CodeModuleDisabled, "arbiter pubkey is not available")
	}

	maxAttempts := req.MaxRetries + 1
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		chunk, elapsed, fetchErr := worker.fetchChunk(ctx, req.ChunkIndex, chunkHash)
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		_ = worker.closeSession(closeCtx)
		cancel()
		if fetchErr != nil {
			lastErr = fetchErr
			if attempt < maxAttempts {
				continue
			}
			return result, filedownload.NewError(filedownload.CodeTransferFailed, fetchErr.Error())
		}
		if len(chunk) == 0 {
			return result, filedownload.NewError(filedownload.CodeTransferFailed, "chunk bytes are empty")
		}
		speedBps := uint64(0)
		if elapsed > 0 {
			speedBps = uint64(float64(len(chunk)) / elapsed.Seconds())
		}
		return filedownload.ChunkTransferResult{
			Data:     append([]byte(nil), chunk...),
			PaidSat:  selectedQuote.ChunkPrice,
			SpeedBps: speedBps,
		}, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("transfer failed")
	}
	return result, filedownload.NewError(filedownload.CodeTransferFailed, lastErr.Error())
}

func chunkTransferPoolAmount(chunkPrice uint64) uint64 {
	if chunkPrice == 0 {
		return 0
	}
	const minimumCushion = uint64(2000)
	return chunkPrice + minimumCushion
}

func firstNonEmpty(items ...string) string {
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			return item
		}
	}
	return ""
}

var _ filedownload.TransferRunner = (*downloadFileTransferAdapter)(nil)
