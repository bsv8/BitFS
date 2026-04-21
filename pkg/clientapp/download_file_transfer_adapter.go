package clientapp

import (
	"context"
	"strings"

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

func (a *downloadFileTransferAdapter) RunTransferByStrategy(ctx context.Context, req filedownload.StrategyTransferRequest) (filedownload.StrategyTransferResult, error) {
	if a == nil || a.env == nil || a.store == nil {
		return filedownload.StrategyTransferResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "transfer runner is not available")
	}
	frontOrderID := strings.TrimSpace(req.FrontOrderID)
	demandID := strings.TrimSpace(req.DemandID)
	seedHash := normalizeSeedHashHex(req.SeedHash)
	if frontOrderID == "" || demandID == "" || seedHash == "" {
		return filedownload.StrategyTransferResult{}, filedownload.NewError(filedownload.CodeBadRequest, "front_order_id/demand_id/seed_hash are required")
	}
	res, err := TriggerTransferChunksByStrategy(ctx, a.store, a.env, TransferChunksByStrategyParams{
		JobID:           strings.TrimSpace(req.JobID),
		FrontOrderID:    frontOrderID,
		DemandID:        demandID,
		SeedHash:        seedHash,
		ChunkCount:      req.ChunkCount,
		ArbiterPubHex:   strings.TrimSpace(req.ArbiterPubHex),
		MaxSeedPrice:    req.MaxSeedPrice,
		MaxChunkPrice:   req.MaxChunkPrice,
		Strategy:        strings.TrimSpace(req.Strategy),
		PoolAmount:      req.PoolAmountSat,
		MaxChunkRetries: req.MaxChunkRetries,
		Candidates:      append([]filedownload.QuoteReport(nil), req.Candidates...),
	})
	if err != nil {
		return filedownload.StrategyTransferResult{}, err
	}
	return filedownload.StrategyTransferResult{
		ChunkCount: res.ChunkCount,
		Chunks:     append([]filedownload.TransferChunkResult(nil), res.Chunks...),
		Sellers:    convertTransferSellerStats(res.Sellers),
	}, nil
}

func convertTransferSellerStats(items []TransferSellerStatItem) []filedownload.TransferSellerStatItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]filedownload.TransferSellerStatItem, 0, len(items))
	for _, item := range items {
		out = append(out, filedownload.TransferSellerStatItem{
			SellerPubHex:        item.SellerPubHex,
			ChunkPrice:          item.ChunkPrice,
			SeedPrice:           item.SeedPrice,
			SuccessChunks:       item.SuccessChunks,
			FailedChunks:        item.FailedChunks,
			AvgBytesPerSecond:   item.AvgBytesPerSecond,
			EMASpeedBytesPerSec: item.EMASpeedBytesPerSec,
			Pruned:              item.Pruned,
			Broken:              item.Broken,
		})
	}
	return out
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

var _ filedownload.StrategyTransferRunner = (*downloadFileTransferAdapter)(nil)
