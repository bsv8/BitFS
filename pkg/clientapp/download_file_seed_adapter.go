package clientapp

import (
	"context"
	"sort"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type downloadFileSeedAdapter struct {
	store     *clientDB
	rt        *Runtime
	cfgSource configSnapshotter
	seedStore moduleapi.SeedStorage
}

func newDownloadFileSeedAdapter(store *clientDB, rt *Runtime, cfgSource configSnapshotter, seedStore moduleapi.SeedStorage) *downloadFileSeedAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileSeedAdapter{
		store:     store,
		rt:        rt,
		cfgSource: cfgSource,
		seedStore: seedStore,
	}
}

func (a *downloadFileSeedAdapter) SaveSeed(ctx context.Context, input filedownload.SaveSeedInput) error {
	if a == nil || a.store == nil {
		return filedownload.NewError(filedownload.CodeModuleDisabled, "seed store is not available")
	}
	seedBytes := append([]byte(nil), input.SeedBytes...)
	if len(seedBytes) == 0 {
		return filedownload.NewError(filedownload.CodeBadRequest, "seed bytes are required")
	}
	meta, err := parseSeedV1(seedBytes)
	if err != nil {
		return err
	}
	if input.SeedHash != "" && !strings.EqualFold(strings.TrimSpace(input.SeedHash), meta.SeedHashHex) {
		return filedownload.NewError(filedownload.CodeBadRequest, "seed hash mismatch")
	}
	if input.ChunkCount > 0 && input.ChunkCount != meta.ChunkCount {
		return filedownload.NewError(filedownload.CodeBadRequest, "seed chunk count mismatch")
	}
	if input.FileSize > 0 && input.FileSize != meta.FileSize {
		return filedownload.NewError(filedownload.CodeBadRequest, "seed file size mismatch")
	}
	if a.seedStore == nil {
		return filedownload.NewError(filedownload.CodeModuleDisabled, "seed storage is not available")
	}
	_, err = a.seedStore.SaveSeed(ctx, moduleapi.SeedSaveInput{
		SeedHash:    meta.SeedHashHex,
		SeedBytes:   seedBytes,
		ChunkHashes: append([]string(nil), meta.ChunkHashes...),
		ChunkCount:  meta.ChunkCount,
		FileSize:    meta.FileSize,
	})
	return err
}

func (a *downloadFileSeedAdapter) ResolveAndSaveSeedMeta(ctx context.Context, req filedownload.SeedResolveRequest) (filedownload.SeedMeta, error) {
	if a == nil || a.store == nil {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeModuleDisabled, "seed store is not available")
	}
	seedHash := normalizeSeedHashHex(req.SeedHash)
	if seedHash == "" {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	meta, found, err := a.LoadSeedMeta(ctx, seedHash)
	if err != nil {
		return filedownload.SeedMeta{}, err
	}
	if found {
		return meta, nil
	}
	if a.rt == nil {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeModuleDisabled, "seed resolver is not available")
	}
	if strings.TrimSpace(req.DemandID) == "" {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeBadRequest, "demand_id is required")
	}
	frontOrderID := strings.TrimSpace(req.FrontOrderID)
	if frontOrderID == "" {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeBadRequest, "front_order_id is required")
	}
	if len(req.Candidates) == 0 {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeQuoteUnavailable, "seed meta is not available")
	}

	quotes := make([]DirectQuoteItem, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		if strings.TrimSpace(candidate.SellerPubkey) == "" {
			continue
		}
		quotes = append(quotes, DirectQuoteItem{
			DemandID:              req.DemandID,
			SellerPubHex:          candidate.SellerPubkey,
			SeedPrice:             candidate.SeedPriceSat,
			ChunkPrice:            candidate.ChunkPriceSat,
			ChunkCount:            candidate.ChunkCount,
			FileSizeBytes:         candidate.FileSizeBytes,
			RecommendedFileName:   candidate.RecommendedFileName,
			MimeType:              candidate.MimeType,
			SellerArbiterPubHexes: append([]string(nil), candidate.SellerArbiterPubHexes...),
			AvailableChunkIndexes: append([]uint32(nil), candidate.AvailableChunks...),
		})
	}
	if len(quotes) == 0 {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeQuoteUnavailable, "seed meta is not available")
	}
	sort.Slice(quotes, func(i, j int) bool {
		if quotes[i].ChunkPrice == quotes[j].ChunkPrice {
			return quotes[i].SeedPrice < quotes[j].SeedPrice
		}
		return quotes[i].ChunkPrice < quotes[j].ChunkPrice
	})

	workers, seedV1, seedBytes, err := prepareSpeedPriceWorkersAndSeed(speedPriceBootstrapParams{
		Ctx:          ctx,
		Buyer:        a.rt,
		Store:        a.store,
		FrontOrderID: frontOrderID,
		Quotes:       quotes,
		SeedHash:     seedHash,
	})
	if err != nil {
		return filedownload.SeedMeta{}, err
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	defer closeTransferWorkers(closeCtx, workers)
	if len(seedBytes) == 0 {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeModuleDisabled, "seed bytes are empty")
	}
	if err := a.SaveSeed(ctx, filedownload.SaveSeedInput{
		SeedHash:   seedHash,
		SeedBytes:  seedBytes,
		ChunkCount: seedV1.ChunkCount,
		FileSize:   seedV1.FileSize,
	}); err != nil {
		return filedownload.SeedMeta{}, err
	}
	meta, found, err = a.LoadSeedMeta(ctx, seedHash)
	if err != nil {
		return filedownload.SeedMeta{}, err
	}
	if !found {
		return filedownload.SeedMeta{}, filedownload.NewError(filedownload.CodeModuleDisabled, "seed meta is not available")
	}
	return meta, nil
}

func (a *downloadFileSeedAdapter) LoadSeedMeta(ctx context.Context, seedHash string) (filedownload.SeedMeta, bool, error) {
	if a == nil || a.store == nil {
		return filedownload.SeedMeta{}, false, filedownload.NewError(filedownload.CodeModuleDisabled, "seed store is not available")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return filedownload.SeedMeta{}, false, filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	seed, found, err := dbLoadSellerSeedSnapshot(ctx, a.store, seedHash)
	if err != nil {
		return filedownload.SeedMeta{}, false, err
	}
	if !found {
		return filedownload.SeedMeta{}, false, nil
	}
	if seed.ChunkCount == 0 || len(seed.ChunkHashes) != int(seed.ChunkCount) {
		return filedownload.SeedMeta{}, false, filedownload.NewError(filedownload.CodeModuleDisabled, "seed meta is not available")
	}
	return filedownload.SeedMeta{
		SeedHash:    seed.SeedHash,
		ChunkHashes: append([]string(nil), seed.ChunkHashes...),
		ChunkCount:  seed.ChunkCount,
		FileSize:    seed.FileSize,
	}, true, nil
}

var _ filedownload.SeedStore = (*downloadFileSeedAdapter)(nil)
var _ filedownload.SeedResolver = (*downloadFileSeedAdapter)(nil)
