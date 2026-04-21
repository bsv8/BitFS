package clientapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileSeedAdapter struct {
	store     *clientDB
	rt        *Runtime
	cfgSource configSnapshotter
}

func newDownloadFileSeedAdapter(store *clientDB, rt *Runtime, cfgSource configSnapshotter) *downloadFileSeedAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileSeedAdapter{
		store:     store,
		rt:        rt,
		cfgSource: cfgSource,
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
	dataDir, err := a.seedDataDir()
	if err != nil {
		return err
	}
	seedHash := strings.ToLower(strings.TrimSpace(meta.SeedHashHex))
	seedPath := filepath.Join(dataDir, "biz_seeds", seedHash+".bse")
	if err := os.MkdirAll(filepath.Dir(seedPath), 0o755); err != nil {
		return err
	}
	if err := writeIfChanged(seedPath, seedBytes); err != nil {
		return err
	}
	_, err = a.store.ExecContext(ctx,
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint)
		 VALUES(?,?,?,?,?,?)
		 ON CONFLICT(seed_hash) DO UPDATE SET
		   chunk_count=excluded.chunk_count,
		   file_size=excluded.file_size,
		   seed_file_path=excluded.seed_file_path`,
		seedHash,
		int64(meta.ChunkCount),
		int64(meta.FileSize),
		seedPath,
		"",
		"",
	)
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

func (a *downloadFileSeedAdapter) seedDataDir() (string, error) {
	if a.cfgSource == nil {
		return "", fmt.Errorf("seed data dir is required")
	}
	dataDir := strings.TrimSpace(a.cfgSource.ConfigSnapshot().Storage.DataDir)
	if dataDir == "" {
		return "", fmt.Errorf("seed data dir is required")
	}
	return filepath.Clean(dataDir), nil
}

var _ filedownload.SeedStore = (*downloadFileSeedAdapter)(nil)
var _ filedownload.SeedResolver = (*downloadFileSeedAdapter)(nil)
