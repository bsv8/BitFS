package clientapp

import (
	"context"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileSeedAdapter struct {
	store *clientDB
}

func newDownloadFileSeedAdapter(store *clientDB) *downloadFileSeedAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileSeedAdapter{store: store}
}

func (a *downloadFileSeedAdapter) SaveSeed(ctx context.Context, input filedownload.SaveSeedInput) error {
	return filedownload.NewError(filedownload.CodeModuleDisabled, "seed store is not available")
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
	return filedownload.SeedMeta{
		SeedHash:   seed.SeedHash,
		ChunkCount: seed.ChunkCount,
		FileSize:   seed.FileSize,
	}, true, nil
}

var _ filedownload.SeedStore = (*downloadFileSeedAdapter)(nil)
