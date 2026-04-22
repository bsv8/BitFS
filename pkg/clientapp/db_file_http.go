package clientapp

import (
	"context"
	"fmt"
)

type fileDownloadStateRow struct {
	FilePath      string
	FileSize      uint64
	ChunkCount    uint32
	Completed     uint32
	PaidSats      uint64
	Status        string
	DemandID      string
	LastError     string
	StatusJSON    string
	UpdatedAtUnix int64
}

type fileDownloadChunkRow struct {
	ChunkIndex uint32
	Status     string
}

func fileHTTPStore(s *fileHTTPServer) *clientDB {
	if s == nil {
		return nil
	}
	return s.store
}

func dbGetFileDownloadState(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return store.GetFileDownloadState(ctx, seedHash, true)
}

func dbGetFileDownloadStateNoUpdated(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return store.GetFileDownloadState(ctx, seedHash, false)
}

func dbEnsureFileDownloadQueued(ctx context.Context, store *clientDB, seedHash string, partPath string, statusJSON string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.EnsureFileDownloadQueued(ctx, seedHash, partPath, statusJSON)
}

func dbListFileDownloadChunks(ctx context.Context, store *clientDB, seedHash string) ([]fileDownloadChunkRow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return store.ListFileDownloadChunks(ctx, seedHash)
}

func dbUpsertFileDownloadState(ctx context.Context, store *clientDB, seedHash string, row fileDownloadStateRow) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.UpsertFileDownloadState(ctx, seedHash, row)
}

func dbUpsertFileDownloadChunkDone(ctx context.Context, store *clientDB, seedHash string, idx uint32, sellerPeerID string, price uint64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.UpsertFileDownloadChunkDone(ctx, seedHash, idx, sellerPeerID, price)
}

func dbFindLatestWorkspaceFileBySeedHash(ctx context.Context, store *clientDB, seedHash string) (string, uint64, error) {
	if store == nil {
		return "", 0, fmt.Errorf("client db is nil")
	}
	return store.FindLatestWorkspaceFileBySeedHash(ctx, seedHash)
}
