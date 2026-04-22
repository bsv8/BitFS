package clientapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
)

type downloadFileWorkspaceAdapter struct {
	store *clientDB
}

func newDownloadFileWorkspaceAdapter(store *clientDB) *downloadFileWorkspaceAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileWorkspaceAdapter{store: store}
}

func (a *downloadFileWorkspaceAdapter) FindCompleteFile(ctx context.Context, seedHash string) (filedownload.LocalFile, bool, error) {
	if a == nil || a.store == nil {
		return filedownload.LocalFile{}, false, filedownload.NewError(filedownload.CodeModuleDisabled, "file store is not available")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return filedownload.LocalFile{}, false, filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	path, size, err := dbFindLatestWorkspaceFileBySeedHash(ctx, a.store, seedHash)
	if err != nil {
		if os.IsNotExist(err) || gen.IsNotFound(err) {
			return filedownload.LocalFile{}, false, nil
		}
		return filedownload.LocalFile{}, false, err
	}
	if strings.TrimSpace(path) == "" {
		return filedownload.LocalFile{}, false, nil
	}
	if st, statErr := os.Stat(path); statErr != nil {
		if os.IsNotExist(statErr) {
			return filedownload.LocalFile{}, false, nil
		}
		return filedownload.LocalFile{}, false, statErr
	} else if st.Size() > 0 {
		size = uint64(st.Size())
	}
	return filedownload.LocalFile{
		FilePath: path,
		FileSize: size,
		SeedHash: seedHash,
	}, true, nil
}

func (a *downloadFileWorkspaceAdapter) PreparePartFile(ctx context.Context, input filedownload.PreparePartFileInput) (filedownload.PartFile, error) {
	if a == nil || a.store == nil {
		return filedownload.PartFile{}, filedownload.NewError(filedownload.CodeModuleDisabled, "file store is not available")
	}
	seedHash := normalizeSeedHashHex(input.SeedHash)
	if seedHash == "" {
		return filedownload.PartFile{}, filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	partPath, err := downloadFileWorkspacePartPath(ctx, a.store, seedHash)
	if err != nil {
		return filedownload.PartFile{}, err
	}
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		return filedownload.PartFile{}, err
	}
	return filedownload.PartFile{
		PartFilePath: partPath,
		SeedHash:     seedHash,
	}, nil
}

func (a *downloadFileWorkspaceAdapter) MarkChunkStored(ctx context.Context, input filedownload.StoredChunkInput) error {
	if a == nil || a.store == nil {
		return filedownload.NewError(filedownload.CodeModuleDisabled, "file store is not available")
	}
	seedHash := normalizeSeedHashHex(input.SeedHash)
	if seedHash == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	return downloadFileWorkspaceMarkChunkStored(ctx, a.store, seedHash, input.ChunkIndex, input.ChunkBytes)
}

func (a *downloadFileWorkspaceAdapter) CompleteFile(ctx context.Context, input filedownload.CompleteFileInput) (filedownload.LocalFile, error) {
	if a == nil || a.store == nil {
		return filedownload.LocalFile{}, filedownload.NewError(filedownload.CodeModuleDisabled, "file store is not available")
	}
	seedHash := normalizeSeedHashHex(input.SeedHash)
	if seedHash == "" {
		return filedownload.LocalFile{}, filedownload.NewError(filedownload.CodeBadRequest, "seed_hash is required")
	}
	partPath := strings.TrimSpace(input.PartFilePath)
	if partPath == "" {
		return filedownload.LocalFile{}, filedownload.NewError(filedownload.CodeBadRequest, "part_file_path is required")
	}
	return downloadFileWorkspaceComplete(ctx, a.store, filedownload.CompleteFileInput{
		SeedHash:              seedHash,
		PartFilePath:          partPath,
		RecommendedFileName:   input.RecommendedFileName,
		MimeType:              input.MimeType,
		AvailableChunkIndexes: input.AvailableChunkIndexes,
	})
}

var _ filedownload.FileStore = (*downloadFileWorkspaceAdapter)(nil)
