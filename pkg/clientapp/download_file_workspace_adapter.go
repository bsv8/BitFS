package clientapp

import (
	"context"
	"os"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
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
	return filedownload.PartFile{}, filedownload.NewError(filedownload.CodeModuleDisabled, "part file store is not available")
}

func (a *downloadFileWorkspaceAdapter) MarkChunkStored(ctx context.Context, input filedownload.StoredChunkInput) error {
	return filedownload.NewError(filedownload.CodeModuleDisabled, "chunk storage is not available")
}

func (a *downloadFileWorkspaceAdapter) CompleteFile(ctx context.Context, input filedownload.CompleteFileInput) (filedownload.LocalFile, error) {
	return filedownload.LocalFile{}, filedownload.NewError(filedownload.CodeModuleDisabled, "complete file store is not available")
}

var _ filedownload.FileStore = (*downloadFileWorkspaceAdapter)(nil)
