package clientapp

import (
	"context"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileTransferAdapter struct{}

func newDownloadFileTransferAdapter() *downloadFileTransferAdapter {
	return &downloadFileTransferAdapter{}
}

func (a *downloadFileTransferAdapter) RunChunkTransfer(ctx context.Context, req filedownload.ChunkTransferRequest) (filedownload.ChunkTransferResult, error) {
	return filedownload.ChunkTransferResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "transfer runner is not available")
}

var _ filedownload.TransferRunner = (*downloadFileTransferAdapter)(nil)
