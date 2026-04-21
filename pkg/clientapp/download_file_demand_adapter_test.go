package clientapp

import (
	"context"
	"testing"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func TestDownloadFileDemandAdapterWithoutEnvReturnsDisabled(t *testing.T) {
	t.Parallel()

	adapter := newDownloadFileDemandAdapter(nil, nil)
	_, err := adapter.PublishDemand(context.Background(), filedownload.PublishDemandRequest{
		SeedHash:   "ab",
		ChunkCount: 1,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if code := filedownload.CodeOf(err); code != filedownload.CodeModuleDisabled {
		t.Fatalf("expected MODULE_DISABLED, got %s", code)
	}
}
