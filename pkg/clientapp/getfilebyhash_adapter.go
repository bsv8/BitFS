package clientapp

import (
	"context"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/getfilebyhash"
)

// GetFileByHashModuleCaps getfilebyhash 模块能力集（第2步施工单）。
// 设计约束：
// - 从 httpAPIServer 中取已准备好的 store 能力；
// - 只做能力转接，不写业务流程；
// - 运行入口禁止使用 NewMemoryJobStoreForTest。
type GetFileByHashModuleCaps struct {
	store    *clientDB
	jobStore *getFileByHashJobStore
}

func newGetFileByHashModuleCaps(store *clientDB) *GetFileByHashModuleCaps {
	if store == nil {
		return nil
	}
	return &GetFileByHashModuleCaps{
		store:    store,
		jobStore: newGetFileByHashJobStore(store),
	}
}

func (c *GetFileByHashModuleCaps) JobStore() getfilebyhash.JobStore {
	if c == nil {
		return nil
	}
	return c.jobStore
}

func (c *GetFileByHashModuleCaps) HTTPHandler() *getfilebyhashHTTPHandler {
	if c == nil {
		return nil
	}
	return newGetFileByHashHTTPHandler(c)
}

func (c *GetFileByHashModuleCaps) StartDownload(ctx context.Context, req getfilebyhash.StartRequest) (getfilebyhash.StartResult, error) {
	if c.jobStore == nil {
		return getfilebyhash.StartResult{}, getfilebyhash.NewError(getfilebyhash.CodeModuleDisabled, "job store is not available")
	}
	result, err := getfilebyhash.StartDownload(ctx, c.jobStore, req)
	if err != nil {
		obs.Info("getfilebyhash", "start_download_failed", map[string]any{
			"seed_hash":  req.SeedHash,
			"error":     err.Error(),
		})
		return result, err
	}
	return result, nil
}

func (c *GetFileByHashModuleCaps) GetStatus(ctx context.Context, jobID string) (getfilebyhash.Status, error) {
	if c.jobStore == nil {
		return getfilebyhash.Status{}, getfilebyhash.NewError(getfilebyhash.CodeModuleDisabled, "job store is not available")
	}
	return getfilebyhash.GetStatus(ctx, c.jobStore, jobID)
}

func (c *GetFileByHashModuleCaps) ListChunks(ctx context.Context, jobID string) ([]getfilebyhash.ChunkReport, error) {
	if c.jobStore == nil {
		return nil, getfilebyhash.NewError(getfilebyhash.CodeModuleDisabled, "job store is not available")
	}
	return getfilebyhash.ListChunks(ctx, c.jobStore, jobID)
}

func (c *GetFileByHashModuleCaps) ListNodes(ctx context.Context, jobID string) ([]getfilebyhash.NodeReport, error) {
	if c.jobStore == nil {
		return nil, getfilebyhash.NewError(getfilebyhash.CodeModuleDisabled, "job store is not available")
	}
	return getfilebyhash.ListNodes(ctx, c.jobStore, jobID)
}

func (c *GetFileByHashModuleCaps) ListQuotes(ctx context.Context, jobID string) ([]getfilebyhash.QuoteReport, error) {
	if c.jobStore == nil {
		return nil, getfilebyhash.NewError(getfilebyhash.CodeModuleDisabled, "job store is not available")
	}
	return getfilebyhash.ListQuotes(ctx, c.jobStore, jobID)
}