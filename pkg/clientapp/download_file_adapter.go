package clientapp

import (
	"context"

	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

// DownloadFileCaps 静态下载编排能力集。
// 设计约束：
// - 从 httpAPIServer 中取已准备好的 store 能力；
// - 只做能力转接，不写业务流程；
// - 运行入口禁止使用 NewMemoryJobStoreForTest。
type DownloadFileCaps struct {
	transferEnv     transferRuntimeCaps
	cfgSource       configSnapshotter
	store           *clientDB
	jobStore        filedownload.JobStore
	frontBinder     filedownload.FrontOrderBinder
	seedStore       filedownload.SeedStore
	seedResolver    filedownload.SeedResolver
	fileStore       filedownload.FileStore
	demandPublisher filedownload.DemandPublisher
	quoteReader     filedownload.QuoteReader
	quoteWaiter     filedownload.QuoteWaiter
	policy          filedownload.DownloadPolicy
	transferRunner  filedownload.StrategyTransferRunner
}

func newDownloadFileCaps(rt *Runtime, store *clientDB, cfgSource configSnapshotter) *DownloadFileCaps {
	if store == nil {
		return nil
	}
	return &DownloadFileCaps{
		transferEnv:     rt,
		cfgSource:       cfgSource,
		store:           store,
		jobStore:        newDownloadFileJobStoreAdapter(store),
		frontBinder:     newDownloadFileOrderAdapter(store, firstNonEmpty(runtimeClientID(rt))),
		seedStore:       newDownloadFileSeedAdapter(store, rt, cfgSource, rt.SeedStorage),
		seedResolver:    newDownloadFileSeedAdapter(store, rt, cfgSource, rt.SeedStorage),
		fileStore:       newDownloadFileWorkspaceAdapter(store),
		demandPublisher: newDownloadFileDemandAdapter(store, rt),
		quoteReader:     newDownloadFileQuoteAdapter(store),
		quoteWaiter:     newDownloadFileQuoteAdapter(store),
		policy:          newDownloadFilePolicyAdapter(),
		transferRunner:  newDownloadFileTransferAdapter(rt, store),
	}
}

func runtimeClientID(rt *Runtime) string {
	if rt == nil {
		return ""
	}
	return rt.ClientID()
}

func (c *DownloadFileCaps) JobStore() filedownload.JobStore {
	if c == nil {
		return nil
	}
	return c.jobStore
}

func (c *DownloadFileCaps) HTTPHandler() *downloadFileHTTPHandler {
	if c == nil {
		return nil
	}
	return newDownloadFileHTTPHandler(c)
}

func (c *DownloadFileCaps) caps() filedownload.DownloadCaps {
	if c == nil {
		return filedownload.DownloadCaps{}
	}
	return filedownload.DownloadCaps{
		Jobs:         c.jobStore,
		FrontOrders:  c.frontBinder,
		Seeds:        c.seedStore,
		SeedResolver: c.seedResolver,
		Files:        c.fileStore,
		Demands:      c.demandPublisher,
		Quotes:       c.quoteReader,
		QuoteWaiter:  c.quoteWaiter,
		Policy:       c.policy,
		Transfers:    c.transferRunner,
	}
}

func (c *DownloadFileCaps) StartByHash(ctx context.Context, req filedownload.StartRequest) (filedownload.StartResult, error) {
	if c == nil {
		return filedownload.StartResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "download caps are not available")
	}
	if c.jobStore == nil {
		return filedownload.StartResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "job store is not available")
	}
	result, err := filedownload.StartByHash(ctx, c.caps(), req)
	if err != nil {
		obs.Info("getfilebyhash", "start_download_failed", map[string]any{
			"seed_hash": req.SeedHash,
			"error":     err.Error(),
		})
		return result, err
	}
	return result, nil
}

func (c *DownloadFileCaps) GetStatus(ctx context.Context, jobID string) (filedownload.Status, error) {
	if c == nil {
		return filedownload.Status{}, filedownload.NewError(filedownload.CodeModuleDisabled, "download caps are not available")
	}
	if c.jobStore == nil {
		return filedownload.Status{}, filedownload.NewError(filedownload.CodeModuleDisabled, "job store is not available")
	}
	return filedownload.GetStatus(ctx, c.jobStore, jobID)
}

func (c *DownloadFileCaps) ListChunks(ctx context.Context, jobID string) ([]filedownload.ChunkReport, error) {
	if c == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "download caps are not available")
	}
	if c.jobStore == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "job store is not available")
	}
	return filedownload.ListChunks(ctx, c.jobStore, jobID)
}

func (c *DownloadFileCaps) ListNodes(ctx context.Context, jobID string) ([]filedownload.NodeReport, error) {
	if c == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "download caps are not available")
	}
	if c.jobStore == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "job store is not available")
	}
	return filedownload.ListNodes(ctx, c.jobStore, jobID)
}

func (c *DownloadFileCaps) ListQuotes(ctx context.Context, jobID string) ([]filedownload.QuoteReport, error) {
	if c == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "download caps are not available")
	}
	if c.jobStore == nil {
		return nil, filedownload.NewError(filedownload.CodeModuleDisabled, "job store is not available")
	}
	return filedownload.ListQuotes(ctx, c.jobStore, jobID)
}
