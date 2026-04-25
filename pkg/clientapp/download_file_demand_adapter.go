package clientapp

import (
	"context"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
)

// downloadFileDemandAdapter 通过 gatewayclient 模块实现 demand 发布。
type downloadFileDemandAdapter struct {
	store *clientDB
	rt    *Runtime
}

func newDownloadFileDemandAdapter(store *clientDB, rt *Runtime) *downloadFileDemandAdapter {
	return &downloadFileDemandAdapter{store: store, rt: rt}
}

func (a *downloadFileDemandAdapter) PublishDemand(ctx context.Context, req filedownload.PublishDemandRequest) (filedownload.PublishDemandResult, error) {
	if a == nil || a.rt == nil {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "demand publisher is not available")
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if seedHash == "" || req.ChunkCount == 0 {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeBadRequest, "invalid demand request")
	}

	// 获取 gatewayclient 模块 store
	gwStore := a.rt.GetModuleStore(gatewayclient.ModuleIdentity)
	if gwStore == nil {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "gatewayclient module not available")
	}

	store, ok := gwStore.(gatewayclient.Store)
	if !ok || store == nil {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "gatewayclient store not available")
	}

	// 使用 moduleHost 作为 bizHost 创建 gatewayclient service
	host := newModuleHost(a.rt, a.store)
	svc := gatewayclient.NewService(host, store)
	result, err := svc.PublishDemand(ctx, gatewayclient.DemandPublishParams{
		SeedHash:      seedHash,
		ChunkCount:    req.ChunkCount,
		GatewayPeerID: strings.TrimSpace(req.GatewayID),
	})
	if err != nil {
		return filedownload.PublishDemandResult{}, err
	}

	return filedownload.PublishDemandResult{
		DemandID: result.DemandID,
		Status:   result.Status,
	}, nil
}

var _ filedownload.DemandPublisher = (*downloadFileDemandAdapter)(nil)
