package clientapp

import (
	"context"
	"strings"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

type downloadFileDemandAdapter struct {
	store ClientStore
	env   gatewayDemandPublishChainTxEnv
}

func newDownloadFileDemandAdapter(store ClientStore, env gatewayDemandPublishChainTxEnv) *downloadFileDemandAdapter {
	return &downloadFileDemandAdapter{store: store, env: env}
}

func (a *downloadFileDemandAdapter) PublishDemand(ctx context.Context, req filedownload.PublishDemandRequest) (filedownload.PublishDemandResult, error) {
	if a == nil || a.store == nil || a.env == nil {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeModuleDisabled, "demand publisher is not available")
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if seedHash == "" || req.ChunkCount == 0 {
		return filedownload.PublishDemandResult{}, filedownload.NewError(filedownload.CodeBadRequest, "invalid demand request")
	}
	resp, err := TriggerGatewayDemandPublishChainTxQuotePay(ctx, a.store, a.env, PublishDemandParams{
		SeedHash:      seedHash,
		ChunkCount:    req.ChunkCount,
		GatewayPeerID: req.GatewayID,
	})
	if err != nil {
		return filedownload.PublishDemandResult{}, err
	}
	return filedownload.PublishDemandResult{
		DemandID: resp.DemandID,
		Status:   resp.Status,
	}, nil
}

var _ filedownload.DemandPublisher = (*downloadFileDemandAdapter)(nil)
