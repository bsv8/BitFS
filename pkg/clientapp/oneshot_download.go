package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

// OneShotDownloadParams 描述“输入 seed hash 后一键直连下载”的触发参数。
type OneShotDownloadParams struct {
	SeedHash string `json:"seed_hash"`
	// GatewayPeerID 对应业务统一网关 ID（gateway_pubkey_hex）。
	// 规则：直连下载发布 demand 时必须显式带上，避免多网关场景误路由。
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`

	QuoteMaxRetry int           `json:"quote_max_retry,omitempty"`
	QuoteInterval time.Duration `json:"quote_interval,omitempty"`

	ArbiterPubHex string `json:"arbiter_pubkey_hex,omitempty"`
}

// OneShotDownloadResult 输出一次性下载的关键链路信息。
type OneShotDownloadResult struct {
	JobID          string `json:"job_id"`
	FrontOrderID   string `json:"front_order_id,omitempty"`
	DemandID       string `json:"demand_id,omitempty"`
	OutputFilePath string `json:"output_file_path,omitempty"`
	SeedHash       string `json:"seed_hash"`
	ChunkCount     uint32 `json:"chunk_count"`
	State          string `json:"state,omitempty"`
	Message        string `json:"message,omitempty"`
}

// TriggerOneShotDirectDownload 触发“发布需求 -> 等待报价 -> 主流程下载”的一次性下载流程。
// 这里显式收 store，避免入口层偷偷回头取 rt.store。
func TriggerOneShotDirectDownload(ctx context.Context, store *clientDB, rt *Runtime, p OneShotDownloadParams) (OneShotDownloadResult, error) {
	if rt == nil {
		return OneShotDownloadResult{}, fmt.Errorf("runtime not initialized")
	}
	if store == nil {
		return OneShotDownloadResult{}, fmt.Errorf("client db is nil")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return OneShotDownloadResult{}, fmt.Errorf("seed_hash required")
	}
	caps := newDownloadFileCaps(rt, store, rt)
	if caps == nil {
		return OneShotDownloadResult{}, fmt.Errorf("download caps not available")
	}
	req := filedownload.StartRequest{
		SeedHash:             seedHash,
		GatewayPubkey:        strings.TrimSpace(p.GatewayPeerID),
		QuoteMaxRetry:        p.QuoteMaxRetry,
		QuoteIntervalSeconds: int(p.QuoteInterval / time.Second),
		ArbiterPubkeyHex:     strings.TrimSpace(p.ArbiterPubHex),
		Strategy:             TransferStrategySmart,
	}
	download, err := filedownload.StartByHash(ctx, caps.caps(), req)
	if err != nil {
		return OneShotDownloadResult{}, err
	}
	return OneShotDownloadResult{
		JobID:          download.JobID,
		FrontOrderID:   download.Status.FrontOrderID,
		DemandID:       download.Status.DemandID,
		OutputFilePath: download.Status.OutputFilePath,
		SeedHash:       seedHash,
		ChunkCount:     download.Status.ChunkCount,
		State:          download.Status.State,
		Message:        download.Message,
	}, nil
}
