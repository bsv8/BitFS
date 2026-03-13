package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// OneShotDownloadParams 描述“输入 seed hash 后一键直连下载”的触发参数。
type OneShotDownloadParams struct {
	SeedHash string `json:"seed_hash"`

	QuoteMaxRetry int           `json:"quote_max_retry,omitempty"`
	QuoteInterval time.Duration `json:"quote_interval,omitempty"`

	ArbiterPeerID string `json:"arbiter_pubkey_hex,omitempty"`
}

// OneShotDownloadResult 输出一次性下载的关键链路信息与下载结果。
type OneShotDownloadResult struct {
	DemandID   string                         `json:"demand_id"`
	Quote      DirectQuoteItem                `json:"quote"`
	Transfer   TransferChunksByStrategyResult `json:"transfer"`
	Seed       []byte                         `json:"seed"`
	SeedHash   string                         `json:"seed_hash"`
	ChunkCount uint32                         `json:"chunk_count"`
	FileName   string                         `json:"file_name"`
}

// TriggerOneShotDirectDownload 触发“发布需求 -> 等待直连报价 -> 全量传输 -> 拉取 seed”的一次性下载流程。
func TriggerOneShotDirectDownload(ctx context.Context, rt *Runtime, p OneShotDownloadParams) (OneShotDownloadResult, error) {
	if rt == nil {
		return OneShotDownloadResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return OneShotDownloadResult{}, fmt.Errorf("seed_hash required")
	}
	download, err := runDirectDownloadCore(ctx, rt, directDownloadCoreParams{
		SeedHash:           seedHash,
		DemandChunkCount:   1, // CLI 直连模式只需要触发需求，实际下载块数由策略层和 seed 元信息决定。
		TransferChunkCount: 0,
		QuoteMaxRetry:      p.QuoteMaxRetry,
		QuoteInterval:      p.QuoteInterval,
		ArbiterPeerID:      p.ArbiterPeerID,
		Strategy:           TransferStrategySmart,
	}, directDownloadCoreHooks{})
	if err != nil {
		return OneShotDownloadResult{}, err
	}
	if len(download.Transfer.Seed) == 0 {
		return OneShotDownloadResult{}, fmt.Errorf("seed bytes missing in strategy transfer result")
	}
	meta, err := parseSeedV1(download.Transfer.Seed)
	if err != nil {
		return OneShotDownloadResult{}, err
	}
	if !strings.EqualFold(meta.SeedHashHex, seedHash) {
		return OneShotDownloadResult{}, fmt.Errorf("seed hash mismatch: expect=%s got=%s", seedHash, meta.SeedHashHex)
	}
	var q DirectQuoteItem
	if len(download.Quotes) > 0 {
		q = download.Quotes[0]
	}

	return OneShotDownloadResult{
		DemandID:   download.DemandID,
		Quote:      q,
		Transfer:   download.Transfer,
		Seed:       download.Transfer.Seed,
		SeedHash:   seedHash,
		ChunkCount: meta.ChunkCount,
		FileName:   download.FileName,
	}, nil
}
