package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

// downloadByHashParams 只保留旧入口参数翻译需要的字段。
// 这里不是新业务线，只是兼容旧调用点的薄壳。
type downloadByHashParams struct {
	SeedHash           string
	TransferChunkCount uint32
	GatewayPeerID      string
	QuoteMaxRetry      int
	QuoteInterval      time.Duration
	ArbiterPubHex      string
	MaxSeedPrice       uint64
	MaxChunkPrice      uint64
	Strategy           string
	PoolAmount         uint64
	MaxChunkRetries    int
}

// downloadByHashResult 只返回 job/status 视角结果。
type downloadByHashResult struct {
	JobID          string
	FrontOrderID   string
	DemandID       string
	OutputFilePath string
	SeedHash       string
	ChunkCount     uint32
	State          string
	Message        string
}

func runDownloadByHash(ctx context.Context, store *clientDB, rt *Runtime, p downloadByHashParams) (downloadByHashResult, error) {
	if rt == nil {
		return downloadByHashResult{}, fmt.Errorf("runtime not initialized")
	}
	if rt.kernel != nil {
		return rt.kernel.runDownloadByHashImpl(ctx, p)
	}
	return runDownloadByHashCore(ctx, store, rt, p)
}

func runDownloadByHashCore(ctx context.Context, store *clientDB, rt *Runtime, p downloadByHashParams) (downloadByHashResult, error) {
	if rt == nil {
		return downloadByHashResult{}, fmt.Errorf("runtime not initialized")
	}
	if store == nil {
		return downloadByHashResult{}, fmt.Errorf("client db is nil")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return downloadByHashResult{}, fmt.Errorf("seed_hash required")
	}
	caps := newDownloadFileCaps(rt, store, rt, rt.config)
	if caps == nil {
		return downloadByHashResult{}, fmt.Errorf("download caps are not available")
	}
	req := filedownload.StartRequest{
		SeedHash:             seedHash,
		ChunkCount:           p.TransferChunkCount,
		GatewayPubkey:        strings.TrimSpace(p.GatewayPeerID),
		QuoteMaxRetry:        p.QuoteMaxRetry,
		QuoteIntervalSeconds: int(p.QuoteInterval / time.Second),
		ArbiterPubkeyHex:     strings.TrimSpace(p.ArbiterPubHex),
		MaxSeedPrice:         p.MaxSeedPrice,
		MaxChunkPrice:        p.MaxChunkPrice,
		Strategy:             strings.TrimSpace(p.Strategy),
		PoolAmountSat:        p.PoolAmount,
		MaxChunkRetries:      p.MaxChunkRetries,
	}
	res, err := filedownload.StartByHash(ctx, caps.caps(), req)
	if err != nil {
		return downloadByHashResult{}, err
	}
	return downloadByHashResult{
		JobID:          res.JobID,
		FrontOrderID:   res.Status.FrontOrderID,
		DemandID:       res.Status.DemandID,
		OutputFilePath: res.Status.OutputFilePath,
		SeedHash:       res.Status.SeedHash,
		ChunkCount:     res.Status.ChunkCount,
		State:          res.Status.State,
		Message:        res.Message,
	}, nil
}
