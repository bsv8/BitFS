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
	cmdID := newActionID()
	startAt := time.Now()
	out, err := runDownloadByHashCore(ctx, store, rt, p)
	status := "applied"
	errCode := ""
	errMsg := ""
	if err != nil {
		status = "failed"
		errCode = "download_by_hash_failed"
		errMsg = err.Error()
	}
	_ = dbAppendCommandJournal(ctx, store, commandJournalEntry{
		CommandID:     cmdID,
		CommandType:   commandTypeDownloadByHash,
		GatewayPeerID: strings.TrimSpace(p.GatewayPeerID),
		AggregateID:   "seed:" + strings.ToLower(strings.TrimSpace(p.SeedHash)),
		RequestedBy:   "client_kernel",
		RequestedAt:   time.Now().Unix(),
		Accepted:      true,
		Status:        status,
		ErrorCode:     errCode,
		ErrorMessage:  errMsg,
		StateBefore:   "direct_download",
		StateAfter:    "direct_download",
		DurationMS:    time.Since(startAt).Milliseconds(),
		TriggerKey:    "",
		Payload: map[string]any{
			"seed_hash":            strings.ToLower(strings.TrimSpace(p.SeedHash)),
			"transfer_chunk_count": p.TransferChunkCount,
			"gateway_pubkey_hex":   strings.TrimSpace(p.GatewayPeerID),
			"strategy":             strings.TrimSpace(p.Strategy),
			"max_chunk_price":      p.MaxChunkPrice,
			"max_seed_price":       p.MaxSeedPrice,
		},
		Result: map[string]any{
			"status":         status,
			"job_id":         strings.TrimSpace(out.JobID),
			"front_order_id": strings.TrimSpace(out.FrontOrderID),
			"demand_id":      strings.TrimSpace(out.DemandID),
			"output_path":    strings.TrimSpace(out.OutputFilePath),
			"state":          strings.TrimSpace(out.State),
		},
	})
	return out, err
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
