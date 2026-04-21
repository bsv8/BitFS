package clientapp

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

const (
	downloadStepPublishDemand = "publish_demand"
	downloadStepListQuotes    = "list_biz_demand_quotes"
	downloadStepTransfer      = "direct_transfer"
)

// downloadByHashParams 只描述“共同下载中段”，不包含 CLI/HTTP 各自前后业务。
type downloadByHashParams struct {
	SeedHash           string
	DemandChunkCount   uint32
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

type downloadByHashResult struct {
	GatewayPeerID string
	DemandID      string
	Quotes        []DirectQuoteItem
	FileName      string
	Transfer      TransferChunksByStrategyResult
}

// downloadByHashHooks 允许不同业务流程接收步骤事件，但不耦合具体管理层实现。
type downloadByHashHooks struct {
	OnStepStart func(name string, detail map[string]string)
	OnStepDone  func(name string, detail map[string]string)
	OnStepFail  func(name string, err error, detail map[string]string)
}

func runDownloadByHash(ctx context.Context, rt *Runtime, p downloadByHashParams, hooks downloadByHashHooks) (downloadByHashResult, error) {
	if rt == nil {
		return downloadByHashResult{}, fmt.Errorf("runtime not initialized")
	}
	if rt.kernel == nil {
		return downloadByHashResult{}, fmt.Errorf("client kernel not initialized")
	}
	return rt.kernel.runDownloadByHashImpl(ctx, p, hooks)
}

func runDownloadByHashImpl(ctx context.Context, store *clientDB, rt *Runtime, p downloadByHashParams, hooks downloadByHashHooks) (downloadByHashResult, error) {
	if rt == nil {
		return downloadByHashResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return downloadByHashResult{}, fmt.Errorf("seed_hash required")
	}
	if store == nil {
		return downloadByHashResult{}, fmt.Errorf("client db is nil")
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
	fileName := filepath.Base(strings.TrimSpace(res.Status.OutputFilePath))
	if fileName == "" {
		fileName = fmt.Sprintf("%s.bin", seedHash)
	}
	return downloadByHashResult{
		GatewayPeerID: strings.TrimSpace(p.GatewayPeerID),
		DemandID:      strings.TrimSpace(res.Status.DemandID),
		FileName:      fileName,
		Transfer: TransferChunksByStrategyResult{
			ChunkCount: res.Status.ChunkCount,
		},
	}, nil
}

func waitDirectQuotes(ctx context.Context, store *clientDB, rt *Runtime, demandID string, maxRetry int, interval time.Duration, maxSeedPrice uint64, maxChunkPrice uint64) ([]DirectQuoteItem, error) {
	if maxRetry <= 0 {
		maxRetry = 12
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for i := 0; i < maxRetry; i++ {
		quotes, err := TriggerClientListDirectQuotes(ctx, store, demandID)
		if err != nil {
			return nil, err
		}
		filtered := make([]DirectQuoteItem, 0, len(quotes))
		for _, q := range quotes {
			if maxSeedPrice > 0 && q.SeedPrice > maxSeedPrice {
				continue
			}
			if maxChunkPrice > 0 && q.ChunkPrice > maxChunkPrice {
				continue
			}
			filtered = append(filtered, q)
		}
		if len(filtered) > 0 {
			return filtered, nil
		}
		if i+1 >= maxRetry {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
	return nil, nil
}

func pickRecommendedFileName(quotes []DirectQuoteItem, seedHash string) string {
	defaultName := fmt.Sprintf("%s.bin", strings.ToLower(strings.TrimSpace(seedHash)))
	if len(quotes) == 0 {
		return defaultName
	}
	type fileNameStat struct {
		count    int
		firstPos int
	}
	stats := map[string]fileNameStat{}
	bestName := ""
	bestCount := -1
	bestPos := 1 << 30
	for i, q := range quotes {
		name := sanitizeRecommendedFileName(q.RecommendedFileName)
		if name == "" {
			continue
		}
		st, ok := stats[name]
		if !ok {
			st = fileNameStat{count: 0, firstPos: i}
		}
		st.count++
		stats[name] = st
		if st.count > bestCount || (st.count == bestCount && st.firstPos < bestPos) {
			bestName = name
			bestCount = st.count
			bestPos = st.firstPos
		}
	}
	if bestName != "" {
		return bestName
	}
	return defaultName
}

func pickRecommendedMIMEHint(quotes []DirectQuoteItem) string {
	if len(quotes) == 0 {
		return ""
	}
	type mimeStat struct {
		count    int
		firstPos int
	}
	stats := map[string]mimeStat{}
	bestMime := ""
	bestCount := -1
	bestPos := 1 << 30
	for i, q := range quotes {
		mimeHint := sanitizeMIMEHint(q.MimeType)
		if mimeHint == "" {
			continue
		}
		st, ok := stats[mimeHint]
		if !ok {
			st = mimeStat{count: 0, firstPos: i}
		}
		st.count++
		stats[mimeHint] = st
		if st.count > bestCount || (st.count == bestCount && st.firstPos < bestPos) {
			bestMime = mimeHint
			bestCount = st.count
			bestPos = st.firstPos
		}
	}
	return bestMime
}

func startStep(h downloadByHashHooks, name string, detail map[string]string) {
	if h.OnStepStart != nil {
		h.OnStepStart(name, cloneStepDetail(detail))
	}
}

func doneStep(h downloadByHashHooks, name string, detail map[string]string) {
	if h.OnStepDone != nil {
		h.OnStepDone(name, cloneStepDetail(detail))
	}
}

func failStep(h downloadByHashHooks, name string, err error, detail map[string]string) {
	if h.OnStepFail == nil {
		return
	}
	d := cloneStepDetail(detail)
	if d == nil {
		d = map[string]string{}
	}
	if err != nil && strings.TrimSpace(d["error"]) == "" {
		d["error"] = err.Error()
	}
	h.OnStepFail(name, err, d)
}

func cloneStepDetail(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
