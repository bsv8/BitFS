package clientapp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	downloadStepPublishDemand = "publish_demand"
	downloadStepListQuotes    = "list_direct_quotes"
	downloadStepTransfer      = "direct_transfer"
)

// directDownloadCoreParams 只描述“共同下载中段”，不包含 CLI/HTTP 各自前后业务。
type directDownloadCoreParams struct {
	SeedHash           string
	DemandChunkCount   uint32
	TransferChunkCount uint32
	GatewayPeerID      string
	QuoteMaxRetry      int
	QuoteInterval      time.Duration
	ArbiterPeerID      string
	MaxSeedPrice       uint64
	MaxChunkPrice      uint64
	Strategy           string
	PoolAmount         uint64
	MaxChunkRetries    int
}

type directDownloadCoreResult struct {
	GatewayPeerID string
	DemandID      string
	Quotes        []DirectQuoteItem
	FileName      string
	Transfer      TransferChunksByStrategyResult
}

// directDownloadCoreHooks 允许不同业务流程接收步骤事件，但不耦合具体管理层实现。
type directDownloadCoreHooks struct {
	OnStepStart func(name string, detail map[string]string)
	OnStepDone  func(name string, detail map[string]string)
	OnStepFail  func(name string, err error, detail map[string]string)
}

func runDirectDownloadCore(ctx context.Context, rt *Runtime, p directDownloadCoreParams, hooks directDownloadCoreHooks) (directDownloadCoreResult, error) {
	if rt == nil {
		return directDownloadCoreResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := ensureClientKernel(rt)
	if kernel == nil {
		return directDownloadCoreResult{}, fmt.Errorf("client kernel not initialized")
	}
	return kernel.runDirectDownloadCore(ctx, p, hooks)
}

func runDirectDownloadCoreLegacy(ctx context.Context, rt *Runtime, p directDownloadCoreParams, hooks directDownloadCoreHooks) (directDownloadCoreResult, error) {
	if rt == nil || rt.Host == nil {
		return directDownloadCoreResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return directDownloadCoreResult{}, fmt.Errorf("seed_hash required")
	}
	demandChunkCount := p.DemandChunkCount
	if demandChunkCount == 0 {
		demandChunkCount = 1
	}

	gw := strings.TrimSpace(p.GatewayPeerID)
	startStep(hooks, downloadStepPublishDemand, map[string]string{
		"seed_hash":   seedHash,
		"chunk_count": strconv.FormatUint(uint64(demandChunkCount), 10),
	})
	pub, err := TriggerGatewayPublishDemand(ctx, rt, PublishDemandParams{
		SeedHash:      seedHash,
		ChunkCount:    demandChunkCount,
		GatewayPeerID: gw,
	})
	if err != nil {
		failStep(hooks, downloadStepPublishDemand, err, nil)
		return directDownloadCoreResult{}, fmt.Errorf("publish demand failed: %w", err)
	}
	doneStep(hooks, downloadStepPublishDemand, map[string]string{
		"demand_id": pub.DemandID,
		"status":    pub.Status,
	})

	startStep(hooks, downloadStepListQuotes, map[string]string{"demand_id": pub.DemandID})
	quotes, err := waitDirectQuotes(ctx, rt, pub.DemandID, p.QuoteMaxRetry, p.QuoteInterval, p.MaxSeedPrice, p.MaxChunkPrice)
	if err != nil {
		failStep(hooks, downloadStepListQuotes, err, nil)
		return directDownloadCoreResult{}, fmt.Errorf("list direct quotes failed: %w", err)
	}
	if len(quotes) == 0 {
		err = fmt.Errorf("no direct quotes for demand_id=%s", pub.DemandID)
		failStep(hooks, downloadStepListQuotes, err, map[string]string{"error": "no_quotes"})
		return directDownloadCoreResult{}, err
	}
	fileName := pickRecommendedFileName(quotes, seedHash)
	doneStep(hooks, downloadStepListQuotes, map[string]string{
		"seller_count":            strconv.Itoa(len(quotes)),
		"selected_file_name":      fileName,
		"first_seller_pubkey_hex": quotes[0].SellerPeerID,
		"first_recommended_name":  quotes[0].RecommendedFileName,
	})

	arbiter := strings.TrimSpace(p.ArbiterPeerID)
	if arbiter == "" && len(rt.HealthyArbiters) > 0 {
		arbiter = rt.HealthyArbiters[0].ID.String()
	}
	startStep(hooks, downloadStepTransfer, map[string]string{
		"chunk_count": strconv.FormatUint(uint64(p.TransferChunkCount), 10),
		"strategy":    strategyNameOrDefault(p.Strategy),
	})
	transfer, err := TriggerTransferChunksByStrategy(ctx, rt, TransferChunksByStrategyParams{
		DemandID:        pub.DemandID,
		SeedHash:        seedHash,
		ChunkCount:      p.TransferChunkCount,
		ArbiterPeerID:   arbiter,
		MaxSeedPrice:    p.MaxSeedPrice,
		MaxChunkPrice:   p.MaxChunkPrice,
		Strategy:        p.Strategy,
		PoolAmount:      p.PoolAmount,
		MaxChunkRetries: p.MaxChunkRetries,
	})
	if err != nil {
		failStep(hooks, downloadStepTransfer, err, nil)
		return directDownloadCoreResult{}, fmt.Errorf("strategy transfer failed: %w", err)
	}
	doneStep(hooks, downloadStepTransfer, map[string]string{
		"sha256":       transfer.SHA256,
		"bytes":        strconv.Itoa(len(transfer.Data)),
		"chunk_count":  strconv.FormatUint(uint64(transfer.ChunkCount), 10),
		"seller_count": strconv.Itoa(len(transfer.Sellers)),
	})

	return directDownloadCoreResult{
		GatewayPeerID: gw,
		DemandID:      pub.DemandID,
		Quotes:        quotes,
		FileName:      fileName,
		Transfer:      transfer,
	}, nil
}

func waitDirectQuotes(ctx context.Context, rt *Runtime, demandID string, maxRetry int, interval time.Duration, maxSeedPrice uint64, maxChunkPrice uint64) ([]DirectQuoteItem, error) {
	if maxRetry <= 0 {
		maxRetry = 12
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for i := 0; i < maxRetry; i++ {
		quotes, err := TriggerClientListDirectQuotes(ctx, rt, demandID)
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

func startStep(h directDownloadCoreHooks, name string, detail map[string]string) {
	if h.OnStepStart != nil {
		h.OnStepStart(name, cloneStepDetail(detail))
	}
}

func doneStep(h directDownloadCoreHooks, name string, detail map[string]string) {
	if h.OnStepDone != nil {
		h.OnStepDone(name, cloneStepDetail(detail))
	}
}

func failStep(h directDownloadCoreHooks, name string, err error, detail map[string]string) {
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
