package clientapp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type speedPriceBootstrapParams struct {
	Ctx             context.Context
	Buyer           transferRuntimeCaps
	Store           *clientDB
	FrontOrderID    string // 本次下载发起唯一，显式传递
	Quotes          []DirectQuoteItem
	SeedHash        string
	ArbiterPubHex   string
	PoolAmount      uint64
	OnQuoteRejected func(q DirectQuoteItem, err error)
	OnQuoteAccepted func(q DirectQuoteItem, arbiterPubHex string)
	OnSeedProbeFail func(w *transferSellerWorker, reason string, err error)
	OnSeedProbeOK   func(w *transferSellerWorker, meta seedV1Meta)
}

// prepareSpeedPriceWorkersAndSeed 负责“卖家 worker 构建 + 可用 seed 元信息探测”。
// 成功返回后，workers 中至少有一个已建立会话并可继续下载分块。
// 额外返回 seed 原文，供上层流程决定是否落地 .bitfs 文件。
func prepareSpeedPriceWorkersAndSeed(p speedPriceBootstrapParams) ([]*transferSellerWorker, seedV1Meta, []byte, error) {
	if p.Ctx == nil {
		return nil, seedV1Meta{}, nil, fmt.Errorf("context is required")
	}
	if p.Buyer == nil {
		return nil, seedV1Meta{}, nil, fmt.Errorf("buyer runtime is required")
	}
	buyerRuntime, ok := p.Buyer.(*Runtime)
	if !ok || buyerRuntime == nil {
		return nil, seedV1Meta{}, nil, fmt.Errorf("buyer runtime is required")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" {
		return nil, seedV1Meta{}, nil, fmt.Errorf("seed hash is required")
	}
	if len(p.Quotes) == 0 {
		return nil, seedV1Meta{}, nil, fmt.Errorf("quotes are required")
	}

	quotes := append([]DirectQuoteItem(nil), p.Quotes...)
	sort.Slice(quotes, func(i, j int) bool {
		if quotes[i].ChunkPrice == quotes[j].ChunkPrice {
			return quotes[i].SeedPrice < quotes[j].SeedPrice
		}
		return quotes[i].ChunkPrice < quotes[j].ChunkPrice
	})

	workers := make([]*transferSellerWorker, 0, len(quotes))
	for _, q := range quotes {
		arbiterPubHex, err := resolveDealArbiter(buyerRuntime, q.SellerArbiterPubHexes, p.ArbiterPubHex)
		if err != nil {
			if p.OnQuoteRejected != nil {
				p.OnQuoteRejected(q, err)
			}
			continue
		}
		if p.OnQuoteAccepted != nil {
			p.OnQuoteAccepted(q, arbiterPubHex)
		}
		workers = append(workers, &transferSellerWorker{
			buyer:           p.Buyer,
			store:           p.Store,
			frontOrderID:    p.FrontOrderID,
			quote:           q,
			arbiterPubHex:   arbiterPubHex,
			seedHash:        seedHash,
			poolAmount:      p.PoolAmount,
			availableChunks: chunkIndexSet(q.AvailableChunkIndexes),
			assignCh:        make(chan uint32),
		})
	}
	if len(workers) == 0 {
		return nil, seedV1Meta{}, nil, fmt.Errorf("no quote with available arbiter")
	}

	for _, w := range workers {
		if err := w.ensureSession(p.Ctx); err != nil {
			w.broken = true
			w.brokenReason = "open_session_failed"
			if p.OnSeedProbeFail != nil {
				p.OnSeedProbeFail(w, "open_session_failed", err)
			}
			continue
		}
		seedRes, err := TriggerClientSeedGet(p.Ctx, buyerRuntime, SeedGetParams{
			SellerPeerID: w.quote.SellerPubHex,
			SessionID:    w.sessionID,
			SeedHash:     seedHash,
		})
		if err != nil {
			w.broken = true
			w.brokenReason = "seed_get_failed"
			if p.OnSeedProbeFail != nil {
				p.OnSeedProbeFail(w, "seed_get_failed", err)
			}
			continue
		}
		meta, err := parseSeedV1(seedRes.Seed)
		if err != nil {
			w.broken = true
			w.brokenReason = "seed_parse_failed"
			if p.OnSeedProbeFail != nil {
				p.OnSeedProbeFail(w, "seed_parse_failed", err)
			}
			continue
		}
		if !strings.EqualFold(meta.SeedHashHex, seedHash) {
			w.broken = true
			w.brokenReason = "seed_hash_mismatch"
			if p.OnSeedProbeFail != nil {
				p.OnSeedProbeFail(w, "seed_hash_mismatch", fmt.Errorf("seed hash mismatch"))
			}
			continue
		}
		if len(w.quote.AvailableChunkIndexes) > 0 {
			w.availableChunks = chunkIndexSet(normalizeChunkIndexes(w.quote.AvailableChunkIndexes, meta.ChunkCount))
		}
		if p.OnSeedProbeOK != nil {
			p.OnSeedProbeOK(w, meta)
		}
		w.recordPurchaseDone(p.Ctx, 0, p.SeedHash, w.quote.SeedPrice)
		return workers, meta, append([]byte(nil), seedRes.Seed...), nil
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(p.Ctx), 2*time.Minute)
	defer cancel()
	_ = closeTransferWorkers(closeCtx, workers)
	return nil, seedV1Meta{}, nil, fmt.Errorf("seed metadata load failed from all sellers")
}
