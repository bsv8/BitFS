package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

const (
	TransferStrategySmart      = "smart"
	TransferStrategyMaxPrice   = "max_price_all"
	defaultChunkRetryMax       = 6
	defaultSaturationEpsilon   = 0.03
	defaultSaturationWindowDur = 2 * time.Second
)

// TransferChunksByStrategyParams 描述多卖家分块下载输入。
// 说明：只放业务参数，不依赖 HTTP 管理接口。
type TransferChunksByStrategyParams struct {
	DemandID        string `json:"demand_id"`
	SeedHash        string `json:"seed_hash"`
	ChunkCount      uint32 `json:"chunk_count"`
	ArbiterPeerID   string `json:"arbiter_peer_id,omitempty"`
	MaxSeedPrice    uint64 `json:"max_seed_price,omitempty"`
	MaxChunkPrice   uint64 `json:"max_chunk_price,omitempty"`
	Strategy        string `json:"strategy,omitempty"`
	PoolAmount      uint64 `json:"pool_amount,omitempty"`
	MaxChunkRetries int    `json:"max_chunk_retries,omitempty"`
}

type TransferChunksByStrategyResult struct {
	Data       []byte                   `json:"data"`
	SHA256     string                   `json:"sha256"`
	ChunkCount uint32                   `json:"chunk_count"`
	Sellers    []TransferSellerStatItem `json:"sellers"`
}

type TransferSellerStatItem struct {
	SellerPeerID        string  `json:"seller_peer_id"`
	ChunkPrice          uint64  `json:"chunk_price"`
	SeedPrice           uint64  `json:"seed_price"`
	SuccessChunks       uint32  `json:"success_chunks"`
	FailedChunks        uint32  `json:"failed_chunks"`
	AvgBytesPerSecond   float64 `json:"avg_bytes_per_second"`
	EMASpeedBytesPerSec float64 `json:"ema_speed_bytes_per_second"`
	Pruned              bool    `json:"pruned"`
	Broken              bool    `json:"broken"`
}

type transferChunkResult struct {
	sellerIndex int
	chunkIndex  uint32
	chunk       []byte
	elapsed     time.Duration
	err         error
}

type transferSellerWorker struct {
	buyer         *Runtime
	quote         DirectQuoteItem
	arbiterPeerID string
	seedHash      string
	poolAmount    uint64

	dealID    string
	sessionID string
	opened    bool

	successCount uint32
	failedCount  uint32
	totalBytes   uint64
	totalNanos   int64
	emaBPS       float64
	pruned       bool
	broken       bool

	consecutiveFailures int
	assignCh            chan uint32
}

func shortID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if len(s) <= 12 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}

func logTransferStrategy(event string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	obs.Business("bitcast-client", event, fields)
}

func (w *transferSellerWorker) updateSuccess(bytes int, elapsed time.Duration) {
	w.successCount++
	w.totalBytes += uint64(bytes)
	w.totalNanos += elapsed.Nanoseconds()
	if elapsed <= 0 {
		return
	}
	inst := float64(bytes) / elapsed.Seconds()
	if w.emaBPS == 0 {
		w.emaBPS = inst
		return
	}
	const alpha = 0.35
	w.emaBPS = alpha*inst + (1-alpha)*w.emaBPS
}

func (w *transferSellerWorker) updateFailure() {
	w.failedCount++
	w.consecutiveFailures++
	if w.consecutiveFailures >= 3 {
		w.broken = true
	}
}

func (w *transferSellerWorker) resetFailureStreak() {
	w.consecutiveFailures = 0
}

func (w *transferSellerWorker) score() float64 {
	if w.broken {
		return 0
	}
	price := float64(w.quote.ChunkPrice)
	if price <= 0 {
		price = 1
	}
	speed := w.emaBPS
	if speed <= 0 {
		speed = 1
	}
	reliability := float64(w.successCount+1) / float64(w.successCount+w.failedCount+2)
	return (speed / price) * reliability
}

func (w *transferSellerWorker) ensureSession(ctx context.Context) error {
	if w.opened {
		return nil
	}
	logTransferStrategy("evt_transfer_strategy_open_session_begin", map[string]any{
		"seller_peer_id": shortID(w.quote.SellerPeerID),
		"demand_id":      strings.TrimSpace(w.quote.DemandID),
		"seed_hash":      shortID(w.seedHash),
		"chunk_price":    w.quote.ChunkPrice,
		"seed_price":     w.quote.SeedPrice,
	})
	openRes, err := triggerDirectTransferPoolOpen(ctx, w.buyer, directTransferPoolOpenParams{
		SellerPeerID:  w.quote.SellerPeerID,
		ArbiterPeerID: w.arbiterPeerID,
		DemandID:      w.quote.DemandID,
		SeedHash:      w.seedHash,
		SeedPrice:     w.quote.SeedPrice,
		ChunkPrice:    w.quote.ChunkPrice,
		ExpiresAtUnix: w.quote.ExpiresAtUnix,
		PoolAmount:    w.poolAmount,
	})
	if err != nil {
		logTransferStrategy("evt_transfer_strategy_open_session_failed", map[string]any{
			"seller_peer_id": shortID(w.quote.SellerPeerID),
			"demand_id":      strings.TrimSpace(w.quote.DemandID),
			"error":          err.Error(),
		})
		return err
	}
	w.dealID = openRes.DealID
	w.sessionID = openRes.SessionID
	w.opened = true
	logTransferStrategy("evt_transfer_strategy_open_session_ok", map[string]any{
		"seller_peer_id": shortID(w.quote.SellerPeerID),
		"deal_id":        strings.TrimSpace(w.dealID),
		"session_id":     strings.TrimSpace(w.sessionID),
	})
	return nil
}

func (w *transferSellerWorker) fetchChunk(ctx context.Context, chunkIndex uint32, chunkHash string) ([]byte, time.Duration, error) {
	if err := w.ensureSession(ctx); err != nil {
		return nil, 0, err
	}
	begin := time.Now()
	payRes, err := triggerDirectTransferPoolPay(ctx, w.buyer, directTransferPoolPayParams{
		SellerPeerID: w.quote.SellerPeerID,
		SessionID:    w.sessionID,
		Amount:       w.quote.ChunkPrice,
		SeedHash:     w.seedHash,
		ChunkHash:    chunkHash,
		ChunkIndex:   chunkIndex,
	})
	if err != nil {
		return nil, 0, err
	}
	return payRes.Chunk, time.Since(begin), nil
}

func (w *transferSellerWorker) closeSession(ctx context.Context) error {
	if !w.opened || strings.TrimSpace(w.sessionID) == "" {
		return nil
	}
	_, err := triggerDirectTransferPoolClose(ctx, w.buyer, directTransferPoolCloseParams{
		SellerPeerID: w.quote.SellerPeerID,
		SessionID:    w.sessionID,
	})
	_, _ = TriggerClientCloseDirectSession(ctx, w.buyer, CloseDirectSessionParams{
		SellerPeerID: w.quote.SellerPeerID,
		SessionID:    w.sessionID,
	})
	return err
}

type transferDispatchStrategy interface {
	// 设计说明：
	// - SelectReady 只负责“在当前可派发卖家中选一个”，不直接做网络动作；
	// - OnChunkDone 只消费观测数据，用于在线更新策略状态（例如饱和判定与裁剪）。
	SelectReady(ready []int, workers []*transferSellerWorker) (int, bool)
	OnChunkDone(now time.Time, completedBytes uint64, workers []*transferSellerWorker)
}

type maxPriceAllStrategy struct {
	rrCursor int
}

func (m *maxPriceAllStrategy) SelectReady(ready []int, workers []*transferSellerWorker) (int, bool) {
	if len(ready) == 0 {
		return 0, false
	}
	candidates := make([]int, 0, len(ready))
	for _, idx := range ready {
		if !workers[idx].broken {
			candidates = append(candidates, idx)
		}
	}
	if len(candidates) == 0 {
		return 0, false
	}
	sort.Ints(candidates)
	pick := candidates[m.rrCursor%len(candidates)]
	m.rrCursor++
	return pick, true
}

func (m *maxPriceAllStrategy) OnChunkDone(_ time.Time, _ uint64, _ []*transferSellerWorker) {}

type smartDispatchStrategy struct {
	epsilon            float64
	windowDur          time.Duration
	lastWindowAt       time.Time
	lastWindowBytes    uint64
	lastWindowBPS      float64
	slowWindowStreak   int
	alreadySaturated   bool
	lastPrunedSellerID string
}

func newSmartDispatchStrategy() *smartDispatchStrategy {
	return &smartDispatchStrategy{
		epsilon:      defaultSaturationEpsilon,
		windowDur:    defaultSaturationWindowDur,
		lastWindowAt: time.Now(),
	}
}

func (s *smartDispatchStrategy) SelectReady(ready []int, workers []*transferSellerWorker) (int, bool) {
	if len(ready) == 0 {
		return 0, false
	}
	best := -1
	bestScore := -1.0
	for _, idx := range ready {
		w := workers[idx]
		if w.broken || w.pruned {
			continue
		}
		score := w.score()
		if score > bestScore {
			best = idx
			bestScore = score
		}
	}
	if best >= 0 {
		return best, true
	}
	// 兜底：若全部被裁剪或故障，至少保留一个非故障卖家继续传。
	for _, idx := range ready {
		if !workers[idx].broken {
			return idx, true
		}
	}
	return 0, false
}

func (s *smartDispatchStrategy) OnChunkDone(now time.Time, completedBytes uint64, workers []*transferSellerWorker) {
	if now.Sub(s.lastWindowAt) < s.windowDur {
		return
	}
	deltaBytes := completedBytes - s.lastWindowBytes
	deltaSeconds := now.Sub(s.lastWindowAt).Seconds()
	if deltaSeconds <= 0 {
		return
	}
	curBPS := float64(deltaBytes) / deltaSeconds
	if s.lastWindowBPS > 0 {
		growth := (curBPS - s.lastWindowBPS) / s.lastWindowBPS
		logTransferStrategy("evt_transfer_strategy_smart_window", map[string]any{
			"window_seconds":     now.Sub(s.lastWindowAt).Seconds(),
			"delta_bytes":        deltaBytes,
			"cur_bps":            curBPS,
			"prev_bps":           s.lastWindowBPS,
			"growth_ratio":       growth,
			"saturation_epsilon": s.epsilon,
		})
		if growth < s.epsilon {
			s.slowWindowStreak++
		} else {
			s.slowWindowStreak = 0
		}
	}
	if s.slowWindowStreak >= 2 {
		s.alreadySaturated = true
		s.pruneWorstSeller(workers)
		s.slowWindowStreak = 0
	}
	s.lastWindowBPS = curBPS
	s.lastWindowAt = now
	s.lastWindowBytes = completedBytes
}

func (s *smartDispatchStrategy) pruneWorstSeller(workers []*transferSellerWorker) {
	active := 0
	for _, w := range workers {
		if !w.broken && !w.pruned {
			active++
		}
	}
	if active <= 1 {
		return
	}
	worstIdx := -1
	worstScore := 1e100
	for i := range workers {
		w := workers[i]
		if w.broken || w.pruned {
			continue
		}
		score := w.score()
		if score < worstScore {
			worstScore = score
			worstIdx = i
		}
	}
	if worstIdx < 0 {
		return
	}
	workers[worstIdx].pruned = true
	s.lastPrunedSellerID = workers[worstIdx].quote.SellerPeerID
	logTransferStrategy("evt_transfer_strategy_smart_pruned", map[string]any{
		"seller_peer_id": shortID(workers[worstIdx].quote.SellerPeerID),
		"score":          worstScore,
		"chunk_price":    workers[worstIdx].quote.ChunkPrice,
		"ema_bps":        workers[worstIdx].emaBPS,
		"failed_chunks":  workers[worstIdx].failedCount,
		"success_chunks": workers[worstIdx].successCount,
	})
}

func buildTransferStrategy(name string) transferDispatchStrategy {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "", TransferStrategySmart:
		return newSmartDispatchStrategy()
	case TransferStrategyMaxPrice:
		return &maxPriceAllStrategy{}
	default:
		return newSmartDispatchStrategy()
	}
}

func TriggerTransferChunksByStrategy(ctx context.Context, buyer *Runtime, p TransferChunksByStrategyParams) (TransferChunksByStrategyResult, error) {
	// 设计说明：
	// 1) 先做报价硬过滤（价格上限 + 可用仲裁）；
	// 2) 再构造卖家 worker（每个 worker 维护独立 transfer-pool 会话）；
	// 3) 用“中心调度 + worker 执行”模型按块并发下载；
	// 4) 失败块重排回队列，超过重试阈值则整体失败；
	// 5) 全部完成后关闭各自会话并汇总卖家统计，保证链上状态闭环。
	if buyer == nil || buyer.Host == nil {
		return TransferChunksByStrategyResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if strings.TrimSpace(p.DemandID) == "" || seedHash == "" {
		return TransferChunksByStrategyResult{}, fmt.Errorf("demand_id and seed_hash are required")
	}
	if p.ChunkCount == 0 {
		return TransferChunksByStrategyResult{}, fmt.Errorf("chunk_count must be > 0")
	}
	if p.MaxChunkRetries <= 0 {
		p.MaxChunkRetries = defaultChunkRetryMax
	}
	logTransferStrategy("evt_transfer_strategy_begin", map[string]any{
		"demand_id":         strings.TrimSpace(p.DemandID),
		"seed_hash":         shortID(seedHash),
		"chunk_count":       p.ChunkCount,
		"strategy":          strings.TrimSpace(p.Strategy),
		"max_seed_price":    p.MaxSeedPrice,
		"max_chunk_price":   p.MaxChunkPrice,
		"pool_amount":       p.PoolAmount,
		"max_chunk_retries": p.MaxChunkRetries,
		"arbiter_peer_id":   shortID(p.ArbiterPeerID),
	})

	quotes, err := TriggerClientListDirectQuotes(ctx, buyer, strings.TrimSpace(p.DemandID))
	if err != nil {
		return TransferChunksByStrategyResult{}, err
	}
	if len(quotes) == 0 {
		return TransferChunksByStrategyResult{}, fmt.Errorf("no direct quote available")
	}

	filtered := make([]DirectQuoteItem, 0, len(quotes))
	rejectedByPrice := 0
	for _, q := range quotes {
		if p.MaxChunkPrice > 0 && q.ChunkPrice > p.MaxChunkPrice {
			rejectedByPrice++
			continue
		}
		if p.MaxSeedPrice > 0 && q.SeedPrice > p.MaxSeedPrice {
			rejectedByPrice++
			continue
		}
		filtered = append(filtered, q)
	}
	logTransferStrategy("evt_transfer_strategy_quotes_filtered", map[string]any{
		"demand_id":            strings.TrimSpace(p.DemandID),
		"quote_total":          len(quotes),
		"quote_filtered":       len(filtered),
		"quote_rejected_price": rejectedByPrice,
	})
	if len(filtered) == 0 {
		return TransferChunksByStrategyResult{}, fmt.Errorf("no quotes under configured price limits")
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].ChunkPrice == filtered[j].ChunkPrice {
			return filtered[i].SeedPrice < filtered[j].SeedPrice
		}
		return filtered[i].ChunkPrice < filtered[j].ChunkPrice
	})

	workers := make([]*transferSellerWorker, 0, len(filtered))
	rejectedByArbiter := 0
	for _, q := range filtered {
		arbiterPeerID, err := resolveDealArbiter(buyer, q.SellerArbiterPeerIDs, p.ArbiterPeerID)
		if err != nil {
			rejectedByArbiter++
			logTransferStrategy("evt_transfer_strategy_quote_rejected_arbiter", map[string]any{
				"seller_peer_id": shortID(q.SellerPeerID),
				"demand_id":      strings.TrimSpace(q.DemandID),
				"error":          err.Error(),
			})
			continue
		}
		workers = append(workers, &transferSellerWorker{
			buyer:         buyer,
			quote:         q,
			arbiterPeerID: arbiterPeerID,
			seedHash:      seedHash,
			poolAmount:    p.PoolAmount,
			assignCh:      make(chan uint32),
		})
		logTransferStrategy("evt_transfer_strategy_seller_accepted", map[string]any{
			"seller_peer_id":  shortID(q.SellerPeerID),
			"demand_id":       strings.TrimSpace(q.DemandID),
			"arbiter_peer_id": shortID(arbiterPeerID),
			"chunk_price":     q.ChunkPrice,
			"seed_price":      q.SeedPrice,
		})
	}
	logTransferStrategy("evt_transfer_strategy_workers_ready", map[string]any{
		"demand_id":           strings.TrimSpace(p.DemandID),
		"worker_count":        len(workers),
		"rejected_by_arbiter": rejectedByArbiter,
	})
	if len(workers) == 0 {
		return TransferChunksByStrategyResult{}, fmt.Errorf("no quote with available arbiter")
	}

	// 先拿到 seed 元信息，后续才能按 chunk_hash 并发调度分块。
	var seedMeta seedV1Meta
	seedLoaded := false
	for _, w := range workers {
		if err := w.ensureSession(ctx); err != nil {
			w.broken = true
			logTransferStrategy("evt_transfer_strategy_seed_probe_skip", map[string]any{
				"seller_peer_id": shortID(w.quote.SellerPeerID),
				"reason":         "open_session_failed",
				"error":          err.Error(),
			})
			continue
		}
		seedRes, err := TriggerClientSeedGet(ctx, buyer, SeedGetParams{
			SellerPeerID: w.quote.SellerPeerID,
			SessionID:    w.sessionID,
			SeedHash:     seedHash,
		})
		if err != nil {
			w.broken = true
			logTransferStrategy("evt_transfer_strategy_seed_probe_skip", map[string]any{
				"seller_peer_id": shortID(w.quote.SellerPeerID),
				"reason":         "seed_get_failed",
				"error":          err.Error(),
			})
			continue
		}
		seedMeta, err = parseSeedV1(seedRes.Seed)
		if err != nil {
			w.broken = true
			logTransferStrategy("evt_transfer_strategy_seed_probe_skip", map[string]any{
				"seller_peer_id": shortID(w.quote.SellerPeerID),
				"reason":         "seed_parse_failed",
				"error":          err.Error(),
			})
			continue
		}
		if !strings.EqualFold(seedMeta.SeedHashHex, seedHash) {
			w.broken = true
			logTransferStrategy("evt_transfer_strategy_seed_probe_skip", map[string]any{
				"seller_peer_id": shortID(w.quote.SellerPeerID),
				"reason":         "seed_hash_mismatch",
				"seed_hash":      shortID(seedMeta.SeedHashHex),
			})
			continue
		}
		seedLoaded = true
		logTransferStrategy("evt_transfer_strategy_seed_probe_ok", map[string]any{
			"seller_peer_id": shortID(w.quote.SellerPeerID),
			"chunk_count":    seedMeta.ChunkCount,
			"file_size":      seedMeta.FileSize,
		})
		break
	}
	if !seedLoaded {
		_ = closeTransferWorkers(context.Background(), workers)
		return TransferChunksByStrategyResult{}, fmt.Errorf("seed metadata load failed from all sellers")
	}

	want := p.ChunkCount
	if want > seedMeta.ChunkCount {
		want = seedMeta.ChunkCount
	}
	if want == 0 {
		_ = closeTransferWorkers(context.Background(), workers)
		return TransferChunksByStrategyResult{}, fmt.Errorf("seed has zero chunks")
	}

	strategy := buildTransferStrategy(p.Strategy)
	logTransferStrategy("evt_transfer_strategy_dispatch_ready", map[string]any{
		"demand_id":      strings.TrimSpace(p.DemandID),
		"strategy":       strings.TrimSpace(p.Strategy),
		"effective_name": fmt.Sprintf("%T", strategy),
		"chunk_count":    want,
		"worker_count":   len(workers),
	})
	chunkHashes := make([]string, want)
	for i := uint32(0); i < want; i++ {
		chunkHashes[i] = seedMeta.ChunkHashes[i]
	}

	readyCh := make(chan int, len(workers)*2)
	resultCh := make(chan transferChunkResult, len(workers)*2)
	var wg sync.WaitGroup
	for idx := range workers {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			readyCh <- workerIdx
			for chunkIndex := range workers[workerIdx].assignCh {
				if int(chunkIndex) >= len(chunkHashes) {
					resultCh <- transferChunkResult{sellerIndex: workerIdx, chunkIndex: chunkIndex, err: fmt.Errorf("chunk index out of range")}
					readyCh <- workerIdx
					continue
				}
				chunk, elapsed, err := workers[workerIdx].fetchChunk(ctx, chunkIndex, chunkHashes[chunkIndex])
				resultCh <- transferChunkResult{
					sellerIndex: workerIdx,
					chunkIndex:  chunkIndex,
					chunk:       chunk,
					elapsed:     elapsed,
					err:         err,
				}
				readyCh <- workerIdx
			}
		}(idx)
	}

	queue := make([]uint32, 0, want)
	for i := uint32(0); i < want; i++ {
		queue = append(queue, i)
	}
	ready := map[int]bool{}
	retryCount := make([]int, want)
	chunks := make([][]byte, want)
	remaining := int(want)
	var completedBytes uint64

	popQueue := func() (uint32, bool) {
		if len(queue) == 0 {
			return 0, false
		}
		v := queue[0]
		queue = queue[1:]
		return v, true
	}
	requeue := func(idx uint32) {
		queue = append(queue, idx)
	}
	pickReady := func(force bool) (int, bool) {
		readyList := make([]int, 0, len(ready))
		for idx, ok := range ready {
			if ok {
				readyList = append(readyList, idx)
			}
		}
		if len(readyList) == 0 {
			return 0, false
		}
		sel, ok := strategy.SelectReady(readyList, workers)
		if ok {
			return sel, true
		}
		if !force {
			return 0, false
		}
		for _, idx := range readyList {
			if !workers[idx].broken {
				return idx, true
			}
		}
		return 0, false
	}
	closeWorkers := func() {
		for _, w := range workers {
			close(w.assignCh)
		}
		wg.Wait()
	}

	var runErr error
	for remaining > 0 && runErr == nil {
		dispatched := false
		for len(queue) > 0 {
			idx, ok := pickReady(false)
			if !ok {
				break
			}
			chunkIndex, ok := popQueue()
			if !ok {
				break
			}
			ready[idx] = false
			workers[idx].assignCh <- chunkIndex
			logTransferStrategy("evt_transfer_strategy_chunk_assigned", map[string]any{
				"chunk_index":     chunkIndex,
				"seller_peer_id":  shortID(workers[idx].quote.SellerPeerID),
				"queue_remaining": len(queue),
				"strategy":        strings.TrimSpace(p.Strategy),
			})
			dispatched = true
		}

		if remaining == 0 {
			break
		}

		if !dispatched {
			if len(queue) > 0 {
				if idx, ok := pickReady(true); ok {
					chunkIndex, ok := popQueue()
					if ok {
						ready[idx] = false
						workers[idx].assignCh <- chunkIndex
						logTransferStrategy("evt_transfer_strategy_chunk_assigned_forced", map[string]any{
							"chunk_index":     chunkIndex,
							"seller_peer_id":  shortID(workers[idx].quote.SellerPeerID),
							"queue_remaining": len(queue),
							"strategy":        strings.TrimSpace(p.Strategy),
						})
						dispatched = true
					}
				}
			}
		}
		if len(queue) > 0 {
			available := 0
			for _, w := range workers {
				if !w.broken {
					available++
				}
			}
			if available == 0 {
				runErr = fmt.Errorf("all sellers unavailable")
				break
			}
		}

		select {
		case ridx := <-readyCh:
			ready[ridx] = true
		case res := <-resultCh:
			if res.chunkIndex >= uint32(len(retryCount)) {
				runErr = fmt.Errorf("chunk index out of range")
				break
			}
			w := workers[res.sellerIndex]
			if res.err != nil {
				w.updateFailure()
				retryCount[res.chunkIndex]++
				logTransferStrategy("evt_transfer_strategy_chunk_failed", map[string]any{
					"chunk_index":    res.chunkIndex,
					"seller_peer_id": shortID(w.quote.SellerPeerID),
					"retry_count":    retryCount[res.chunkIndex],
					"max_retries":    p.MaxChunkRetries,
					"error":          res.err.Error(),
					"broken":         w.broken,
				})
				if retryCount[res.chunkIndex] > p.MaxChunkRetries {
					runErr = fmt.Errorf("chunk=%d transfer failed after retries: %w", res.chunkIndex, res.err)
					break
				}
				requeue(res.chunkIndex)
				continue
			}
			if res.chunkIndex >= uint32(len(chunks)) {
				runErr = fmt.Errorf("chunk index out of range")
				break
			}
			hash := sha256.Sum256(res.chunk)
			if hex.EncodeToString(hash[:]) != chunkHashes[res.chunkIndex] {
				w.updateFailure()
				retryCount[res.chunkIndex]++
				logTransferStrategy("evt_transfer_strategy_chunk_hash_failed", map[string]any{
					"chunk_index":    res.chunkIndex,
					"seller_peer_id": shortID(w.quote.SellerPeerID),
					"retry_count":    retryCount[res.chunkIndex],
					"max_retries":    p.MaxChunkRetries,
				})
				if retryCount[res.chunkIndex] > p.MaxChunkRetries {
					runErr = fmt.Errorf("chunk=%d hash verify failed", res.chunkIndex)
					break
				}
				requeue(res.chunkIndex)
				continue
			}
			if chunks[res.chunkIndex] == nil {
				chunks[res.chunkIndex] = res.chunk
				remaining--
				completedBytes += uint64(len(res.chunk))
			}
			w.resetFailureStreak()
			w.updateSuccess(len(res.chunk), res.elapsed)
			logTransferStrategy("evt_transfer_strategy_chunk_ok", map[string]any{
				"chunk_index":      res.chunkIndex,
				"seller_peer_id":   shortID(w.quote.SellerPeerID),
				"elapsed_ms":       res.elapsed.Milliseconds(),
				"chunk_bytes":      len(res.chunk),
				"remaining_chunks": remaining,
				"ema_bps":          w.emaBPS,
			})
			strategy.OnChunkDone(time.Now(), completedBytes, workers)
		case <-ctx.Done():
			runErr = ctx.Err()
		}
	}

	closeWorkers()

	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	closeErr := closeTransferWorkers(closeCtx, workers)
	logTransferStrategy("evt_transfer_strategy_closed_workers", map[string]any{
		"demand_id": strings.TrimSpace(p.DemandID),
		"error":     errText(closeErr),
	})
	if runErr != nil {
		logTransferStrategy("evt_transfer_strategy_failed", map[string]any{
			"demand_id": strings.TrimSpace(p.DemandID),
			"error":     runErr.Error(),
		})
		return TransferChunksByStrategyResult{}, runErr
	}
	if closeErr != nil {
		return TransferChunksByStrategyResult{}, closeErr
	}

	data := make([]byte, 0, int(want)*int(seedBlockSize))
	for i := 0; i < int(want); i++ {
		if chunks[i] == nil {
			return TransferChunksByStrategyResult{}, fmt.Errorf("missing chunk index=%d", i)
		}
		data = append(data, chunks[i]...)
	}
	if uint64(len(data)) > seedMeta.FileSize {
		data = data[:seedMeta.FileSize]
	}
	sum := sha256.Sum256(data)

	stats := make([]TransferSellerStatItem, 0, len(workers))
	for _, w := range workers {
		avg := 0.0
		if w.totalNanos > 0 {
			avg = float64(w.totalBytes) / (float64(w.totalNanos) / float64(time.Second))
		}
		stats = append(stats, TransferSellerStatItem{
			SellerPeerID:        w.quote.SellerPeerID,
			ChunkPrice:          w.quote.ChunkPrice,
			SeedPrice:           w.quote.SeedPrice,
			SuccessChunks:       w.successCount,
			FailedChunks:        w.failedCount,
			AvgBytesPerSecond:   avg,
			EMASpeedBytesPerSec: w.emaBPS,
			Pruned:              w.pruned,
			Broken:              w.broken,
		})
	}
	logTransferStrategy("evt_transfer_strategy_done", map[string]any{
		"demand_id":    strings.TrimSpace(p.DemandID),
		"seed_hash":    shortID(seedHash),
		"chunk_count":  want,
		"bytes":        len(data),
		"sha256":       shortID(hex.EncodeToString(sum[:])),
		"seller_count": len(stats),
	})

	return TransferChunksByStrategyResult{
		Data:       data,
		SHA256:     hex.EncodeToString(sum[:]),
		ChunkCount: want,
		Sellers:    stats,
	}, nil
}

func closeTransferWorkers(ctx context.Context, workers []*transferSellerWorker) error {
	var firstErr error
	for _, w := range workers {
		if err := w.closeSession(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
