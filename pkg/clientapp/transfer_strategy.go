package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

const (
	TransferStrategySmart      = "smart"
	defaultChunkRetryMax       = 6
	defaultSaturationEpsilon   = 0.03
	defaultSaturationWindowDur = 2 * time.Second
)

// TransferChunksByStrategyParams 描述多卖家分块下载输入。
// 说明：只放业务参数，不依赖 HTTP 管理接口。
type TransferChunksByStrategyParams struct {
	DemandID        string `json:"demand_id"`
	SeedHash        string `json:"seed_hash"`
	ChunkCount      uint32 `json:"chunk_count"` // 0 表示按 seed 元信息全量下载
	ArbiterPubHex   string `json:"arbiter_pubkey_hex,omitempty"`
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
	Seed       []byte                   `json:"seed,omitempty"`
	Sellers    []TransferSellerStatItem `json:"sellers"`
}

type TransferSellerStatItem struct {
	SellerPubHex        string  `json:"seller_pubkey_hex"`
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
	store         *clientDB
	quote         DirectQuoteItem
	arbiterPubHex string
	seedHash      string
	poolAmount    uint64
	// availableChunks 为空表示“未声明限制（默认认为可提供全部块）”。
	availableChunks map[uint32]struct{}

	dealID    string
	sessionID string
	opened    bool
	openSeq   uint32
	openBase  string
	poolSat   uint64
	payCount  uint32
	lastPay   uint32
	closed    bool
	closeTxID string

	successCount uint32
	failedCount  uint32
	totalBytes   uint64
	totalNanos   int64
	emaBPS       float64
	pruned       bool
	prunedReason string
	broken       bool
	brokenReason string

	consecutiveFailures int
	assignCh            chan uint32
}

func (w *transferSellerWorker) hasChunk(chunkIndex uint32) bool {
	if w == nil {
		return false
	}
	if len(w.availableChunks) == 0 {
		return true
	}
	_, ok := w.availableChunks[chunkIndex]
	return ok
}

func chunkIndexSet(indexes []uint32) map[uint32]struct{} {
	indexes = normalizeChunkIndexes(indexes, 0)
	if len(indexes) == 0 {
		return nil
	}
	out := make(map[uint32]struct{}, len(indexes))
	for _, idx := range indexes {
		out[idx] = struct{}{}
	}
	return out
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

func (w *transferSellerWorker) updateFailure(reason string) {
	w.failedCount++
	w.consecutiveFailures++
	if w.consecutiveFailures >= 3 {
		w.broken = true
		if strings.TrimSpace(reason) == "" {
			reason = "consecutive_failures"
		}
		w.brokenReason = reason
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

func (w *transferSellerWorker) recordPurchaseDone(ctx context.Context, chunkIndex uint32, objectHash string, amount uint64) {
	if w == nil || w.store == nil {
		return
	}
	if err := dbAppendPurchaseDone(ctx, w.store, purchaseDoneEntry{
		DemandID:      strings.TrimSpace(w.quote.DemandID),
		SellerPubHex:  strings.TrimSpace(w.quote.SellerPubHex),
		ArbiterPubHex: strings.TrimSpace(w.arbiterPubHex),
		ChunkIndex:    chunkIndex,
		ObjectHash:    strings.ToLower(strings.TrimSpace(objectHash)),
		AmountSatoshi: amount,
	}); err != nil {
		logTransferStrategy("evt_transfer_strategy_purchase_record_failed", map[string]any{
			"demand_id":         strings.TrimSpace(w.quote.DemandID),
			"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
			"chunk_index":       chunkIndex,
			"error":             err.Error(),
		})
	}
}

func (w *transferSellerWorker) ensureSession(ctx context.Context) error {
	if w.opened {
		return nil
	}
	logTransferStrategy("evt_transfer_strategy_open_session_begin", map[string]any{
		"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
		"demand_id":         strings.TrimSpace(w.quote.DemandID),
		"seed_hash":         shortID(w.seedHash),
		"chunk_price":       w.quote.ChunkPrice,
		"seed_price":        w.quote.SeedPrice,
	})
	openRes, err := triggerDirectTransferPoolOpen(ctx, w.store, w.buyer, directTransferPoolOpenParams{
		SellerPubHex:  w.quote.SellerPubHex,
		ArbiterPubHex: w.arbiterPubHex,
		DemandID:      w.quote.DemandID,
		SeedHash:      w.seedHash,
		SeedPrice:     w.quote.SeedPrice,
		ChunkPrice:    w.quote.ChunkPrice,
		ExpiresAtUnix: w.quote.ExpiresAtUnix,
		PoolAmount:    w.poolAmount,
	})
	if err != nil {
		logTransferStrategy("evt_transfer_strategy_open_session_failed", map[string]any{
			"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
			"demand_id":         strings.TrimSpace(w.quote.DemandID),
			"error":             err.Error(),
		})
		return err
	}
	w.dealID = openRes.DealID
	w.sessionID = openRes.SessionID
	w.opened = true
	w.openSeq = openRes.Sequence
	w.openBase = strings.TrimSpace(openRes.BaseTxID)
	w.poolSat = openRes.PoolAmount
	w.payCount = 0
	w.lastPay = 0
	w.closed = false
	w.closeTxID = ""
	logTransferStrategy("evt_transfer_strategy_open_session_ok", map[string]any{
		"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
		"deal_id":           strings.TrimSpace(w.dealID),
		"session_id":        strings.TrimSpace(w.sessionID),
	})
	return nil
}

func (w *transferSellerWorker) fetchChunk(ctx context.Context, chunkIndex uint32, chunkHash string) ([]byte, time.Duration, error) {
	if err := w.ensureSession(ctx); err != nil {
		return nil, 0, err
	}
	begin := time.Now()
	payRes, err := triggerDirectTransferPoolPay(ctx, w.store, w.buyer, directTransferPoolPayParams{
		SellerPubHex: w.quote.SellerPubHex,
		SessionID:    w.sessionID,
		Amount:       w.quote.ChunkPrice,
		SeedHash:     w.seedHash,
		ChunkHash:    chunkHash,
		ChunkIndex:   chunkIndex,
	})
	if err != nil {
		return nil, 0, err
	}
	w.payCount++
	w.lastPay = payRes.Sequence
	elapsed := time.Since(begin)
	w.recordPurchaseDone(ctx, chunkIndex, chunkHash, w.quote.ChunkPrice)
	return payRes.Chunk, elapsed, nil
}

func (w *transferSellerWorker) closeSession(ctx context.Context) error {
	if !w.opened || strings.TrimSpace(w.sessionID) == "" {
		return nil
	}
	closeRes, err := triggerDirectTransferPoolClose(ctx, w.store, w.buyer, directTransferPoolCloseParams{
		SellerPubHex: w.quote.SellerPubHex,
		SessionID:    w.sessionID,
	})
	if err == nil {
		w.closed = true
		if strings.TrimSpace(closeRes.FinalTxID) != "" {
			w.closeTxID = strings.TrimSpace(closeRes.FinalTxID)
		}
	}
	return err
}

type transferDispatchStrategy interface {
	// 设计说明：
	// - SelectReady 只负责“在当前可派发卖家中选一个”，不直接做网络动作；
	// - OnChunkDone 只消费观测数据，用于在线更新策略状态（例如饱和判定与裁剪）。
	SelectReady(ready []int, workers []*transferSellerWorker) (int, bool)
	OnChunkDone(now time.Time, completedBytes uint64, workers []*transferSellerWorker)
}

type smartDispatchStrategy struct {
	epsilon            float64
	windowDur          time.Duration
	lastWindowAt       time.Time
	lastWindowBytes    uint64
	lastWindowBPS      float64
	slowWindowStreak   int
	alreadySaturated   bool
	lastPrunedSellerID string
	lastPrunedReason   string
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
	workers[worstIdx].prunedReason = "saturation_low_growth"
	s.lastPrunedSellerID = workers[worstIdx].quote.SellerPubHex
	s.lastPrunedReason = "saturation_low_growth"
	logTransferStrategy("evt_transfer_strategy_smart_pruned", map[string]any{
		"seller_pubkey_hex": shortID(workers[worstIdx].quote.SellerPubHex),
		"score":             worstScore,
		"chunk_price":       workers[worstIdx].quote.ChunkPrice,
		"ema_bps":           workers[worstIdx].emaBPS,
		"failed_chunks":     workers[worstIdx].failedCount,
		"success_chunks":    workers[worstIdx].successCount,
	})
}

func buildTransferStrategy(name string) transferDispatchStrategy {
	_ = name
	// 设计说明：
	// - 当前阶段只保留速价动态策略，删除占位策略实现；
	// - 后续若新增策略，必须满足可观测与回归测试约束后再接入。
	return newSmartDispatchStrategy()
}

func strategyNameOrDefault(name string) string {
	if strings.TrimSpace(strings.ToLower(name)) == TransferStrategySmart {
		return TransferStrategySmart
	}
	return TransferStrategySmart
}

func TriggerTransferChunksByStrategy(ctx context.Context, store *clientDB, buyer *Runtime, p TransferChunksByStrategyParams) (TransferChunksByStrategyResult, error) {
	if buyer == nil {
		return TransferChunksByStrategyResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := buyer.kernel
	if kernel == nil {
		return TransferChunksByStrategyResult{}, fmt.Errorf("client kernel not initialized")
	}
	return kernel.runTransferChunksByStrategy(ctx, p)
}

func triggerTransferChunksByStrategyImpl(ctx context.Context, store *clientDB, buyer *Runtime, p TransferChunksByStrategyParams) (TransferChunksByStrategyResult, error) {
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
	if p.MaxChunkRetries <= 0 {
		p.MaxChunkRetries = defaultChunkRetryMax
	}
	logTransferStrategy("evt_transfer_strategy_begin", map[string]any{
		"demand_id":          strings.TrimSpace(p.DemandID),
		"seed_hash":          shortID(seedHash),
		"chunk_count":        p.ChunkCount,
		"strategy":           strategyNameOrDefault(p.Strategy),
		"strategy_input":     strings.TrimSpace(p.Strategy),
		"max_seed_price":     p.MaxSeedPrice,
		"max_chunk_price":    p.MaxChunkPrice,
		"pool_amount":        p.PoolAmount,
		"max_chunk_retries":  p.MaxChunkRetries,
		"arbiter_pubkey_hex": shortID(p.ArbiterPubHex),
	})

	quotes, err := TriggerClientListDirectQuotes(ctx, store, strings.TrimSpace(p.DemandID))
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

	rejectedByArbiter := 0
	var seedMeta seedV1Meta
	var seedBytes []byte
	workers, seedMeta, seedBytes, err := prepareSpeedPriceWorkersAndSeed(speedPriceBootstrapParams{
		Ctx:           ctx,
		Buyer:         buyer,
		Store:         store,
		Quotes:        filtered,
		SeedHash:      seedHash,
		ArbiterPubHex: p.ArbiterPubHex,
		PoolAmount:    p.PoolAmount,
		OnQuoteRejected: func(q DirectQuoteItem, err error) {
			rejectedByArbiter++
			logTransferStrategy("evt_transfer_strategy_quote_rejected_arbiter", map[string]any{
				"seller_pubkey_hex": shortID(q.SellerPubHex),
				"demand_id":         strings.TrimSpace(q.DemandID),
				"error":             err.Error(),
			})
		},
		OnQuoteAccepted: func(q DirectQuoteItem, arbiterPubHex string) {
			logTransferStrategy("evt_transfer_strategy_seller_accepted", map[string]any{
				"seller_pubkey_hex":  shortID(q.SellerPubHex),
				"demand_id":          strings.TrimSpace(q.DemandID),
				"arbiter_pubkey_hex": shortID(arbiterPubHex),
				"chunk_price":        q.ChunkPrice,
				"seed_price":         q.SeedPrice,
				"chunk_have":         len(q.AvailableChunkIndexes),
			})
		},
		OnSeedProbeFail: func(w *transferSellerWorker, reason string, err error) {
			fields := map[string]any{
				"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
				"reason":            reason,
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			logTransferStrategy("evt_transfer_strategy_seed_probe_skip", fields)
		},
		OnSeedProbeOK: func(w *transferSellerWorker, meta seedV1Meta) {
			logTransferStrategy("evt_transfer_strategy_seed_probe_ok", map[string]any{
				"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
				"chunk_count":       meta.ChunkCount,
				"file_size":         meta.FileSize,
			})
		},
	})
	logTransferStrategy("evt_transfer_strategy_workers_ready", map[string]any{
		"demand_id":           strings.TrimSpace(p.DemandID),
		"worker_count":        len(workers),
		"rejected_by_arbiter": rejectedByArbiter,
	})
	if err != nil {
		return TransferChunksByStrategyResult{}, err
	}

	want := p.ChunkCount
	if want == 0 || want > seedMeta.ChunkCount {
		want = seedMeta.ChunkCount
	}
	if want == 0 {
		_ = closeTransferWorkers(context.Background(), workers)
		return TransferChunksByStrategyResult{}, fmt.Errorf("seed has zero chunks")
	}
	missing := missingChunkCoverage(want, workers)
	if len(missing) > 0 {
		_ = closeTransferWorkers(context.Background(), workers)
		logTransferStrategy("evt_transfer_strategy_missing_chunks", map[string]any{
			"demand_id":      strings.TrimSpace(p.DemandID),
			"missing_count":  len(missing),
			"first_missing":  missing[0],
			"required_chunk": want,
		})
		return TransferChunksByStrategyResult{}, fmt.Errorf("missing chunk providers")
	}

	effectiveStrategyName := strategyNameOrDefault(p.Strategy)
	strategy := buildTransferStrategy(effectiveStrategyName)
	logTransferStrategy("evt_transfer_strategy_dispatch_ready", map[string]any{
		"demand_id":      strings.TrimSpace(p.DemandID),
		"strategy":       effectiveStrategyName,
		"effective_name": fmt.Sprintf("%T", strategy),
		"chunk_count":    want,
		"worker_count":   len(workers),
	})
	chunkHashes := make([]string, want)
	for i := uint32(0); i < want; i++ {
		chunkHashes[i] = seedMeta.ChunkHashes[i]
	}

	pending := make(map[uint32]bool, want)
	for i := uint32(0); i < want; i++ {
		pending[i] = true
	}
	chunks := make([][]byte, want)
	_, runErr := runSpeedPriceChunkScheduler(speedPriceChunkSchedulerParams{
		Ctx:          ctx,
		Workers:      workers,
		ChunkHashes:  chunkHashes,
		Pending:      pending,
		MaxRetries:   p.MaxChunkRetries,
		Strategy:     strategy,
		StrategyName: effectiveStrategyName,
		SelectChunk:  defaultSelectChunk,
		OnChunk: func(chunkIndex uint32, chunk []byte, w *transferSellerWorker, elapsed time.Duration) (uint64, error) {
			if chunkIndex >= uint32(len(chunks)) {
				return 0, fmt.Errorf("chunk index out of range")
			}
			if chunks[chunkIndex] == nil {
				chunks[chunkIndex] = chunk
				logTransferStrategy("evt_transfer_strategy_chunk_ok", map[string]any{
					"chunk_index":       chunkIndex,
					"seller_pubkey_hex": shortID(w.quote.SellerPubHex),
					"elapsed_ms":        elapsed.Milliseconds(),
					"chunk_bytes":       len(chunk),
					"ema_bps":           w.emaBPS,
				})
				return uint64(len(chunk)), nil
			}
			return 0, nil
		},
	})

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
	sellerSessions := make([]map[string]any, 0, len(workers))
	for _, w := range workers {
		avg := 0.0
		if w.totalNanos > 0 {
			avg = float64(w.totalBytes) / (float64(w.totalNanos) / float64(time.Second))
		}
		stats = append(stats, TransferSellerStatItem{
			SellerPubHex:        w.quote.SellerPubHex,
			ChunkPrice:          w.quote.ChunkPrice,
			SeedPrice:           w.quote.SeedPrice,
			SuccessChunks:       w.successCount,
			FailedChunks:        w.failedCount,
			AvgBytesPerSecond:   avg,
			EMASpeedBytesPerSec: w.emaBPS,
			Pruned:              w.pruned,
			Broken:              w.broken,
		})
		sellerSessions = append(sellerSessions, map[string]any{
			"seller_pubkey_hex":     strings.TrimSpace(w.quote.SellerPubHex),
			"demand_id":             strings.TrimSpace(w.quote.DemandID),
			"deal_id":               strings.TrimSpace(w.dealID),
			"session_id":            strings.TrimSpace(w.sessionID),
			"open_sequence":         w.openSeq,
			"open_base_txid":        strings.TrimSpace(w.openBase),
			"pool_amount_satoshi":   w.poolSat,
			"pay_count":             w.payCount,
			"last_pay_sequence":     w.lastPay,
			"closed":                w.closed,
			"close_final_txid":      strings.TrimSpace(w.closeTxID),
			"success_chunks":        w.successCount,
			"failed_chunks":         w.failedCount,
			"avg_bytes_per_second":  avg,
			"ema_speed_bytes_per_s": w.emaBPS,
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
	if buyer != nil {
		primary := map[string]any{}
		primarySessionID := ""
		if len(sellerSessions) > 0 {
			primary = sellerSessions[0]
			primarySessionID = strings.TrimSpace(fmt.Sprint(primary["session_id"]))
		}
		eventID := fmt.Sprintf("demand:%s:completed", strings.TrimSpace(p.DemandID))
		if primarySessionID != "" {
			eventID = primarySessionID + ":completed"
		}
		emitDirectTransferEvent(buyer, "direct_transfer_completed", map[string]any{
			"event_id":                 eventID,
			"demand_id":                strings.TrimSpace(p.DemandID),
			"seed_hash":                seedHash,
			"chunk_count":              want,
			"seller_count":             len(stats),
			"bytes":                    len(data),
			"sha256":                   hex.EncodeToString(sum[:]),
			"primary_deal_id":          primary["deal_id"],
			"primary_session_id":       primary["session_id"],
			"primary_open_sequence":    primary["open_sequence"],
			"primary_open_base_txid":   primary["open_base_txid"],
			"primary_pool_amount_sat":  primary["pool_amount_satoshi"],
			"primary_pay_count":        primary["pay_count"],
			"primary_last_pay_seq":     primary["last_pay_sequence"],
			"primary_close_final_txid": primary["close_final_txid"],
			"seller_sessions":          sellerSessions,
		})
	}

	return TransferChunksByStrategyResult{
		Data:       data,
		SHA256:     hex.EncodeToString(sum[:]),
		ChunkCount: want,
		Seed:       seedBytes,
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

func missingChunkCoverage(want uint32, workers []*transferSellerWorker) []uint32 {
	if want == 0 {
		return nil
	}
	missing := make([]uint32, 0, 8)
	for i := uint32(0); i < want; i++ {
		has := false
		for _, w := range workers {
			if w == nil || w.broken {
				continue
			}
			if w.hasChunk(i) {
				has = true
				break
			}
		}
		if !has {
			missing = append(missing, i)
		}
	}
	return missing
}
