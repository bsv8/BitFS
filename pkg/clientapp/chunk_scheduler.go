package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type speedPriceSellerState struct {
	SellerPubHex        string  `json:"seller_pubkey_hex"`
	ChunkPrice          uint64  `json:"chunk_price"`
	SeedPrice           uint64  `json:"seed_price"`
	SuccessChunks       uint32  `json:"success_chunks"`
	FailedChunks        uint32  `json:"failed_chunks"`
	EMASpeedBytesPerSec float64 `json:"ema_speed_bytes_per_sec"`
	Pruned              bool    `json:"pruned"`
	PrunedReason        string  `json:"pruned_reason"`
	Broken              bool    `json:"broken"`
	BrokenReason        string  `json:"broken_reason"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
}

type speedPriceSchedulerState struct {
	StrategyName          string                  `json:"strategy_name"`
	DebugLogEnabled       bool                    `json:"debug_log_enabled"`
	PendingChunks         uint32                  `json:"pending_chunks"`
	InflightChunks        uint32                  `json:"inflight_chunks"`
	CompletedChunks       uint32                  `json:"completed_chunks"`
	FailedAttempts        uint32                  `json:"failed_attempts"`
	FailureRate           float64                 `json:"failure_rate"`
	RetryHistogram        map[string]uint32       `json:"retry_histogram"`
	PruneReasonHistogram  map[string]uint32       `json:"prune_reason_histogram"`
	BrokenReasonHistogram map[string]uint32       `json:"broken_reason_histogram"`
	SellerPruneReasons    map[string]string       `json:"seller_prune_reasons"`
	SellerBrokenReasons   map[string]string       `json:"seller_broken_reasons"`
	ThroughputBPS         float64                 `json:"throughput_bps"`
	WindowBPS             float64                 `json:"window_bps"`
	LastPrunedSeller      string                  `json:"last_pruned_seller"`
	LastPruneReason       string                  `json:"last_prune_reason"`
	LastEvent             string                  `json:"last_event"`
	LastError             string                  `json:"last_error"`
	StartedAtUnix         int64                   `json:"started_at_unix"`
	UpdatedAtUnix         int64                   `json:"updated_at_unix"`
	Sellers               []speedPriceSellerState `json:"sellers"`
}

// 设计说明：
// - runSpeedPriceChunkScheduler 负责统一并发下载调度框架；
// - 调度算法固定为速价动态策略（smart）；
// - 业务层通过回调决定“下一个块怎么选”和“块写入后怎么落盘/入库”。
type speedPriceChunkSchedulerParams struct {
	Ctx          context.Context
	Workers      []*transferSellerWorker
	ChunkHashes  []string
	Pending      map[uint32]bool
	MaxRetries   int
	Strategy     transferDispatchStrategy
	StrategyName string

	SelectChunk func(pending map[uint32]bool, inflight map[uint32]bool) (uint32, bool)
	OnChunk     func(chunkIndex uint32, chunk []byte, w *transferSellerWorker, elapsed time.Duration) (uint64, error)
	OnState     func(state speedPriceSchedulerState)

	DebugLogEnabled bool
	Logf            func(event string, fields map[string]any)
}

func runSpeedPriceChunkScheduler(p speedPriceChunkSchedulerParams) (uint64, error) {
	if p.Ctx == nil {
		return 0, fmt.Errorf("context is required")
	}
	if len(p.Workers) == 0 {
		return 0, fmt.Errorf("workers are required")
	}
	if len(p.ChunkHashes) == 0 {
		return 0, fmt.Errorf("chunk hashes are required")
	}
	if len(p.Pending) == 0 {
		return 0, nil
	}
	if p.OnChunk == nil {
		return 0, fmt.Errorf("on chunk callback is required")
	}
	if p.MaxRetries <= 0 {
		p.MaxRetries = defaultChunkRetryMax
	}
	if p.Strategy == nil {
		p.Strategy = buildTransferStrategy("")
	}
	if p.StrategyName == "" {
		p.StrategyName = TransferStrategySmart
	}
	if p.SelectChunk == nil {
		p.SelectChunk = defaultSelectChunk
	}

	for i := range p.Workers {
		if p.Workers[i].assignCh == nil {
			p.Workers[i].assignCh = make(chan uint32)
		}
	}

	readyCh := make(chan int, len(p.Workers)*2)
	resultCh := make(chan transferChunkResult, len(p.Workers)*2)
	var wg sync.WaitGroup
	for idx := range p.Workers {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			readyCh <- workerIdx
			for chunkIndex := range p.Workers[workerIdx].assignCh {
				if int(chunkIndex) >= len(p.ChunkHashes) {
					resultCh <- transferChunkResult{sellerIndex: workerIdx, chunkIndex: chunkIndex, err: fmt.Errorf("chunk index out of range")}
					readyCh <- workerIdx
					continue
				}
				chunk, elapsed, err := p.Workers[workerIdx].fetchChunk(p.Ctx, chunkIndex, p.ChunkHashes[chunkIndex])
				resultCh <- transferChunkResult{sellerIndex: workerIdx, chunkIndex: chunkIndex, chunk: chunk, elapsed: elapsed, err: err}
				readyCh <- workerIdx
			}
		}(idx)
	}

	closeWorkers := func() {
		for _, w := range p.Workers {
			close(w.assignCh)
		}
		wg.Wait()
	}

	ready := map[int]bool{}
	inflight := map[uint32]bool{}
	retryCount := map[uint32]int{}
	retryHistogram := map[string]uint32{}
	var completedBytes uint64
	var completedChunks uint32
	var failedAttempts uint32
	startedAt := time.Now()
	lastEmitAt := startedAt
	lastEmitBytes := uint64(0)

	hasAvailableWorker := func() bool {
		for _, w := range p.Workers {
			if !w.broken {
				return true
			}
		}
		return false
	}

	snapshot := func(event string, lastErr error) speedPriceSchedulerState {
		now := time.Now()
		elapsed := now.Sub(startedAt).Seconds()
		throughput := 0.0
		if elapsed > 0 {
			throughput = float64(completedBytes) / elapsed
		}
		windowElapsed := now.Sub(lastEmitAt).Seconds()
		windowBPS := 0.0
		if windowElapsed > 0 {
			windowBPS = float64(completedBytes-lastEmitBytes) / windowElapsed
		}
		lastEmitAt = now
		lastEmitBytes = completedBytes

		sellers := make([]speedPriceSellerState, 0, len(p.Workers))
		pruneReasonHistogram := map[string]uint32{}
		brokenReasonHistogram := map[string]uint32{}
		sellerPruneReasons := map[string]string{}
		sellerBrokenReasons := map[string]string{}
		for _, w := range p.Workers {
			if w.pruned {
				reason := strings.TrimSpace(w.prunedReason)
				if reason == "" {
					reason = "unknown"
				}
				pruneReasonHistogram[reason]++
				sellerPruneReasons[w.quote.SellerPubHex] = reason
			}
			if w.broken {
				reason := strings.TrimSpace(w.brokenReason)
				if reason == "" {
					reason = "unknown"
				}
				brokenReasonHistogram[reason]++
				sellerBrokenReasons[w.quote.SellerPubHex] = reason
			}
			sellers = append(sellers, speedPriceSellerState{
				SellerPubHex:        w.quote.SellerPubHex,
				ChunkPrice:          w.quote.ChunkPrice,
				SeedPrice:           w.quote.SeedPrice,
				SuccessChunks:       w.successCount,
				FailedChunks:        w.failedCount,
				EMASpeedBytesPerSec: w.emaBPS,
				Pruned:              w.pruned,
				PrunedReason:        w.prunedReason,
				Broken:              w.broken,
				BrokenReason:        w.brokenReason,
				ConsecutiveFailures: w.consecutiveFailures,
			})
		}
		totalAttempts := completedChunks + failedAttempts
		failureRate := 0.0
		if totalAttempts > 0 {
			failureRate = float64(failedAttempts) / float64(totalAttempts)
		}
		state := speedPriceSchedulerState{
			StrategyName:          p.StrategyName,
			DebugLogEnabled:       p.DebugLogEnabled,
			PendingChunks:         uint32(len(p.Pending)),
			InflightChunks:        uint32(len(inflight)),
			CompletedChunks:       completedChunks,
			FailedAttempts:        failedAttempts,
			FailureRate:           failureRate,
			RetryHistogram:        cloneRetryHistogram(retryHistogram),
			PruneReasonHistogram:  cloneRetryHistogram(pruneReasonHistogram),
			BrokenReasonHistogram: cloneRetryHistogram(brokenReasonHistogram),
			SellerPruneReasons:    cloneStringMap(sellerPruneReasons),
			SellerBrokenReasons:   cloneStringMap(sellerBrokenReasons),
			ThroughputBPS:         throughput,
			WindowBPS:             windowBPS,
			LastEvent:             event,
			StartedAtUnix:         startedAt.Unix(),
			UpdatedAtUnix:         now.Unix(),
			Sellers:               sellers,
		}
		if lastErr != nil {
			state.LastError = lastErr.Error()
		}
		if s, ok := p.Strategy.(*smartDispatchStrategy); ok {
			state.LastPrunedSeller = s.lastPrunedSellerID
			state.LastPruneReason = s.lastPrunedReason
		}
		return state
	}

	emit := func(event string, lastErr error, fields map[string]any) {
		state := snapshot(event, lastErr)
		if p.OnState != nil {
			p.OnState(state)
		}
		if p.DebugLogEnabled && p.Logf != nil {
			payload := map[string]any{
				"event":            state.LastEvent,
				"strategy":         state.StrategyName,
				"pending_chunks":   state.PendingChunks,
				"inflight_chunks":  state.InflightChunks,
				"completed_chunks": state.CompletedChunks,
				"failed_attempts":  state.FailedAttempts,
				"failure_rate":     state.FailureRate,
				"throughput_bps":   state.ThroughputBPS,
				"window_bps":       state.WindowBPS,
				"last_error":       state.LastError,
				"last_pruned":      state.LastPrunedSeller,
			}
			for k, v := range fields {
				payload[k] = v
			}
			p.Logf("scheduler_"+event, payload)
		}
	}

	emit("start", nil, map[string]any{"worker_count": len(p.Workers), "chunk_count": len(p.ChunkHashes)})

	for {
		dispatched := false
		for {
			readyList := make([]int, 0, len(ready))
			for idx, ok := range ready {
				if ok {
					readyList = append(readyList, idx)
				}
			}
			if len(readyList) == 0 {
				break
			}
			chunkIdx, readyForChunk, ok := pickDispatchChunk(readyList, p.Workers, p.Pending, inflight, p.SelectChunk)
			if !ok {
				break
			}
			workerIdx, ok := p.Strategy.SelectReady(readyForChunk, p.Workers)
			if !ok {
				workerIdx = firstHealthyReady(readyForChunk, p.Workers)
				if workerIdx < 0 {
					break
				}
			}
			delete(p.Pending, chunkIdx)
			inflight[chunkIdx] = true
			ready[workerIdx] = false
			p.Workers[workerIdx].assignCh <- chunkIdx
			dispatched = true
			emit("assign", nil, map[string]any{"chunk_index": chunkIdx, "seller_pubkey_hex": p.Workers[workerIdx].quote.SellerPubHex})
		}

		if len(p.Pending) == 0 && len(inflight) == 0 {
			break
		}
		if !dispatched && len(inflight) == 0 {
			if !hasAvailableWorker() {
				closeWorkers()
				err := fmt.Errorf("all sellers unavailable")
				emit("failed", err, nil)
				return completedBytes, err
			}
			if !hasAnyProviderForPending(p.Pending, p.Workers) {
				closeWorkers()
				err := fmt.Errorf("no seller can serve remaining chunks")
				emit("failed", err, nil)
				return completedBytes, err
			}
		}

		select {
		case ridx := <-readyCh:
			ready[ridx] = true
		case res := <-resultCh:
			delete(inflight, res.chunkIndex)
			if int(res.chunkIndex) >= len(p.ChunkHashes) {
				closeWorkers()
				err := fmt.Errorf("chunk index out of range")
				emit("failed", err, nil)
				return completedBytes, err
			}
			w := p.Workers[res.sellerIndex]
			if res.err != nil {
				w.updateFailure("chunk_fetch_failed")
				failedAttempts++
				retryCount[res.chunkIndex]++
				retryHistogram[strconv.Itoa(retryCount[res.chunkIndex])]++
				if retryCount[res.chunkIndex] > p.MaxRetries {
					closeWorkers()
					err := fmt.Errorf("chunk=%d transfer failed after retries: %w", res.chunkIndex, res.err)
					emit("failed", err, map[string]any{"chunk_index": res.chunkIndex})
					return completedBytes, err
				}
				p.Pending[res.chunkIndex] = true
				emit("retry", res.err, map[string]any{"chunk_index": res.chunkIndex, "retry_count": retryCount[res.chunkIndex], "seller_pubkey_hex": w.quote.SellerPubHex})
				continue
			}
			hash := sha256.Sum256(res.chunk)
			if hex.EncodeToString(hash[:]) != p.ChunkHashes[res.chunkIndex] {
				w.updateFailure("chunk_hash_mismatch")
				failedAttempts++
				retryCount[res.chunkIndex]++
				retryHistogram[strconv.Itoa(retryCount[res.chunkIndex])]++
				if retryCount[res.chunkIndex] > p.MaxRetries {
					closeWorkers()
					err := fmt.Errorf("chunk=%d hash verify failed", res.chunkIndex)
					emit("failed", err, map[string]any{"chunk_index": res.chunkIndex})
					return completedBytes, err
				}
				p.Pending[res.chunkIndex] = true
				emit("retry_hash", fmt.Errorf("chunk hash mismatch"), map[string]any{"chunk_index": res.chunkIndex, "retry_count": retryCount[res.chunkIndex], "seller_pubkey_hex": w.quote.SellerPubHex})
				continue
			}
			written, err := p.OnChunk(res.chunkIndex, res.chunk, w, res.elapsed)
			if err != nil {
				closeWorkers()
				emit("failed", err, map[string]any{"chunk_index": res.chunkIndex})
				return completedBytes, err
			}
			completedBytes += written
			if written > 0 {
				completedChunks++
			}
			w.resetFailureStreak()
			w.updateSuccess(len(res.chunk), res.elapsed)
			p.Strategy.OnChunkDone(time.Now(), completedBytes, p.Workers)
			emit("chunk_ok", nil, map[string]any{"chunk_index": res.chunkIndex, "seller_pubkey_hex": w.quote.SellerPubHex, "chunk_bytes": len(res.chunk), "elapsed_ms": res.elapsed.Milliseconds()})
		case <-p.Ctx.Done():
			closeWorkers()
			err := p.Ctx.Err()
			emit("canceled", err, nil)
			return completedBytes, err
		}
	}

	closeWorkers()
	emit("done", nil, nil)
	return completedBytes, nil
}

func firstHealthyReady(ready []int, workers []*transferSellerWorker) int {
	for _, idx := range ready {
		if idx < 0 || idx >= len(workers) {
			continue
		}
		if !workers[idx].broken {
			return idx
		}
	}
	return -1
}

func defaultSelectChunk(pending map[uint32]bool, inflight map[uint32]bool) (uint32, bool) {
	if len(pending) == 0 {
		return 0, false
	}
	keys := make([]uint32, 0, len(pending))
	for idx := range pending {
		if inflight[idx] {
			continue
		}
		keys = append(keys, idx)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if len(keys) == 0 {
		return 0, false
	}
	return keys[0], true
}

func readyWorkersForChunk(ready []int, workers []*transferSellerWorker, chunkIndex uint32) []int {
	out := make([]int, 0, len(ready))
	for _, idx := range ready {
		if idx < 0 || idx >= len(workers) {
			continue
		}
		w := workers[idx]
		if w == nil || w.broken {
			continue
		}
		if !w.hasChunk(chunkIndex) {
			continue
		}
		out = append(out, idx)
	}
	return out
}

func pickDispatchChunk(
	ready []int,
	workers []*transferSellerWorker,
	pending map[uint32]bool,
	inflight map[uint32]bool,
	selectChunk func(pending map[uint32]bool, inflight map[uint32]bool) (uint32, bool),
) (uint32, []int, bool) {
	tried := map[uint32]struct{}{}
	if selectChunk != nil {
		if idx, ok := selectChunk(pending, inflight); ok {
			tried[idx] = struct{}{}
			candidates := readyWorkersForChunk(ready, workers, idx)
			if len(candidates) > 0 {
				return idx, candidates, true
			}
		}
	}
	keys := make([]uint32, 0, len(pending))
	for idx := range pending {
		if inflight[idx] {
			continue
		}
		if _, ok := tried[idx]; ok {
			continue
		}
		keys = append(keys, idx)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, idx := range keys {
		candidates := readyWorkersForChunk(ready, workers, idx)
		if len(candidates) == 0 {
			continue
		}
		return idx, candidates, true
	}
	return 0, nil, false
}

func hasAnyProviderForPending(pending map[uint32]bool, workers []*transferSellerWorker) bool {
	for chunkIdx := range pending {
		for _, w := range workers {
			if w == nil || w.broken {
				continue
			}
			if w.hasChunk(chunkIdx) {
				return true
			}
		}
	}
	return false
}

func cloneRetryHistogram(in map[string]uint32) map[string]uint32 {
	if len(in) == 0 {
		return map[string]uint32{}
	}
	out := make(map[string]uint32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
