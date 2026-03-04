package clientapp

import (
	"sort"
	"sync"
	"time"
)

type fileChunkOrderPolicy interface {
	AddDemand(start, end uint32)
	RemoveDemand(start, end uint32)
	SelectNext(pending map[uint32]bool, inflight map[uint32]bool, completed map[uint32]bool, chunkCount uint32) (uint32, bool)
	SetFocus(start uint32)
	Snapshot() fileChunkOrderSnapshot
}

type demandPrefetchBackfillPolicyConfig struct {
	PrefetchDistance uint32
	DebugLogEnabled  bool
	Logf             func(event string, fields map[string]any)
}

type demandPrefetchBackfillPolicy struct {
	mu sync.Mutex

	cfg demandPrefetchBackfillPolicyConfig

	demand         map[uint32]int
	focusChunk     uint32
	prefetchCursor uint32

	lastSelectedChunk uint32
	lastSelectPhase   string
	lastSelectReason  string
	updatedAtUnix     int64
}

func newDemandPrefetchBackfillPolicy(cfg demandPrefetchBackfillPolicyConfig) *demandPrefetchBackfillPolicy {
	if cfg.PrefetchDistance == 0 {
		cfg.PrefetchDistance = 8
	}
	return &demandPrefetchBackfillPolicy{
		cfg:           cfg,
		demand:        map[uint32]int{},
		updatedAtUnix: time.Now().Unix(),
	}
}

func (p *demandPrefetchBackfillPolicy) AddDemand(start, end uint32) {
	p.mu.Lock()
	if len(p.demand) == 0 || start < p.focusChunk {
		p.focusChunk = start
		p.prefetchCursor = start
	}
	for i := start; i <= end; i++ {
		p.demand[i]++
		if i == ^uint32(0) {
			break
		}
	}
	p.updatedAtUnix = time.Now().Unix()
	p.mu.Unlock()
	p.log("chunk_policy_add_demand", map[string]any{"start_chunk": start, "end_chunk": end})
}

func (p *demandPrefetchBackfillPolicy) RemoveDemand(start, end uint32) {
	p.mu.Lock()
	for i := start; i <= end; i++ {
		n := p.demand[i]
		if n <= 1 {
			delete(p.demand, i)
		} else {
			p.demand[i] = n - 1
		}
		if i == ^uint32(0) {
			break
		}
	}
	p.updatedAtUnix = time.Now().Unix()
	p.mu.Unlock()
	p.log("chunk_policy_remove_demand", map[string]any{"start_chunk": start, "end_chunk": end})
}

func (p *demandPrefetchBackfillPolicy) SelectNext(pending map[uint32]bool, inflight map[uint32]bool, completed map[uint32]bool, chunkCount uint32) (uint32, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1) 当前需求优先。
	demandKeys := make([]uint32, 0, len(p.demand))
	for idx, n := range p.demand {
		if n <= 0 {
			continue
		}
		if !pending[idx] || inflight[idx] || completed[idx] {
			continue
		}
		demandKeys = append(demandKeys, idx)
	}
	sort.Slice(demandKeys, func(i, j int) bool { return demandKeys[i] < demandKeys[j] })
	if len(demandKeys) > 0 {
		ch := demandKeys[0]
		p.focusChunk = ch
		p.prefetchCursor = ch + 1
		p.setLast(ch, "demand", "active_http_range")
		return ch, true
	}

	// 2) 预读区块：从 prefetchCursor 往后固定窗口。
	if chunkCount > 0 {
		start := p.prefetchCursor % chunkCount
		for step := uint32(0); step < p.cfg.PrefetchDistance; step++ {
			idx := (start + step) % chunkCount
			if !pending[idx] || inflight[idx] || completed[idx] {
				continue
			}
			p.prefetchCursor = idx + 1
			p.setLast(idx, "prefetch", "forward_window")
			return idx, true
		}
	}

	// 3) 回补缺口：按索引升序补齐。
	backfill := make([]uint32, 0, len(pending))
	for idx := range pending {
		if inflight[idx] || completed[idx] {
			continue
		}
		backfill = append(backfill, idx)
	}
	sort.Slice(backfill, func(i, j int) bool { return backfill[i] < backfill[j] })
	if len(backfill) > 0 {
		ch := backfill[0]
		p.setLast(ch, "backfill", "lowest_pending_chunk")
		return ch, true
	}
	return 0, false
}

func (p *demandPrefetchBackfillPolicy) Snapshot() fileChunkOrderSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fileChunkOrderSnapshot{
		Name:               "demand_prefetch_backfill",
		ActiveDemandChunks: uint32(len(p.demand)),
		FocusChunk:         p.focusChunk,
		PrefetchCursor:     p.prefetchCursor,
		PrefetchDistance:   p.cfg.PrefetchDistance,
		LastSelectedChunk:  p.lastSelectedChunk,
		LastSelectPhase:    p.lastSelectPhase,
		LastSelectReason:   p.lastSelectReason,
		UpdatedAtUnix:      p.updatedAtUnix,
	}
}

func (p *demandPrefetchBackfillPolicy) SetFocus(start uint32) {
	p.mu.Lock()
	p.focusChunk = start
	p.prefetchCursor = start
	p.updatedAtUnix = time.Now().Unix()
	p.mu.Unlock()
	p.log("chunk_policy_set_focus", map[string]any{"focus_chunk": start})
}

func (p *demandPrefetchBackfillPolicy) setLast(chunk uint32, phase string, reason string) {
	p.lastSelectedChunk = chunk
	p.lastSelectPhase = phase
	p.lastSelectReason = reason
	p.updatedAtUnix = time.Now().Unix()
	p.log("chunk_policy_select", map[string]any{
		"chunk_index": chunk,
		"phase":       phase,
		"reason":      reason,
	})
}

func (p *demandPrefetchBackfillPolicy) log(event string, fields map[string]any) {
	if !p.cfg.DebugLogEnabled || p.cfg.Logf == nil {
		return
	}
	p.cfg.Logf(event, fields)
}
