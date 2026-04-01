package clientapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

type LiveFollowStatus struct {
	StreamURI              string               `json:"stream_uri"`
	StreamID               string               `json:"stream_id"`
	PublisherPubKey        string               `json:"publisher_pubkey"`
	HaveSegmentIndex       int64                `json:"have_segment_index"`
	LastBoughtSegmentIndex uint64               `json:"last_bought_segment_index,omitempty"`
	LastBoughtSeedHash     string               `json:"last_bought_seed_hash,omitempty"`
	LastOutputFilePath     string               `json:"last_output_file_path,omitempty"`
	LastQuoteSellerPubHex  string               `json:"last_quote_seller_pubkey_hex,omitempty"`
	LastDecision           LivePurchaseDecision `json:"last_decision"`
	Status                 string               `json:"status"`
	LastError              string               `json:"last_error,omitempty"`
	UpdatedAtUnix          int64                `json:"updated_at_unix"`
}

type liveAutoBuyResult struct {
	SeedHash       string
	SegmentIndex   uint64
	OutputFilePath string
}

type liveFollowState struct {
	mu     sync.Mutex
	status LiveFollowStatus
	cancel context.CancelFunc
}

func normalizeLiveFollowStatus(st LiveFollowStatus) LiveFollowStatus {
	st.StreamID = strings.ToLower(strings.TrimSpace(st.StreamID))
	st.PublisherPubKey = strings.ToLower(strings.TrimSpace(st.PublisherPubKey))
	st.LastBoughtSeedHash = strings.ToLower(strings.TrimSpace(st.LastBoughtSeedHash))
	st.LastQuoteSellerPubHex = strings.ToLower(strings.TrimSpace(st.LastQuoteSellerPubHex))
	if st.HaveSegmentIndex == 0 && st.LastBoughtSeedHash == "" {
		st.HaveSegmentIndex = -1
	}
	if strings.TrimSpace(st.Status) == "" {
		st.Status = "idle"
	}
	return st
}

func persistLiveFollowStatus(store *clientDB, st LiveFollowStatus) error {
	return dbPersistLiveFollowStatus(context.Background(), store, st)
}

func loadLiveFollowStatus(store *clientDB, streamID string) (LiveFollowStatus, bool, error) {
	return dbLoadLiveFollowStatus(context.Background(), store, streamID)
}

func listRunningLiveFollowStatuses(store *clientDB) ([]LiveFollowStatus, error) {
	return dbListRunningLiveFollowStatuses(context.Background(), store)
}

func (lr *liveRuntime) setFollowStatus(streamID string, update func(*LiveFollowStatus)) {
	if lr == nil {
		return
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	st, ok := lr.follows[streamID]
	if !ok {
		st = &liveFollowState{}
		lr.follows[streamID] = st
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	update(&st.status)
	st.status.UpdatedAtUnix = time.Now().Unix()
}

func (lr *liveRuntime) followStatus(streamID string) (LiveFollowStatus, bool) {
	if lr == nil {
		return LiveFollowStatus{}, false
	}
	lr.mu.RLock()
	st, ok := lr.follows[strings.ToLower(strings.TrimSpace(streamID))]
	lr.mu.RUnlock()
	if !ok {
		return LiveFollowStatus{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return normalizeLiveFollowStatus(st.status), true
}

func (lr *liveRuntime) setFollowCancel(streamID string, cancel context.CancelFunc) {
	if lr == nil {
		return
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	st, ok := lr.follows[streamID]
	if !ok {
		st = &liveFollowState{}
		lr.follows[streamID] = st
	}
	st.cancel = cancel
}

func (lr *liveRuntime) stopFollow(streamID string) bool {
	if lr == nil {
		return false
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	lr.mu.Lock()
	st, ok := lr.follows[streamID]
	lr.mu.Unlock()
	if !ok || st == nil || st.cancel == nil {
		return false
	}
	st.cancel()
	return true
}

func liveAutoBuySegment(ctx context.Context, store *clientDB, rt *Runtime, decision LivePurchaseDecision, snapshot LiveSubscriberSnapshot) (liveAutoBuyResult, error) {
	if rt == nil || rt.Workspace == nil {
		return liveAutoBuyResult{}, fmt.Errorf("runtime not initialized")
	}
	download, err := runDirectDownloadCore(ctx, rt, directDownloadCoreParams{
		SeedHash:           decision.SeedHash,
		DemandChunkCount:   1,
		TransferChunkCount: 0,
		QuoteMaxRetry:      8,
		QuoteInterval:      2 * time.Second,
		MaxChunkPrice:      decision.EstimatedChunkPrice,
		Strategy:           TransferStrategySmart,
	}, directDownloadCoreHooks{})
	if err != nil {
		return liveAutoBuyResult{}, err
	}
	outPath, err := rt.Workspace.SelectLiveSegmentOutputPath(snapshot.StreamID, decision.TargetSegmentIndex, uint64(len(download.Transfer.Data)))
	if err != nil {
		return liveAutoBuyResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return liveAutoBuyResult{}, err
	}
	if err := os.WriteFile(outPath, download.Transfer.Data, 0o644); err != nil {
		return liveAutoBuyResult{}, err
	}
	if _, err := rt.Workspace.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              outPath,
		Seed:                  download.Transfer.Seed,
		AvailableChunkIndexes: contiguousChunkIndexes(download.Transfer.ChunkCount),
		RecommendedFileName:   filepath.Base(outPath),
		MIMEHint:              "",
	}); err != nil {
		return liveAutoBuyResult{}, err
	}
	if err := rt.Workspace.EnforceLiveCacheLimit(rt.runIn.Live.CacheMaxBytes); err != nil {
		return liveAutoBuyResult{}, err
	}
	return liveAutoBuyResult{
		SeedHash:       decision.SeedHash,
		SegmentIndex:   decision.TargetSegmentIndex,
		OutputFilePath: outPath,
	}, nil
}

func TriggerLiveFollowStart(ctx context.Context, store *clientDB, rt *Runtime, rawURI string) (LiveFollowStatus, error) {
	if rt == nil || rt.live == nil {
		return LiveFollowStatus{}, fmt.Errorf("runtime not initialized")
	}
	parsed, err := ParseLiveSubscribeURI(rawURI)
	if err != nil {
		return LiveFollowStatus{}, err
	}
	subRes, err := TriggerLiveSubscribe(ctx, rt, rawURI, rt.runIn.Live.Publish.BroadcastWindow)
	if err != nil {
		return LiveFollowStatus{}, err
	}
	streamID := strings.ToLower(strings.TrimSpace(subRes.StreamID))
	persisted, found, err := dbLoadLiveFollowStatus(ctx, store, streamID)
	if err != nil {
		return LiveFollowStatus{}, err
	}
	tick := time.Duration(rt.runIn.Live.Publish.BroadcastIntervalSec) * time.Second
	if tick <= 0 {
		tick = 3 * time.Second
	}
	scheduler := ensureRuntimeTaskScheduler(rt, store)
	if scheduler == nil {
		return LiveFollowStatus{}, fmt.Errorf("task scheduler not initialized")
	}
	taskName := liveFollowTaskName(streamID)
	registered := false
	if err := scheduler.RegisterOrReplacePeriodicTask(context.Background(), periodicTaskSpec{
		Name:      taskName,
		Owner:     "live_follow",
		Mode:      "dynamic",
		Interval:  tick,
		Immediate: true,
		Run: func(runCtx context.Context, _ string) (map[string]any, error) {
			return runLiveFollowLoop(runCtx, store, rt, streamID)
		},
	}); err != nil {
		return LiveFollowStatus{}, err
	}
	registered = true
	rt.live.setFollowCancel(streamID, func() {
		scheduler.CancelTask(taskName)
	})
	rt.live.setFollowStatus(streamID, func(st *LiveFollowStatus) {
		st.StreamURI = rawURI
		st.StreamID = streamID
		st.PublisherPubKey = parsed.PublisherPubKey
		if found {
			st.HaveSegmentIndex = persisted.HaveSegmentIndex
			st.LastBoughtSegmentIndex = persisted.LastBoughtSegmentIndex
			st.LastBoughtSeedHash = persisted.LastBoughtSeedHash
			st.LastOutputFilePath = persisted.LastOutputFilePath
			st.LastDecision = persisted.LastDecision
		}
		st.Status = "running"
		st.LastError = ""
	})
	st, ok := rt.live.followStatus(streamID)
	if !ok {
		if registered {
			scheduler.CancelTask(taskName)
		}
		return LiveFollowStatus{}, fmt.Errorf("follow state missing")
	}
	if err := dbPersistLiveFollowStatus(ctx, store, st); err != nil {
		if registered {
			scheduler.CancelTask(taskName)
		}
		return LiveFollowStatus{}, err
	}
	return st, nil
}

func runLiveFollowLoop(ctx context.Context, store *clientDB, rt *Runtime, streamID string) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if err := liveFollowOnce(ctx, store, rt, streamID); err != nil {
		rt.live.setFollowStatus(streamID, func(st *LiveFollowStatus) {
			st.Status = "running"
			st.LastError = err.Error()
		})
		if st, ok := rt.live.followStatus(streamID); ok {
			_ = dbPersistLiveFollowStatus(ctx, store, st)
		}
		return nil, err
	}
	return map[string]any{
		"stream_id": streamID,
		"result":    "tick_ok",
	}, nil
}

func liveFollowOnce(ctx context.Context, store *clientDB, rt *Runtime, streamID string) error {
	status, _ := rt.live.followStatus(streamID)
	snap, sellerPeerID, err := discoverLiveSnapshotForFollow(ctx, store, rt, streamID, status.HaveSegmentIndex)
	if err != nil {
		return err
	}
	plan, err := TriggerLivePlan(ctx, rt, LivePlanParams{
		StreamID:         streamID,
		HaveSegmentIndex: status.HaveSegmentIndex,
	})
	if err != nil {
		return err
	}
	decision := plan.Decision
	rt.live.setFollowStatus(streamID, func(st *LiveFollowStatus) {
		st.LastDecision = decision
		st.LastQuoteSellerPubHex = sellerPeerID
	})
	if st, ok := rt.live.followStatus(streamID); ok {
		_ = dbPersistLiveFollowStatus(ctx, store, st)
	}
	if int64(decision.TargetSegmentIndex) <= status.HaveSegmentIndex {
		return nil
	}
	autoBuy := rt.live.autoBuyFn
	if autoBuy == nil {
		autoBuy = liveAutoBuySegment
	}
	res, err := autoBuy(ctx, store, rt, decision, snap)
	if err != nil {
		return err
	}
	rt.live.setFollowStatus(streamID, func(st *LiveFollowStatus) {
		st.HaveSegmentIndex = int64(res.SegmentIndex)
		st.LastBoughtSegmentIndex = res.SegmentIndex
		st.LastBoughtSeedHash = res.SeedHash
		st.LastOutputFilePath = res.OutputFilePath
		st.LastError = ""
		st.Status = "running"
	})
	if st, ok := rt.live.followStatus(streamID); ok {
		_ = dbPersistLiveFollowStatus(ctx, store, st)
	}
	obs.Business("bitcast-client", "live_follow_bought_segment", map[string]any{
		"stream_id":     streamID,
		"segment_index": res.SegmentIndex,
		"seed_hash":     res.SeedHash,
		"output_file":   res.OutputFilePath,
	})
	return nil
}

func discoverLiveSnapshotForFollow(ctx context.Context, store *clientDB, rt *Runtime, streamID string, haveSegmentIndex int64) (LiveSubscriberSnapshot, string, error) {
	if rt != nil && len(rt.HealthyGWs) > 0 {
		window := rt.runIn.Live.Publish.BroadcastWindow
		if window == 0 {
			window = 10
		}
		pub, err := TriggerGatewayPublishLiveDemand(ctx, store, rt, PublishLiveDemandParams{
			StreamID:         streamID,
			HaveSegmentIndex: haveSegmentIndex,
			Window:           window,
		})
		if err == nil && strings.TrimSpace(pub.DemandID) != "" {
			if snap, sellerPeerID, ok := waitBestLiveQuoteSnapshot(ctx, store, rt, streamID, strings.TrimSpace(pub.DemandID)); ok {
				return snap, sellerPeerID, nil
			}
		}
	}
	snap, err := TriggerLiveGetLatest(rt, streamID)
	return snap, "", err
}

func waitBestLiveQuoteSnapshot(ctx context.Context, store *clientDB, rt *Runtime, streamID, demandID string) (LiveSubscriberSnapshot, string, bool) {
	deadline := time.Now().Add(4 * time.Second)
	for {
		quotes, err := TriggerClientListLiveQuotes(ctx, store, demandID)
		if err == nil {
			if snap, sellerPeerID, ok := bestLiveQuoteSnapshot(streamID, quotes); ok {
				return snap, sellerPeerID, true
			}
		}
		if time.Now().After(deadline) {
			return LiveSubscriberSnapshot{}, "", false
		}
		select {
		case <-ctx.Done():
			return LiveSubscriberSnapshot{}, "", false
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func bestLiveQuoteSnapshot(streamID string, quotes []LiveQuoteItem) (LiveSubscriberSnapshot, string, bool) {
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	var best *LiveQuoteItem
	for i := range quotes {
		q := &quotes[i]
		if !strings.EqualFold(strings.TrimSpace(q.StreamID), streamID) || len(q.RecentSegments) == 0 {
			continue
		}
		if best == nil || q.LatestSegmentIndex > best.LatestSegmentIndex || (q.LatestSegmentIndex == best.LatestSegmentIndex && len(q.RecentSegments) > len(best.RecentSegments)) {
			best = q
		}
	}
	if best == nil {
		return LiveSubscriberSnapshot{}, "", false
	}
	recent := make([]LiveSegmentRef, 0, len(best.RecentSegments))
	for _, seg := range best.RecentSegments {
		recent = append(recent, LiveSegmentRef{
			SegmentIndex: seg.SegmentIndex,
			SeedHash:     strings.ToLower(strings.TrimSpace(seg.SeedHash)),
		})
	}
	return LiveSubscriberSnapshot{
		StreamID:       streamID,
		RecentSegments: normalizeLiveSegmentRefs(recent),
		UpdatedAtUnix:  time.Now().Unix(),
	}, strings.ToLower(strings.TrimSpace(best.SellerPubHex)), true
}

func TriggerLiveFollowStop(store *clientDB, rt *Runtime, streamID string) error {
	if store == nil || rt == nil || rt.live == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if !rt.live.stopFollow(streamID) {
		return fmt.Errorf("follow not found")
	}
	if st, ok := rt.live.followStatus(streamID); ok {
		st.Status = "stopped"
		_ = dbPersistLiveFollowStatus(context.Background(), store, st)
	}
	return nil
}

func TriggerLiveFollowStatus(store *clientDB, rt *Runtime, streamID string) (LiveFollowStatus, error) {
	if store == nil || rt == nil || rt.live == nil {
		return LiveFollowStatus{}, fmt.Errorf("runtime not initialized")
	}
	st, ok := rt.live.followStatus(streamID)
	if !ok {
		loaded, found, err := dbLoadLiveFollowStatus(context.Background(), store, streamID)
		if err != nil {
			return LiveFollowStatus{}, err
		}
		if !found {
			return LiveFollowStatus{}, fmt.Errorf("follow not found")
		}
		rt.live.setFollowStatus(streamID, func(dst *LiveFollowStatus) { *dst = loaded })
		return loaded, nil
	}
	return st, nil
}

func restorePersistedLiveFollows(ctx context.Context, store *clientDB, rt *Runtime) {
	if rt == nil || store == nil {
		return
	}
	items, err := dbListRunningLiveFollowStatuses(ctx, store)
	if err != nil {
		obs.Error("bitcast-client", "live_follow_restore_failed", map[string]any{"error": err.Error()})
		return
	}
	for _, it := range items {
		if strings.TrimSpace(it.StreamURI) == "" {
			continue
		}
		if _, err := TriggerLiveFollowStart(ctx, store, rt, it.StreamURI); err != nil {
			obs.Error("bitcast-client", "live_follow_restore_item_failed", map[string]any{
				"stream_id": it.StreamID,
				"error":     err.Error(),
			})
			continue
		}
		obs.Business("bitcast-client", "live_follow_restored", map[string]any{
			"stream_id":          it.StreamID,
			"have_segment_index": it.HaveSegmentIndex,
		})
	}
}

func liveFollowTaskName(streamID string) string {
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if streamID == "" {
		streamID = "unknown"
	}
	return "live_follow_tick:" + streamID
}
