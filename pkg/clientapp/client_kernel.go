package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	clientKernelCommandFeePoolEnsureActive = "feepool.ensure_active"
	clientKernelCommandFeePoolCycleTick    = "feepool.cycle_tick"
	clientKernelCommandFeePoolMaintain     = "feepool.maintain"
	clientKernelCommandLivePlanPurchase    = "live.plan_purchase"
	clientKernelCommandWorkspaceSync       = "workspace.sync"
	clientKernelCommandDirectDownloadCore  = "direct.download_core"
	clientKernelCommandTransferByStrategy  = "direct.transfer_by_strategy"
)

type clientKernelCommand struct {
	CommandID       string
	CommandType     string
	GatewayPeerID   string
	RequestedBy     string
	RequestedAt     int64
	AllowWhenPaused bool
	// TriggerKey 是来源链路键，不是命令自身主键
	// - orchestrator 发起时，TriggerKey = orchestrator.idempotency_key
	// - 非 orchestrator 发起时，TriggerKey = ''
	TriggerKey string
	Payload    map[string]any
}

type clientKernelResult struct {
	Accepted     bool           `json:"accepted"`
	Status       string         `json:"status"`
	ErrorCode    string         `json:"error_code,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
	StateBefore  string         `json:"state_before,omitempty"`
	StateAfter   string         `json:"state_after,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
}

// clientKernel 是客户端统一业务内核入口：外部只发命令，不直接调用域内业务函数。
type clientKernel struct {
	rt      *Runtime
	store   *clientDB
	feePool *feePoolKernel
}

func newClientKernel(rt *Runtime, store *clientDB) *clientKernel {
	return &clientKernel{
		rt:      rt,
		store:   store,
		feePool: newFeePoolKernel(rt, store),
	}
}

// prepareClientKernelCommand 只在命令创建/入口组装时补齐默认值。
// 设计说明：
// - 这里负责把“一个空壳命令”变成可执行命令；
// - 不放到 normalize 里，避免内核层悄悄兜底，掩盖调用方遗漏；
// - 这类默认值属于入口语义，不属于状态清洗。
func prepareClientKernelCommand(cmd clientKernelCommand) clientKernelCommand {
	if strings.TrimSpace(cmd.CommandID) == "" {
		cmd.CommandID = newKernelCommandID()
	}
	if cmd.RequestedAt <= 0 {
		cmd.RequestedAt = time.Now().Unix()
	}
	if strings.TrimSpace(cmd.RequestedBy) == "" {
		cmd.RequestedBy = "system"
	}
	if cmd.Payload == nil {
		cmd.Payload = map[string]any{}
	}
	return cmd
}

// normalizeClientKernelCommand 只做清洗，不补默认值。
func normalizeClientKernelCommand(cmd clientKernelCommand) clientKernelCommand {
	cmd.CommandID = strings.TrimSpace(cmd.CommandID)
	cmd.CommandType = strings.TrimSpace(cmd.CommandType)
	cmd.GatewayPeerID = strings.TrimSpace(cmd.GatewayPeerID)
	cmd.RequestedBy = strings.TrimSpace(cmd.RequestedBy)
	// TriggerKey 不做默认值生成，只做 trim，空就是空
	cmd.TriggerKey = strings.TrimSpace(cmd.TriggerKey)
	return cmd
}

func resolveFeePoolMaintainCommandType(rt *Runtime, gatewayPeerID string) string {
	if rt == nil {
		return feePoolCommandEnsureActive
	}
	sess, ok := rt.getFeePool(strings.TrimSpace(gatewayPeerID))
	if ok && sess != nil && strings.TrimSpace(sess.SpendTxID) != "" && strings.TrimSpace(sess.Status) == "active" {
		return feePoolCommandCycleTick
	}
	return feePoolCommandEnsureActive
}

func (k *clientKernel) rejectWithAudit(cmd clientKernelCommand, gatewayPeerID, aggregateID, stateBefore, stateAfter, errorCode, errorMessage string) clientKernelResult {
	cmd = normalizeClientKernelCommand(cmd)
	if strings.TrimSpace(gatewayPeerID) == "" {
		gatewayPeerID = strings.TrimSpace(cmd.GatewayPeerID)
	}
	if strings.TrimSpace(aggregateID) == "" {
		switch strings.TrimSpace(cmd.CommandType) {
		case clientKernelCommandWorkspaceSync:
			aggregateID = "workspace:default"
		case clientKernelCommandLivePlanPurchase:
			streamID := strings.ToLower(strings.TrimSpace(anyToString(cmd.Payload["stream_id"])))
			if streamID == "" {
				streamID = "unknown"
			}
			aggregateID = "stream:" + streamID
		case clientKernelCommandFeePoolEnsureActive, clientKernelCommandFeePoolCycleTick, clientKernelCommandFeePoolMaintain:
			gw := strings.TrimSpace(cmd.GatewayPeerID)
			if gw == "" {
				gw = "auto"
			}
			aggregateID = "gateway:" + gw
		default:
			aggregateID = strings.TrimSpace(cmd.CommandType)
			if aggregateID == "" {
				aggregateID = "unknown"
			}
		}
	}
	if strings.TrimSpace(stateBefore) == "" {
		stateBefore = "kernel"
	}
	if strings.TrimSpace(stateAfter) == "" {
		stateAfter = stateBefore
	}
	if k != nil && k.rt != nil {
		_ = dbAppendCommandJournal(context.Background(), k.store, commandJournalEntry{
			CommandID:     cmd.CommandID,
			CommandType:   cmd.CommandType,
			GatewayPeerID: strings.TrimSpace(gatewayPeerID),
			AggregateID:   strings.TrimSpace(aggregateID),
			RequestedBy:   cmd.RequestedBy,
			RequestedAt:   cmd.RequestedAt,
			Accepted:      false,
			Status:        "rejected",
			ErrorCode:     strings.TrimSpace(errorCode),
			ErrorMessage:  strings.TrimSpace(errorMessage),
			StateBefore:   strings.TrimSpace(stateBefore),
			StateAfter:    strings.TrimSpace(stateAfter),
			DurationMS:    0,
			TriggerKey:    cmd.TriggerKey, // 传递链路键，reject 路径也要写
			Payload:       cmd.Payload,
			Result: map[string]any{
				"accepted": false,
				"status":   "rejected",
			},
		})
	}
	return clientKernelResult{
		Accepted:     false,
		Status:       "rejected",
		ErrorCode:    strings.TrimSpace(errorCode),
		ErrorMessage: strings.TrimSpace(errorMessage),
		StateBefore:  strings.TrimSpace(stateBefore),
		StateAfter:   strings.TrimSpace(stateAfter),
	}
}

func (k *clientKernel) isFeePoolPaused(gatewayPeerID string) bool {
	if k == nil || k.feePool == nil {
		return false
	}
	return k.feePool.isPaused(gatewayPeerID)
}

func (k *clientKernel) tryResumeFeePoolPausedGateway(ctx context.Context, gwID string) {
	if k == nil || k.feePool == nil || k.rt == nil {
		return
	}
	gw, ok := findGatewayByPeerID(k.rt, gwID)
	if !ok {
		return
	}
	k.feePool.tryResumePausedGateway(ctx, gw)
}

func (k *clientKernel) dispatch(ctx context.Context, cmd clientKernelCommand) clientKernelResult {
	if k == nil || k.rt == nil {
		return clientKernelResult{Accepted: false, Status: "rejected", ErrorCode: "runtime_not_initialized", ErrorMessage: "runtime not initialized"}
	}
	cmd = normalizeClientKernelCommand(prepareClientKernelCommand(cmd))
	if strings.TrimSpace(cmd.CommandType) == "" {
		return k.rejectWithAudit(cmd, "kernel", "kernel", "kernel", "kernel", "command_type_required", "command type required")
	}

	switch cmd.CommandType {
	case clientKernelCommandFeePoolEnsureActive, clientKernelCommandFeePoolCycleTick, clientKernelCommandFeePoolMaintain:
		if k.feePool == nil {
			return k.rejectWithAudit(cmd, cmd.GatewayPeerID, "", "feepool", "feepool", "feepool_kernel_missing", "fee pool kernel not initialized")
		}
		gw, err := pickGatewayForBusiness(k.rt, cmd.GatewayPeerID)
		if err != nil {
			return k.rejectWithAudit(cmd, cmd.GatewayPeerID, "", "feepool", "feepool", "gateway_unavailable", err.Error())
		}
		fpCmd := feePoolKernelCommand{
			CommandID:       cmd.CommandID,
			GatewayPeer:     gw.ID.String(),
			RequestedBy:     cmd.RequestedBy,
			RequestedAt:     cmd.RequestedAt,
			AllowWhenPaused: cmd.AllowWhenPaused,
			Payload:         cmd.Payload,
			TriggerKey:      cmd.TriggerKey, // 显式依赖传递：链路键从外层命令透传
		}
		if cmd.CommandType == clientKernelCommandFeePoolEnsureActive {
			fpCmd.CommandType = feePoolCommandEnsureActive
		} else if cmd.CommandType == clientKernelCommandFeePoolCycleTick {
			fpCmd.CommandType = feePoolCommandCycleTick
		} else {
			// 维护命令语义：只暴露一个入口，由内核入口按当前会话状态决定走 ensure_active 还是 cycle_tick。
			// 这样调度层不需要感知费用池状态，避免无会话时误调 cycle_tick 造成失败风暴。
			fpCmd.CommandType = resolveFeePoolMaintainCommandType(k.rt, gw.ID.String())
		}
		res := k.feePool.dispatch(ctx, gw, fpCmd)
		return clientKernelResult{
			Accepted:     res.Accepted,
			Status:       res.Status,
			ErrorCode:    res.ErrorCode,
			ErrorMessage: res.ErrorMessage,
			StateBefore:  res.StateBefore,
			StateAfter:   res.StateAfter,
			Data:         res.Meta,
		}

	case clientKernelCommandLivePlanPurchase:
		return k.dispatchLivePlanPurchase(ctx, cmd)
	case clientKernelCommandWorkspaceSync:
		return k.dispatchWorkspaceSync(ctx, cmd)

	default:
		return k.rejectWithAudit(cmd, cmd.GatewayPeerID, "", "kernel", "kernel", "unsupported_command", fmt.Sprintf("unsupported command: %s", cmd.CommandType))
	}
}

func (k *clientKernel) dispatchWorkspaceSync(ctx context.Context, cmd clientKernelCommand) clientKernelResult {
	// 设计约束：workspace 扫描由调度层触发，kernel 只负责执行与审计。
	cmd = normalizeClientKernelCommand(cmd)
	if k == nil || k.rt == nil || k.rt.Workspace == nil {
		return k.rejectWithAudit(cmd, "workspace", "workspace:default", "workspace_sync", "workspace_sync", "workspace_not_initialized", "workspace not initialized")
	}
	startAt := time.Now()
	out, err := TriggerWorkspaceSyncOnce(ctx, k.rt)
	status := "applied"
	errCode := ""
	errMsg := ""
	if err != nil {
		status = "failed"
		errCode = "workspace_sync_failed"
		errMsg = err.Error()
	}
	_ = dbAppendCommandJournal(ctx, k.store, commandJournalEntry{
		CommandID:     cmd.CommandID,
		CommandType:   cmd.CommandType,
		GatewayPeerID: "workspace",
		AggregateID:   "workspace:default",
		RequestedBy:   cmd.RequestedBy,
		RequestedAt:   cmd.RequestedAt,
		Accepted:      true,
		Status:        status,
		ErrorCode:     errCode,
		ErrorMessage:  errMsg,
		StateBefore:   "workspace_sync",
		StateAfter:    "workspace_sync",
		DurationMS:    time.Since(startAt).Milliseconds(),
		TriggerKey:    cmd.TriggerKey, // 传递链路键
		Payload:       cmd.Payload,
		Result: map[string]any{
			"status":     status,
			"seed_count": out.SeedCount,
		},
	})
	if err != nil {
		return clientKernelResult{
			Accepted:     true,
			Status:       "failed",
			ErrorCode:    errCode,
			ErrorMessage: errMsg,
			StateBefore:  "workspace_sync",
			StateAfter:   "workspace_sync",
		}
	}
	return clientKernelResult{
		Accepted:    true,
		Status:      "applied",
		StateBefore: "workspace_sync",
		StateAfter:  "workspace_sync",
		Data: map[string]any{
			"seed_count": out.SeedCount,
		},
	}
}

func (k *clientKernel) runDirectDownloadCoreImpl(ctx context.Context, p directDownloadCoreParams, hooks directDownloadCoreHooks) (directDownloadCoreResult, error) {
	if k == nil || k.rt == nil {
		return directDownloadCoreResult{}, fmt.Errorf("runtime not initialized")
	}
	cmdID := newKernelCommandID()
	startAt := time.Now()
	out, err := runDirectDownloadCoreImpl(ctx, k.store, k.rt, p, hooks)
	status := "applied"
	errCode := ""
	errMsg := ""
	if err != nil {
		status = "failed"
		errCode = "direct_download_core_failed"
		errMsg = err.Error()
	}
	_ = dbAppendCommandJournal(ctx, k.store, commandJournalEntry{
		CommandID:     cmdID,
		CommandType:   clientKernelCommandDirectDownloadCore,
		GatewayPeerID: strings.TrimSpace(out.GatewayPeerID),
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
		TriggerKey:    "", // 直接调用 kernel 的命令，非 orchestrator 发起，链路键为空
		Payload: map[string]any{
			"seed_hash":            strings.ToLower(strings.TrimSpace(p.SeedHash)),
			"demand_chunk_count":   p.DemandChunkCount,
			"transfer_chunk_count": p.TransferChunkCount,
			"gateway_pubkey_hex":   strings.TrimSpace(p.GatewayPeerID),
			"strategy":             strings.TrimSpace(p.Strategy),
			"max_chunk_price":      p.MaxChunkPrice,
			"max_seed_price":       p.MaxSeedPrice,
		},
		Result: map[string]any{
			"status":     status,
			"gateway_id": strings.TrimSpace(out.GatewayPeerID),
			"demand_id":  strings.TrimSpace(out.DemandID),
			"file_name":  strings.TrimSpace(out.FileName),
		},
	})
	return out, err
}

func (k *clientKernel) runTransferChunksByStrategy(ctx context.Context, p TransferChunksByStrategyParams) (TransferChunksByStrategyResult, error) {
	if k == nil || k.rt == nil {
		return TransferChunksByStrategyResult{}, fmt.Errorf("runtime not initialized")
	}
	cmdID := newKernelCommandID()
	startAt := time.Now()
	out, err := triggerTransferChunksByStrategyImpl(ctx, k.store, k.rt, p)
	status := "applied"
	errCode := ""
	errMsg := ""
	if err != nil {
		status = "failed"
		errCode = "direct_transfer_by_strategy_failed"
		errMsg = err.Error()
	}
	_ = dbAppendCommandJournal(ctx, k.store, commandJournalEntry{
		CommandID:     cmdID,
		CommandType:   clientKernelCommandTransferByStrategy,
		GatewayPeerID: "direct",
		AggregateID:   "seed:" + strings.ToLower(strings.TrimSpace(p.SeedHash)),
		RequestedBy:   "client_kernel",
		RequestedAt:   time.Now().Unix(),
		Accepted:      true,
		Status:        status,
		ErrorCode:     errCode,
		ErrorMessage:  errMsg,
		StateBefore:   "direct_transfer",
		StateAfter:    "direct_transfer",
		DurationMS:    time.Since(startAt).Milliseconds(),
		TriggerKey:    "", // 直接调用 kernel 的命令，非 orchestrator 发起，链路键为空
		Payload: map[string]any{
			"demand_id":          strings.TrimSpace(p.DemandID),
			"seed_hash":          strings.ToLower(strings.TrimSpace(p.SeedHash)),
			"chunk_count":        p.ChunkCount,
			"arbiter_pubkey_hex": strings.TrimSpace(p.ArbiterPubHex),
			"strategy":           strings.TrimSpace(p.Strategy),
			"pool_amount":        p.PoolAmount,
		},
		Result: map[string]any{
			"status":       status,
			"chunk_count":  out.ChunkCount,
			"seller_count": len(out.Sellers),
			"sha256":       shortToken(strings.TrimSpace(out.SHA256)),
		},
	})
	return out, err
}

func (k *clientKernel) dispatchLivePlanPurchase(ctx context.Context, cmd clientKernelCommand) clientKernelResult {
	cmd = normalizeClientKernelCommand(cmd)
	streamID := strings.ToLower(strings.TrimSpace(anyToString(cmd.Payload["stream_id"])))
	if !isSeedHashHex(streamID) {
		agg := streamID
		if agg == "" {
			agg = "unknown"
		}
		return k.rejectWithAudit(cmd, "live", "stream:"+agg, "live_plan", "live_plan", "invalid_stream_id", "invalid stream_id")
	}
	haveSegmentIndex := anyToInt64(cmd.Payload["have_segment_index"])
	now := time.Now()
	snap, err := TriggerLiveGetLatest(k.rt, streamID)
	if err != nil {
		_ = dbAppendEffectLog(ctx, k.store, effectLogEntry{
			CommandID:     cmd.CommandID,
			GatewayPeerID: "live",
			EffectType:    "live_snapshot",
			Stage:         "query_latest",
			Status:        "failed",
			ErrorMessage:  err.Error(),
			Payload:       map[string]any{"stream_id": streamID},
		})
		_ = dbAppendCommandJournal(ctx, k.store, commandJournalEntry{
			CommandID:     cmd.CommandID,
			CommandType:   cmd.CommandType,
			GatewayPeerID: "live",
			AggregateID:   "stream:" + streamID,
			RequestedBy:   cmd.RequestedBy,
			RequestedAt:   cmd.RequestedAt,
			Accepted:      true,
			Status:        "failed",
			ErrorCode:     "live_snapshot_failed",
			ErrorMessage:  err.Error(),
			StateBefore:   "live_plan",
			StateAfter:    "live_plan",
			DurationMS:    0,
			TriggerKey:    cmd.TriggerKey, // 传递链路键
			Payload:       cmd.Payload,
			Result:        map[string]any{"accepted": true, "status": "failed"},
		})
		return clientKernelResult{
			Accepted:     true,
			Status:       "failed",
			ErrorCode:    "live_snapshot_failed",
			ErrorMessage: err.Error(),
			StateBefore:  "live_plan",
			StateAfter:   "live_plan",
		}
	}
	cfg := k.rt.ConfigSnapshot()
	decision, err := PlanLivePurchase(snap, haveSegmentIndex, LiveBuyerStrategy{
		TargetLagSegments:   cfg.Live.Buyer.TargetLagSegments,
		MaxBudgetPerMinute:  cfg.Live.Buyer.MaxBudgetPerMinute,
		PreferOlderSegments: cfg.Live.Buyer.PreferOlderSegments,
	}, LiveSellerPricing{
		BasePriceSatPer64K:  cfg.Seller.Pricing.LiveBasePriceSatPer64K,
		FloorPriceSatPer64K: cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
		DecayPerMinuteBPS:   cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
	}, now)
	if err != nil {
		_ = dbAppendEffectLog(ctx, k.store, effectLogEntry{
			CommandID:     cmd.CommandID,
			GatewayPeerID: "live",
			EffectType:    "live_plan",
			Stage:         "compute_decision",
			Status:        "failed",
			ErrorMessage:  err.Error(),
			Payload: map[string]any{
				"stream_id":          streamID,
				"have_segment_index": haveSegmentIndex,
			},
		})
		return clientKernelResult{
			Accepted:     true,
			Status:       "failed",
			ErrorCode:    "live_plan_failed",
			ErrorMessage: err.Error(),
			StateBefore:  "live_plan",
			StateAfter:   "live_plan",
		}
	}
	_ = dbAppendDomainEvent(ctx, k.store, domainEventEntry{
		CommandID:     cmd.CommandID,
		GatewayPeerID: "live",
		EventName:     "live_plan_decided",
		StateBefore:   "live_plan",
		StateAfter:    "live_plan",
		Payload: map[string]any{
			"stream_id":            streamID,
			"have_segment_index":   haveSegmentIndex,
			"target_segment_index": decision.TargetSegmentIndex,
			"target_seed_hash":     shortToken(decision.SeedHash),
		},
	})
	_ = dbAppendStateSnapshot(ctx, k.store, stateSnapshotEntry{
		CommandID:     cmd.CommandID,
		GatewayPeerID: "live",
		State:         "live_plan",
		PauseReason:   "",
		PauseNeedSat:  0,
		PauseHaveSat:  0,
		LastError:     "",
		Payload: map[string]any{
			"stream_id":            streamID,
			"have_segment_index":   haveSegmentIndex,
			"target_segment_index": decision.TargetSegmentIndex,
		},
	})
	_ = dbAppendEffectLog(ctx, k.store, effectLogEntry{
		CommandID:     cmd.CommandID,
		GatewayPeerID: "live",
		EffectType:    "live_plan",
		Stage:         "compute_decision",
		Status:        "ok",
		ErrorMessage:  "",
		Payload: map[string]any{
			"stream_id":            streamID,
			"target_segment_index": decision.TargetSegmentIndex,
		},
	})
	out := clientKernelResult{
		Accepted:    true,
		Status:      "applied",
		StateBefore: "live_plan",
		StateAfter:  "live_plan",
		Data: map[string]any{
			"stream_id":          streamID,
			"have_segment_index": haveSegmentIndex,
			"decision":           decision,
		},
	}
	_ = dbAppendCommandJournal(ctx, k.store, commandJournalEntry{
		CommandID:     cmd.CommandID,
		CommandType:   cmd.CommandType,
		GatewayPeerID: "live",
		AggregateID:   "stream:" + streamID,
		RequestedBy:   cmd.RequestedBy,
		RequestedAt:   cmd.RequestedAt,
		Accepted:      true,
		Status:        "applied",
		ErrorCode:     "",
		ErrorMessage:  "",
		StateBefore:   "live_plan",
		StateAfter:    "live_plan",
		DurationMS:    0,
		TriggerKey:    cmd.TriggerKey, // 传递链路键
		Payload:       cmd.Payload,
		Result:        out,
	})
	return out
}

func findGatewayByPeerID(rt *Runtime, gwID string) (peer.AddrInfo, bool) {
	if rt == nil || strings.TrimSpace(gwID) == "" {
		return peer.AddrInfo{}, false
	}
	for _, gw := range snapshotHealthyGateways(rt) {
		if strings.EqualFold(strings.TrimSpace(gw.ID.String()), strings.TrimSpace(gwID)) {
			return gw, true
		}
	}
	return peer.AddrInfo{}, false
}

func anyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

func anyToInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case uint64:
		if x > ^uint64(0)>>1 {
			return int64(^uint64(0) >> 1)
		}
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	default:
		return 0
	}
}
