package clientapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	feePoolKernelStateIdle               = "idle"
	feePoolKernelStateActive             = "active"
	feePoolKernelStateRotating           = "rotating"
	feePoolKernelStatePausedInsufficient = "paused_insufficient"
	feePoolKernelStateFailed             = "failed"
)

const (
	feePoolCommandEnsureActive = "ensure_active"
	feePoolCommandCycleTick    = "cycle_tick"
)

var reNeedHave = regexp.MustCompile(`need\s+(\d+)\D+have\s+(\d+)`)

type feePoolKernelCommand struct {
	CommandID       string
	CommandType     string
	GatewayPeer     string
	AggregateID     string
	RequestedBy     string
	RequestedAt     int64
	Payload         map[string]any
	AllowWhenPaused bool
}

type feePoolKernelResult struct {
	Accepted      bool           `json:"accepted"`
	Status        string         `json:"status"`
	ErrorCode     string         `json:"error_code"`
	ErrorMessage  string         `json:"error_message"`
	StateBefore   string         `json:"state_before"`
	StateAfter    string         `json:"state_after"`
	EmittedEvents []string       `json:"emitted_events"`
	Meta          map[string]any `json:"meta,omitempty"`
}

type feePoolKernelGatewayState struct {
	State        string
	PauseReason  string
	PauseNeedSat uint64
	PauseHaveSat uint64
	LastError    string
}

// feePoolKernel 负责费用池业务主状态机：外部只能发命令，状态迁移只在内核发生。
type feePoolKernel struct {
	rt *Runtime

	mu     sync.Mutex
	states map[string]feePoolKernelGatewayState
	locks  map[string]*sync.Mutex
}

func newFeePoolKernel(rt *Runtime) *feePoolKernel {
	return &feePoolKernel{
		rt:     rt,
		states: map[string]feePoolKernelGatewayState{},
		locks:  map[string]*sync.Mutex{},
	}
}

func (k *feePoolKernel) getGatewayLock(gatewayPeerID string) *sync.Mutex {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = map[string]*sync.Mutex{}
	}
	if m, ok := k.locks[gatewayPeerID]; ok {
		return m
	}
	m := &sync.Mutex{}
	k.locks[gatewayPeerID] = m
	return m
}

func (k *feePoolKernel) getState(gatewayPeerID string) feePoolKernelGatewayState {
	k.mu.Lock()
	defer k.mu.Unlock()
	s, ok := k.states[gatewayPeerID]
	if !ok || strings.TrimSpace(s.State) == "" {
		if sess, ok := k.rt.getFeePool(gatewayPeerID); ok && sess != nil && strings.TrimSpace(sess.SpendTxID) != "" && strings.TrimSpace(sess.Status) == "active" {
			s.State = feePoolKernelStateActive
		} else {
			s.State = feePoolKernelStateIdle
		}
		k.states[gatewayPeerID] = s
	}
	return s
}

func (k *feePoolKernel) setState(gatewayPeerID string, s feePoolKernelGatewayState) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.states == nil {
		k.states = map[string]feePoolKernelGatewayState{}
	}
	if strings.TrimSpace(s.State) == "" {
		s.State = feePoolKernelStateIdle
	}
	k.states[gatewayPeerID] = s
}

func (k *feePoolKernel) isPaused(gatewayPeerID string) bool {
	s := k.getState(gatewayPeerID)
	return strings.EqualFold(strings.TrimSpace(s.State), feePoolKernelStatePausedInsufficient)
}

func (k *feePoolKernel) tryResumePausedGateway(ctx context.Context, gw peer.AddrInfo) {
	if k == nil || k.rt == nil || strings.TrimSpace(gw.ID.String()) == "" {
		return
	}
	gwID := gw.ID.String()
	gwBusinessID := gatewayBusinessID(k.rt, gw.ID)
	lock := k.getGatewayLock(gwID)
	lock.Lock()
	defer lock.Unlock()

	st := k.getState(gwID)
	if st.State != feePoolKernelStatePausedInsufficient {
		return
	}
	sum, err := walletUTXOBalanceSatoshi(k.rt)
	if err != nil {
		return
	}
	if st.PauseNeedSat > 0 && sum < st.PauseNeedSat {
		st.PauseHaveSat = sum
		k.setState(gwID, st)
		return
	}
	before := st.State
	st.State = feePoolKernelStateIdle
	st.PauseReason = ""
	st.PauseNeedSat = 0
	st.PauseHaveSat = sum
	st.LastError = ""
	k.setState(gwID, st)
	dbAppendDomainEvent(ctx, runtimeStore(k.rt), domainEventEntry{
		CommandID:     "",
		GatewayPeerID: gwBusinessID,
		EventName:     "fee_pool_resumed_by_wallet_probe",
		StateBefore:   before,
		StateAfter:    st.State,
		Payload: map[string]any{
			"wallet_balance_satoshi": sum,
		},
	})
	obs.Business("bitcast-client", "fee_pool_resume_ready", map[string]any{
		"gateway":                gwBusinessID,
		"wallet_balance_satoshi": sum,
	})
}

func (k *feePoolKernel) dispatch(ctx context.Context, gw peer.AddrInfo, cmd feePoolKernelCommand) feePoolKernelResult {
	if k == nil || k.rt == nil {
		return feePoolKernelResult{Accepted: false, Status: "rejected", ErrorCode: "runtime_not_initialized", ErrorMessage: "runtime not initialized"}
	}
	gwID := strings.TrimSpace(gw.ID.String())
	if gwID == "" {
		return feePoolKernelResult{Accepted: false, Status: "rejected", ErrorCode: "gateway_required", ErrorMessage: "gateway peer id required"}
	}
	gwBusinessID := gatewayBusinessID(k.rt, gw.ID)
	if strings.TrimSpace(cmd.CommandType) == "" {
		return feePoolKernelResult{Accepted: false, Status: "rejected", ErrorCode: "command_type_required", ErrorMessage: "command type required"}
	}
	if strings.TrimSpace(cmd.CommandID) == "" {
		cmd.CommandID = newKernelCommandID()
	}
	if strings.TrimSpace(cmd.RequestedBy) == "" {
		cmd.RequestedBy = "system"
	}
	if cmd.RequestedAt <= 0 {
		cmd.RequestedAt = time.Now().Unix()
	}
	cmd.GatewayPeer = gwBusinessID
	if strings.TrimSpace(cmd.AggregateID) == "" {
		cmd.AggregateID = "gateway:" + gwBusinessID
	}

	startAt := time.Now()
	lock := k.getGatewayLock(gwID)
	lock.Lock()
	defer lock.Unlock()

	beforeState := k.getState(gwID)
	result := feePoolKernelResult{
		Accepted:    true,
		Status:      "applied",
		StateBefore: beforeState.State,
		StateAfter:  beforeState.State,
		Meta:        map[string]any{},
	}
	events := make([]string, 0, 4)
	addEvent := func(name string, payload map[string]any, fromState string, toState string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		events = append(events, name)
		dbAppendDomainEvent(ctx, runtimeStore(k.rt), domainEventEntry{
			CommandID:     cmd.CommandID,
			GatewayPeerID: gwBusinessID,
			EventName:     name,
			StateBefore:   fromState,
			StateAfter:    toState,
			Payload:       payload,
		})
	}
	logEffect := func(effectType string, stage string, status string, err error, payload map[string]any) {
		msg := ""
		if err != nil {
			msg = strings.TrimSpace(err.Error())
		}
		dbAppendEffectLog(ctx, runtimeStore(k.rt), effectLogEntry{
			CommandID:     cmd.CommandID,
			GatewayPeerID: gwBusinessID,
			EffectType:    effectType,
			Stage:         stage,
			Status:        status,
			ErrorMessage:  msg,
			Payload:       payload,
		})
	}
	persist := func(state feePoolKernelGatewayState) feePoolKernelResult {
		result.EmittedEvents = append([]string(nil), events...)
		result.StateAfter = state.State
		dbAppendStateSnapshot(ctx, runtimeStore(k.rt), stateSnapshotEntry{
			CommandID:     cmd.CommandID,
			GatewayPeerID: gwBusinessID,
			State:         state.State,
			PauseReason:   state.PauseReason,
			PauseNeedSat:  state.PauseNeedSat,
			PauseHaveSat:  state.PauseHaveSat,
			LastError:     state.LastError,
			Payload:       map[string]any{"command_type": cmd.CommandType},
		})
		dbAppendCommandJournal(ctx, runtimeStore(k.rt), commandJournalEntry{
			CommandID:     cmd.CommandID,
			CommandType:   cmd.CommandType,
			GatewayPeerID: gwBusinessID,
			AggregateID:   cmd.AggregateID,
			RequestedBy:   cmd.RequestedBy,
			RequestedAt:   cmd.RequestedAt,
			Accepted:      result.Accepted,
			Status:        result.Status,
			ErrorCode:     result.ErrorCode,
			ErrorMessage:  result.ErrorMessage,
			StateBefore:   result.StateBefore,
			StateAfter:    result.StateAfter,
			DurationMS:    time.Since(startAt).Milliseconds(),
			Payload:       cmd.Payload,
			Result:        result,
		})
		return result
	}

	if beforeState.State == feePoolKernelStatePausedInsufficient && !cmd.AllowWhenPaused {
		result.Status = "paused"
		result.ErrorCode = "wallet_insufficient_paused"
		result.ErrorMessage = "fee pool paused due to insufficient wallet balance"
		return persist(beforeState)
	}

	st := beforeState
	switch cmd.CommandType {
	case feePoolCommandEnsureActive:
		info, infoErr := callNodePoolInfo(ctx, k.rt, gw.ID)
		if infoErr != nil {
			st.State = feePoolKernelStateFailed
			st.LastError = infoErr.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_info", infoErr)
			result.Status = "failed"
			result.ErrorCode = "fee_pool_info_failed"
			result.ErrorMessage = infoErr.Error()
			obs.Error("bitcast-client", "fee_pool_info_failed", map[string]any{"gateway": gwID, "error": infoErr.Error()})
			logEffect("rpc", "fee_pool_info", "failed", infoErr, nil)
			addEvent("fee_pool_info_failed", map[string]any{"error": infoErr.Error()}, beforeState.State, st.State)
			return persist(st)
		}
		obs.Business("bitcast-client", "fee_pool_info_ack", map[string]any{
			"gateway":                     gwID,
			"billing_cycle_seconds":       info.BillingCycleSeconds,
			"single_cycle_fee_satoshi":    info.SingleCycleFeeSatoshi,
			"single_publish_fee_satoshi":  info.SinglePublishFeeSatoshi,
			"single_query_fee_satoshi":    info.SingleQueryFeeSatoshi,
			"renew_notify_before_seconds": info.RenewNotifyBeforeSeconds,
			"minimum_pool_amount_satoshi": info.MinimumPoolAmountSatoshi,
			"lock_blocks":                 info.LockBlocks,
			"fee_rate_sat_per_byte":       info.FeeRateSatPerByte,
		})
		addEvent("fee_pool_info_ack", map[string]any{
			"billing_cycle_seconds":       info.BillingCycleSeconds,
			"single_cycle_fee_satoshi":    info.SingleCycleFeeSatoshi,
			"minimum_pool_amount_satoshi": info.MinimumPoolAmountSatoshi,
		}, beforeState.State, beforeState.State)
		logEffect("rpc", "fee_pool_info", "ok", nil, map[string]any{
			"billing_cycle_seconds": info.BillingCycleSeconds,
		})
		// 内核规则：
		// - 余额不足属于“状态判定”，不是 open 执行失败；
		// - 判定不足时直接进入 paused_insufficient，不进入创建流程。
		need, have, probeErr := probeListenOpenNeedAndWallet(k.rt, info)
		if probeErr != nil {
			result.Status = "failed"
			result.ErrorCode = "wallet_probe_failed"
			result.ErrorMessage = probeErr.Error()
			st.State = feePoolKernelStateFailed
			st.LastError = probeErr.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "wallet_probe", probeErr)
			logEffect("wallet", "fee_pool_open_precheck", "failed", probeErr, nil)
			addEvent("fee_pool_open_precheck_failed", map[string]any{"error": probeErr.Error()}, beforeState.State, st.State)
			return persist(st)
		}
		if have < need {
			errMsg := fmt.Sprintf("wallet precheck insufficient: need %d, have %d", need, have)
			errInsufficient := errors.New(errMsg)
			st.State = feePoolKernelStatePausedInsufficient
			st.PauseReason = "wallet_insufficient"
			st.PauseNeedSat = need
			st.PauseHaveSat = have
			st.LastError = errMsg
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_open_precheck", errInsufficient)
			result.Status = "paused"
			result.ErrorCode = "wallet_insufficient"
			result.ErrorMessage = errMsg
			obs.Error("bitcast-client", "fee_pool_insufficient", map[string]any{
				"gateway": gwID,
				"need":    need,
				"have":    have,
				"stage":   "open_precheck",
			})
			logEffect("wallet", "fee_pool_open_precheck", "paused", errInsufficient, map[string]any{"need": need, "have": have})
			addEvent("fee_pool_paused_insufficient", map[string]any{"need": need, "have": have, "stage": "open_precheck"}, beforeState.State, st.State)
			return persist(st)
		}

		sess, err := ensureActiveFeePool(ctx, k.rt, gw, k.rt.runIn.Listen.AutoRenewRounds, info)
		if err != nil {
			if isWalletInsufficientForListen(err) {
				need, have := parseNeedHave(err.Error())
				st.State = feePoolKernelStatePausedInsufficient
				st.PauseReason = "wallet_insufficient"
				st.PauseNeedSat = need
				st.PauseHaveSat = have
				st.LastError = err.Error()
				k.setState(gwID, st)
				recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_open", err)
				result.Status = "paused"
				result.ErrorCode = "wallet_insufficient"
				result.ErrorMessage = err.Error()
				obs.Error("bitcast-client", "fee_pool_insufficient", map[string]any{
					"gateway": gwID,
					"need":    need,
					"have":    have,
					"stage":   "open",
				})
				logEffect("chain", "fee_pool_open", "paused", err, map[string]any{"need": need, "have": have})
				addEvent("fee_pool_paused_insufficient", map[string]any{"need": need, "have": have, "stage": "open"}, beforeState.State, st.State)
				return persist(st)
			}
			st.State = feePoolKernelStateFailed
			st.LastError = err.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_open", err)
			result.Status = "failed"
			result.ErrorCode = "fee_pool_open_failed"
			result.ErrorMessage = err.Error()
			obs.Error("bitcast-client", "fee_pool_open_failed", map[string]any{"gateway": gwID, "error": err.Error()})
			logEffect("chain", "fee_pool_open", "failed", err, nil)
			addEvent("fee_pool_open_failed", map[string]any{"error": err.Error()}, beforeState.State, st.State)
			return persist(st)
		}
		st.State = feePoolKernelStateActive
		st.PauseReason = ""
		st.PauseNeedSat = 0
		st.PauseHaveSat = 0
		st.LastError = ""
		k.setState(gwID, st)
		clearGatewayRuntimeError(k.rt, gw.ID)
		addEvent("fee_pool_active_ready", map[string]any{
			"spend_txid": shortToken(sess.SpendTxID),
			"base_txid":  shortToken(sess.BaseTxID),
		}, beforeState.State, st.State)
		logEffect("chain", "fee_pool_open", "ok", nil, map[string]any{
			"spend_txid": shortToken(sess.SpendTxID),
			"base_txid":  shortToken(sess.BaseTxID),
		})
		result.Meta["billing_cycle_seconds"] = sess.BillingCycleSeconds
		return persist(st)

	case feePoolCommandCycleTick:
		sess, ok := k.rt.getFeePool(gwID)
		if !ok || sess == nil || strings.TrimSpace(sess.SpendTxID) == "" || strings.TrimSpace(sess.Status) != "active" {
			result.Status = "failed"
			result.ErrorCode = "session_missing"
			result.ErrorMessage = "active fee pool session missing"
			st.State = feePoolKernelStateFailed
			st.LastError = result.ErrorMessage
			k.setState(gwID, st)
			addEvent("fee_pool_cycle_skipped_missing_session", map[string]any{}, beforeState.State, st.State)
			return persist(st)
		}
		payErr := payOneListenCycle(ctx, k.rt, gw.ID, sess)
		if payErr == nil {
			st.State = feePoolKernelStateActive
			st.LastError = ""
			k.setState(gwID, st)
			clearGatewayRuntimeError(k.rt, gw.ID)
			addEvent("fee_pool_cycle_paid", map[string]any{
				"spend_txid": shortToken(sess.SpendTxID),
				"sequence":   sess.Sequence,
			}, beforeState.State, st.State)
			logEffect("rpc", "fee_pool_pay", "ok", nil, map[string]any{"sequence": sess.Sequence})
			return persist(st)
		}
		if !strings.Contains(strings.ToLower(payErr.Error()), strings.ToLower(errListenFeePoolRotateRequired.Error())) {
			st.State = feePoolKernelStateFailed
			st.LastError = payErr.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_pay", payErr)
			result.Status = "failed"
			result.ErrorCode = "fee_pool_pay_failed"
			result.ErrorMessage = payErr.Error()
			obs.Error("bitcast-client", "fee_pool_listen_pay_failed", map[string]any{"gateway": gwID, "error": payErr.Error()})
			logEffect("rpc", "fee_pool_pay", "failed", payErr, nil)
			addEvent("fee_pool_pay_failed", map[string]any{"error": payErr.Error()}, beforeState.State, st.State)
			return persist(st)
		}

		st.State = feePoolKernelStateRotating
		k.setState(gwID, st)
		addEvent("fee_pool_rotate_begin", map[string]any{
			"spend_txid": shortToken(sess.SpendTxID),
		}, beforeState.State, st.State)
		info, err := callNodePoolInfo(ctx, k.rt, gw.ID)
		if err != nil {
			st.State = feePoolKernelStateFailed
			st.LastError = err.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_info", err)
			result.Status = "failed"
			result.ErrorCode = "fee_pool_info_failed"
			result.ErrorMessage = err.Error()
			logEffect("rpc", "fee_pool_info", "failed", err, nil)
			return persist(st)
		}
		need, have, probeErr := probeListenOpenNeedAndWallet(k.rt, info)
		if probeErr != nil {
			result.Status = "failed"
			result.ErrorCode = "wallet_probe_failed"
			result.ErrorMessage = probeErr.Error()
			st.State = feePoolKernelStateFailed
			st.LastError = probeErr.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "wallet_probe", probeErr)
			logEffect("wallet", "fee_pool_rotate_precheck", "failed", probeErr, nil)
			addEvent("fee_pool_rotate_precheck_failed", map[string]any{"error": probeErr.Error()}, beforeState.State, st.State)
			return persist(st)
		}
		if have < need {
			errMsg := fmt.Sprintf("wallet precheck insufficient: need %d, have %d", need, have)
			errInsufficient := errors.New(errMsg)
			st.State = feePoolKernelStatePausedInsufficient
			st.PauseReason = "wallet_insufficient"
			st.PauseNeedSat = need
			st.PauseHaveSat = have
			st.LastError = errMsg
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_rotate_precheck", errInsufficient)
			result.Status = "paused"
			result.ErrorCode = "wallet_insufficient"
			result.ErrorMessage = errMsg
			obs.Error("bitcast-client", "fee_pool_insufficient", map[string]any{
				"gateway": gwID,
				"need":    need,
				"have":    have,
				"stage":   "rotate_precheck",
			})
			logEffect("wallet", "fee_pool_rotate_precheck", "paused", errInsufficient, map[string]any{"need": need, "have": have})
			addEvent("fee_pool_paused_insufficient", map[string]any{"need": need, "have": have, "stage": "rotate_precheck"}, beforeState.State, st.State)
			return persist(st)
		}
		next, rotateErr := rotateListenFeePool(ctx, k.rt, gw, sess, k.rt.runIn.Listen.AutoRenewRounds, info)
		if rotateErr != nil {
			if strings.Contains(strings.ToLower(rotateErr.Error()), strings.ToLower(errListenFeePoolStop.Error())) {
				need, have := parseNeedHave(rotateErr.Error())
				st.State = feePoolKernelStatePausedInsufficient
				st.PauseReason = "wallet_insufficient"
				st.PauseNeedSat = need
				st.PauseHaveSat = have
				st.LastError = rotateErr.Error()
				k.setState(gwID, st)
				recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_rotate_open", rotateErr)
				result.Status = "paused"
				result.ErrorCode = "wallet_insufficient"
				result.ErrorMessage = rotateErr.Error()
				obs.Error("bitcast-client", "fee_pool_listen_stopped", map[string]any{"gateway": gwID, "error": rotateErr.Error()})
				obs.Error("bitcast-client", "fee_pool_insufficient", map[string]any{
					"gateway": gwID,
					"need":    need,
					"have":    have,
					"stage":   "rotate",
				})
				logEffect("chain", "fee_pool_rotate_open", "paused", rotateErr, map[string]any{"need": need, "have": have})
				addEvent("fee_pool_paused_insufficient", map[string]any{"need": need, "have": have, "stage": "rotate"}, beforeState.State, st.State)
				return persist(st)
			}
			st.State = feePoolKernelStateFailed
			st.LastError = rotateErr.Error()
			k.setState(gwID, st)
			recordGatewayRuntimeError(k.rt, gw.ID, "fee_pool_rotate_open", rotateErr)
			result.Status = "failed"
			result.ErrorCode = "fee_pool_rotate_failed"
			result.ErrorMessage = rotateErr.Error()
			obs.Error("bitcast-client", "fee_pool_rotate_failed", map[string]any{"gateway": gwID, "error": rotateErr.Error()})
			logEffect("chain", "fee_pool_rotate_open", "failed", rotateErr, nil)
			addEvent("fee_pool_rotate_failed", map[string]any{"error": rotateErr.Error()}, beforeState.State, st.State)
			return persist(st)
		}
		st.State = feePoolKernelStateActive
		st.PauseReason = ""
		st.PauseNeedSat = 0
		st.PauseHaveSat = 0
		st.LastError = ""
		k.setState(gwID, st)
		clearGatewayRuntimeError(k.rt, gw.ID)
		addEvent("fee_pool_rotate_switched", map[string]any{
			"new_spend_txid": shortToken(next.SpendTxID),
		}, beforeState.State, st.State)
		logEffect("chain", "fee_pool_rotate_open", "ok", nil, map[string]any{
			"new_spend_txid": shortToken(next.SpendTxID),
		})
		return persist(st)

	default:
		result.Accepted = false
		result.Status = "rejected"
		result.ErrorCode = "unsupported_command"
		result.ErrorMessage = fmt.Sprintf("unsupported command: %s", cmd.CommandType)
		return persist(beforeState)
	}
}

func parseNeedHave(errMsg string) (uint64, uint64) {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	m := reNeedHave.FindStringSubmatch(msg)
	if len(m) != 3 {
		return 0, 0
	}
	need, _ := strconv.ParseUint(strings.TrimSpace(m[1]), 10, 64)
	have, _ := strconv.ParseUint(strings.TrimSpace(m[2]), 10, 64)
	return need, have
}

func newKernelCommandID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("cmd-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("cmd-%d-%s", time.Now().UnixNano(), strings.ToLower(hex.EncodeToString(b[:])))
}

func shortToken(s string) string {
	v := strings.TrimSpace(s)
	if len(v) <= 12 {
		return v
	}
	return v[:4] + "..." + v[len(v)-4:]
}

func probeListenOpenNeedAndWallet(rt *Runtime, info poolcore.InfoResp) (uint64, uint64, error) {
	if rt == nil {
		return 0, 0, fmt.Errorf("runtime not initialized")
	}
	need, err := listenPoolAmountByRounds(rt.runIn.Listen.AutoRenewRounds, info.SingleCycleFeeSatoshi, 0)
	if err != nil {
		return 0, 0, err
	}
	if info.MinimumPoolAmountSatoshi > 0 && need < info.MinimumPoolAmountSatoshi {
		need = info.MinimumPoolAmountSatoshi
	}
	have, err := walletUTXOBalanceSatoshi(rt)
	if err != nil {
		return 0, 0, err
	}
	return need, have, nil
}

func walletUTXOBalanceSatoshi(rt *Runtime) (uint64, error) {
	if rt == nil {
		return 0, fmt.Errorf("runtime not initialized")
	}
	_, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return 0, err
	}
	_, bal, err := getWalletBalanceFromDB(rt)
	if err != nil {
		return 0, err
	}
	return bal, nil
}

type commandJournalEntry struct {
	CommandID     string
	CommandType   string
	GatewayPeerID string
	AggregateID   string
	RequestedBy   string
	RequestedAt   int64
	Accepted      bool
	Status        string
	ErrorCode     string
	ErrorMessage  string
	StateBefore   string
	StateAfter    string
	DurationMS    int64
	Payload       any
	Result        any
}

type domainEventEntry struct {
	CommandID     string
	GatewayPeerID string
	EventName     string
	StateBefore   string
	StateAfter    string
	Payload       any
}

type stateSnapshotEntry struct {
	CommandID     string
	GatewayPeerID string
	State         string
	PauseReason   string
	PauseNeedSat  uint64
	PauseHaveSat  uint64
	LastError     string
	Payload       any
}

type effectLogEntry struct {
	CommandID     string
	GatewayPeerID string
	EffectType    string
	Stage         string
	Status        string
	ErrorMessage  string
	Payload       any
}

func mustJSON(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
