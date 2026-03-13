package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
)

type FeePoolStateResult struct {
	GatewayPeerID string             `json:"gateway_peer_id"`
	State         dual2of2.StateResp `json:"state"`
}

func TriggerGatewayFeePoolState(ctx context.Context, rt *Runtime) (FeePoolStateResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolStateResult{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return FeePoolStateResult{}, fmt.Errorf("no healthy gateway")
	}
	gw := rt.HealthyGWs[0]
	var resp dual2of2.StateResp
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_state_begin", map[string]any{})
	err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolState, gwSec(rt.rpcTrace), dual2of2.StateReq{ClientID: rt.runIn.ClientID}, &resp)
	if err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_fee_pool_state_failed", map[string]any{"error": err.Error()})
		return FeePoolStateResult{}, err
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_state_end", map[string]any{"gateway_peer_id": gw.ID.String(), "spend_txid": resp.SpendTxID, "sequence": resp.Sequence})
	return FeePoolStateResult{GatewayPeerID: gw.ID.String(), State: resp}, nil
}

type FeePoolCloseResult struct {
	GatewayPeerID string             `json:"gateway_peer_id"`
	Result        dual2of2.CloseResp `json:"result"`
}

// TriggerGatewayFeePoolClose 触发关闭费用池通道并广播 final tx（用于 e2e 清理环境）。
func TriggerGatewayFeePoolClose(ctx context.Context, rt *Runtime) (FeePoolCloseResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return FeePoolCloseResult{}, fmt.Errorf("no healthy gateway")
	}
	gw := rt.HealthyGWs[0]

	sess, ok := rt.getFeePool(gw.ID.String())
	if !ok || sess == nil || sess.SpendTxID == "" {
		return FeePoolCloseResult{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	// 统一走按 spend_txid 的网关状态驱动收尾，避免本地会话金额滞后导致 close 失败。
	return TriggerGatewayFeePoolCloseBySpendTxID(ctx, rt, FeePoolCloseBySpendTxIDParams{
		SpendTxID:     sess.SpendTxID,
		GatewayPeerID: gw.ID.String(),
	})
}

type FeePoolCloseBySpendTxIDParams struct {
	SpendTxID     string `json:"spend_txid"`
	GatewayPeerID string `json:"gateway_peer_id,omitempty"`
}

type FeePoolEnsureActiveParams struct {
	GatewayPeerID   string `json:"gateway_peer_id,omitempty"`
	AllowWhenPaused bool   `json:"allow_when_paused,omitempty"`
	RequestedBy     string `json:"requested_by,omitempty"`
}

type FeePoolEnsureActiveResult struct {
	GatewayPeerID string `json:"gateway_peer_id"`
	Accepted      bool   `json:"accepted"`
	Status        string `json:"status"`
	ErrorCode     string `json:"error_code,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
	StateBefore   string `json:"state_before,omitempty"`
	StateAfter    string `json:"state_after,omitempty"`
}

type FeePoolListenUnderpayProbeParams struct {
	GatewayPeerID string `json:"gateway_peer_id,omitempty"`
	SpendTxID     string `json:"spend_txid,omitempty"`
}

type FeePoolListenUnderpayProbeResult struct {
	GatewayPeerID          string                  `json:"gateway_peer_id"`
	SpendTxID              string                  `json:"spend_txid"`
	ExpectedSingleCycleFee uint64                  `json:"expected_single_cycle_fee_satoshi"`
	AttemptChargeAmount    uint64                  `json:"attempt_charge_amount_satoshi"`
	AttemptSequence        uint32                  `json:"attempt_sequence"`
	AttemptServerAmount    uint64                  `json:"attempt_server_amount_satoshi"`
	Response               dual2of2.PayConfirmResp `json:"response"`
}

// TriggerGatewayFeePoolEnsureActive 显式触发费用池内核执行 ensure_active 命令（用于 e2e/运维触发）。
func TriggerGatewayFeePoolEnsureActive(ctx context.Context, rt *Runtime, p FeePoolEnsureActiveParams) (FeePoolEnsureActiveResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolEnsureActiveResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := ensureClientKernel(rt)
	if kernel == nil {
		return FeePoolEnsureActiveResult{}, fmt.Errorf("client kernel not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolEnsureActiveResult{}, err
	}
	requestedBy := strings.TrimSpace(p.RequestedBy)
	if requestedBy == "" {
		requestedBy = "trigger_fee_pool_ensure_active"
	}
	res := kernel.dispatch(ctx, clientKernelCommand{
		CommandType:     clientKernelCommandFeePoolEnsureActive,
		GatewayPeerID:   gw.ID.String(),
		RequestedBy:     requestedBy,
		AllowWhenPaused: p.AllowWhenPaused,
		Payload: map[string]any{
			"trigger":           "manual_ensure_active",
			"allow_when_paused": p.AllowWhenPaused,
			"gateway_peer_id":   gw.ID.String(),
		},
	})
	return FeePoolEnsureActiveResult{
		GatewayPeerID: gw.ID.String(),
		Accepted:      res.Accepted,
		Status:        res.Status,
		ErrorCode:     res.ErrorCode,
		ErrorMessage:  res.ErrorMessage,
		StateBefore:   res.StateBefore,
		StateAfter:    res.StateAfter,
	}, nil
}

// TriggerGatewayFeePoolListenUnderpayProbe 仅用于 e2e：发送“低于单轮监听费”的 pay_confirm，验证网关拒绝策略。
func TriggerGatewayFeePoolListenUnderpayProbe(ctx context.Context, rt *Runtime, p FeePoolListenUnderpayProbeParams) (FeePoolListenUnderpayProbeResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("runtime not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}
	sess, ok := rt.getFeePool(gw.ID.String())
	if !ok || sess == nil || strings.TrimSpace(sess.SpendTxID) == "" {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	spendTxID := strings.TrimSpace(p.SpendTxID)
	if spendTxID == "" {
		spendTxID = strings.TrimSpace(sess.SpendTxID)
	}
	if !strings.EqualFold(spendTxID, strings.TrimSpace(sess.SpendTxID)) {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("spend_txid does not match local active session")
	}
	if sess.SingleCycleFeeSatoshi <= 1 {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("single_cycle_fee_satoshi must be greater than 1 for underpay probe")
	}
	if strings.TrimSpace(sess.CurrentTxHex) == "" {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("session current tx missing")
	}

	pub := rt.Host.Peerstore().PubKey(gw.ID)
	if pub == nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("missing gateway public key in peerstore")
	}
	raw, err := pub.Raw()
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("read gateway raw pubkey: %w", err)
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("invalid gateway secp256k1 pubkey: %w", err)
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}

	underpay := sess.SingleCycleFeeSatoshi - 1
	nextSeq := sess.Sequence + 1
	nextServerAmount := sess.ServerAmount + underpay
	updatedTx, err := ce.LoadTx(
		sess.CurrentTxHex,
		nil,
		nextSeq,
		nextServerAmount,
		serverPub,
		clientActor.PubKey,
		sess.PoolAmountSat,
	)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("load updated tx failed: %w", err)
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("client sign update failed: %w", err)
	}

	req := dual2of2.PayConfirmReq{
		ClientID:            rt.runIn.ClientID,
		SpendTxID:           spendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		Fee:                 sess.SpendTxFeeSat,
		ClientSig:           append([]byte(nil), (*clientSig)...),
		ChargeReason:        "listen_cycle_fee",
		ChargeAmountSatoshi: underpay,
	}
	var resp dual2of2.PayConfirmResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolPayConfirm, gwSec(rt.rpcTrace), req, &resp); err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}
	return FeePoolListenUnderpayProbeResult{
		GatewayPeerID:          gw.ID.String(),
		SpendTxID:              spendTxID,
		ExpectedSingleCycleFee: sess.SingleCycleFeeSatoshi,
		AttemptChargeAmount:    underpay,
		AttemptSequence:        nextSeq,
		AttemptServerAmount:    nextServerAmount,
		Response:               resp,
	}, nil
}

// TriggerGatewayFeePoolCloseBySpendTxID 按 spend_txid 触发关闭费用池通道并广播 final tx（运维/回收工具用）。
func TriggerGatewayFeePoolCloseBySpendTxID(ctx context.Context, rt *Runtime, p FeePoolCloseBySpendTxIDParams) (FeePoolCloseResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	spendTxID := strings.TrimSpace(p.SpendTxID)
	if spendTxID == "" {
		return FeePoolCloseResult{}, fmt.Errorf("spend_txid required")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolCloseResult{}, err
	}

	var st dual2of2.StateResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolState, gwSec(rt.rpcTrace), dual2of2.StateReq{
		ClientID:  rt.runIn.ClientID,
		SpendTxID: spendTxID,
	}, &st); err != nil {
		return FeePoolCloseResult{}, err
	}
	if strings.TrimSpace(st.Status) == "not_found" {
		return FeePoolCloseResult{}, fmt.Errorf("session not found by spend_txid: %s", spendTxID)
	}
	if strings.TrimSpace(st.Status) == "closed" {
		appendWalletFundFlow(rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + spendTxID,
			FlowType:        "fee_pool",
			RefID:           spendTxID,
			Stage:           "close_settle",
			Direction:       "settle",
			Purpose:         "fee_pool_close",
			AmountSatoshi:   0,
			UsedSatoshi:     int64(st.ServerAmountSat),
			ReturnedSatoshi: int64(st.ClientAmountSat),
			RelatedTxID:     strings.TrimSpace(st.FinalTxID),
			Note:            "already_closed",
			Payload:         st,
		})
		return FeePoolCloseResult{
			GatewayPeerID: gw.ID.String(),
			Result: dual2of2.CloseResp{
				Success:        true,
				Status:         "closed",
				Broadcasted:    true,
				FinalSpendTxID: strings.TrimSpace(st.FinalTxID),
			},
		}, nil
	}
	if strings.TrimSpace(st.Status) == "close_submitted" {
		finalState, waitErr := waitGatewayFeePoolClosedBySpendTxID(ctx, rt, gw.ID.String(), spendTxID)
		if waitErr != nil {
			return FeePoolCloseResult{}, waitErr
		}
		appendWalletFundFlow(rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + spendTxID,
			FlowType:        "fee_pool",
			RefID:           spendTxID,
			Stage:           "close_settle",
			Direction:       "settle",
			Purpose:         "fee_pool_close",
			AmountSatoshi:   0,
			UsedSatoshi:     int64(finalState.ServerAmountSat),
			ReturnedSatoshi: int64(finalState.ClientAmountSat),
			RelatedTxID:     strings.TrimSpace(finalState.FinalTxID),
			Note:            "already_close_submitted",
			Payload:         finalState,
		})
		return FeePoolCloseResult{
			GatewayPeerID: gw.ID.String(),
			Result: dual2of2.CloseResp{
				Success:        true,
				Status:         "closed",
				Broadcasted:    true,
				FinalSpendTxID: strings.TrimSpace(finalState.FinalTxID),
			},
		}, nil
	}
	if len(st.CurrentTx) == 0 {
		return FeePoolCloseResult{}, fmt.Errorf("state.current_tx empty for spend_txid=%s", spendTxID)
	}

	gwPub := rt.Host.Peerstore().PubKey(gw.ID)
	if gwPub == nil {
		return FeePoolCloseResult{}, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := gwPub.Raw()
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return FeePoolCloseResult{}, err
	}

	finalLock := uint32(0xffffffff)
	finalSeq := uint32(0xffffffff)
	finalTx, err := ce.LoadTx(hex.EncodeToString(st.CurrentTx), &finalLock, finalSeq, st.ServerAmountSat, serverPub, clientActor.PubKey, st.PoolAmountSat)
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	clientSig, err := ce.SpendTXDualFeePoolClientSign(finalTx, st.PoolAmountSat, clientActor.PrivKey, serverPub)
	if err != nil {
		return FeePoolCloseResult{}, err
	}

	var resp dual2of2.CloseResp
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_close_by_spend_txid_begin", map[string]any{
		"gateway_peer_id": gw.ID.String(),
		"spend_txid":      spendTxID,
	})
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolClose, gwSec(rt.rpcTrace), dual2of2.CloseReq{
		ClientID:     rt.runIn.ClientID,
		SpendTxID:    spendTxID,
		ServerAmount: st.ServerAmountSat,
		Fee:          st.SpendTxFeeSat,
		ClientSig:    append([]byte(nil), (*clientSig)...),
	}, &resp); err != nil {
		return FeePoolCloseResult{}, err
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_close_by_spend_txid_end", map[string]any{
		"gateway_peer_id": gw.ID.String(),
		"spend_txid":      spendTxID,
		"final_txid":      resp.FinalSpendTxID,
		"status":          resp.Status,
	})
	if resp.Success && strings.TrimSpace(resp.Status) == "closed" {
		appendWalletFundFlow(rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + spendTxID,
			FlowType:        "fee_pool",
			RefID:           spendTxID,
			Stage:           "close_settle",
			Direction:       "settle",
			Purpose:         "fee_pool_close",
			AmountSatoshi:   0,
			UsedSatoshi:     int64(st.ServerAmountSat),
			ReturnedSatoshi: int64(st.ClientAmountSat),
			RelatedTxID:     strings.TrimSpace(resp.FinalSpendTxID),
			Note:            fmt.Sprintf("gateway=%s", gw.ID.String()),
			Payload:         resp,
		})
	}
	if resp.Success && strings.TrimSpace(resp.Status) == "close_submitted" {
		finalState, waitErr := waitGatewayFeePoolClosedBySpendTxID(ctx, rt, gw.ID.String(), spendTxID)
		if waitErr != nil {
			return FeePoolCloseResult{}, waitErr
		}
		resp.Status = "closed"
		resp.FinalSpendTxID = strings.TrimSpace(finalState.FinalTxID)
		appendWalletFundFlow(rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + spendTxID,
			FlowType:        "fee_pool",
			RefID:           spendTxID,
			Stage:           "close_settle",
			Direction:       "settle",
			Purpose:         "fee_pool_close",
			AmountSatoshi:   0,
			UsedSatoshi:     int64(finalState.ServerAmountSat),
			ReturnedSatoshi: int64(finalState.ClientAmountSat),
			RelatedTxID:     strings.TrimSpace(finalState.FinalTxID),
			Note:            fmt.Sprintf("gateway=%s close_submitted_wait_done", gw.ID.String()),
			Payload:         finalState,
		})
	}
	return FeePoolCloseResult{GatewayPeerID: gw.ID.String(), Result: resp}, nil
}

func waitGatewayFeePoolClosedBySpendTxID(ctx context.Context, rt *Runtime, gatewayPeerID string, spendTxID string) (dual2of2.StateResp, error) {
	if rt == nil || rt.Host == nil {
		return dual2of2.StateResp{}, fmt.Errorf("runtime not initialized")
	}
	txid := strings.TrimSpace(spendTxID)
	if txid == "" {
		return dual2of2.StateResp{}, fmt.Errorf("spend_txid required")
	}
	gw, err := pickGatewayForBusiness(rt, strings.TrimSpace(gatewayPeerID))
	if err != nil {
		return dual2of2.StateResp{}, err
	}
	for {
		select {
		case <-ctx.Done():
			return dual2of2.StateResp{}, fmt.Errorf("wait close finalized timeout/canceled: %w", ctx.Err())
		default:
		}
		var st dual2of2.StateResp
		if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolState, gwSec(rt.rpcTrace), dual2of2.StateReq{
			ClientID:  rt.runIn.ClientID,
			SpendTxID: txid,
		}, &st); err != nil {
			return dual2of2.StateResp{}, err
		}
		if strings.TrimSpace(st.Status) == "closed" && strings.TrimSpace(st.FinalTxID) != "" {
			return st, nil
		}
		time.Sleep(800 * time.Millisecond)
	}
}
