package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/obs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	oldproto "github.com/golang/protobuf/proto"
)

type FeePoolStateResult struct {
	GatewayPeerID string             `json:"gateway_pubkey_hex"`
	State         poolcore.StateResp `json:"state"`
}

type FeePoolGatewayParams struct {
	GatewayPeerID string `json:"gateway_pubkey_hex"`
}

func TriggerGatewayFeePoolState(ctx context.Context, rt *Runtime, p FeePoolGatewayParams) (FeePoolStateResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolStateResult{}, fmt.Errorf("runtime not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolStateResult{}, err
	}
	gatewayID := gatewayBusinessID(rt, gw.ID)
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_state_begin", map[string]any{"gateway_pubkey_hex": gatewayID})
	resp, err := callNodePoolSessionState(ctx, rt, gw.ID, "")
	if err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_fee_pool_state_failed", map[string]any{"error": err.Error()})
		return FeePoolStateResult{}, err
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_state_end", map[string]any{"gateway_pubkey_hex": gatewayID, "spend_txid": resp.SpendTxID, "sequence": resp.Sequence})
	return FeePoolStateResult{GatewayPeerID: gatewayID, State: resp}, nil
}

type FeePoolCloseResult struct {
	GatewayPeerID string             `json:"gateway_pubkey_hex"`
	Result        poolcore.CloseResp `json:"result"`
}

// TriggerGatewayFeePoolClose 触发关闭费用池通道并广播 final tx（用于 e2e 清理环境）。
func TriggerGatewayFeePoolClose(ctx context.Context, store *clientDB, rt *Runtime, p FeePoolGatewayParams) (FeePoolCloseResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	if store == nil {
		return FeePoolCloseResult{}, fmt.Errorf("client db is nil")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	gatewayID := gatewayBusinessID(rt, gw.ID)

	sess, ok := rt.getFeePool(gw.ID.String())
	if !ok || sess == nil || sess.SpendTxID == "" {
		return FeePoolCloseResult{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	// 统一走按 spend_txid 的网关状态驱动收尾，避免本地会话金额滞后导致 close 失败。
	return TriggerGatewayFeePoolCloseBySpendTxID(ctx, store, rt, FeePoolCloseBySpendTxIDParams{
		SpendTxID:     sess.SpendTxID,
		GatewayPeerID: gatewayID,
	})
}

func TriggerGatewayFeePoolCloseRuntime(ctx context.Context, rt *Runtime, p FeePoolGatewayParams) (FeePoolCloseResult, error) {
	if rt == nil {
		return FeePoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	return TriggerGatewayFeePoolClose(ctx, rt.store, rt, p)
}

type FeePoolCloseBySpendTxIDParams struct {
	SpendTxID     string `json:"spend_txid"`
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
}

type FeePoolEnsureActiveParams struct {
	GatewayPeerID   string `json:"gateway_pubkey_hex,omitempty"`
	AllowWhenPaused bool   `json:"allow_when_paused,omitempty"`
	RequestedBy     string `json:"requested_by,omitempty"`
}

type FeePoolEnsureActiveResult struct {
	GatewayPeerID string `json:"gateway_pubkey_hex"`
	CommandID     string `json:"command_id,omitempty"`
	Accepted      bool   `json:"accepted"`
	Status        string `json:"status"`
	ErrorCode     string `json:"error_code,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
	StateBefore   string `json:"state_before,omitempty"`
	StateAfter    string `json:"state_after,omitempty"`
}

type FeePoolListenUnderpayProbeParams struct {
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
	SpendTxID     string `json:"spend_txid,omitempty"`
}

type FeePoolListenUnderpayProbeResult struct {
	GatewayPeerID          string                  `json:"gateway_pubkey_hex"`
	SpendTxID              string                  `json:"spend_txid"`
	ExpectedSingleCycleFee uint64                  `json:"expected_single_cycle_fee_satoshi"`
	QuoteStatus            string                  `json:"quote_status"`
	QuotedChargeAmount     uint64                  `json:"quoted_charge_amount_satoshi"`
	QuotedDurationSeconds  uint32                  `json:"quoted_duration_seconds"`
	AttemptChargeAmount    uint64                  `json:"attempt_charge_amount_satoshi"`
	AttemptSequence        uint32                  `json:"attempt_sequence"`
	AttemptServerAmount    uint64                  `json:"attempt_server_amount_satoshi"`
	Response               poolcore.PayConfirmResp `json:"response"`
}

// TriggerGatewayFeePoolEnsureActive 显式触发费用池内核执行 ensure_active 命令（用于 e2e/运维触发）。
func TriggerGatewayFeePoolEnsureActive(ctx context.Context, store *clientDB, rt *Runtime, p FeePoolEnsureActiveParams) (FeePoolEnsureActiveResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolEnsureActiveResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := rt.kernel
	if kernel == nil {
		return FeePoolEnsureActiveResult{}, fmt.Errorf("client kernel not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolEnsureActiveResult{}, err
	}
	gatewayID := gatewayBusinessID(rt, gw.ID)
	requestedBy := strings.TrimSpace(p.RequestedBy)
	if requestedBy == "" {
		requestedBy = "trigger_fee_pool_ensure_active"
	}
	cmd := prepareClientKernelCommand(clientKernelCommand{
		CommandType:     clientKernelCommandFeePoolEnsureActive,
		GatewayPeerID:   gw.ID.String(),
		RequestedBy:     requestedBy,
		AllowWhenPaused: p.AllowWhenPaused,
		Payload: map[string]any{
			"trigger":            "manual_ensure_active",
			"allow_when_paused":  p.AllowWhenPaused,
			"gateway_pubkey_hex": gatewayID,
		},
	})
	res := kernel.dispatch(ctx, cmd)
	return FeePoolEnsureActiveResult{
		GatewayPeerID: gatewayID,
		CommandID:     cmd.CommandID,
		Accepted:      res.Accepted,
		Status:        res.Status,
		ErrorCode:     res.ErrorCode,
		ErrorMessage:  res.ErrorMessage,
		StateBefore:   res.StateBefore,
		StateAfter:    res.StateAfter,
	}, nil
}

func TriggerGatewayFeePoolEnsureActiveRuntime(ctx context.Context, rt *Runtime, p FeePoolEnsureActiveParams) (FeePoolEnsureActiveResult, error) {
	return TriggerGatewayFeePoolEnsureActive(ctx, nil, rt, p)
}

// TriggerGatewayFeePoolListenUnderpayProbe 仅用于 e2e：发送“低于单轮监听费”的 pay_confirm，验证网关拒绝策略。
func TriggerGatewayFeePoolListenUnderpayProbe(ctx context.Context, store *clientDB, rt *Runtime, p FeePoolListenUnderpayProbeParams) (FeePoolListenUnderpayProbeResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("runtime not initialized")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}
	gatewayID := gatewayBusinessID(rt, gw.ID)
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

	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}

	underpay := sess.SingleCycleFeeSatoshi - 1
	listenReq := &contractmessage.ListenCycleReq{
		RequestedDurationSeconds: sess.BillingCycleSeconds,
		ProposedPaymentSatoshi:   listenOfferPaymentSatoshi(rt, sess),
	}
	if sess.BillingCycleSeconds > 0 {
		listenReq.RequestedUntilUnix = time.Now().Add(time.Duration(sess.BillingCycleSeconds) * time.Second).Unix()
	}
	payloadRaw, err := oldproto.Marshal(listenReq)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		Session:       sess,
		GatewayPeerID: gw.ID,
		Route:         string(contractroute.RouteBroadcastV1ListenCycle),
		ContentType:   contractmessage.ContentTypeProto,
		Body:          payloadRaw,
	})
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("request listen quote failed: %w", err)
	}
	nextSeq := quoted.NextSequence
	nextServerAmount := sess.ServerAmount + underpay
	built, err := buildFeePoolUpdatedTxWithProof(feePoolProofArgs{
		Session:             sess,
		ClientActor:         clientActor,
		GatewayPub:          quoted.GatewayPub,
		ServiceQuoteRaw:     quoted.ServiceQuoteRaw,
		ServiceQuote:        quoted.ServiceQuote,
		ChargeReason:        quoted.ChargeReason,
		NextSequence:        nextSeq,
		NextServerAmount:    nextServerAmount,
		ServiceDeadlineUnix: quoted.GrantedServiceDeadlineUnix,
	})
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("build proof update failed: %w", err)
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(built.UpdatedTx, clientActor.PrivKey, quoted.GatewayPub)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, fmt.Errorf("client sign update failed: %w", err)
	}

	req := poolcore.PayConfirmReq{
		ClientID:            rt.runIn.ClientID,
		SpendTxID:           spendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		Fee:                 sess.SpendTxFeeSat,
		ClientSig:           append([]byte(nil), (*clientSig)...),
		ChargeReason:        "listen_cycle_fee",
		ChargeAmountSatoshi: underpay,
		ProofIntent:         append([]byte(nil), built.ProofIntent...),
		SignedProofCommit:   append([]byte(nil), built.SignedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	}
	resp, err := callNodePoolPayConfirm(ctx, rt, gw.ID, req)
	if err != nil {
		return FeePoolListenUnderpayProbeResult{}, err
	}
	return FeePoolListenUnderpayProbeResult{
		GatewayPeerID:          gatewayID,
		SpendTxID:              spendTxID,
		ExpectedSingleCycleFee: sess.SingleCycleFeeSatoshi,
		QuoteStatus:            quoted.QuoteStatus,
		QuotedChargeAmount:     quoted.ServiceQuote.ChargeAmountSatoshi,
		QuotedDurationSeconds:  quoted.GrantedDurationSeconds,
		AttemptChargeAmount:    underpay,
		AttemptSequence:        nextSeq,
		AttemptServerAmount:    nextServerAmount,
		Response:               resp,
	}, nil
}

// TriggerGatewayFeePoolCloseBySpendTxID 按 spend_txid 触发关闭费用池通道并广播 final tx（运维/回收工具用）。
func TriggerGatewayFeePoolCloseBySpendTxID(ctx context.Context, store *clientDB, rt *Runtime, p FeePoolCloseBySpendTxIDParams) (FeePoolCloseResult, error) {
	if rt == nil || rt.Host == nil {
		return FeePoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	if store == nil {
		return FeePoolCloseResult{}, fmt.Errorf("client db is nil")
	}
	spendTxID := strings.TrimSpace(p.SpendTxID)
	if spendTxID == "" {
		return FeePoolCloseResult{}, fmt.Errorf("spend_txid required")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	gatewayID := gatewayBusinessID(rt, gw.ID)

	st, err := callNodePoolSessionState(ctx, rt, gw.ID, spendTxID)
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	if strings.TrimSpace(st.Status) == "not_found" {
		return FeePoolCloseResult{}, fmt.Errorf("session not found by spend_txid: %s", spendTxID)
	}
	if strings.TrimSpace(st.Status) == "closed" {
		finalTxID := strings.ToLower(strings.TrimSpace(st.FinalTxID))
		if finalTxID == "" {
			return FeePoolCloseResult{}, fmt.Errorf("closed session missing final_spend_txid")
		}
		if err := dbRecordFeePoolCloseAccounting(ctx, store, spendTxID, finalTxID, hex.EncodeToString(st.CurrentTx), gatewayID); err != nil {
			return FeePoolCloseResult{}, err
		}
		if sess, ok := rt.getFeePool(gw.ID.String()); ok && sess != nil {
			sess.Status = "closed"
			sess.FinalTxID = finalTxID
			rt.setFeePool(gw.ID.String(), sess)
		}
		return FeePoolCloseResult{
			GatewayPeerID: gatewayID,
			Result: poolcore.CloseResp{
				Success:        true,
				Status:         "closed",
				Broadcasted:    true,
				FinalSpendTxID: finalTxID,
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
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_close_by_spend_txid_begin", map[string]any{
		"gateway_pubkey_hex": gatewayID,
		"spend_txid":         spendTxID,
	})
	resp, err := callNodePoolClose(ctx, rt, gw.ID, poolcore.CloseReq{
		ClientID:     rt.runIn.ClientID,
		SpendTxID:    spendTxID,
		ServerAmount: st.ServerAmountSat,
		Fee:          st.SpendTxFeeSat,
		ClientSig:    append([]byte(nil), (*clientSig)...),
	})
	if err != nil {
		return FeePoolCloseResult{}, err
	}
	finalTxID := strings.ToLower(strings.TrimSpace(resp.FinalSpendTxID))
	if finalTxID == "" {
		return FeePoolCloseResult{}, fmt.Errorf("close response missing final_spend_txid")
	}
	if err := dbRecordFeePoolCloseAccounting(ctx, store, spendTxID, finalTxID, hex.EncodeToString(st.CurrentTx), gatewayID); err != nil {
		return FeePoolCloseResult{}, err
	}
	if sess, ok := rt.getFeePool(gw.ID.String()); ok && sess != nil {
		sess.Status = "closed"
		sess.FinalTxID = finalTxID
		rt.setFeePool(gw.ID.String(), sess)
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_fee_pool_close_by_spend_txid_end", map[string]any{
		"gateway_pubkey_hex": gatewayID,
		"spend_txid":         spendTxID,
		"final_txid":         finalTxID,
		"status":             resp.Status,
	})
	resp.FinalSpendTxID = finalTxID
	return FeePoolCloseResult{GatewayPeerID: gatewayID, Result: resp}, nil
}
