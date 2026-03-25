package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
	te "github.com/bsv8/MultisigPool/pkg/triple_endpoint"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// 说明：
// - 这里的函数是明确的“测试触发入口”，e2e 只应通过这些入口推进流程。
// - 触发函数尽量保持同步语义：返回时表示该动作已完成或已明确失败。
// - 所有业务消息必须走 libp2p；HTTP 只允许调用这些触发入口，不应内嵌业务协议实现。

type WorkspaceSeed struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
	SeedPrice  uint64 `json:"seed_price"`
	ChunkPrice uint64 `json:"chunk_price"`
}

type WorkspaceSyncResult struct {
	SeedCount int             `json:"seed_count"`
	Seeds     []WorkspaceSeed `json:"seeds,omitempty"`
}

// TriggerWorkspaceSyncOnce 触发一次 workspace 扫描与同步（用于 e2e）。
func TriggerWorkspaceSyncOnce(ctx context.Context, rt *Runtime) (WorkspaceSyncResult, error) {
	if rt == nil || rt.Workspace == nil {
		return WorkspaceSyncResult{}, fmt.Errorf("runtime not initialized")
	}

	obs.Business("bitcast-client", "evt_trigger_workspace_sync_once_begin", map[string]any{})
	seeds, err := rt.Workspace.SyncOnce(ctx)
	if err != nil {
		obs.Error("bitcast-client", "evt_trigger_workspace_sync_once_failed", map[string]any{"error": err.Error()})
		return WorkspaceSyncResult{}, err
	}

	out := make([]WorkspaceSeed, 0, len(seeds))
	for _, s := range seeds {
		out = append(out, WorkspaceSeed{
			SeedHash:   strings.ToLower(strings.TrimSpace(s.SeedHash)),
			ChunkCount: s.ChunkCount,
			SeedPrice:  s.SeedPrice,
			ChunkPrice: s.ChunkPrice,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SeedHash < out[j].SeedHash })

	obs.Business("bitcast-client", "evt_trigger_workspace_sync_once_end", map[string]any{"seed_count": len(out)})
	return WorkspaceSyncResult{SeedCount: len(out), Seeds: out}, nil
}

type PublishDemandParams struct {
	SeedHash      string `json:"seed_hash"`
	ChunkCount    uint32 `json:"chunk_count"`
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
}

type PublishDemandBatchItem struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
}

type PublishDemandBatchParams struct {
	Items         []PublishDemandBatchItem `json:"items,omitempty"`
	GatewayPeerID string                   `json:"gateway_pubkey_hex,omitempty"`
}

type PublishLiveDemandParams struct {
	StreamID         string `json:"stream_id"`
	HaveSegmentIndex int64  `json:"have_segment_index"`
	Window           uint32 `json:"window"`
	GatewayPeerID    string `json:"gateway_pubkey_hex,omitempty"`
}

type LivePlanParams struct {
	StreamID         string `json:"stream_id"`
	HaveSegmentIndex int64  `json:"have_segment_index"`
}

type LivePlanResult struct {
	StreamID         string               `json:"stream_id"`
	HaveSegmentIndex int64                `json:"have_segment_index"`
	Decision         LivePurchaseDecision `json:"decision"`
}

// TriggerLivePlan 触发直播价速策略命令，统一走客户端业务内核。
func TriggerLivePlan(ctx context.Context, rt *Runtime, p LivePlanParams) (LivePlanResult, error) {
	if rt == nil || rt.Host == nil {
		return LivePlanResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := ensureClientKernel(rt)
	if kernel == nil {
		return LivePlanResult{}, fmt.Errorf("client kernel not initialized")
	}
	streamID := strings.ToLower(strings.TrimSpace(p.StreamID))
	res := kernel.dispatch(ctx, clientKernelCommand{
		CommandType: clientKernelCommandLivePlanPurchase,
		RequestedBy: "trigger_live_plan",
		Payload: map[string]any{
			"stream_id":          streamID,
			"have_segment_index": p.HaveSegmentIndex,
		},
	})
	if !res.Accepted || strings.TrimSpace(res.Status) != "applied" {
		msg := strings.TrimSpace(res.ErrorMessage)
		if msg == "" {
			msg = strings.TrimSpace(res.ErrorCode)
		}
		if msg == "" {
			msg = "live plan failed"
		}
		return LivePlanResult{}, fmt.Errorf("%s", msg)
	}
	rawDecision, ok := res.Data["decision"]
	if !ok {
		return LivePlanResult{}, fmt.Errorf("live plan decision missing")
	}
	dec, ok := rawDecision.(LivePurchaseDecision)
	if !ok {
		return LivePlanResult{}, fmt.Errorf("live plan decision invalid")
	}
	return LivePlanResult{
		StreamID:         streamID,
		HaveSegmentIndex: p.HaveSegmentIndex,
		Decision:         dec,
	}, nil
}

func TriggerGatewayPublishDemand(ctx context.Context, rt *Runtime, p PublishDemandParams) (dual2of2.DemandPublishPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("no healthy gateway")
	}

	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" || p.ChunkCount == 0 {
		return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("invalid params")
	}

	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}

	var info dual2of2.InfoResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolInfo, gwSec(rt.rpcTrace), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}
	if kernel := ensureClientKernel(rt); kernel != nil {
		kres := kernel.dispatch(ctx, clientKernelCommand{
			CommandType:   clientKernelCommandFeePoolEnsureActive,
			GatewayPeerID: gw.ID.String(),
			RequestedBy:   "trigger_publish_demand",
			Payload: map[string]any{
				"trigger":     "demand_publish",
				"seed_hash":   seedHash,
				"chunk_count": p.ChunkCount,
			},
			AllowWhenPaused: true,
		})
		if kres.Status != "applied" {
			if strings.TrimSpace(kres.ErrorMessage) != "" {
				return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.ErrorMessage)
			}
			return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.Status)
		}
	}
	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	charge := info.SinglePublishFeeSatoshi
	if charge == 0 {
		charge = 1
	}

	gwPub := rt.Host.Peerstore().PubKey(gw.ID)
	if gwPub == nil {
		return dual2of2.DemandPublishPaidResp{}, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := gwPub.Raw()
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}
	nextSeq := session.Sequence + 1
	nextServerAmount := session.ServerAmount + charge
	updatedTx, err := ce.LoadTx(session.CurrentTxHex, nil, nextSeq, nextServerAmount, serverPub, clientActor.PubKey, session.PoolAmountSat)
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return dual2of2.DemandPublishPaidResp{}, err
	}

	req := dual2of2.DemandPublishPaidReq{
		ClientID:            rt.runIn.ClientID,
		SeedHash:            seedHash,
		ChunkCount:          p.ChunkCount,
		BuyerAddrs:          localAdvertiseAddrs(rt),
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		ChargeAmountSatoshi: charge,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        "demand_publish_fee",
	}
	var resp dual2of2.DemandPublishPaidResp
	obs.Business("bitcast-client", "evt_trigger_gateway_demand_publish_begin", map[string]any{"seed_hash": seedHash, "chunk_count": p.ChunkCount})
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoDemandPublishPaid, gwSec(rt.rpcTrace), req, &resp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_demand_publish_failed", map[string]any{"error": err.Error()})
		return dual2of2.DemandPublishPaidResp{}, err
	}
	if err := validateDemandPublishPaidResp(resp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_demand_publish_failed", map[string]any{
			"error":   err.Error(),
			"status":  strings.TrimSpace(resp.Status),
			"success": resp.Success,
		})
		return dual2of2.DemandPublishPaidResp{}, err
	}
	if resp.Success {
		// 发布扣费成功后必须推进本地会话，否则下次请求会重复旧 sequence/server_amount。
		applyFeePoolChargeToSession(session, nextSeq, nextServerAmount, updatedTx.Hex())
		appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + session.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           session.SpendTxID,
			Stage:           "use_publish",
			Direction:       "out",
			Purpose:         "demand_publish_fee",
			AmountSatoshi:   -int64(charge),
			UsedSatoshi:     int64(charge),
			ReturnedSatoshi: 0,
			RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
			Note:            fmt.Sprintf("demand_id=%s", strings.TrimSpace(resp.DemandID)),
			Payload:         resp,
		})
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_demand_publish_end", map[string]any{
		"demand_id": resp.DemandID,
		"status":    resp.Status,
		"success":   resp.Success,
		"error":     strings.TrimSpace(resp.Error),
	})
	return resp, nil
}

func TriggerGatewayPublishDemandBatch(ctx context.Context, rt *Runtime, p PublishDemandBatchParams) (dual2of2.DemandPublishBatchPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	items, err := normalizeDemandBatchItems(p.Items)
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}

	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	var info dual2of2.InfoResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolInfo, gwSec(rt.rpcTrace), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	if kernel := ensureClientKernel(rt); kernel != nil {
		payloadItems := make([]map[string]any, 0, len(items))
		for _, item := range items {
			payloadItems = append(payloadItems, map[string]any{
				"seed_hash":   item.SeedHash,
				"chunk_count": item.ChunkCount,
			})
		}
		kres := kernel.dispatch(ctx, clientKernelCommand{
			CommandType:   clientKernelCommandFeePoolEnsureActive,
			GatewayPeerID: gw.ID.String(),
			RequestedBy:   "trigger_publish_demand_batch",
			Payload: map[string]any{
				"trigger":    "demand_publish_batch",
				"item_count": len(items),
				"items":      payloadItems,
			},
			AllowWhenPaused: true,
		})
		if kres.Status != "applied" {
			if strings.TrimSpace(kres.ErrorMessage) != "" {
				return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.ErrorMessage)
			}
			return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.Status)
		}
	}
	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	charge := info.SinglePublishFeeSatoshi
	if charge == 0 {
		charge = 1
	}
	gwPub := rt.Host.Peerstore().PubKey(gw.ID)
	if gwPub == nil {
		return dual2of2.DemandPublishBatchPaidResp{}, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := gwPub.Raw()
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	nextSeq := session.Sequence + 1
	nextServerAmount := session.ServerAmount + charge
	updatedTx, err := ce.LoadTx(session.CurrentTxHex, nil, nextSeq, nextServerAmount, serverPub, clientActor.PubKey, session.PoolAmountSat)
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	reqItems := make([]*dual2of2.DemandPublishBatchPaidItem, 0, len(items))
	for _, item := range items {
		reqItems = append(reqItems, &dual2of2.DemandPublishBatchPaidItem{
			SeedHash:   item.SeedHash,
			ChunkCount: item.ChunkCount,
		})
	}
	req := dual2of2.DemandPublishBatchPaidReq{
		ClientID:            rt.runIn.ClientID,
		Items:               reqItems,
		BuyerAddrs:          localAdvertiseAddrs(rt),
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		ChargeAmountSatoshi: charge,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        "demand_publish_batch_fee",
	}
	var resp dual2of2.DemandPublishBatchPaidResp
	obs.Business("bitcast-client", "evt_trigger_gateway_demand_publish_batch_begin", map[string]any{"item_count": len(items)})
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoDemandPublishBatchPaid, gwSec(rt.rpcTrace), req, &resp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_demand_publish_batch_failed", map[string]any{"error": err.Error()})
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	if err := validateDemandPublishBatchPaidResp(resp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_gateway_demand_publish_batch_failed", map[string]any{
			"error":   err.Error(),
			"status":  strings.TrimSpace(resp.Status),
			"success": resp.Success,
		})
		return dual2of2.DemandPublishBatchPaidResp{}, err
	}
	if resp.Success {
		applyFeePoolChargeToSession(session, nextSeq, nextServerAmount, updatedTx.Hex())
		demandIDs := make([]string, 0, len(resp.Items))
		for _, item := range resp.Items {
			if item == nil {
				continue
			}
			if id := strings.TrimSpace(item.DemandID); id != "" {
				demandIDs = append(demandIDs, id)
			}
		}
		appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + session.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           session.SpendTxID,
			Stage:           "use_publish",
			Direction:       "out",
			Purpose:         "demand_publish_batch_fee",
			AmountSatoshi:   -int64(charge),
			UsedSatoshi:     int64(charge),
			ReturnedSatoshi: 0,
			RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
			Note:            fmt.Sprintf("published=%d demand_ids=%s", len(demandIDs), strings.Join(demandIDs, ",")),
			Payload:         resp,
		})
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_demand_publish_batch_end", map[string]any{
		"published_count": resp.PublishedCount,
		"status":          resp.Status,
		"success":         resp.Success,
		"error":           strings.TrimSpace(resp.Error),
	})
	return resp, nil
}

func TriggerGatewayPublishLiveDemand(ctx context.Context, rt *Runtime, p PublishLiveDemandParams) (dual2of2.LiveDemandPublishPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	streamID := strings.ToLower(strings.TrimSpace(p.StreamID))
	if !isSeedHashHex(streamID) || p.Window == 0 {
		return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("invalid params")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	var info dual2of2.InfoResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolInfo, gwSec(rt.rpcTrace), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	if kernel := ensureClientKernel(rt); kernel != nil {
		kres := kernel.dispatch(ctx, clientKernelCommand{
			CommandType:   clientKernelCommandFeePoolEnsureActive,
			GatewayPeerID: gw.ID.String(),
			RequestedBy:   "trigger_publish_live_demand",
			Payload: map[string]any{
				"trigger":            "live_demand_publish",
				"stream_id":          streamID,
				"have_segment_index": p.HaveSegmentIndex,
				"window":             p.Window,
			},
			AllowWhenPaused: true,
		})
		if kres.Status != "applied" {
			if strings.TrimSpace(kres.ErrorMessage) != "" {
				return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.ErrorMessage)
			}
			return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.Status)
		}
	}
	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	charge := info.SinglePublishFeeSatoshi
	if charge == 0 {
		charge = 1
	}
	gwPub := rt.Host.Peerstore().PubKey(gw.ID)
	if gwPub == nil {
		return dual2of2.LiveDemandPublishPaidResp{}, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := gwPub.Raw()
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	nextSeq := session.Sequence + 1
	nextServerAmount := session.ServerAmount + charge
	updatedTx, err := ce.LoadTx(session.CurrentTxHex, nil, nextSeq, nextServerAmount, serverPub, clientActor.PubKey, session.PoolAmountSat)
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	req := dual2of2.LiveDemandPublishPaidReq{
		ClientID:            rt.runIn.ClientID,
		StreamID:            streamID,
		HaveSegmentIndex:    p.HaveSegmentIndex,
		Window:              p.Window,
		BuyerAddrs:          localAdvertiseAddrs(rt),
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		ChargeAmountSatoshi: charge,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        "live_demand_publish_fee",
	}
	var resp dual2of2.LiveDemandPublishPaidResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoLiveDemandPublishPaid, gwSec(rt.rpcTrace), req, &resp); err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	if err := validateLiveDemandPublishPaidResp(resp); err != nil {
		return dual2of2.LiveDemandPublishPaidResp{}, err
	}
	if resp.Success {
		// 直播需求发布同样会推进费用池状态，必须同步回写本地会话。
		applyFeePoolChargeToSession(session, nextSeq, nextServerAmount, updatedTx.Hex())
		appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + session.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           session.SpendTxID,
			Stage:           "use_live_publish",
			Direction:       "out",
			Purpose:         "live_demand_publish_fee",
			AmountSatoshi:   -int64(charge),
			UsedSatoshi:     int64(charge),
			ReturnedSatoshi: 0,
			RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
			Note:            fmt.Sprintf("live_demand_id=%s", strings.TrimSpace(resp.DemandID)),
			Payload:         resp,
		})
	}
	return resp, nil
}

// applyFeePoolChargeToSession 在扣费成功后推进本地费用池会话。
// 设计约束：以“池总额 - 服务器额 - fee”回算 client_amount，避免累计误差。
func applyFeePoolChargeToSession(session *feePoolSession, nextSeq uint32, nextServerAmount uint64, updatedTxHex string) {
	if session == nil {
		return
	}
	session.Sequence = nextSeq
	session.ServerAmount = nextServerAmount
	session.CurrentTxHex = strings.TrimSpace(updatedTxHex)
	if session.PoolAmountSat >= session.ServerAmount+session.SpendTxFeeSat {
		session.ClientAmount = session.PoolAmountSat - session.ServerAmount - session.SpendTxFeeSat
		return
	}
	session.ClientAmount = 0
}

// validateDemandPublishPaidResp 统一校验网关 publish_demand 业务响应。
// 设计约束：
// - RPC 成功 != 业务成功，必须检查 success 位；
// - success=true 时 demand_id 必须存在，否则后续 list quotes 必然失败。
func validateDemandPublishPaidResp(resp dual2of2.DemandPublishPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish demand failed"
		}
		return fmt.Errorf("gateway demand publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if strings.TrimSpace(resp.DemandID) == "" {
		return fmt.Errorf("gateway demand publish returned empty demand_id")
	}
	return nil
}

func validateDemandPublishBatchPaidResp(resp dual2of2.DemandPublishBatchPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish demand batch failed"
		}
		return fmt.Errorf("gateway demand batch publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if len(resp.Items) == 0 {
		return fmt.Errorf("gateway demand batch publish returned empty items")
	}
	for _, item := range resp.Items {
		if item == nil {
			return fmt.Errorf("gateway demand batch publish returned nil item")
		}
		if strings.TrimSpace(item.DemandID) == "" {
			return fmt.Errorf("gateway demand batch publish returned empty demand_id")
		}
	}
	return nil
}

// validateLiveDemandPublishPaidResp 统一校验网关 publish_live_demand 业务响应。
func validateLiveDemandPublishPaidResp(resp dual2of2.LiveDemandPublishPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish live demand failed"
		}
		return fmt.Errorf("gateway live demand publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if strings.TrimSpace(resp.DemandID) == "" {
		return fmt.Errorf("gateway live demand publish returned empty demand_id")
	}
	return nil
}

func normalizeDemandBatchItems(items []PublishDemandBatchItem) ([]PublishDemandBatchItem, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("invalid params")
	}
	out := make([]PublishDemandBatchItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		seedHash := strings.ToLower(strings.TrimSpace(item.SeedHash))
		if seedHash == "" || item.ChunkCount == 0 {
			return nil, fmt.Errorf("invalid params")
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, PublishDemandBatchItem{
			SeedHash:   seedHash,
			ChunkCount: item.ChunkCount,
		})
	}
	return out, nil
}

func pickGatewayForBusiness(rt *Runtime, gatewayPeerID string) (peer.AddrInfo, error) {
	if rt == nil {
		return peer.AddrInfo{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("no healthy gateway")
	}
	override := strings.TrimSpace(gatewayPeerID)
	if override == "" {
		return rt.HealthyGWs[0], nil
	}
	for _, gw := range rt.HealthyGWs {
		if strings.EqualFold(gw.ID.String(), override) || strings.EqualFold(gatewayBusinessID(rt, gw.ID), override) {
			return gw, nil
		}
	}
	return peer.AddrInfo{}, fmt.Errorf("gateway_pubkey_hex not connected: %s", override)
}

// gatewayBusinessID 返回业务层统一网关 ID（优先使用配置中的网关公钥）。
// 说明：内部连接仍以 libp2p peer.ID 路由；对外观测/触发参数统一为公钥 ID。
func gatewayBusinessID(rt *Runtime, pid peer.ID) string {
	peerID := strings.TrimSpace(pid.String())
	if peerID == "" || rt == nil {
		return peerID
	}
	for _, g := range rt.runIn.Network.Gateways {
		ai, err := parseAddr(g.Addr)
		if err != nil || ai == nil || ai.ID != pid {
			continue
		}
		pubkey := strings.ToLower(strings.TrimSpace(g.Pubkey))
		if pubkey != "" {
			return pubkey
		}
		break
	}
	return peerID
}

type DirectQuoteParams struct {
	DemandID                string   `json:"demand_id"`
	BuyerPeerID             string   `json:"buyer_pubkey_hex"`
	BuyerAddrs              []string `json:"buyer_addrs"`
	SeedPrice               uint64   `json:"seed_price"`
	ChunkPrice              uint64   `json:"chunk_price"`
	ChunkCount              uint32   `json:"chunk_count"`
	FileSize                uint64   `json:"file_size"`
	ExpiresAtUnix           int64    `json:"expires_at_unix"`
	RecommendedFileName     string   `json:"recommended_file_name,omitempty"`
	MIMEHint                string   `json:"mime_hint,omitempty"`
	ArbiterPeerIDs          []string `json:"arbiter_pubkey_hexes,omitempty"`
	AvailableChunkBitmapHex string   `json:"available_chunk_bitmap_hex,omitempty"`
}

func TriggerClientSubmitDirectQuote(ctx context.Context, seller *Runtime, p DirectQuoteParams) error {
	if seller == nil || seller.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return submitDirectQuote(ctx, seller.Host, seller.rpcTrace, p)
}

type DirectQuoteItem struct {
	DemandID                string   `json:"demand_id"`
	SellerPeerID            string   `json:"seller_pubkey_hex"`
	SeedPrice               uint64   `json:"seed_price"`
	ChunkPrice              uint64   `json:"chunk_price"`
	ChunkCount              uint32   `json:"chunk_count"`
	FileSize                uint64   `json:"file_size"`
	ExpiresAtUnix           int64    `json:"expires_at_unix"`
	RecommendedFileName     string   `json:"recommended_file_name,omitempty"`
	MIMEHint                string   `json:"mime_hint,omitempty"`
	SellerArbiterPeerIDs    []string `json:"seller_arbiter_pubkey_hexes,omitempty"`
	AvailableChunkBitmapHex string   `json:"available_chunk_bitmap_hex,omitempty"`
	AvailableChunkIndexes   []uint32 `json:"available_chunk_indexes,omitempty"`
}

type LiveQuoteSegment struct {
	SegmentIndex uint64 `json:"segment_index"`
	SeedHash     string `json:"seed_hash"`
}

type LiveQuoteParams struct {
	DemandID           string             `json:"demand_id"`
	BuyerPeerID        string             `json:"buyer_pubkey_hex"`
	BuyerAddrs         []string           `json:"buyer_addrs"`
	StreamID           string             `json:"stream_id"`
	LatestSegmentIndex uint64             `json:"latest_segment_index"`
	RecentSegments     []LiveQuoteSegment `json:"recent_segments"`
	ExpiresAtUnix      int64              `json:"expires_at_unix"`
}

type LiveQuoteItem struct {
	DemandID           string             `json:"demand_id"`
	SellerPeerID       string             `json:"seller_pubkey_hex"`
	StreamID           string             `json:"stream_id"`
	LatestSegmentIndex uint64             `json:"latest_segment_index"`
	RecentSegments     []LiveQuoteSegment `json:"recent_segments"`
	ExpiresAtUnix      int64              `json:"expires_at_unix"`
}

func TriggerClientListDirectQuotes(ctx context.Context, rt *Runtime, demandID string) ([]DirectQuoteItem, error) {
	_ = ctx
	if rt == nil || rt.DB == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	demandID = strings.TrimSpace(demandID)
	if demandID == "" {
		return nil, fmt.Errorf("demand_id required")
	}
	rows, err := rt.DB.Query(`SELECT demand_id,seller_pubkey_hex,seed_price,chunk_price,chunk_count,file_size,expires_at_unix,recommended_file_name,mime_hint,seller_arbiter_pubkey_hexes_json,available_chunk_bitmap_hex FROM direct_quotes WHERE demand_id=? ORDER BY created_at_unix ASC`, demandID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DirectQuoteItem, 0, 4)
	now := time.Now().Unix()
	for rows.Next() {
		var it DirectQuoteItem
		var sellerArbitersJSON string
		if err := rows.Scan(&it.DemandID, &it.SellerPeerID, &it.SeedPrice, &it.ChunkPrice, &it.ChunkCount, &it.FileSize, &it.ExpiresAtUnix, &it.RecommendedFileName, &it.MIMEHint, &sellerArbitersJSON, &it.AvailableChunkBitmapHex); err != nil {
			return nil, err
		}
		it.RecommendedFileName = sanitizeRecommendedFileName(it.RecommendedFileName)
		it.MIMEHint = sanitizeMIMEHint(it.MIMEHint)
		if strings.TrimSpace(sellerArbitersJSON) != "" {
			var ids []string
			if err := json.Unmarshal([]byte(sellerArbitersJSON), &ids); err == nil {
				it.SellerArbiterPeerIDs = normalizePeerIDList(ids)
			}
		}
		if strings.TrimSpace(it.AvailableChunkBitmapHex) != "" {
			norm, err := normalizeChunkBitmapHex(it.AvailableChunkBitmapHex)
			if err != nil {
				continue
			}
			it.AvailableChunkBitmapHex = norm
			indexes, err := chunkIndexesFromBitmapHex(norm, 0)
			if err != nil {
				continue
			}
			it.AvailableChunkIndexes = indexes
		}
		if it.ExpiresAtUnix > 0 && it.ExpiresAtUnix < now {
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

func TriggerClientSubmitLiveQuote(ctx context.Context, seller *Runtime, p LiveQuoteParams) error {
	if seller == nil || seller.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return submitLiveQuote(ctx, seller.Host, seller.rpcTrace, p)
}

func TriggerClientListLiveQuotes(ctx context.Context, rt *Runtime, demandID string) ([]LiveQuoteItem, error) {
	_ = ctx
	if rt == nil || rt.DB == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	demandID = strings.TrimSpace(demandID)
	if demandID == "" {
		return nil, fmt.Errorf("demand_id required")
	}
	rows, err := rt.DB.Query(`SELECT demand_id,seller_pubkey_hex,stream_id,latest_segment_index,recent_segments_json,expires_at_unix FROM live_quotes WHERE demand_id=? ORDER BY created_at_unix ASC`, demandID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now().Unix()
	out := make([]LiveQuoteItem, 0, 4)
	for rows.Next() {
		var it LiveQuoteItem
		var recentJSON string
		if err := rows.Scan(&it.DemandID, &it.SellerPeerID, &it.StreamID, &it.LatestSegmentIndex, &recentJSON, &it.ExpiresAtUnix); err != nil {
			return nil, err
		}
		if it.ExpiresAtUnix > 0 && it.ExpiresAtUnix < now {
			continue
		}
		if strings.TrimSpace(recentJSON) != "" {
			_ = json.Unmarshal([]byte(recentJSON), &it.RecentSegments)
		}
		out = append(out, it)
	}
	return out, nil
}

type SeedGetParams struct {
	SellerPeerID string `json:"seller_pubkey_hex"`
	SessionID    string `json:"session_id"`
	SeedHash     string `json:"seed_hash"`
}

type SeedGetResult struct {
	Seed []byte `json:"seed"`
}

func TriggerClientSeedGet(ctx context.Context, rt *Runtime, p SeedGetParams) (SeedGetResult, error) {
	if rt == nil || rt.Host == nil {
		return SeedGetResult{}, fmt.Errorf("runtime not initialized")
	}
	sellerPeerID := strings.TrimSpace(p.SellerPeerID)
	sessionID := strings.TrimSpace(p.SessionID)
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if sellerPeerID == "" || sessionID == "" || seedHash == "" {
		return SeedGetResult{}, fmt.Errorf("invalid params")
	}
	seller, err := peerIDFromClientID(sellerPeerID)
	if err != nil {
		return SeedGetResult{}, err
	}
	var resp seedGetResp
	err = p2prpc.CallProto(ctx, rt.Host, seller, ProtoSeedGet, clientSec(rt.rpcTrace), seedGetReq{
		SessionID: sessionID,
		SeedHash:  seedHash,
	}, &resp)
	if err != nil {
		return SeedGetResult{}, err
	}
	return SeedGetResult{Seed: append([]byte(nil), resp.Seed...)}, nil
}

type directTransferPoolOpenParams struct {
	SellerPeerID  string
	ArbiterPeerID string
	DemandID      string
	SeedHash      string
	SeedPrice     uint64
	ChunkPrice    uint64
	ExpiresAtUnix int64
	DealID        string
	SessionID     string
	PoolAmount    uint64
}

type directTransferPoolOpenResult struct {
	DealID     string
	SessionID  string
	BaseTxID   string
	Sequence   uint32
	PoolAmount uint64
}

type directTransferPoolPayParams struct {
	SellerPeerID string
	SessionID    string
	Amount       uint64
	SeedHash     string
	ChunkHash    string
	ChunkIndex   uint32
}

type directTransferPoolPayResult struct {
	Sequence uint32
	Chunk    []byte
}

type directTransferPoolCloseParams struct {
	SellerPeerID string
	SessionID    string
}

type directTransferPoolCloseResult struct {
	FinalTxID string
}

func triggerDirectTransferPoolOpen(ctx context.Context, buyer *Runtime, p directTransferPoolOpenParams) (directTransferPoolOpenResult, error) {
	if buyer == nil || buyer.Host == nil || buyer.ActionChain == nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("runtime not initialized")
	}
	lock := buyer.transferPoolOpenMutex()
	lock.Lock()
	defer lock.Unlock()

	dealID := strings.TrimSpace(p.DealID)
	if dealID == "" {
		deal, err := TriggerClientAcceptDirectDeal(ctx, buyer, DirectDealAcceptParams{
			SellerPeerID:  p.SellerPeerID,
			DemandID:      p.DemandID,
			SeedHash:      p.SeedHash,
			SeedPrice:     p.SeedPrice,
			ChunkPrice:    p.ChunkPrice,
			ExpiresAtUnix: p.ExpiresAtUnix,
			ArbiterPeerID: p.ArbiterPeerID,
		})
		if err != nil {
			return directTransferPoolOpenResult{}, err
		}
		dealID = strings.TrimSpace(deal.DealID)
	}
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		sess, err := TriggerClientOpenDirectSession(ctx, buyer, OpenDirectSessionParams{
			SellerPeerID: p.SellerPeerID,
			DealID:       dealID,
		})
		if err != nil {
			return directTransferPoolOpenResult{}, err
		}
		sessionID = strings.TrimSpace(sess.SessionID)
	}

	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	sellerPubHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	sellerPub, err := ec.PublicKeyFromString(sellerPubHex)
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	arbiterPID, err := peer.Decode(strings.TrimSpace(p.ArbiterPeerID))
	if err != nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("invalid arbiter peer id: %w", err)
	}
	arbiterPub := buyer.Host.Peerstore().PubKey(arbiterPID)
	if arbiterPub == nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("missing arbiter pubkey in peerstore")
	}
	arbiterRaw, err := arbiterPub.Raw()
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	arbiterPubHex := strings.ToLower(hex.EncodeToString(arbiterRaw))
	arbiterPubKey, err := ec.PublicKeyFromString(arbiterPubHex)
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}

	clientPrivHex := strings.TrimSpace(buyer.runIn.EffectivePrivKeyHex)
	clientActor, err := dual2of2.BuildActor("buyer", clientPrivHex, strings.ToLower(strings.TrimSpace(buyer.runIn.BSV.Network)) == "main")
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	clientLockScript := ""
	isMainnet := strings.ToLower(strings.TrimSpace(buyer.runIn.BSV.Network)) == "main"
	if addr, addrErr := kmlibs.GetAddressFromPubKey(clientActor.PubKey, isMainnet); addrErr == nil {
		if lock, lockErr := p2pkh.Lock(addr); lockErr == nil {
			clientLockScript = strings.TrimSpace(lock.String())
		}
	}

	target := p.PoolAmount
	if target == 0 {
		target = 200
	}

	const maxOpenAttempt = 4
	sessionPinned := strings.TrimSpace(p.SessionID) != ""
	var lastErr error
	for attempt := 1; attempt <= maxOpenAttempt; attempt++ {
		curSessionID := sessionID
		if attempt > 1 && !sessionPinned {
			sess, err := TriggerClientOpenDirectSession(ctx, buyer, OpenDirectSessionParams{
				SellerPeerID: p.SellerPeerID,
				DealID:       dealID,
			})
			if err != nil {
				return directTransferPoolOpenResult{}, err
			}
			curSessionID = strings.TrimSpace(sess.SessionID)
		}

		poolInputs, splitTxID, err := func() ([]dual2of2.UTXO, string, error) {
			// 钱包 UTXO 分配必须单步串行：查询/选取/拆分在同一临界区完成。
			allocMu := buyer.walletAllocMutex()
			allocMu.Lock()
			defer allocMu.Unlock()

			utxos, err := getWalletUTXOsFromDB(buyer)
			if err != nil {
				return nil, "", fmt.Errorf("load wallet utxos from snapshot failed: %w", err)
			}
			if len(utxos) == 0 {
				return nil, "", fmt.Errorf("no utxos for buyer address")
			}
			selected, err := pickUTXOsForTarget(utxos, target)
			if err != nil {
				return nil, "", err
			}
			return splitUTXOsToTarget(ctx, buyer, "direct_pool:"+curSessionID, clientActor, selected, target, 0.5)
		}()
		if err != nil {
			if isRetryableTransferPoolSplitErr(err) && attempt < maxOpenAttempt {
				lastErr = err
				wait := time.Duration(attempt) * 800 * time.Millisecond
				obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_open_retry", map[string]any{
					"session_id": reqSessionIDOrFallback(curSessionID, sessionID),
					"deal_id":    dealID,
					"attempt":    attempt,
					"wait_ms":    wait.Milliseconds(),
					"error":      err.Error(),
					"stage":      "split_fund",
				})
				select {
				case <-ctx.Done():
					return directTransferPoolOpenResult{}, ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
			return directTransferPoolOpenResult{}, fmt.Errorf("prepare exact pool utxo failed: %w", err)
		}
		obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_open_fund_prepared", map[string]any{
			"session_id":     curSessionID,
			"deal_id":        dealID,
			"attempt":        attempt,
			"target_amount":  target,
			"input_count":    len(poolInputs),
			"split_txid":     splitTxID,
			"split_executed": strings.TrimSpace(splitTxID) != "",
		})

		kmUTXOs := make([]kmlibs.UTXO, 0, len(poolInputs))
		for _, u := range poolInputs {
			kmUTXOs = append(kmUTXOs, kmlibs.UTXO{TxID: u.TxID, Vout: u.Vout, Value: u.Value})
		}

		baseResp, err := te.BuildTripleFeePoolBaseTx(&kmUTXOs, arbiterPubKey, clientActor.PrivKey, sellerPub, false, 0.5)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("build transfer pool base tx failed: %w", err)
		}
		tip, err := getTipHeightFromDB(buyer)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("load tip height from snapshot failed: %w", err)
		}
		lockBlocks := uint32(6)
		spendTx, buyerOpenSig, buyerAmount, err := te.BuildTripleFeePoolSpendTX(baseResp.Tx, baseResp.Amount, tip+lockBlocks, arbiterPubKey, clientActor.PrivKey, sellerPub, false, 0.5)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("build transfer pool spend tx failed: %w", err)
		}
		spendTxBytes, err := hex.DecodeString(spendTx.Hex())
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("encode spend tx bytes failed: %w", err)
		}
		baseTxBytes, err := hex.DecodeString(baseResp.Tx.Hex())
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("encode base tx bytes failed: %w", err)
		}
		spendFee := dual2of2.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
		if spendFee == 0 {
			spendFee = 1
		}
		req := directTransferPoolOpenReq{
			SessionID:      curSessionID,
			DealID:         dealID,
			BuyerPeerID:    strings.ToLower(strings.TrimSpace(buyer.runIn.ClientID)),
			ArbiterPeerID:  strings.TrimSpace(p.ArbiterPeerID),
			ArbiterPubKey:  arbiterPubHex,
			PoolAmount:     baseResp.Amount,
			SpendTxFee:     spendFee,
			Sequence:       1,
			SellerAmount:   0,
			BuyerAmount:    buyerAmount,
			CurrentTx:      spendTxBytes,
			BuyerSig:       append([]byte(nil), (*buyerOpenSig)...),
			BaseTx:         baseTxBytes,
			BaseTxID:       baseResp.Tx.TxID().String(),
			FeeRateSatByte: 0.5,
			LockBlocks:     lockBlocks,
		}
		obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_open_begin", map[string]any{
			"session_id": req.SessionID,
			"deal_id":    req.DealID,
			"attempt":    attempt,
		})
		var openResp directTransferPoolOpenResp
		if err := p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolOpen, clientSec(buyer.rpcTrace), req, &openResp); err != nil {
			obs.Error("bitcast-client", "evt_trigger_direct_transfer_pool_open_failed", map[string]any{"error": err.Error()})
			return directTransferPoolOpenResult{}, err
		}
		if strings.TrimSpace(openResp.Status) != "active" || len(openResp.SellerSig) == 0 {
			return directTransferPoolOpenResult{}, fmt.Errorf("transfer pool open rejected: %s", strings.TrimSpace(openResp.Error))
		}
		sellerSig := append([]byte(nil), openResp.SellerSig...)
		merged, err := te.MergeTripleFeePoolSigForSpendTx(spendTx.Hex(), buyerOpenSig, &sellerSig)
		if err != nil {
			return directTransferPoolOpenResult{}, err
		}
		baseTxID, err := buyer.ActionChain.Broadcast(baseResp.Tx.Hex())
		if err != nil {
			if isRetryableTransferPoolBaseTxBroadcastErr(err) && attempt < maxOpenAttempt {
				lastErr = err
				wait := time.Duration(attempt) * 800 * time.Millisecond
				obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_open_retry", map[string]any{
					"session_id": req.SessionID,
					"deal_id":    req.DealID,
					"attempt":    attempt,
					"wait_ms":    wait.Milliseconds(),
					"error":      err.Error(),
				})
				select {
				case <-ctx.Done():
					return directTransferPoolOpenResult{}, ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
			return directTransferPoolOpenResult{}, fmt.Errorf("broadcast transfer pool base tx failed: %w", err)
		}
		buyer.setTriplePool(&triplePoolSession{
			DemandID:         strings.TrimSpace(p.DemandID),
			SessionID:        curSessionID,
			DealID:           dealID,
			SellerPeerID:     strings.TrimSpace(p.SellerPeerID),
			ArbiterPeerID:    req.ArbiterPeerID,
			PoolAmountSat:    req.PoolAmount,
			SpendTxFeeSat:    req.SpendTxFee,
			OpenSequence:     req.Sequence,
			Sequence:         req.Sequence,
			SellerAmount:     req.SellerAmount,
			BuyerAmount:      req.BuyerAmount,
			CurrentTxHex:     merged.Hex(),
			BaseTxHex:        baseResp.Tx.Hex(),
			BaseTxID:         baseTxID,
			FeeRateSatByte:   req.FeeRateSatByte,
			LockBlocks:       req.LockBlocks,
			SellerPubKeyHex:  sellerPubHex,
			BuyerPubKeyHex:   strings.ToLower(clientActor.PubHex),
			ArbiterPubKeyHex: arbiterPubHex,
			PayCount:         0,
			LastPaySequence:  0,
		})
		emitDirectTransferEvent(buyer, "direct_transfer_context_opened", map[string]any{
			"event_id":                fmt.Sprintf("%s:open", curSessionID),
			"demand_id":               strings.TrimSpace(p.DemandID),
			"deal_id":                 dealID,
			"session_id":              curSessionID,
			"seller_pubkey_hex":       strings.TrimSpace(p.SellerPeerID),
			"arbiter_pubkey_hex":      req.ArbiterPeerID,
			"open_sequence":           req.Sequence,
			"open_base_txid":          baseTxID,
			"pool_amount_satoshi":     req.PoolAmount,
			"spend_tx_fee_satoshi":    req.SpendTxFee,
			"fee_rate_sat_per_byte":   req.FeeRateSatByte,
			"lock_blocks":             req.LockBlocks,
			"recommended_file_source": "direct_transfer_pool_open",
		})
		appendWalletFundFlowFromContext(ctx, buyer.DB, walletFundFlowEntry{
			FlowID:          "direct_pool:" + curSessionID,
			FlowType:        "direct_transfer_pool",
			RefID:           curSessionID,
			Stage:           "open_lock",
			Direction:       "lock",
			Purpose:         "direct_transfer_pool_open",
			AmountSatoshi:   int64(req.PoolAmount),
			UsedSatoshi:     0,
			ReturnedSatoshi: 0,
			RelatedTxID:     baseTxID,
			Note:            fmt.Sprintf("deal_id=%s", dealID),
			Payload: map[string]any{
				"deal_id":              dealID,
				"pool_amount_satoshi":  req.PoolAmount,
				"spend_tx_fee_satoshi": req.SpendTxFee,
				"sequence":             req.Sequence,
			},
		})
		recordDirectPoolOpenAccounting(buyer.DB, directPoolOpenAccountingInput{
			SessionID:         curSessionID,
			DealID:            dealID,
			BaseTxID:          baseTxID,
			BaseTxHex:         baseResp.Tx.Hex(),
			ClientLockScript:  clientLockScript,
			PoolAmountSatoshi: req.PoolAmount,
			SellerPeerID:      strings.TrimSpace(p.SellerPeerID),
		})
		obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_open_end", map[string]any{
			"session_id": req.SessionID,
			"base_txid":  baseTxID,
			"status":     openResp.Status,
			"attempt":    attempt,
		})
		return directTransferPoolOpenResult{DealID: dealID, SessionID: curSessionID, BaseTxID: baseTxID, Sequence: 1, PoolAmount: req.PoolAmount}, nil
	}
	return directTransferPoolOpenResult{}, fmt.Errorf("broadcast transfer pool base tx failed after retries: %w", lastErr)
}

func splitUTXOsToTarget(ctx context.Context, rt *Runtime, flowID string, actor *dual2of2.Actor, selected []dual2of2.UTXO, target uint64, feeRate float64) ([]dual2of2.UTXO, string, error) {
	if rt == nil || rt.ActionChain == nil {
		return nil, "", fmt.Errorf("runtime chain not initialized")
	}
	if actor == nil {
		return nil, "", fmt.Errorf("actor is nil")
	}
	if len(selected) == 0 {
		return nil, "", fmt.Errorf("selected utxos is empty")
	}
	if target == 0 {
		return nil, "", fmt.Errorf("target pool amount must be > 0")
	}
	total := sumUTXOValue(selected)
	if total < target {
		return nil, "", fmt.Errorf("insufficient selected utxos: have=%d target=%d", total, target)
	}
	if total == target {
		return selected, "", nil
	}

	isMainnet := strings.ToLower(strings.TrimSpace(rt.runIn.BSV.Network)) == "main"
	clientAddr, err := kmlibs.GetAddressFromPubKey(actor.PubKey, isMainnet)
	if err != nil {
		return nil, "", fmt.Errorf("derive client address failed: %w", err)
	}
	lockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return nil, "", fmt.Errorf("build p2pkh lock script failed: %w", err)
	}
	prevLockHex := hex.EncodeToString(lockScript.Bytes())
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return nil, "", fmt.Errorf("build p2pkh unlock template failed: %w", err)
	}

	splitTx := tx.NewTransaction()
	for _, u := range selected {
		if err := splitTx.AddInputFrom(u.TxID, u.Vout, prevLockHex, u.Value, unlockTpl); err != nil {
			return nil, "", fmt.Errorf("add split input failed: %w", err)
		}
	}
	splitTx.AddOutput(&tx.TransactionOutput{
		Satoshis:      target,
		LockingScript: lockScript,
	})
	// 先放一个临时找零，签名后按真实大小估算手续费再回填。
	splitTx.AddOutput(&tx.TransactionOutput{
		Satoshis:      total - target,
		LockingScript: lockScript,
	})
	if err := signP2PKHAllInputs(splitTx, unlockTpl); err != nil {
		return nil, "", fmt.Errorf("pre-sign split tx failed: %w", err)
	}
	fee := uint64(float64(splitTx.Size()) / 1000.0 * feeRate)
	if fee == 0 {
		fee = 1
	}
	if total <= target+fee {
		return nil, "", fmt.Errorf("insufficient selected utxos for split fee: have=%d target=%d fee=%d", total, target, fee)
	}
	splitTx.Outputs[1].Satoshis = total - target - fee
	if err := signP2PKHAllInputs(splitTx, unlockTpl); err != nil {
		return nil, "", fmt.Errorf("final-sign split tx failed: %w", err)
	}

	localTxID := strings.ToLower(strings.TrimSpace(splitTx.TxID().String()))
	broadcastTxID, err := rt.ActionChain.Broadcast(splitTx.Hex())
	if err != nil {
		return nil, "", fmt.Errorf("broadcast split tx failed: %w", err)
	}
	splitTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if splitTxID == "" {
		splitTxID = localTxID
	}
	change := int64(total - target - fee)
	appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
		FlowID:          flowID,
		FlowType:        "direct_transfer_pool",
		RefID:           strings.TrimPrefix(flowID, "direct_pool:"),
		Stage:           "fund_split",
		Direction:       "internal",
		Purpose:         "prepare_exact_pool_amount",
		AmountSatoshi:   int64(target),
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     splitTxID,
		Note:            fmt.Sprintf("selected_total=%d split_fee=%d change=%d", total, fee, change),
		Payload: map[string]any{
			"selected_total_satoshi": total,
			"target_satoshi":         target,
			"split_fee_satoshi":      fee,
			"change_satoshi":         change,
		},
	})

	deadline := time.Now().Add(20 * time.Second)
	for {
		utxos, err := getWalletUTXOsFromDB(rt)
		if err == nil {
			for _, u := range utxos {
				if strings.EqualFold(strings.TrimSpace(u.TxID), splitTxID) && u.Vout == 0 && u.Value == target {
					return []dual2of2.UTXO{u}, splitTxID, nil
				}
			}
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return nil, "", fmt.Errorf("split tx output not visible in utxo set: txid=%s target=%d", splitTxID, target)
}

func signP2PKHAllInputs(t *tx.Transaction, unlockTpl *p2pkh.P2PKH) error {
	for i := range t.Inputs {
		unlockScript, err := unlockTpl.Sign(t, uint32(i))
		if err != nil {
			return err
		}
		t.Inputs[i].UnlockingScript = unlockScript
	}
	return nil
}

func sumUTXOValue(utxos []dual2of2.UTXO) uint64 {
	var sum uint64
	for _, u := range utxos {
		sum += u.Value
	}
	return sum
}

func triggerDirectTransferPoolPay(ctx context.Context, buyer *Runtime, p directTransferPoolPayParams) (directTransferPoolPayResult, error) {
	if buyer == nil || buyer.Host == nil {
		return directTransferPoolPayResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(p.ChunkHash))
	if seedHash == "" || chunkHash == "" {
		return directTransferPoolPayResult{}, fmt.Errorf("seed_hash and chunk_hash are required")
	}
	lock := buyer.transferPoolSessionMutex(p.SessionID)
	lock.Lock()
	defer lock.Unlock()

	session, ok := buyer.getTriplePool(strings.TrimSpace(p.SessionID))
	if !ok || session == nil {
		return directTransferPoolPayResult{}, fmt.Errorf("transfer pool session missing")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	clientActor, err := dual2of2.BuildActor("buyer", strings.TrimSpace(buyer.runIn.EffectivePrivKeyHex), strings.ToLower(strings.TrimSpace(buyer.runIn.BSV.Network)) == "main")
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	sellerPub, err := ec.PublicKeyFromString(session.SellerPubKeyHex)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	arbiterPub, err := ec.PublicKeyFromString(session.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	nextSeq := session.Sequence + 1
	nextSellerAmount := session.SellerAmount + p.Amount
	updatedTx, err := te.TripleFeePoolLoadTx(
		session.CurrentTxHex,
		nil,
		nextSeq,
		nextSellerAmount,
		arbiterPub,
		clientActor.PubKey,
		sellerPub,
		session.PoolAmountSat,
	)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	buyerSig, err := te.ClientATripleFeePoolSpendTXUpdateSign(updatedTx, arbiterPub, clientActor.PrivKey, sellerPub)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	updatedTxBytes, err := hex.DecodeString(updatedTx.Hex())
	if err != nil {
		return directTransferPoolPayResult{}, fmt.Errorf("encode updated tx bytes failed: %w", err)
	}
	req := directTransferPoolPayReq{
		SessionID:    session.SessionID,
		SeedHash:     seedHash,
		ChunkHash:    chunkHash,
		ChunkIndex:   p.ChunkIndex,
		Sequence:     nextSeq,
		SellerAmount: nextSellerAmount,
		BuyerAmount:  session.PoolAmountSat - nextSellerAmount - session.SpendTxFeeSat,
		CurrentTx:    updatedTxBytes,
		BuyerSig:     append([]byte(nil), (*buyerSig)...),
	}
	obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_pay_begin", map[string]any{
		"session_id":    req.SessionID,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
		"chunk_hash":    chunkHash,
		"chunk_index":   p.ChunkIndex,
	})
	var payResp directTransferPoolPayResp
	if err := p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolPay, clientSec(buyer.rpcTrace), req, &payResp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_direct_transfer_pool_pay_failed", map[string]any{"error": err.Error()})
		return directTransferPoolPayResult{}, err
	}
	if strings.TrimSpace(payResp.Status) != "active" || len(payResp.SellerSig) == 0 {
		return directTransferPoolPayResult{}, fmt.Errorf("transfer pool pay rejected: %s", strings.TrimSpace(payResp.Error))
	}
	chunk := append([]byte(nil), payResp.Chunk...)
	if len(chunk) == 0 {
		return directTransferPoolPayResult{}, fmt.Errorf("transfer pool pay rejected: chunk missing")
	}
	got := sha256.Sum256(chunk)
	if hex.EncodeToString(got[:]) != chunkHash {
		return directTransferPoolPayResult{}, fmt.Errorf("chunk hash mismatch")
	}
	sellerSig := append([]byte(nil), payResp.SellerSig...)
	merged, err := te.MergeTripleFeePoolSigForSpendTx(updatedTx.Hex(), buyerSig, &sellerSig)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	session.Sequence = req.Sequence
	session.SellerAmount = req.SellerAmount
	session.BuyerAmount = req.BuyerAmount
	session.PayCount++
	session.LastPaySequence = req.Sequence
	session.CurrentTxHex = merged.Hex()
	buyer.setTriplePool(session)
	emitDirectTransferEvent(buyer, "direct_transfer_chunk_paid", map[string]any{
		"event_id":            fmt.Sprintf("%s:%d:%d", session.SessionID, req.Sequence, p.ChunkIndex),
		"demand_id":           strings.TrimSpace(session.DemandID),
		"deal_id":             strings.TrimSpace(session.DealID),
		"session_id":          session.SessionID,
		"seller_pubkey_hex":   strings.TrimSpace(session.SellerPeerID),
		"arbiter_pubkey_hex":  strings.TrimSpace(session.ArbiterPeerID),
		"seed_hash":           seedHash,
		"chunk_hash":          chunkHash,
		"chunk_index":         p.ChunkIndex,
		"chunk_bytes":         len(chunk),
		"sequence":            req.Sequence,
		"pay_count":           session.PayCount,
		"last_pay_sequence":   session.LastPaySequence,
		"seller_amount_sat":   session.SellerAmount,
		"buyer_amount_sat":    session.BuyerAmount,
		"pool_amount_satoshi": session.PoolAmountSat,
		"chunk_price_satoshi": p.Amount,
	})
	appendWalletFundFlowFromContext(ctx, buyer.DB, walletFundFlowEntry{
		FlowID:          "direct_pool:" + session.SessionID,
		FlowType:        "direct_transfer_pool",
		RefID:           session.SessionID,
		Stage:           "use_pay",
		Direction:       "out",
		Purpose:         "direct_transfer_chunk_pay",
		AmountSatoshi:   -int64(p.Amount),
		UsedSatoshi:     int64(p.Amount),
		ReturnedSatoshi: 0,
		RelatedTxID:     strings.TrimSpace(merged.TxID().String()),
		Note:            fmt.Sprintf("chunk_index=%d", p.ChunkIndex),
		Payload: map[string]any{
			"chunk_hash":  chunkHash,
			"chunk_index": p.ChunkIndex,
			"sequence":    req.Sequence,
		},
	})
	recordDirectPoolPayAccounting(
		buyer.DB,
		session.SessionID,
		req.Sequence,
		p.Amount,
		strings.TrimSpace(session.SellerPeerID),
		strings.TrimSpace(merged.TxID().String()),
	)
	obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_pay_end", map[string]any{
		"session_id":    req.SessionID,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
		"status":        payResp.Status,
		"chunk_hash":    chunkHash,
		"chunk_index":   p.ChunkIndex,
	})
	return directTransferPoolPayResult{Sequence: req.Sequence, Chunk: chunk}, nil
}

func triggerDirectTransferPoolClose(ctx context.Context, buyer *Runtime, p directTransferPoolCloseParams) (directTransferPoolCloseResult, error) {
	if buyer == nil || buyer.Host == nil || buyer.ActionChain == nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	lock := buyer.transferPoolSessionMutex(p.SessionID)
	lock.Lock()
	defer lock.Unlock()

	session, ok := buyer.getTriplePool(strings.TrimSpace(p.SessionID))
	if !ok || session == nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("transfer pool session missing")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	clientActor, err := dual2of2.BuildActor("buyer", strings.TrimSpace(buyer.runIn.EffectivePrivKeyHex), strings.ToLower(strings.TrimSpace(buyer.runIn.BSV.Network)) == "main")
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	sellerPub, err := ec.PublicKeyFromString(session.SellerPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	arbiterPub, err := ec.PublicKeyFromString(session.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	locktime := uint32(0xffffffff)
	finalTx, err := te.TripleFeePoolLoadTx(
		session.CurrentTxHex,
		&locktime,
		0xffffffff,
		session.SellerAmount,
		arbiterPub,
		clientActor.PubKey,
		sellerPub,
		session.PoolAmountSat,
	)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	buyerSig, err := te.ClientATripleFeePoolSpendTXUpdateSign(finalTx, arbiterPub, clientActor.PrivKey, sellerPub)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	finalTxBytes, err := hex.DecodeString(finalTx.Hex())
	if err != nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("encode final tx bytes failed: %w", err)
	}
	req := directTransferPoolCloseReq{
		SessionID:    session.SessionID,
		Sequence:     session.Sequence,
		SellerAmount: session.SellerAmount,
		BuyerAmount:  session.BuyerAmount,
		CurrentTx:    finalTxBytes,
		BuyerSig:     append([]byte(nil), (*buyerSig)...),
	}
	obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_close_begin", map[string]any{
		"session_id": req.SessionID,
		"sequence":   req.Sequence,
	})
	var closeResp directTransferPoolCloseResp
	if err := p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolClose, clientSec(buyer.rpcTrace), req, &closeResp); err != nil {
		obs.Error("bitcast-client", "evt_trigger_direct_transfer_pool_close_failed", map[string]any{"error": err.Error()})
		return directTransferPoolCloseResult{}, err
	}
	if len(closeResp.SellerSig) == 0 {
		return directTransferPoolCloseResult{}, fmt.Errorf("transfer pool close rejected: %s", strings.TrimSpace(closeResp.Error))
	}
	sellerSig := append([]byte(nil), closeResp.SellerSig...)
	merged, err := te.MergeTripleFeePoolSigForSpendTx(finalTx.Hex(), buyerSig, &sellerSig)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	finalTxID, err := buyer.ActionChain.Broadcast(merged.Hex())
	if err != nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("broadcast transfer pool final tx failed: %w", err)
	}
	session.FinalTxID = finalTxID
	session.CurrentTxHex = merged.Hex()
	buyer.setTriplePool(session)
	emitDirectTransferEvent(buyer, "direct_transfer_context_closed", map[string]any{
		"event_id":              fmt.Sprintf("%s:close:%d", session.SessionID, session.Sequence),
		"demand_id":             strings.TrimSpace(session.DemandID),
		"deal_id":               strings.TrimSpace(session.DealID),
		"session_id":            session.SessionID,
		"seller_pubkey_hex":     strings.TrimSpace(session.SellerPeerID),
		"arbiter_pubkey_hex":    strings.TrimSpace(session.ArbiterPeerID),
		"open_sequence":         session.OpenSequence,
		"open_base_txid":        strings.TrimSpace(session.BaseTxID),
		"pool_amount_satoshi":   session.PoolAmountSat,
		"pay_count":             session.PayCount,
		"last_pay_sequence":     session.LastPaySequence,
		"close_final_txid":      finalTxID,
		"final_seller_amount":   session.SellerAmount,
		"final_buyer_amount":    session.BuyerAmount,
		"spend_tx_fee_satoshi":  session.SpendTxFeeSat,
		"close_reason":          "transfer_completed",
		"transfer_state":        "closed",
		"transfer_entry_source": "direct_transfer_pool_close",
	})
	appendWalletFundFlowFromContext(ctx, buyer.DB, walletFundFlowEntry{
		FlowID:          "direct_pool:" + session.SessionID,
		FlowType:        "direct_transfer_pool",
		RefID:           session.SessionID,
		Stage:           "close_settle",
		Direction:       "settle",
		Purpose:         "direct_transfer_pool_close",
		AmountSatoshi:   0,
		UsedSatoshi:     int64(session.SellerAmount),
		ReturnedSatoshi: int64(session.BuyerAmount),
		RelatedTxID:     finalTxID,
		Note:            fmt.Sprintf("sequence=%d", session.Sequence),
		Payload: map[string]any{
			"seller_amount_satoshi": session.SellerAmount,
			"buyer_amount_satoshi":  session.BuyerAmount,
			"pool_amount_satoshi":   session.PoolAmountSat,
		},
	})
	recordDirectPoolCloseAccounting(
		buyer.DB,
		session.SessionID,
		finalTxID,
		merged.Hex(),
		session.SellerAmount,
		session.BuyerAmount,
		strings.TrimSpace(session.SellerPeerID),
	)
	buyer.deleteTriplePool(session.SessionID)
	buyer.releaseTransferPoolSessionMutex(session.SessionID)
	obs.Business("bitcast-client", "evt_trigger_direct_transfer_pool_close_end", map[string]any{
		"session_id": session.SessionID,
		"final_txid": finalTxID,
		"status":     "closed",
	})
	return directTransferPoolCloseResult{FinalTxID: finalTxID}, nil
}

func pickUTXOsForTarget(all []dual2of2.UTXO, target uint64) ([]dual2of2.UTXO, error) {
	if len(all) == 0 {
		return nil, fmt.Errorf("no utxos available")
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Value < all[j].Value })
	selected := make([]dual2of2.UTXO, 0, 4)
	var sum uint64
	for _, u := range all {
		selected = append(selected, u)
		sum += u.Value
		if sum >= target {
			return selected, nil
		}
	}
	return nil, fmt.Errorf("insufficient balance for transfer pool target=%d", target)
}

func isRetryableTransferPoolBaseTxBroadcastErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "txn-mempool-conflict") ||
		strings.Contains(msg, "missing inputs") ||
		strings.Contains(msg, "bad-txns-inputs-spent")
}

func emitDirectTransferEvent(rt *Runtime, name string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	if rt != nil {
		clientID := strings.ToLower(strings.TrimSpace(rt.runIn.ClientID))
		if clientID != "" {
			if _, ok := fields["client_pubkey_hex"]; !ok {
				fields["client_pubkey_hex"] = clientID
			}
		}
		if rt.Host != nil {
			if _, ok := fields["client_transport_peer_id"]; !ok {
				fields["client_transport_peer_id"] = rt.Host.ID().String()
			}
		}
	}
	obs.Business("bitcast-client", name, fields)
}

func isRetryableTransferPoolSplitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "txn-mempool-conflict") ||
		strings.Contains(msg, "missing inputs") ||
		strings.Contains(msg, "bad-txns-inputs-spent")
}

func reqSessionIDOrFallback(current string, fallback string) string {
	current = strings.TrimSpace(current)
	if current != "" {
		return current
	}
	return strings.TrimSpace(fallback)
}

type seedV1Meta struct {
	FileSize    uint64
	ChunkCount  uint32
	ChunkHashes []string
	SeedHashHex string
}

func parseSeedV1(seed []byte) (seedV1Meta, error) {
	if len(seed) < 22 {
		return seedV1Meta{}, fmt.Errorf("invalid seed length")
	}
	if string(seed[:4]) != "BSE1" {
		return seedV1Meta{}, fmt.Errorf("invalid seed magic")
	}
	chunkSize := binary.BigEndian.Uint32(seed[6:10])
	if chunkSize != seedBlockSize {
		return seedV1Meta{}, fmt.Errorf("unsupported chunk size")
	}
	fileSize := binary.BigEndian.Uint64(seed[10:18])
	chunkCount := binary.BigEndian.Uint32(seed[18:22])
	expect := 22 + int(chunkCount)*32
	if len(seed) != expect {
		return seedV1Meta{}, fmt.Errorf("invalid seed body length")
	}
	hashes := make([]string, 0, chunkCount)
	offset := 22
	for i := uint32(0); i < chunkCount; i++ {
		hashes = append(hashes, hex.EncodeToString(seed[offset:offset+32]))
		offset += 32
	}
	h := sha256.Sum256(seed)
	return seedV1Meta{
		FileSize:    fileSize,
		ChunkCount:  chunkCount,
		ChunkHashes: hashes,
		SeedHashHex: hex.EncodeToString(h[:]),
	}, nil
}

func resolveDealArbiter(buyer *Runtime, sellerArbiters []string, override string) (string, error) {
	if buyer == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	own := ownArbiterPeerIDs(buyer)
	if len(own) == 0 {
		return "", fmt.Errorf("buyer has no configured arbiter")
	}
	sellerSet := map[string]struct{}{}
	for _, id := range normalizePeerIDList(sellerArbiters) {
		sellerSet[id] = struct{}{}
	}
	pin := strings.TrimSpace(override)
	if pin != "" {
		if _, err := peer.Decode(pin); err != nil {
			return "", fmt.Errorf("invalid arbiter_pubkey_hex override: %w", err)
		}
		if _, ok := sellerSet[pin]; !ok {
			return "", fmt.Errorf("arbiter override not supported by seller: %s", pin)
		}
		found := false
		for _, id := range own {
			if id == pin {
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("arbiter override not configured by buyer: %s", pin)
		}
		return pin, nil
	}
	for _, id := range own {
		if _, ok := sellerSet[id]; ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("no common arbiter between buyer and seller")
}

func ownArbiterPeerIDs(rt *Runtime) []string {
	if rt == nil {
		return nil
	}
	ids := make([]string, 0, len(rt.runIn.Network.Arbiters))
	for _, a := range rt.runIn.Network.Arbiters {
		if !a.Enabled {
			continue
		}
		ai, err := parseAddr(strings.TrimSpace(a.Addr))
		if err != nil || ai == nil {
			continue
		}
		ids = append(ids, ai.ID.String())
	}
	ids = normalizePeerIDList(ids)
	if len(ids) > 0 {
		return ids
	}
	// 回退：用已连接仲裁列表（保持连接顺序）。
	fallback := make([]string, 0, len(rt.HealthyArbiters))
	for _, ai := range rt.HealthyArbiters {
		fallback = append(fallback, ai.ID.String())
	}
	return normalizePeerIDList(fallback)
}

type DirectDealAcceptParams struct {
	SellerPeerID  string `json:"seller_pubkey_hex"`
	DemandID      string `json:"demand_id"`
	SeedHash      string `json:"seed_hash"`
	SeedPrice     uint64 `json:"seed_price"`
	ChunkPrice    uint64 `json:"chunk_price"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
	ArbiterPeerID string `json:"arbiter_pubkey_hex,omitempty"`
}

func TriggerClientAcceptDirectDeal(ctx context.Context, buyer *Runtime, p DirectDealAcceptParams) (directDealAcceptResp, error) {
	if buyer == nil || buyer.Host == nil {
		return directDealAcceptResp{}, fmt.Errorf("runtime not initialized")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directDealAcceptResp{}, err
	}
	var resp directDealAcceptResp
	err = p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoDirectDealAccept, clientSec(buyer.rpcTrace), directDealAcceptReq{
		DemandID:      strings.TrimSpace(p.DemandID),
		BuyerPeerID:   strings.ToLower(strings.TrimSpace(buyer.runIn.ClientID)),
		SeedHash:      strings.ToLower(strings.TrimSpace(p.SeedHash)),
		SeedPrice:     p.SeedPrice,
		ChunkPrice:    p.ChunkPrice,
		ExpiresAtUnix: p.ExpiresAtUnix,
		ArbiterPeerID: strings.TrimSpace(p.ArbiterPeerID),
	}, &resp)
	if err != nil {
		return directDealAcceptResp{}, err
	}
	return resp, nil
}

type OpenDirectSessionParams struct {
	SellerPeerID string `json:"seller_pubkey_hex"`
	DealID       string `json:"deal_id"`
}

func TriggerClientOpenDirectSession(ctx context.Context, buyer *Runtime, p OpenDirectSessionParams) (directSessionOpenResp, error) {
	if buyer == nil || buyer.Host == nil {
		return directSessionOpenResp{}, fmt.Errorf("runtime not initialized")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directSessionOpenResp{}, err
	}
	var resp directSessionOpenResp
	err = p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoDirectSessionOpen, clientSec(buyer.rpcTrace), directSessionOpenReq{
		DealID: strings.TrimSpace(p.DealID),
	}, &resp)
	if err != nil {
		return directSessionOpenResp{}, err
	}
	return resp, nil
}

type CloseDirectSessionParams struct {
	SellerPeerID string `json:"seller_pubkey_hex"`
	SessionID    string `json:"session_id"`
}

func TriggerClientCloseDirectSession(ctx context.Context, buyer *Runtime, p CloseDirectSessionParams) (directSessionCloseResp, error) {
	if buyer == nil || buyer.Host == nil {
		return directSessionCloseResp{}, fmt.Errorf("runtime not initialized")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPeerID))
	if err != nil {
		return directSessionCloseResp{}, err
	}
	var resp directSessionCloseResp
	err = p2prpc.CallProto(ctx, buyer.Host, sellerPID, ProtoDirectSessionClose, clientSec(buyer.rpcTrace), directSessionCloseReq{
		SessionID: strings.TrimSpace(p.SessionID),
	}, &resp)
	if err != nil {
		return directSessionCloseResp{}, err
	}
	return resp, nil
}

func localAdvertiseAddrs(rt *Runtime) []string {
	if rt == nil || rt.Host == nil {
		return nil
	}
	out := make([]string, 0, len(rt.Host.Addrs()))
	for _, a := range rt.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a.String(), rt.Host.ID().String()))
	}
	return out
}

func peerIDFromClientID(clientID string) (peer.ID, error) {
	pubHex, err := normalizeCompressedPubKeyHex(clientID)
	if err != nil {
		return "", fmt.Errorf("client_pubkey_hex invalid: %w", err)
	}
	b, err := hex.DecodeString(pubHex)
	if err != nil {
		return "", fmt.Errorf("decode client_pubkey_hex: %w", err)
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(b)
	if err != nil {
		return "", fmt.Errorf("unmarshal client pubkey: %w", err)
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("derive peer id: %w", err)
	}
	return pid, nil
}
