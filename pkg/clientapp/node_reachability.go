package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	"github.com/bsv8/BFTP/pkg/obs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	autoNodeReachabilityAnnounceCheckInterval time.Duration = 15 * time.Second
	autoNodeReachabilityAnnounceRenewLead     time.Duration = 60 * time.Second
)

type AnnounceNodeReachabilityParams struct {
	TTLSeconds    uint32 `json:"ttl_seconds"`
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
}

type QueryNodeReachabilityParams struct {
	TargetNodePubkeyHex string `json:"target_node_pubkey_hex"`
	GatewayPeerID       string `json:"gateway_pubkey_hex,omitempty"`
}

type selfNodeReachabilityState struct {
	NodePubkeyHex string
	HeadHeight    uint64
	Seq           uint64
}

type autoNodeReachabilityAnnounceState struct {
	LastFingerprint      string
	LastGatewayPubkeyHex string
	LastExpiresAtUnix    int64
	LastAttemptError     string
	LastAttemptReason    string
	LastAnnouncedAtUnix  int64
	LastAnnouncedNodeID  string
}

type autoNodeReachabilityNotifiee struct {
	enqueue func(reason string)
}

func currentLocalNodeReachabilitySnapshot(rt *Runtime) (string, []string, string, error) {
	if rt == nil || rt.Host == nil {
		return "", nil, "", fmt.Errorf("runtime not initialized")
	}
	nodePubkeyHex, err := localPubKeyHex(rt.Host)
	if err != nil {
		return "", nil, "", err
	}
	multiaddrs, err := broadcastmodule.NormalizeNodeReachabilityAddrs(nodePubkeyHex, localAdvertiseAddrs(rt))
	if err != nil {
		return "", nil, "", err
	}
	fingerprint, err := marshalReachabilityStringList(multiaddrs)
	if err != nil {
		return "", nil, "", err
	}
	return nodePubkeyHex, multiaddrs, fingerprint, nil
}

func shouldAutoNodeReachabilityAnnounce(nowUnix int64, state autoNodeReachabilityAnnounceState, fingerprint string, gatewayPubkeyHex string) bool {
	if strings.TrimSpace(fingerprint) == "" || strings.TrimSpace(gatewayPubkeyHex) == "" {
		return false
	}
	if strings.TrimSpace(state.LastFingerprint) == "" || strings.TrimSpace(state.LastGatewayPubkeyHex) == "" {
		return true
	}
	if state.LastFingerprint != fingerprint {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(state.LastGatewayPubkeyHex), strings.TrimSpace(gatewayPubkeyHex)) {
		return true
	}
	renewAtUnix := state.LastExpiresAtUnix - int64(autoNodeReachabilityAnnounceRenewLead/time.Second)
	if state.LastExpiresAtUnix <= 0 || nowUnix >= renewAtUnix {
		return true
	}
	return false
}

func startAutoNodeReachabilityAnnounceLoop(ctx context.Context, rt *Runtime) {
	if rt == nil || rt.Host == nil {
		return
	}
	go func() {
		eventCh := make(chan string, 1)
		enqueue := func(reason string) {
			select {
			case eventCh <- reason:
			default:
			}
		}
		notifiee := &autoNodeReachabilityNotifiee{enqueue: enqueue}
		rt.Host.Network().Notify(notifiee)
		defer rt.Host.Network().StopNotify(notifiee)
		ticker := time.NewTicker(autoNodeReachabilityAnnounceCheckInterval)
		defer ticker.Stop()
		state := autoNodeReachabilityAnnounceState{}
		runAutoNodeReachabilityAnnouncePass(ctx, rt, "startup", &state)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runAutoNodeReachabilityAnnouncePass(ctx, rt, "tick", &state)
			case reason := <-eventCh:
				runAutoNodeReachabilityAnnouncePass(ctx, rt, reason, &state)
			}
		}
	}()
}

func (n *autoNodeReachabilityNotifiee) Listen(libnetwork.Network, ma.Multiaddr) {
	if n == nil || n.enqueue == nil {
		return
	}
	n.enqueue("listen_changed")
}

func (n *autoNodeReachabilityNotifiee) ListenClose(libnetwork.Network, ma.Multiaddr) {
	if n == nil || n.enqueue == nil {
		return
	}
	n.enqueue("listen_changed")
}

func (n *autoNodeReachabilityNotifiee) Connected(libnetwork.Network, libnetwork.Conn)      {}
func (n *autoNodeReachabilityNotifiee) Disconnected(libnetwork.Network, libnetwork.Conn)   {}
func (n *autoNodeReachabilityNotifiee) OpenedStream(libnetwork.Network, libnetwork.Stream) {}
func (n *autoNodeReachabilityNotifiee) ClosedStream(libnetwork.Network, libnetwork.Stream) {}

func runAutoNodeReachabilityAnnouncePass(ctx context.Context, rt *Runtime, reason string, state *autoNodeReachabilityAnnounceState) {
	if rt == nil || rt.Host == nil || state == nil {
		return
	}
	if !cfgBool(rt.runIn.Reachability.AutoAnnounceEnabled, true) {
		return
	}
	if len(rt.HealthyGWs) == 0 {
		return
	}
	ttlSeconds := rt.runIn.Reachability.AnnounceTTLSeconds
	if ttlSeconds == 0 {
		ttlSeconds = 3600
	}
	gateway := gatewayBusinessID(rt, rt.HealthyGWs[0].ID)
	nodePubkeyHex, _, fingerprint, err := currentLocalNodeReachabilitySnapshot(rt)
	if err != nil {
		msg := err.Error()
		if state.LastAttemptError != msg || state.LastAttemptReason != reason {
			obs.Error("bitcast-client", "auto_node_reachability_snapshot_failed", map[string]any{
				"reason": reason,
				"error":  msg,
			})
			state.LastAttemptError = msg
			state.LastAttemptReason = reason
		}
		return
	}
	nowUnix := time.Now().Unix()
	if !shouldAutoNodeReachabilityAnnounce(nowUnix, *state, fingerprint, gateway) {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := TriggerGatewayAnnounceNodeReachability(callCtx, rt, AnnounceNodeReachabilityParams{
		TTLSeconds:    ttlSeconds,
		GatewayPeerID: gateway,
	}); err != nil {
		msg := err.Error()
		if state.LastAttemptError != msg || state.LastAttemptReason != reason {
			obs.Error("bitcast-client", "auto_node_reachability_announce_failed", map[string]any{
				"reason":             reason,
				"gateway_pubkey_hex": gateway,
				"node_pubkey_hex":    nodePubkeyHex,
				"error":              msg,
			})
			state.LastAttemptError = msg
			state.LastAttemptReason = reason
		}
		return
	}
	state.LastFingerprint = fingerprint
	state.LastGatewayPubkeyHex = gateway
	state.LastExpiresAtUnix = nowUnix + int64(ttlSeconds)
	state.LastAttemptError = ""
	state.LastAttemptReason = reason
	state.LastAnnouncedAtUnix = nowUnix
	state.LastAnnouncedNodeID = nodePubkeyHex
	obs.Business("bitcast-client", "auto_node_reachability_announced", map[string]any{
		"reason":             reason,
		"gateway_pubkey_hex": gateway,
		"node_pubkey_hex":    nodePubkeyHex,
		"expires_at_unix":    state.LastExpiresAtUnix,
	})
}

// TriggerGatewayAnnounceNodeReachability 让节点把“我当前在哪些地址上可达”发布到 gateway 目录。
// 设计说明：
// - head_height + seq 是声明版本，不和费用池 sequence 混用；
// - TTL 属于节点本地设置，gateway 不替节点决定声明寿命；
// - 广播声明必须由节点自己签名，方便以后跨 gateway 转发时仍能验证主体。
func TriggerGatewayAnnounceNodeReachability(ctx context.Context, rt *Runtime, p AnnounceNodeReachabilityParams) (broadcastmodule.NodeReachabilityAnnouncePaidResp, error) {
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("no healthy gateway")
	}
	ttlSeconds := p.TTLSeconds
	if ttlSeconds == 0 {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("ttl_seconds must be >= 1")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	info, err := callNodePoolInfo(ctx, rt, gw.ID)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	if kernel := ensureClientKernel(rt); kernel != nil {
		kres := kernel.dispatch(ctx, clientKernelCommand{
			CommandType:   clientKernelCommandFeePoolEnsureActive,
			GatewayPeerID: gw.ID.String(),
			RequestedBy:   "trigger_node_reachability_announce",
			Payload: map[string]any{
				"trigger":     "node_reachability_announce",
				"ttl_seconds": ttlSeconds,
			},
			AllowWhenPaused: true,
		})
		if kres.Status != "applied" {
			if strings.TrimSpace(kres.ErrorMessage) != "" {
				return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.ErrorMessage)
			}
			return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.Status)
		}
	}
	nodePubkeyHex, err := localPubKeyHex(rt.Host)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	_, multiaddrs, _, err := currentLocalNodeReachabilitySnapshot(rt)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	headHeight, err := rt.ActionChain.GetTipHeight()
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("query head height failed: %w", err)
	}
	state, _, err := loadSelfNodeReachabilityState(rt.DB, nodePubkeyHex)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	announceSeq := uint64(1)
	if state.HeadHeight == uint64(headHeight) {
		announceSeq = state.Seq + 1
	}
	publishedAtUnix := time.Now().Unix()
	expiresAtUnix := publishedAtUnix + int64(ttlSeconds)
	signPayload, err := broadcastmodule.BuildNodeReachabilitySignPayload(nodePubkeyHex, multiaddrs, uint64(headHeight), announceSeq, publishedAtUnix, expiresAtUnix)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	hostPriv := rt.Host.Peerstore().PrivKey(rt.Host.ID())
	if hostPriv == nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("missing host private key")
	}
	announcementSig, err := hostPriv.Sign(signPayload)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("sign node reachability announcement failed: %w", err)
	}
	signedAnnouncement, err := broadcastmodule.MarshalSignedNodeReachabilityAnnouncement(broadcastmodule.NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      multiaddrs,
		HeadHeight:      uint64(headHeight),
		Seq:             announceSeq,
		PublishedAtUnix: publishedAtUnix,
		ExpiresAtUnix:   expiresAtUnix,
		Signature:       append([]byte(nil), announcementSig...),
	})
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	charge := info.SinglePublishFeeSatoshi
	if charge == 0 {
		charge = 1
	}
	payloadRaw, err := broadcastmodule.MarshalNodeReachabilityAnnounceQuotePayload(signedAnnouncement)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		Session:              session,
		GatewayPeerID:        gw.ID,
		ServiceType:          broadcastmodule.QuoteServiceTypeNodeReachabilityAnnounce,
		Target:               nodePubkeyHex,
		ServiceParamsPayload: payloadRaw,
		PricingMode:          poolcore.ServiceOfferPricingModeFixedPrice,
		ProposedPaymentSat:   charge,
	})
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("request node reachability announce quote failed: %w", err)
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	built, err := buildFeePoolUpdatedTxWithProof(feePoolProofArgs{
		Session:         session,
		ClientActor:     clientActor,
		GatewayPub:      quoted.GatewayPub,
		ServiceQuoteRaw: quoted.ServiceQuoteRaw,
		ServiceQuote:    quoted.ServiceQuote,
	})
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(built.UpdatedTx, clientActor.PrivKey, quoted.GatewayPub)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	req := broadcastmodule.NodeReachabilityAnnouncePaidReq{
		ClientID:            rt.runIn.ClientID,
		SignedAnnouncement:  append([]byte(nil), signedAnnouncement...),
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      quoted.ServiceQuote.SequenceNumber,
		ServerAmount:        quoted.ServiceQuote.ServerAmountAfter,
		ChargeAmountSatoshi: quoted.ServiceQuote.ChargeAmountSatoshi,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        quoted.ServiceQuote.ChargeReason,
		ProofIntent:         append([]byte(nil), built.ProofIntent...),
		SignedProofCommit:   append([]byte(nil), built.SignedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	}
	var resp broadcastmodule.NodeReachabilityAnnouncePaidResp
	if err := pproto.CallProto(ctx, rt.Host, gw.ID, broadcastmodule.ProtoNodeReachabilityAnnouncePaid, gwSec(rt.rpcTrace), req, &resp); err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway announce node reachability failed"
		}
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("gateway node reachability announce rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	payload, err := broadcastmodule.MarshalNodeReachabilityAnnounceServicePayload(resp)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	if err := verifyServiceReceiptOrFreeze(ctx, rt, gw.ID, session, resp.MergedCurrentTx, expectedServiceReceipt{
		ServiceType:        broadcastmodule.ServiceTypeNodeReachabilityAnnounce,
		SpendTxID:          session.SpendTxID,
		SequenceNumber:     quoted.ServiceQuote.SequenceNumber,
		AcceptedChargeHash: built.AcceptedChargeHash,
		ResultCode:         strings.TrimSpace(resp.Status),
		ResultPayloadBytes: payload,
	}, resp.ServiceReceipt); err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	nextTxHex := built.UpdatedTx.Hex()
	if len(resp.MergedCurrentTx) > 0 {
		nextTxHex = strings.ToLower(hex.EncodeToString(resp.MergedCurrentTx))
	}
	applyFeePoolChargeToSession(session, quoted.ServiceQuote.SequenceNumber, quoted.ServiceQuote.ServerAmountAfter, nextTxHex)
	appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
		FlowID:          "fee_pool:" + session.SpendTxID,
		FlowType:        "fee_pool",
		RefID:           session.SpendTxID,
		Stage:           "use_node_reachability_announce",
		Direction:       "out",
		Purpose:         "node_reachability_announce_fee",
		AmountSatoshi:   -int64(quoted.ServiceQuote.ChargeAmountSatoshi),
		UsedSatoshi:     int64(quoted.ServiceQuote.ChargeAmountSatoshi),
		ReturnedSatoshi: 0,
		RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
		Note:            fmt.Sprintf("head_height=%d seq=%d ttl_seconds=%d", headHeight, announceSeq, ttlSeconds),
		Payload:         req,
	})
	if err := saveSelfNodeReachabilityState(rt.DB, selfNodeReachabilityState{
		NodePubkeyHex: nodePubkeyHex,
		HeadHeight:    uint64(headHeight),
		Seq:           announceSeq,
	}); err != nil {
		return broadcastmodule.NodeReachabilityAnnouncePaidResp{}, err
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_node_reachability_announce_end", map[string]any{
		"gateway_pubkey_hex":     gatewayBusinessID(rt, gw.ID),
		"node_pubkey_hex":        nodePubkeyHex,
		"head_height":            headHeight,
		"seq":                    announceSeq,
		"charged_amount_satoshi": charge,
		"updated_txid":           strings.TrimSpace(resp.UpdatedTxID),
	})
	return resp, nil
}

// TriggerGatewayQueryNodeReachability 向 gateway 查询目标节点最新地址声明，并把结果缓存到本地。
// 设计说明：
// - 查询未命中也收费，因此 RPC 成功后不管 found 与否都要落账；
// - 缓存是客户端行为：gateway 只返回最新有效声明，客户端自己决定何时复查；
// - 查询成功后把多地址全部注入 peerstore，连接层不做“最佳地址”裁判。
func TriggerGatewayQueryNodeReachability(ctx context.Context, rt *Runtime, p QueryNodeReachabilityParams) (broadcastmodule.NodeReachabilityQueryPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	if len(rt.HealthyGWs) == 0 {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	targetNodePubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.TargetNodePubkeyHex))
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("target_node_pubkey_hex invalid: %w", err)
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	info, err := callNodePoolInfo(ctx, rt, gw.ID)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	if kernel := ensureClientKernel(rt); kernel != nil {
		kres := kernel.dispatch(ctx, clientKernelCommand{
			CommandType:   clientKernelCommandFeePoolEnsureActive,
			GatewayPeerID: gw.ID.String(),
			RequestedBy:   "trigger_node_reachability_query",
			Payload: map[string]any{
				"trigger":                "node_reachability_query",
				"target_node_pubkey_hex": targetNodePubkeyHex,
			},
			AllowWhenPaused: true,
		})
		if kres.Status != "applied" {
			if strings.TrimSpace(kres.ErrorMessage) != "" {
				return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.ErrorMessage)
			}
			return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("ensure fee pool failed: %s", kres.Status)
		}
	}
	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("fee pool session missing for gateway=%s", gw.ID.String())
	}
	charge := info.SingleQueryFeeSatoshi
	if charge == 0 {
		charge = 1
	}
	payloadRaw, err := broadcastmodule.MarshalNodeReachabilityQueryQuotePayload(targetNodePubkeyHex)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	quoted, err := requestGatewayServiceQuote(ctx, rt, feePoolServiceQuoteArgs{
		Session:              session,
		GatewayPeerID:        gw.ID,
		ServiceType:          broadcastmodule.QuoteServiceTypeNodeReachabilityQuery,
		Target:               targetNodePubkeyHex,
		ServiceParamsPayload: payloadRaw,
		PricingMode:          poolcore.ServiceOfferPricingModeFixedPrice,
		ProposedPaymentSat:   charge,
	})
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("request node reachability query quote failed: %w", err)
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	built, err := buildFeePoolUpdatedTxWithProof(feePoolProofArgs{
		Session:         session,
		ClientActor:     clientActor,
		GatewayPub:      quoted.GatewayPub,
		ServiceQuoteRaw: quoted.ServiceQuoteRaw,
		ServiceQuote:    quoted.ServiceQuote,
	})
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(built.UpdatedTx, clientActor.PrivKey, quoted.GatewayPub)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	req := broadcastmodule.NodeReachabilityQueryPaidReq{
		ClientID:            rt.runIn.ClientID,
		TargetNodePubkeyHex: targetNodePubkeyHex,
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      quoted.ServiceQuote.SequenceNumber,
		ServerAmount:        quoted.ServiceQuote.ServerAmountAfter,
		ChargeAmountSatoshi: quoted.ServiceQuote.ChargeAmountSatoshi,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        quoted.ServiceQuote.ChargeReason,
		ProofIntent:         append([]byte(nil), built.ProofIntent...),
		SignedProofCommit:   append([]byte(nil), built.SignedProofCommit...),
		ServiceQuote:        append([]byte(nil), quoted.ServiceQuoteRaw...),
	}
	var resp broadcastmodule.NodeReachabilityQueryPaidResp
	if err := pproto.CallProto(ctx, rt.Host, gw.ID, broadcastmodule.ProtoNodeReachabilityQueryPaid, gwSec(rt.rpcTrace), req, &resp); err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway query node reachability failed"
		}
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, fmt.Errorf("gateway node reachability query rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	payload, err := broadcastmodule.MarshalNodeReachabilityQueryServicePayload(resp)
	if err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	if err := verifyServiceReceiptOrFreeze(ctx, rt, gw.ID, session, resp.MergedCurrentTx, expectedServiceReceipt{
		ServiceType:        broadcastmodule.ServiceTypeNodeReachabilityQuery,
		SpendTxID:          session.SpendTxID,
		SequenceNumber:     quoted.ServiceQuote.SequenceNumber,
		AcceptedChargeHash: built.AcceptedChargeHash,
		ResultCode:         strings.TrimSpace(resp.Status),
		ResultPayloadBytes: payload,
	}, resp.ServiceReceipt); err != nil {
		return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
	}
	nextTxHex := built.UpdatedTx.Hex()
	if len(resp.MergedCurrentTx) > 0 {
		nextTxHex = strings.ToLower(hex.EncodeToString(resp.MergedCurrentTx))
	}
	applyFeePoolChargeToSession(session, quoted.ServiceQuote.SequenceNumber, quoted.ServiceQuote.ServerAmountAfter, nextTxHex)
	appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
		FlowID:          "fee_pool:" + session.SpendTxID,
		FlowType:        "fee_pool",
		RefID:           session.SpendTxID,
		Stage:           "use_node_reachability_query",
		Direction:       "out",
		Purpose:         "node_reachability_query_fee",
		AmountSatoshi:   -int64(quoted.ServiceQuote.ChargeAmountSatoshi),
		UsedSatoshi:     int64(quoted.ServiceQuote.ChargeAmountSatoshi),
		ReturnedSatoshi: 0,
		RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
		Note:            fmt.Sprintf("target_node_pubkey_hex=%s found=%t", targetNodePubkeyHex, resp.Found),
		Payload:         resp,
	})
	if resp.Found {
		ann, err := announcementFromQueryResp(resp)
		if err != nil {
			return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
		}
		if err := saveNodeReachabilityCache(rt.DB, gatewayBusinessID(rt, gw.ID), ann); err != nil {
			return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
		}
		if err := injectNodeReachabilityAnnouncement(rt, ann); err != nil {
			return broadcastmodule.NodeReachabilityQueryPaidResp{}, err
		}
	}
	obs.Business("bitcast-client", "evt_trigger_gateway_node_reachability_query_end", map[string]any{
		"gateway_pubkey_hex":     gatewayBusinessID(rt, gw.ID),
		"target_node_pubkey_hex": targetNodePubkeyHex,
		"found":                  resp.Found,
		"charged_amount_satoshi": charge,
		"updated_txid":           strings.TrimSpace(resp.UpdatedTxID),
	})
	return resp, nil
}

func loadCachedNodeReachability(db *sql.DB, targetNodePubkeyHex string, nowUnix int64) (broadcastmodule.NodeReachabilityAnnouncement, bool, error) {
	if db == nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, nil
	}
	targetNodePubkeyHex, err := normalizeCompressedPubKeyHex(targetNodePubkeyHex)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, err
	}
	var (
		sourceGatewayPubkeyHex string
		headHeight             uint64
		seq                    uint64
		multiaddrsJSON         string
		publishedAtUnix        int64
		expiresAtUnix          int64
		signature              []byte
	)
	err = db.QueryRow(
		`SELECT source_gateway_pubkey_hex,head_height,seq,multiaddrs_json,published_at_unix,expires_at_unix,signature
		   FROM node_reachability_cache
		  WHERE target_node_pubkey_hex=? AND expires_at_unix>?`,
		targetNodePubkeyHex,
		nowUnix,
	).Scan(&sourceGatewayPubkeyHex, &headHeight, &seq, &multiaddrsJSON, &publishedAtUnix, &expiresAtUnix, &signature)
	if err == sql.ErrNoRows {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, nil
	}
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, err
	}
	addrs, err := unmarshalReachabilityStringList(multiaddrsJSON)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, false, err
	}
	return broadcastmodule.NodeReachabilityAnnouncement{
		NodePubkeyHex:   targetNodePubkeyHex,
		Multiaddrs:      addrs,
		HeadHeight:      headHeight,
		Seq:             seq,
		PublishedAtUnix: publishedAtUnix,
		ExpiresAtUnix:   expiresAtUnix,
		Signature:       append([]byte(nil), signature...),
	}, true, nil
}

func saveNodeReachabilityCache(db *sql.DB, sourceGatewayPubkeyHex string, ann broadcastmodule.NodeReachabilityAnnouncement) error {
	if db == nil {
		return nil
	}
	ann.NodePubkeyHex = strings.ToLower(strings.TrimSpace(ann.NodePubkeyHex))
	multiaddrsJSON, err := marshalReachabilityStringList(ann.Multiaddrs)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	_, err = db.Exec(
		`INSERT INTO node_reachability_cache(
			target_node_pubkey_hex,source_gateway_pubkey_hex,head_height,seq,multiaddrs_json,published_at_unix,expires_at_unix,signature,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(target_node_pubkey_hex) DO UPDATE SET
			source_gateway_pubkey_hex=excluded.source_gateway_pubkey_hex,
			head_height=excluded.head_height,
			seq=excluded.seq,
			multiaddrs_json=excluded.multiaddrs_json,
			published_at_unix=excluded.published_at_unix,
			expires_at_unix=excluded.expires_at_unix,
			signature=excluded.signature,
			updated_at_unix=excluded.updated_at_unix`,
		ann.NodePubkeyHex,
		strings.ToLower(strings.TrimSpace(sourceGatewayPubkeyHex)),
		ann.HeadHeight,
		ann.Seq,
		multiaddrsJSON,
		ann.PublishedAtUnix,
		ann.ExpiresAtUnix,
		append([]byte(nil), ann.Signature...),
		now,
	)
	return err
}

func saveSelfNodeReachabilityState(db *sql.DB, state selfNodeReachabilityState) error {
	if db == nil {
		return nil
	}
	_, err := db.Exec(
		`INSERT INTO self_node_reachability_state(node_pubkey_hex,head_height,seq,updated_at_unix) VALUES(?,?,?,?)
		 ON CONFLICT(node_pubkey_hex) DO UPDATE SET
			head_height=excluded.head_height,
			seq=excluded.seq,
			updated_at_unix=excluded.updated_at_unix`,
		strings.ToLower(strings.TrimSpace(state.NodePubkeyHex)),
		state.HeadHeight,
		state.Seq,
		time.Now().Unix(),
	)
	return err
}

func loadSelfNodeReachabilityState(db *sql.DB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	if db == nil {
		return selfNodeReachabilityState{}, false, nil
	}
	var out selfNodeReachabilityState
	err := db.QueryRow(`SELECT node_pubkey_hex,head_height,seq FROM self_node_reachability_state WHERE node_pubkey_hex=?`, strings.ToLower(strings.TrimSpace(nodePubkeyHex))).Scan(
		&out.NodePubkeyHex,
		&out.HeadHeight,
		&out.Seq,
	)
	if err == sql.ErrNoRows {
		return selfNodeReachabilityState{}, false, nil
	}
	if err != nil {
		return selfNodeReachabilityState{}, false, err
	}
	return out, true, nil
}

func announcementFromQueryResp(resp broadcastmodule.NodeReachabilityQueryPaidResp) (broadcastmodule.NodeReachabilityAnnouncement, error) {
	ann, err := broadcastmodule.UnmarshalSignedNodeReachabilityAnnouncement(resp.SignedAnnouncement)
	if err != nil {
		return broadcastmodule.NodeReachabilityAnnouncement{}, err
	}
	return ann, nil
}

func injectNodeReachabilityAnnouncement(rt *Runtime, ann broadcastmodule.NodeReachabilityAnnouncement) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	pid, err := poolcore.PeerIDFromClientID(ann.NodePubkeyHex)
	if err != nil {
		return err
	}
	ttl := time.Until(time.Unix(ann.ExpiresAtUnix, 0))
	if ttl <= 0 {
		return fmt.Errorf("announcement expired")
	}
	for _, raw := range ann.Multiaddrs {
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return err
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return err
		}
		if info.ID != pid {
			return fmt.Errorf("announcement peer id mismatch")
		}
		rt.Host.Peerstore().AddAddrs(pid, info.Addrs, ttl)
	}
	return nil
}

func announcementSemanticallyEqual(a, b broadcastmodule.NodeReachabilityAnnouncement) bool {
	if !strings.EqualFold(strings.TrimSpace(a.NodePubkeyHex), strings.TrimSpace(b.NodePubkeyHex)) {
		return false
	}
	if a.HeadHeight != b.HeadHeight || a.Seq != b.Seq || a.PublishedAtUnix != b.PublishedAtUnix || a.ExpiresAtUnix != b.ExpiresAtUnix {
		return false
	}
	if len(a.Multiaddrs) != len(b.Multiaddrs) || len(a.Signature) != len(b.Signature) {
		return false
	}
	for i := range a.Multiaddrs {
		if a.Multiaddrs[i] != b.Multiaddrs[i] {
			return false
		}
	}
	for i := range a.Signature {
		if a.Signature[i] != b.Signature[i] {
			return false
		}
	}
	return true
}

func connectViaPeerstore(ctx context.Context, rt *Runtime, pid peer.ID) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if rt.Host.Network().Connectedness(pid) == libnetwork.Connected {
		return nil
	}
	addrs := rt.Host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		return fmt.Errorf("target peer addrs unavailable")
	}
	return rt.Host.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: addrs})
}

func marshalReachabilityStringList(items []string) (string, error) {
	raw, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalReachabilityStringList(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func ensureTargetPeerReachable(ctx context.Context, rt *Runtime, targetPubkeyHex string, pid peer.ID) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if rt.Host.Network().Connectedness(pid) == libnetwork.Connected {
		return nil
	}
	nowUnix := time.Now().Unix()
	cachedAnn, cached, err := loadCachedNodeReachability(rt.DB, targetPubkeyHex, nowUnix)
	if err != nil {
		return err
	}
	if cached {
		if err := injectNodeReachabilityAnnouncement(rt, cachedAnn); err != nil {
			return err
		}
		if err := connectViaPeerstore(ctx, rt, pid); err == nil {
			return nil
		}
	}
	if err := connectViaPeerstore(ctx, rt, pid); err == nil {
		return nil
	}
	queryResp, err := TriggerGatewayQueryNodeReachability(ctx, rt, QueryNodeReachabilityParams{
		TargetNodePubkeyHex: targetPubkeyHex,
	})
	if err != nil {
		return err
	}
	if !queryResp.Found {
		return fmt.Errorf("target peer addrs unavailable")
	}
	latestAnn, err := announcementFromQueryResp(queryResp)
	if err != nil {
		return err
	}
	if cached && announcementSemanticallyEqual(cachedAnn, latestAnn) {
		return fmt.Errorf("target peer connect failed with cached announcement")
	}
	if err := injectNodeReachabilityAnnouncement(rt, latestAnn); err != nil {
		return err
	}
	if err := connectViaPeerstore(ctx, rt, pid); err != nil {
		return fmt.Errorf("target peer connect failed with latest announcement: %w", err)
	}
	return nil
}
