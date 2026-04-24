package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	broadcastmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/broadcast"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
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

func startAutoNodeReachabilityAnnounceLoop(ctx context.Context, rt *Runtime, store *clientDB) {
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
		runAutoNodeReachabilityAnnouncePass(ctx, rt, store, "startup", &state)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runAutoNodeReachabilityAnnouncePass(ctx, rt, store, "tick", &state)
			case reason := <-eventCh:
				runAutoNodeReachabilityAnnouncePass(ctx, rt, store, reason, &state)
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

func runAutoNodeReachabilityAnnouncePass(ctx context.Context, rt *Runtime, store *clientDB, reason string, state *autoNodeReachabilityAnnounceState) {
	if rt == nil || rt.Host == nil || state == nil {
		return
	}
	cfg := rt.ConfigSnapshot()
	if !cfgBool(cfg.Reachability.AutoAnnounceEnabled, true) {
		return
	}
	if rt.gwManager == nil {
		return
	}
	gatewayPeerID := rt.gwManager.GetMasterGateway()
	if gatewayPeerID == "" {
		return
	}
	ttlSeconds := cfg.Reachability.AnnounceTTLSeconds
	if ttlSeconds == 0 {
		ttlSeconds = 3600
	}
	gateway := gatewayBusinessID(rt, gatewayPeerID)
	nodePubkeyHex, _, fingerprint, err := currentLocalNodeReachabilitySnapshot(rt)
	if err != nil {
		msg := err.Error()
		if state.LastAttemptError != msg || state.LastAttemptReason != reason {
			obs.Error(ServiceName, "auto_node_reachability_snapshot_failed", map[string]any{
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
	if _, err := TriggerGatewayAnnounceNodeReachability(callCtx, store, rt, AnnounceNodeReachabilityParams{
		TTLSeconds:    ttlSeconds,
		GatewayPeerID: gateway,
	}); err != nil {
		msg := err.Error()
		if state.LastAttemptError != msg || state.LastAttemptReason != reason {
			obs.Error(ServiceName, "auto_node_reachability_announce_failed", map[string]any{
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
	obs.Business(ServiceName, "auto_node_reachability_announced", map[string]any{
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
func TriggerGatewayAnnounceNodeReachability(ctx context.Context, store *clientDB, rt *Runtime, p AnnounceNodeReachabilityParams) (contractmessage.NodeReachabilityAnnouncePaidResp, error) {
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("runtime not initialized")
	}
	ttlSeconds := p.TTLSeconds
	if ttlSeconds == 0 {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("ttl_seconds must be >= 1")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if len(rt.HealthyGWs) == 0 {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("no healthy gateway")
	}
	nodePubkeyHex, err := localPubKeyHex(rt.Host)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	_, multiaddrs, _, err := currentLocalNodeReachabilitySnapshot(rt)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	headHeight, err := rt.ActionChain.GetTipHeight()
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("query head height failed: %w", err)
	}
	state, _, err := dbLoadSelfNodeReachabilityState(ctx, store, nodePubkeyHex)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	announceSeq := uint64(1)
	if state.HeadHeight == uint64(headHeight) {
		announceSeq = state.Seq + 1
	}
	publishedAtUnix := time.Now().Unix()
	expiresAtUnix := publishedAtUnix + int64(ttlSeconds)
	signPayload, err := broadcastmodule.BuildNodeReachabilitySignPayload(nodePubkeyHex, multiaddrs, uint64(headHeight), announceSeq, publishedAtUnix, expiresAtUnix)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	hostPriv := rt.Host.Peerstore().PrivKey(rt.Host.ID())
	if hostPriv == nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("missing host private key")
	}
	announcementSig, err := hostPriv.Sign(signPayload)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("sign node reachability announcement failed: %w", err)
	}
	signedAnnouncement, err := marshalSignedNodeReachabilityAnnouncement(broadcastmodule.NodeReachabilityAnnouncement{
		NodePubkeyHex:   nodePubkeyHex,
		Multiaddrs:      multiaddrs,
		HeadHeight:      uint64(headHeight),
		Seq:             announceSeq,
		PublishedAtUnix: publishedAtUnix,
		ExpiresAtUnix:   expiresAtUnix,
		Signature:       append([]byte(nil), announcementSig...),
	})
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	body := &contractmessage.NodeReachabilityAnnounceReq{
		SignedAnnouncement: append([]byte(nil), signedAnnouncement...),
	}
	resp, _, err := triggerTypedPeerCall(ctx, store, rt, gatewayBusinessID(rt, gw.ID), protocol.ID(ncall.ProtoBroadcastV1NodeReachabilityAnnounce), body, decodeNodeReachabilityAnnounceRouteResp)
	if err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway announce node reachability failed"
		}
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, fmt.Errorf("gateway node reachability announce rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if err := dbSaveSelfNodeReachabilityState(ctx, store, selfNodeReachabilityState{
		NodePubkeyHex: nodePubkeyHex,
		HeadHeight:    uint64(headHeight),
		Seq:           announceSeq,
	}); err != nil {
		return contractmessage.NodeReachabilityAnnouncePaidResp{}, err
	}
	obs.Business(ServiceName, "evt_trigger_gateway_node_reachability_announce_end", map[string]any{
		"gateway_pubkey_hex":     gatewayBusinessID(rt, gw.ID),
		"node_pubkey_hex":        nodePubkeyHex,
		"head_height":            headHeight,
		"seq":                    announceSeq,
		"charged_amount_satoshi": resp.ChargedAmount,
		"updated_txid":           strings.TrimSpace(resp.UpdatedTxID),
	})
	return resp, nil
}

// TriggerGatewayQueryNodeReachability 向 gateway 查询目标节点最新地址声明，并把结果缓存到本地。
// 设计说明：
// - 查询未命中也收费，因此 RPC 成功后不管 found 与否都要落账；
// - 缓存是客户端行为：gateway 只返回最新有效声明，客户端自己决定何时复查；
// - 查询成功后把多地址全部注入 peerstore，连接层不做“最佳地址”裁判。
func TriggerGatewayQueryNodeReachability(ctx context.Context, store *clientDB, rt *Runtime, p QueryNodeReachabilityParams) (contractmessage.NodeReachabilityQueryPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	targetNodePubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.TargetNodePubkeyHex))
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("target_node_pubkey_hex invalid: %w", err)
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if len(rt.HealthyGWs) == 0 {
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	body := &contractmessage.NodeReachabilityQueryReq{
		TargetNodePubkeyHex: targetNodePubkeyHex,
	}
	resp, _, err := triggerTypedPeerCall(ctx, store, rt, gatewayBusinessID(rt, gw.ID), protocol.ID(ncall.ProtoBroadcastV1NodeReachabilityQuery), body, decodeNodeReachabilityQueryRouteResp)
	if err != nil {
		return contractmessage.NodeReachabilityQueryPaidResp{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway query node reachability failed"
		}
		return contractmessage.NodeReachabilityQueryPaidResp{}, fmt.Errorf("gateway node reachability query rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if resp.Found {
		ann, err := announcementFromQueryResp(resp)
		if err != nil {
			return contractmessage.NodeReachabilityQueryPaidResp{}, err
		}
		if err := dbSaveNodeReachabilityCache(ctx, store, gatewayBusinessID(rt, gw.ID), ann); err != nil {
			return contractmessage.NodeReachabilityQueryPaidResp{}, err
		}
		if err := injectNodeReachabilityAnnouncement(rt, ann); err != nil {
			return contractmessage.NodeReachabilityQueryPaidResp{}, err
		}
	}
	obs.Business(ServiceName, "evt_trigger_gateway_node_reachability_query_end", map[string]any{
		"gateway_pubkey_hex":     gatewayBusinessID(rt, gw.ID),
		"target_node_pubkey_hex": targetNodePubkeyHex,
		"found":                  resp.Found,
		"charged_amount_satoshi": resp.ChargedAmount,
		"updated_txid":           strings.TrimSpace(resp.UpdatedTxID),
	})
	return resp, nil
}

func announcementFromQueryResp(resp contractmessage.NodeReachabilityQueryPaidResp) (broadcastmodule.NodeReachabilityAnnouncement, error) {
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

func ensureTargetPeerReachable(ctx context.Context, store *clientDB, rt *Runtime, targetPubkeyHex string, pid peer.ID) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if rt.Host.Network().Connectedness(pid) == libnetwork.Connected {
		return nil
	}
	nowUnix := time.Now().Unix()
	cachedAnn, cached, err := dbLoadCachedNodeReachability(ctx, store, targetPubkeyHex, nowUnix)
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
	queryResp, err := TriggerGatewayQueryNodeReachability(ctx, store, rt, QueryNodeReachabilityParams{
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
