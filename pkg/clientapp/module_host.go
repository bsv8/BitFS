package clientapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	contractmessage "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/pproto"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/bsv8/BitFS/pkg/clientapp/seedstorage"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

type moduleHost struct {
	rt    *Runtime
	store moduleBootstrapStore
	seed  moduleapi.SeedStorage
	once  sync.Once
}

func validateModuleSpec(spec moduleapi.ModuleSpec) error {
	for _, hr := range spec.HTTP {
		if hr.Handler == nil {
			return fmt.Errorf("http route %s: handler is required", hr.Path)
		}
	}
	for _, lp := range spec.LibP2P {
		if lp.Handler == nil {
			return fmt.Errorf("libp2p %s: handler is required", lp.ProtocolID)
		}
	}
	for _, sa := range spec.Settings {
		if sa.Handler == nil {
			return fmt.Errorf("settings action %s: handler is required", sa.Action)
		}
	}
	for _, oa := range spec.OBS {
		if oa.Handler == nil {
			return fmt.Errorf("obs action %s: handler is required", oa.Action)
		}
	}
	for _, dr := range spec.DomainResolvers {
		if dr.Handler == nil {
			return fmt.Errorf("domain resolver %s: handler is required", dr.Name)
		}
	}
	return nil
}

func newModuleHost(rt *Runtime, store moduleBootstrapStore) moduleapi.Host {
	if rt == nil {
		return nil
	}
	return &moduleHost{rt: rt, store: store}
}

func (h *moduleHost) Store() moduleapi.Store {
	if h == nil || h.store == nil {
		return nil
	}
	return moduleStoreAdapter{store: h.store}
}

func (h *moduleHost) ConfigPath() string {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return ""
	}
	return h.rt.RuntimeConfigService().ConfigPath()
}

func (h *moduleHost) NodePubkeyHex() string {
	if h == nil || h.rt == nil {
		return ""
	}
	return h.rt.NodePubkeyHex()
}

func (h *moduleHost) ClientPubkeyHex() string {
	return h.NodePubkeyHex()
}

func (h *moduleHost) SeedStorage() moduleapi.SeedStorage {
	if h == nil || h.rt == nil {
		return nil
	}
	h.once.Do(func() {
		h.seed = seedstorage.NewService(h)
	})
	return h.seed
}

func (h *moduleHost) SeedStore() moduleapi.SeedStore {
	if h == nil {
		return nil
	}
	if h.rt != nil && h.rt.store != nil {
		return h.rt.store
	}
	if store, ok := h.store.(*clientDB); ok {
		return store
	}
	return nil
}

func (h *moduleHost) WorkspaceStore() moduleapi.WorkspaceStore {
	if h == nil {
		return nil
	}
	if h.rt != nil && h.rt.store != nil {
		return h.rt.store
	}
	if store, ok := h.store.(*clientDB); ok {
		return store
	}
	return nil
}

func (h *moduleHost) FSWatchEnabled() bool {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return false
	}
	return h.rt.RuntimeConfigService().FSWatchEnabled()
}

func (h *moduleHost) FSRescanIntervalSeconds() uint32 {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return 0
	}
	return h.rt.RuntimeConfigService().FSRescanIntervalSeconds()
}

func (h *moduleHost) StartupFullScan() bool {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return false
	}
	return h.rt.RuntimeConfigService().StartupFullScan()
}

func (h *moduleHost) SellerFloorPriceSatPer64K() uint64 {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return 0
	}
	return h.rt.RuntimeConfigService().SellerFloorPriceSatPer64K()
}

func (h *moduleHost) SellerResaleDiscountBPS() uint64 {
	if h == nil || h.rt == nil || h.rt.RuntimeConfigService() == nil {
		return 0
	}
	return h.rt.RuntimeConfigService().SellerResaleDiscountBPS()
}

func (h *moduleHost) RegisterCapabilityItem(item *contractmessage.CapabilityItem) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if item == nil {
		return nil, fmt.Errorf("capability item is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.registerCapabilityHook(func() *contractmessage.CapabilityItem {
		return &contractmessage.CapabilityItem{
			ID:         strings.TrimSpace(item.ID),
			Version:    item.Version,
			ProtocolID: strings.TrimSpace(item.ProtocolID),
		}
	})
}

func (h *moduleHost) RegisterLibP2P(protocolID protocol.ID, hook moduleapi.LibP2PHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterLibP2PHook(protocolID, func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		out, err := hook(ctx, moduleapi.LibP2PEvent{
			Protocol:        ev.Protocol,
			MessageID:       ev.Meta.MessageID,
			SenderPubkeyHex: ev.Meta.SenderPubkeyHex,
			Request: moduleapi.LibP2PRequest{
				To:             ev.Req.To,
				Route:          ev.Req.Route,
				ContentType:    ev.Req.ContentType,
				Body:           append([]byte(nil), ev.Req.Body...),
				PaymentScheme:  ev.Req.PaymentScheme,
				PaymentPayload: append([]byte(nil), ev.Req.PaymentPayload...),
			},
		})
		if err != nil {
			if code := moduleapi.CodeOf(err); code != "" {
				return LibP2PResult{}, newModuleHookError(code, moduleapi.MessageOf(err))
			}
			return LibP2PResult{}, err
		}
		return LibP2PResult{
			CallResp: toInternalCallResponse(out.CallResp),
		}, nil
	})
}

func (h *moduleHost) RegisterHTTPRoute(path string, handler moduleapi.HTTPHandler) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	cleanPath := normalizeModuleHTTPPath(path)
	return reg.RegisterHTTPAPIHook(func(s *httpAPIServer, mux *http.ServeMux, prefix string) {
		if s == nil || mux == nil {
			return
		}
		mux.HandleFunc(prefix+cleanPath, s.withAuth(http.HandlerFunc(handler)))
	})
}

func (h *moduleHost) RegisterSettingsAction(action string, hook moduleapi.SettingsHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterSettingsHook(action, func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
		out, err := hook(ctx, gotAction, payload)
		if err != nil {
			if code := moduleapi.CodeOf(err); code != "" {
				return nil, newModuleHookError(code, moduleapi.MessageOf(err))
			}
			return nil, err
		}
		return out, nil
	})
}

func (h *moduleHost) RegisterOBSAction(action string, hook moduleapi.OBSControlHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterOBSControlHook(action, func(ctx context.Context, gotAction string, payload map[string]any) (OBSActionResponse, error) {
		out, err := hook(ctx, gotAction, payload)
		if err != nil {
			if code := moduleapi.CodeOf(err); code != "" {
				return OBSActionResponse{}, newModuleHookError(code, moduleapi.MessageOf(err))
			}
			return OBSActionResponse{}, err
		}
		return toInternalOBSActionResponse(out), nil
	})
}

func (h *moduleHost) RegisterDomainResolveHook(name string, hook moduleapi.DomainResolveHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterDomainResolveHook(name, func(ctx context.Context, domain string) (string, error) {
		return hook(ctx, domain)
	})
}

func (h *moduleHost) RegisterOpenHook(hook moduleapi.OpenHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterOpenHook(func(ctx context.Context) error {
		if err := hook(ctx); err != nil {
			if code := moduleapi.CodeOf(err); code != "" {
				return newModuleHookError(code, moduleapi.MessageOf(err))
			}
			return err
		}
		return nil
	})
}

func (h *moduleHost) RegisterCloseHook(hook moduleapi.CloseHook) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if hook == nil {
		return nil, fmt.Errorf("hook is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.RegisterCloseHook(func(ctx context.Context) error {
		if err := hook(ctx); err != nil {
			if code := moduleapi.CodeOf(err); code != "" {
				return newModuleHookError(code, moduleapi.MessageOf(err))
			}
			return err
		}
		return nil
	})
}

func (h *moduleHost) PeerCall(ctx context.Context, req moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	var out moduleapi.PeerCallResponse
	if h == nil || h.rt == nil {
		return out, fmt.Errorf("runtime is required")
	}
	clientStore, ok := h.store.(*clientDB)
	if !ok || clientStore == nil {
		return out, fmt.Errorf("peer call store is unavailable")
	}
	resp, err := TriggerPeerCall(ctx, h.rt, TriggerPeerCallParams{
		To:                   strings.TrimSpace(req.To),
		ProtocolID:           req.ProtocolID,
		ContentType:          strings.TrimSpace(req.ContentType),
		Body:                 append([]byte(nil), req.Body...),
		Store:                clientStore,
		PaymentMode:          strings.TrimSpace(req.PaymentMode),
		PaymentScheme:        strings.TrimSpace(req.PaymentScheme),
		ServiceQuote:         append([]byte(nil), req.ServiceQuote...),
		RequireActiveFeePool: req.RequireActiveFeePool,
	})
	if err != nil {
		return out, err
	}
	return toModuleAPICallResponse(resp), nil
}

func (h *moduleHost) GatewaySnapshot() []moduleapi.PeerNode {
	if h == nil || h.rt == nil {
		return nil
	}
	cfgSvc := h.rt.RuntimeConfigService()
	if cfgSvc == nil {
		return nil
	}
	raw := cfgSvc.GatewaySnapshot()
	if len(raw) == 0 {
		return nil
	}
	out := make([]moduleapi.PeerNode, 0, len(raw))
	for _, item := range raw {
		pubkeyHex, err := normalizeCompressedPubKeyHex(item.Pubkey)
		if err != nil {
			continue
		}
		out = append(out, moduleapi.PeerNode{
			Enabled:                   item.Enabled,
			Addr:                      strings.TrimSpace(item.Addr),
			Pubkey:                    pubkeyHex,
			ListenOfferPaymentSatoshi: item.ListenOfferPaymentSatoshi,
		})
	}
	return out
}

func (h *moduleHost) PreferredGatewayPubkeyHex() string {
	if h == nil || h.rt == nil {
		return ""
	}
	masterPID := h.rt.MasterGateway()
	if masterPID == "" {
		return ""
	}
	return peerIDToPubkeyHex(masterPID, h.rt.Host)
}

func (h *moduleHost) HealthyGatewaySnapshot() []moduleapi.PeerNode {
	if h == nil || h.rt == nil {
		return nil
	}
	if len(h.rt.HealthyGWs) == 0 {
		return nil
	}
	out := make([]moduleapi.PeerNode, 0, len(h.rt.HealthyGWs))
	for _, ai := range h.rt.HealthyGWs {
		pubkeyHex := peerIDToPubkeyHex(ai.ID, h.rt.Host)
		addrs := make([]string, 0, len(ai.Addrs))
		for _, a := range ai.Addrs {
			addrs = append(addrs, a.String())
		}
		out = append(out, moduleapi.PeerNode{
			Enabled: true,
			Addr:    strings.Join(addrs, ","),
			Pubkey:  pubkeyHex,
		})
	}
	return out
}

func (h *moduleHost) LocalAdvertiseAddrs() []string {
	if h == nil || h.rt == nil {
		return nil
	}
	return localAdvertiseAddrs(h.rt)
}

func (h *moduleHost) CurrentHeadHeight(ctx context.Context) (uint64, error) {
	if h == nil || h.rt == nil || h.rt.ActionChain == nil {
		return 0, fmt.Errorf("runtime not initialized")
	}
	height, err := h.rt.ActionChain.GetTipHeight()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

func (h *moduleHost) SignLocalNodePayload(ctx context.Context, payload []byte) ([]byte, error) {
	if h == nil || h.rt == nil || h.rt.Host == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("payload is empty")
	}
	hostPriv := h.rt.Host.Peerstore().PrivKey(h.rt.Host.ID())
	if hostPriv == nil {
		return nil, fmt.Errorf("missing host private key")
	}
	return hostPriv.Sign(payload)
}

func (h *moduleHost) InjectPeerAddrs(ctx context.Context, targetPubkeyHex string, addrs []string, ttlSeconds int64) error {
	if h == nil || h.rt == nil || h.rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(targetPubkeyHex) == "" {
		return fmt.Errorf("target pubkey hex is required")
	}
	if len(addrs) == 0 {
		return fmt.Errorf("addrs is empty")
	}
	if ttlSeconds <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	pid, err := poolcore.PeerIDFromClientID(targetPubkeyHex)
	if err != nil {
		return err
	}
	ttl := time.Duration(ttlSeconds) * time.Second
	for _, raw := range addrs {
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
		h.rt.Host.Peerstore().AddAddrs(pid, info.Addrs, ttl)
	}
	return nil
}

func (h *moduleHost) PeerCallRaw(ctx context.Context, peer string, protocolID string, request moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	if h == nil || h.rt == nil {
		return moduleapi.PeerCallResponse{}, fmt.Errorf("runtime is required")
	}
	clientStore, ok := h.store.(*clientDB)
	if !ok || clientStore == nil {
		return moduleapi.PeerCallResponse{}, fmt.Errorf("peer call store is unavailable")
	}
	resp, err := TriggerPeerCall(ctx, h.rt, TriggerPeerCallParams{
		To:                   strings.TrimSpace(peer),
		ProtocolID:           protocol.ID(protocolID),
		ContentType:          strings.TrimSpace(request.ContentType),
		Body:                 append([]byte(nil), request.Body...),
		Store:                clientStore,
		PaymentMode:          strings.TrimSpace(request.PaymentMode),
		PaymentScheme:        strings.TrimSpace(request.PaymentScheme),
		ServiceQuote:         append([]byte(nil), request.ServiceQuote...),
		RequireActiveFeePool: request.RequireActiveFeePool,
	})
	if err != nil {
		return moduleapi.PeerCallResponse{}, err
	}
	return toModuleAPICallResponse(resp), nil
}

func (h *moduleHost) QuoteService(ctx context.Context, peer string, protocolID string, request moduleapi.ServiceQuoteRequest) (moduleapi.ServiceQuoteResponse, error) {
	return moduleapi.ServiceQuoteResponse{}, fmt.Errorf("QuoteService not implemented: use PaymentFacade instead")
}

func (h *moduleHost) PaymentFacade() moduleapi.PaymentFacade {
	if h == nil || h.rt == nil {
		return nil
	}
	return ensurePaymentFacade(h.rt)
}

func (h *moduleHost) RegisterQuotedServicePayer(scheme string, payer moduleapi.QuotedServicePayer) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return ensurePaymentRegistry(h.rt).RegisterQuotedServicePayer(scheme, payer)
}

func (h *moduleHost) RegisterServiceCoverageSession(scheme string, session moduleapi.ServiceCoverageSession) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return ensurePaymentRegistry(h.rt).RegisterServiceCoverageSession(scheme, session)
}

func (h *moduleHost) RegisterTradeSession(scheme string, session moduleapi.TradeSession) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return ensurePaymentRegistry(h.rt).RegisterTradeSession(scheme, session)
}

func (h *moduleHost) Actor() (*poolcore.Actor, error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	identity, err := h.rt.runtimeIdentity()
	if err != nil {
		return nil, err
	}
	return identity.Actor, nil
}

func (h *moduleHost) SendProto(ctx context.Context, peerStr string, protocolID string, in any, out any) error {
	if h == nil || h.rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	peerID, err := peer.Decode(peerStr)
	if err != nil {
		return fmt.Errorf("decode peer id failed: %w", err)
	}
	// 这里不是固定“发给 gateway”，而是把已付费的业务请求发给对应服务端。
	// domain 路由要用 domain 壳，broadcast 路由要用 gateway 壳，避免 envelope 域名写错。
	sec := paidProtoSecurityForRoute(h.rt, protocolID)
	if err := pproto.CallProto[ncall.CallReq, ncall.CallResp](ctx, h.rt.Host, peerID, protocol.ID(protocolID), sec, in.(ncall.CallReq), out.(*ncall.CallResp)); err != nil {
		return fmt.Errorf("send proto failed: %w", err)
	}
	return nil
}

// ApplyLocalBroadcastWalletTxBytes 把已成功广播的钱包交易写回本地投影。
// 这是支付层的后置动作，只负责把“已花/新找零”同步进本地钱包视图。
func (h *moduleHost) ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error {
	if h == nil || h.rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	store, ok := h.store.(*clientDB)
	if !ok || store == nil {
		return fmt.Errorf("client store not available")
	}
	return applyLocalBroadcastWalletTxBytes(ctx, store, h.rt, rawTx, trigger)
}

func paidProtoSecurityForRoute(rt *Runtime, protocolID string) pproto.SecurityConfig {
	trace := pproto.TraceSink(nil)
	if rt != nil {
		trace = rt.TransferRPCTrace()
	}
	switch {
	case strings.Contains(protocolID, "/domain/"):
		return domainSec(trace)
	case strings.Contains(protocolID, "/arbiter/"):
		return arbSec(trace)
	case strings.Contains(protocolID, "/broadcast/"):
		fallthrough
	default:
		return gwSec(trace)
	}
}

func (h *moduleHost) WalletUTXOs(ctx context.Context) ([]poolcore.UTXO, error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	store, ok := h.store.(*clientDB)
	if !ok || store == nil {
		return nil, fmt.Errorf("client store not available")
	}
	return getWalletUTXOsFromDB(ctx, store, h.rt)
}

func (h *moduleHost) GetGatewayPubkey(gatewayPeerID peer.ID) (*ec.PublicKey, error) {
	if h == nil || h.rt == nil || h.rt.Host == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	pub := h.rt.Host.Peerstore().PubKey(gatewayPeerID)
	if pub == nil {
		return nil, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := pub.Raw()
	if err != nil {
		return nil, err
	}
	return ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
}

func (h *moduleHost) PoolInfo(ctx context.Context, peerStr string) (poolcore.InfoResp, error) {
	if h == nil || h.rt == nil {
		return poolcore.InfoResp{}, fmt.Errorf("runtime not initialized")
	}
	peerID, err := peer.Decode(peerStr)
	if err != nil {
		return poolcore.InfoResp{}, fmt.Errorf("decode peer id failed: %w", err)
	}
	return callNodePoolInfo(ctx, h.rt, peerID)
}

type moduleStoreAdapter struct {
	store moduleBootstrapStore
}

func (a moduleStoreAdapter) Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is nil")
	}
	if fn == nil {
		return fmt.Errorf("read func is nil")
	}
	return a.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		return fn(rc)
	})
}

func (a moduleStoreAdapter) WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	if a.store == nil {
		return fmt.Errorf("store is nil")
	}
	if fn == nil {
		return fmt.Errorf("write tx func is nil")
	}
	return a.store.WriteTx(ctx, func(wtc moduleapi.WriteTx) error {
		return fn(wtc)
	})
}

func normalizeModuleHTTPPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func toInternalCallResponse(resp moduleapi.CallResponse) ncall.CallResp {
	out := ncall.CallResp{
		Ok:                   resp.Ok,
		Code:                 strings.TrimSpace(resp.Code),
		Message:              strings.TrimSpace(resp.Message),
		ContentType:          strings.TrimSpace(resp.ContentType),
		Body:                 append([]byte(nil), resp.Body...),
		PaymentSchemes:       toInternalPaymentOptions(resp.PaymentSchemes),
		PaymentReceiptScheme: strings.TrimSpace(resp.PaymentReceiptScheme),
		PaymentReceipt:       append([]byte(nil), resp.PaymentReceipt...),
		ServiceQuote:         append([]byte(nil), resp.ServiceQuote...),
		ServiceReceipt:       append([]byte(nil), resp.ServiceReceipt...),
	}
	return out
}

func toModuleAPICallResponse(resp ncall.CallResp) moduleapi.CallResponse {
	out := moduleapi.CallResponse{
		Ok:                   resp.Ok,
		Code:                 strings.TrimSpace(resp.Code),
		Message:              strings.TrimSpace(resp.Message),
		ContentType:          strings.TrimSpace(resp.ContentType),
		Body:                 append([]byte(nil), resp.Body...),
		PaymentSchemes:       toModuleAPIPaymentOptions(resp.PaymentSchemes),
		PaymentReceiptScheme: strings.TrimSpace(resp.PaymentReceiptScheme),
		PaymentReceipt:       append([]byte(nil), resp.PaymentReceipt...),
		ServiceQuote:         append([]byte(nil), resp.ServiceQuote...),
		ServiceReceipt:       append([]byte(nil), resp.ServiceReceipt...),
	}
	return out
}

func toInternalPaymentOptions(in []*moduleapi.PaymentOption) []*contractmessage.PaymentOption {
	if len(in) == 0 {
		return nil
	}
	out := make([]*contractmessage.PaymentOption, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, &contractmessage.PaymentOption{
			Scheme:                   strings.TrimSpace(item.Scheme),
			PaymentDomain:            strings.TrimSpace(item.PaymentDomain),
			AmountSatoshi:            item.AmountSatoshi,
			Description:              strings.TrimSpace(item.Description),
			MinimumPoolAmountSatoshi: item.MinimumPoolAmountSatoshi,
			FeeRateSatPerByteMilli:   item.FeeRateSatPerByteMilli,
			LockBlocks:               item.LockBlocks,
			PricingMode:              strings.TrimSpace(item.PricingMode),
			ServiceQuantity:          item.ServiceQuantity,
			ServiceQuantityUnit:      strings.TrimSpace(item.ServiceQuantityUnit),
			QuoteStatus:              strings.TrimSpace(item.QuoteStatus),
		})
	}
	return out
}

func toModuleAPIPaymentOptions(in []*contractmessage.PaymentOption) []*moduleapi.PaymentOption {
	if len(in) == 0 {
		return nil
	}
	out := make([]*moduleapi.PaymentOption, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, &moduleapi.PaymentOption{
			Scheme:                   strings.TrimSpace(item.Scheme),
			PaymentDomain:            strings.TrimSpace(item.PaymentDomain),
			AmountSatoshi:            item.AmountSatoshi,
			Description:              strings.TrimSpace(item.Description),
			MinimumPoolAmountSatoshi: item.MinimumPoolAmountSatoshi,
			FeeRateSatPerByteMilli:   item.FeeRateSatPerByteMilli,
			LockBlocks:               item.LockBlocks,
			PricingMode:              strings.TrimSpace(item.PricingMode),
			ServiceQuantity:          item.ServiceQuantity,
			ServiceQuantityUnit:      strings.TrimSpace(item.ServiceQuantityUnit),
			QuoteStatus:              strings.TrimSpace(item.QuoteStatus),
		})
	}
	return out
}

func toInternalOBSActionResponse(resp moduleapi.OBSActionResponse) OBSActionResponse {
	return OBSActionResponse{
		OK:      resp.OK,
		Result:  strings.TrimSpace(resp.Result),
		Error:   strings.TrimSpace(resp.Error),
		Payload: cloneMapAny(resp.Payload),
	}
}

func cloneMapAny(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type moduleRuntimeGate struct {
	flag atomic.Bool
}

func (g *moduleRuntimeGate) isEnabled() bool {
	if g == nil {
		return false
	}
	return g.flag.Load()
}

func (g *moduleRuntimeGate) enable() {
	if g == nil {
		return
	}
	g.flag.Store(true)
}

func (g *moduleRuntimeGate) disable() {
	if g == nil {
		return
	}
	g.flag.Store(false)
}

func writeModuleDisabledHTTP(w http.ResponseWriter) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    "MODULE_DISABLED",
			"message": "module is disabled",
		},
	})
}

func (h *moduleHost) InstallModule(spec moduleapi.ModuleSpec) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if h.store == nil {
		return nil, fmt.Errorf("store is required")
	}

	// 预检查所有 handler/provider/hook 是否为空，避免半安装状态
	if err := validateModuleSpec(spec); err != nil {
		return nil, err
	}

	gate := &moduleRuntimeGate{}
	gate.enable()

	type registeredCleanup struct {
		cleanup func()
		name    string
	}
	var registered []registeredCleanup

	rollBack := func() {
		gate.disable()

		for i := len(registered) - 1; i >= 0; i-- {
			if registered[i].cleanup != nil {
				registered[i].cleanup()
			}
		}
		registered = nil

		if spec.Cleanup != nil {
			spec.Cleanup()
		}
	}

	tryRegister := func(name string, fn func() (func(), error)) (func(), error) {
		cleanup, err := fn()
		if err != nil {
			rollBack()
			return nil, fmt.Errorf("install %s failed: %w", name, err)
		}
		registered = append(registered, registeredCleanup{cleanup: cleanup, name: name})
		return cleanup, nil
	}

	// 注册 capabilities（显式能力宣誓，不再用 ModuleID/Version 自动顶替）
	for _, cap := range spec.Capabilities {
		cap := cap
		if _, err := tryRegister("capability "+cap.ID, func() (func(), error) {
			return h.RegisterCapabilityItem(&contractmessage.CapabilityItem{
				ID:         cap.ID,
				Version:    cap.Version,
				ProtocolID: string(cap.ProtocolID),
			})
		}); err != nil {
			return nil, err
		}
	}

	// 注册 domain resolvers
	for _, dr := range spec.DomainResolvers {
		dr := dr
		if _, err := tryRegister("domain resolver "+dr.Name, func() (func(), error) {
			return h.RegisterDomainResolveHook(dr.Name, dr.Handler)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 libp2p hooks
	for _, lp := range spec.LibP2P {
		lp := lp
		if _, err := tryRegister("libp2p "+string(lp.ProtocolID), func() (func(), error) {
			return h.RegisterLibP2P(lp.ProtocolID, lp.Handler)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 settings actions
	for _, sa := range spec.Settings {
		sa := sa
		if _, err := tryRegister("settings "+sa.Action, func() (func(), error) {
			return h.RegisterSettingsAction(sa.Action, sa.Handler)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 obs actions
	for _, oa := range spec.OBS {
		oa := oa
		if _, err := tryRegister("obs "+oa.Action, func() (func(), error) {
			return h.RegisterOBSAction(oa.Action, oa.Handler)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 http routes
	for _, hr := range spec.HTTP {
		hr := hr
		wrappedHandler := moduleHTTPHandlerForGate(hr.Handler, gate)
		if _, err := tryRegister("http "+hr.Path, func() (func(), error) {
			return h.RegisterHTTPRoute(hr.Path, wrappedHandler)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 open hooks
	for i, oh := range spec.OpenHooks {
		oh := oh
		if _, err := tryRegister(fmt.Sprintf("open hook %d", i), func() (func(), error) {
			return h.RegisterOpenHook(oh)
		}); err != nil {
			return nil, err
		}
	}

	// 注册 close hooks
	for i, ch := range spec.CloseHooks {
		ch := ch
		if _, err := tryRegister(fmt.Sprintf("close hook %d", i), func() (func(), error) {
			return h.RegisterCloseHook(ch)
		}); err != nil {
			return nil, err
		}
	}

	// 组装最终的 cleanup 函数
	var finalCleanupOnce sync.Once
	finalCleanup := func() {
		finalCleanupOnce.Do(func() {
			gate.disable()

			for i := len(registered) - 1; i >= 0; i-- {
				if registered[i].cleanup != nil {
					registered[i].cleanup()
				}
			}
			registered = nil

			if spec.Cleanup != nil {
				spec.Cleanup()
			}
		})
	}

	return finalCleanup, nil
}

func moduleHTTPHandlerForGate(handler moduleapi.HTTPHandler, gate *moduleRuntimeGate) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if gate == nil || !gate.isEnabled() {
			writeModuleDisabledHTTP(w)
			return
		}
		handler(w, r)
	}
}

// 窄能力方法：获取 module 专属的 store。
// 设计说明：
// - 不通过 Host 接口暴露，避免所有 module 都看到所有 store；
// - 只给需要专属 store 的 module 用类型断言获取。
func (h *moduleHost) GetModuleStore(name string) any {
	if h == nil || h.rt == nil {
		return nil
	}
	return h.rt.GetModuleStore(name)
}

// peerIDToPubkeyHex 从 peer.ID 获取公钥 hex。
func peerIDToPubkeyHex(pid peer.ID, h host.Host) string {
	if h == nil || pid == "" {
		return ""
	}
	pub := h.Peerstore().PubKey(pid)
	if pub == nil {
		return ""
	}
	pubBytes, err := pub.Raw()
	if err != nil {
		return ""
	}
	return strings.ToLower(hex.EncodeToString(pubBytes))
}
