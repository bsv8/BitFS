package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	contractmessage "github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type moduleHost struct {
	rt    *Runtime
	store moduleBootstrapStore
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
