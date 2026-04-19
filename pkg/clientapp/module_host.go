package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

type moduleHost struct {
	rt    *Runtime
	store moduleBootstrapStore
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

func (h *moduleHost) RegisterCapability(moduleID string, version uint32) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.registerCapabilityHook(func() *contractmessage.CapabilityItem {
		return &contractmessage.CapabilityItem{
			ID:      strings.TrimSpace(moduleID),
			Version: version,
		}
	})
}

func (h *moduleHost) RegisterLibP2P(protocol moduleapi.LibP2PProtocol, route string, hook moduleapi.LibP2PHook) (func(), error) {
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
	return reg.RegisterLibP2PHook(LibP2PProtocol(protocol), route, func(ctx context.Context, ev LibP2PEvent) (LibP2PResult, error) {
		out, err := hook(ctx, moduleapi.LibP2PEvent{
			Protocol:        moduleapi.LibP2PProtocol(ev.Protocol),
			Route:           ev.Route,
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
			CallResp:        toInternalCallResponse(out.CallResp),
			ResolveManifest: toInternalResolveManifest(out.ResolveManifest),
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

func (h *moduleHost) RegisterModuleLockProvider(module string, provider func() []moduleapi.LockedFunction) (func(), error) {
	if h == nil || h.rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if provider == nil {
		return nil, fmt.Errorf("provider is required")
	}
	reg := ensureModuleRegistry(h.rt)
	if reg == nil {
		return nil, fmt.Errorf("module registry is required")
	}
	return reg.registerModuleLockProvider(module, func() []modulelock.LockedFunction {
		items := provider()
		out := make([]modulelock.LockedFunction, 0, len(items))
		for _, item := range items {
			out = append(out, modulelock.LockedFunction{
				ID:               strings.TrimSpace(item.ID),
				Module:           strings.TrimSpace(item.Module),
				Package:          strings.TrimSpace(item.Package),
				Symbol:           strings.TrimSpace(item.Symbol),
				Signature:        strings.TrimSpace(item.Signature),
				ObsControlAction: strings.TrimSpace(item.ObsControlAction),
				Note:             strings.TrimSpace(item.Note),
			})
		}
		return out
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
		Route:                strings.TrimSpace(req.Route),
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

func (a moduleStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a moduleStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a moduleStoreAdapter) Do(ctx context.Context, fn func(moduleapi.Conn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("store fn is required")
	}
	return a.store.Do(ctx, func(conn SQLConn) error {
		return fn(conn)
	})
}

func (a moduleStoreAdapter) SerialAccess() bool {
	return a.store != nil && a.store.SerialAccess()
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

func toInternalResolveManifest(resp moduleapi.ResolveManifest) routeIndexManifest {
	return routeIndexManifest{
		Route:               strings.TrimSpace(resp.Route),
		SeedHash:            strings.TrimSpace(resp.SeedHash),
		RecommendedFileName: strings.TrimSpace(resp.RecommendedFileName),
		MIMEHint:            strings.TrimSpace(resp.MIMEHint),
		FileSize:            resp.FileSize,
		UpdatedAtUnix:       resp.UpdatedAtUnix,
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
