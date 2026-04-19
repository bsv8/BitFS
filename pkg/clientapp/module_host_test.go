package clientapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func TestModuleHostRegisterHTTPRouteUsesAuthWrapper(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	cleanup, err := host.RegisterHTTPRoute("/v1/module-host/auth", func(w http.ResponseWriter, r *http.Request) {
		meta := requestVisitMetaFromContext(r.Context())
		if meta.VisitID != "visit-1" || meta.VisitLocator != "locator-1" {
			t.Fatalf("request meta not injected: %+v", meta)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	if err != nil {
		t.Fatalf("register http route failed: %v", err)
	}
	t.Cleanup(cleanup)

	srv := newHTTPAPIServer(rt, rt, db, store, nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/module-host/auth", nil)
	req.Header.Set(headerVisitID, "visit-1")
	req.Header.Set(headerVisitLocator, "locator-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestModuleHostRegisterSettingsActionCleanupDisablesDispatch(t *testing.T) {
	t.Parallel()

	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, nil)

	cleanup, err := host.RegisterSettingsAction("settings.module_host.echo", func(context.Context, string, map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})
	if err != nil {
		t.Fatalf("register settings action failed: %v", err)
	}

	resp, err := rt.modules.DispatchSettings(context.Background(), "settings.module_host.echo", map[string]any{})
	if err != nil {
		t.Fatalf("dispatch settings action failed: %v", err)
	}
	if got := resp["ok"]; got != true {
		t.Fatalf("unexpected settings response: %#v", resp)
	}

	cleanup()

	_, err = rt.modules.DispatchSettings(context.Background(), "settings.module_host.echo", map[string]any{})
	if err == nil || moduleHookCode(err) != "MODULE_DISABLED" {
		t.Fatalf("expected MODULE_DISABLED after cleanup, got %v", err)
	}
}

func TestModuleHostRegisterOBSActionKeepsIndependentActions(t *testing.T) {
	t.Parallel()

	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, nil)

	cleanupA, err := host.RegisterOBSAction("settings.module_host.a", func(context.Context, string, map[string]any) (moduleapi.OBSActionResponse, error) {
		return moduleapi.OBSActionResponse{OK: true, Result: "a", Payload: map[string]any{"name": "a"}}, nil
	})
	if err != nil {
		t.Fatalf("register obs action a failed: %v", err)
	}
	t.Cleanup(cleanupA)

	cleanupB, err := host.RegisterOBSAction("settings.module_host.b", func(context.Context, string, map[string]any) (moduleapi.OBSActionResponse, error) {
		return moduleapi.OBSActionResponse{OK: true, Result: "b", Payload: map[string]any{"name": "b"}}, nil
	})
	if err != nil {
		t.Fatalf("register obs action b failed: %v", err)
	}
	t.Cleanup(cleanupB)

	respA, err := rt.modules.DispatchOBSControl(context.Background(), "settings.module_host.a", map[string]any{})
	if err != nil {
		t.Fatalf("dispatch obs action a failed: %v", err)
	}
	respB, err := rt.modules.DispatchOBSControl(context.Background(), "settings.module_host.b", map[string]any{})
	if err != nil {
		t.Fatalf("dispatch obs action b failed: %v", err)
	}
	if !respA.OK || respA.Result != "a" || respA.Payload["name"] != "a" {
		t.Fatalf("unexpected obs response a: %+v", respA)
	}
	if !respB.OK || respB.Result != "b" || respB.Payload["name"] != "b" {
		t.Fatalf("unexpected obs response b: %+v", respB)
	}

	cleanupA()

	_, err = rt.modules.DispatchOBSControl(context.Background(), "settings.module_host.a", map[string]any{})
	if err == nil || moduleHookCode(err) != "MODULE_DISABLED" {
		t.Fatalf("expected action a disabled after cleanup, got %v", err)
	}
	respB, err = rt.modules.DispatchOBSControl(context.Background(), "settings.module_host.b", map[string]any{})
	if err != nil {
		t.Fatalf("expected action b to stay available, got %v", err)
	}
	if !respB.OK || respB.Result != "b" {
		t.Fatalf("unexpected obs response b after a cleanup: %+v", respB)
	}
}

func TestModuleHostRegisterLibP2PTypeConversion(t *testing.T) {
	t.Parallel()

	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, nil)

	cleanup, err := host.RegisterLibP2P(moduleapi.LibP2PProtocolNodeCall, "test.route", func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		if ctx == nil {
			t.Fatal("ctx is nil")
		}
		if ev.Protocol != moduleapi.LibP2PProtocolNodeCall {
			t.Fatalf("unexpected protocol: %s", ev.Protocol)
		}
		if ev.Route != "test.route" {
			t.Fatalf("unexpected route: %s", ev.Route)
		}
		if ev.MessageID != "msg-1" || ev.SenderPubkeyHex != "sender-1" {
			t.Fatalf("unexpected meta: %+v", ev)
		}
		if ev.Request.To != "target-1" || ev.Request.Route != "test.route" || string(ev.Request.Body) != "body-1" {
			t.Fatalf("unexpected request: %+v", ev.Request)
		}
		return moduleapi.LibP2PResult{
			CallResp: moduleapi.CallResponse{
				Ok:          true,
				Code:        "OK",
				ContentType: "text/plain",
				Body:        []byte("ok"),
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("register libp2p hook failed: %v", err)
	}
	t.Cleanup(cleanup)

	out, err := rt.modules.DispatchLibP2P(context.Background(), LibP2PEvent{
		Protocol:    LibP2PProtocolNodeCall,
		Route:       "test.route",
		Meta:        ncall.CallContext{MessageID: "msg-1", SenderPubkeyHex: "sender-1"},
		Req:         ncall.CallReq{To: "target-1", Route: "test.route", ContentType: "text/plain", Body: []byte("body-1")},
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("dispatch libp2p hook failed: %v", err)
	}
	if !out.CallResp.Ok || out.CallResp.Code != "OK" || string(out.CallResp.Body) != "ok" {
		t.Fatalf("unexpected libp2p response: %+v", out.CallResp)
	}
}
