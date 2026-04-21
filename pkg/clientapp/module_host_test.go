package clientapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/libp2p/go-libp2p/core/protocol"
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
	t.Skip("LibP2PProtocolNodeCall + Route 分发模型已废弃，相关能力已移除，需要重写测试")
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, nil)

	protoID := protocol.ID("/bsv-transfer/test/1.0.0")
	cleanup, err := host.RegisterLibP2P(protoID, func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		return moduleapi.LibP2PResult{}, nil
	})
	if err != nil {
		t.Fatalf("register libp2p hook failed: %v", err)
	}
	t.Cleanup(cleanup)

	_ = rt
	_ = host
}

func TestInstallModuleHTTPUsesAuthWrapper(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module",
		Version: 1,
		HTTP: []moduleapi.HTTPRoute{
			{
				Path: "/v1/install-module/auth",
				Handler: func(w http.ResponseWriter, r *http.Request) {
					meta := requestVisitMetaFromContext(r.Context())
					if meta.VisitID != "visit-1" || meta.VisitLocator != "locator-1" {
						t.Fatalf("request meta not injected: %+v", meta)
					}
					writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("install module failed: %v", err)
	}
	t.Cleanup(cleanup)

	srv := newHTTPAPIServer(rt, rt, db, store, nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/install-module/auth", nil)
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

func TestInstallModuleCleanupDisablesHTTPHandler(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module",
		Version: 1,
		HTTP: []moduleapi.HTTPRoute{
			{
				Path: "/v1/install-module/cleanup",
				Handler: func(w http.ResponseWriter, r *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("install module failed: %v", err)
	}

	srv := newHTTPAPIServer(rt, rt, db, store, nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/install-module/cleanup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected handler to be disabled, got=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"MODULE_DISABLED"`) {
		t.Fatalf("unexpected body after cleanup: %s", rec.Body.String())
	}
}

func TestInstallModuleCleanupIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module",
		Version: 1,
		HTTP: []moduleapi.HTTPRoute{
			{
				Path: "/v1/install-module/idempotent",
				Handler: func(w http.ResponseWriter, r *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("install module failed: %v", err)
	}

	cleanup()
	cleanup()
}

func TestInstallModulePreCheckFailsBeforeAnyRegistration(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-precheck",
		Version: 1,
		Settings: []moduleapi.SettingsAction{
			{
				Action: "settings.precheck.test",
				Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
					return map[string]any{"ok": true}, nil
				},
			},
		},
		OBS: []moduleapi.OBSAction{
			{
				Action: "settings.precheck.obs",
				Handler: func(ctx context.Context, gotAction string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
					return moduleapi.OBSActionResponse{OK: true, Result: "ok"}, nil
				},
			},
		},
		HTTP: []moduleapi.HTTPRoute{
			{
				Path:    "/v1/precheck/test",
				Handler: nil,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected pre-check to fail due to nil HTTP handler")
	}

	_, err = rt.modules.DispatchSettings(context.Background(), "settings.precheck.test", map[string]any{})
	if err == nil {
		t.Fatalf("settings action should not be registered after failed install")
	}

	_, err = rt.modules.DispatchOBSControl(context.Background(), "settings.precheck.obs", map[string]any{})
	if err == nil {
		t.Fatalf("obs action should not be registered after failed install")
	}
}

func TestInstallModulePreCheckCatchesNilLibP2PHandler(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-nil-libp2p",
		Version: 1,
		LibP2P: []moduleapi.LibP2PRoute{
			{
				ProtocolID: "/bsv-transfer/test-nil-libp2p/1.0.0",
				Handler:    nil,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected pre-check to fail due to nil libp2p handler")
	}
}

func TestInstallModulePreCheckCatchesNilSettingsHandler(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-nil-settings",
		Version: 1,
		Settings: []moduleapi.SettingsAction{
			{
				Action:  "settings.nil.handler",
				Handler: nil,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected pre-check to fail due to nil settings handler")
	}
}

func TestInstallModulePreCheckCatchesNilOBSHandler(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-nil-obs",
		Version: 1,
		OBS: []moduleapi.OBSAction{
			{
				Action:  "settings.nil.obs",
				Handler: nil,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected pre-check to fail due to nil obs handler")
	}
}

func TestInstallModulePreCheckCatchesNilDomainResolverHandler(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-nil-domain",
		Version: 1,
		DomainResolvers: []moduleapi.DomainResolver{
			{
				Name:    "nil.resolver",
				Handler: nil,
			},
		},
	})
	if err == nil {
		t.Fatalf("expected pre-check to fail due to nil domain resolver handler")
	}
}

func TestInstallModuleRollbackOnFailure(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	host := newModuleHost(rt, store)

	_, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-rollback",
		Version: 1,
		HTTP: []moduleapi.HTTPRoute{
			{
				Path: "/v1/install-module/rollback",
				Handler: func(w http.ResponseWriter, r *http.Request) {
					writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
				},
			},
		},
		Settings: []moduleapi.SettingsAction{
			{
				Action: "settings.module_host.rollback_test",
				Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
					return map[string]any{"ok": true}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("first install should succeed: %v", err)
	}

	_, err = host.InstallModule(moduleapi.ModuleSpec{
		ID:      "test-module-rollback-2",
		Version: 1,
		Settings: []moduleapi.SettingsAction{
			{
				Action: "settings.module_host.rollback_test",
				Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
					return map[string]any{"ok": true}, nil
				},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected duplicate registration failure")
	}

	_, err = rt.modules.DispatchSettings(context.Background(), "settings.module_host.rollback_test", map[string]any{})
	if err != nil {
		t.Fatalf("expected action to still be registered from first module: %v", err)
	}
}
