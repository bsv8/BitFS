package clientapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestModuleRegistrySettingsActionsDispatchIndependently(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	resolveCleanup, err := reg.RegisterSettingsHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		if ctx == nil {
			return nil, newModuleHookError("BAD_REQUEST", "ctx is required")
		}
		if strings.TrimSpace(action) != "settings.index_resolve.resolve" {
			return nil, newModuleHookError("BAD_REQUEST", "unexpected action")
		}
		if got := strings.TrimSpace(fmt.Sprint(payload["route"])); got != "movie" {
			return nil, newModuleHookError("BAD_REQUEST", "unexpected route")
		}
		return map[string]any{
			"route":     "/movie",
			"seed_hash": strings.Repeat("ab", 32),
		}, nil
	})
	if err != nil {
		t.Fatalf("register resolve action failed: %v", err)
	}
	defer resolveCleanup()

	upsertCalled := false
	upsertCleanup, err := reg.RegisterSettingsHook("settings.index_resolve.upsert", func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		upsertCalled = true
		if strings.TrimSpace(action) != "settings.index_resolve.upsert" {
			return nil, newModuleHookError("BAD_REQUEST", "unexpected action")
		}
		if got := strings.TrimSpace(fmt.Sprint(payload["route"])); got != "movie" {
			return nil, newModuleHookError("BAD_REQUEST", "unexpected route")
		}
		return map[string]any{
			"route":           "/movie",
			"seed_hash":       strings.Repeat("cd", 32),
			"updated_at_unix": int64(123),
		}, nil
	})
	if err != nil {
		t.Fatalf("register upsert action failed: %v", err)
	}
	defer upsertCleanup()

	resp, err := reg.DispatchSettings(context.Background(), "settings.index_resolve.resolve", map[string]any{"route": "movie"})
	if err != nil {
		t.Fatalf("dispatch resolve failed: %v", err)
	}
	if got := strings.TrimSpace(fmt.Sprint(resp["route"])); got != "/movie" {
		t.Fatalf("unexpected resolve response: %#v", resp)
	}

	resp, err = reg.DispatchSettings(context.Background(), "settings.index_resolve.upsert", map[string]any{"route": "movie"})
	if err != nil {
		t.Fatalf("dispatch upsert failed: %v", err)
	}
	if !upsertCalled {
		t.Fatal("upsert handler was not called")
	}
	if got := strings.TrimSpace(fmt.Sprint(resp["route"])); got != "/movie" {
		t.Fatalf("unexpected upsert response: %#v", resp)
	}
}

func TestModuleRegistrySettingsActionsReportDisabledAndCanReRegisterAfterCleanup(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	if _, err := reg.DispatchSettings(context.Background(), "pricing.unknown", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "UNSUPPORTED_SETTINGS_ACTION" {
		t.Fatalf("expected unsupported action, got %v", err)
	}

	cleanup, err := reg.RegisterSettingsHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		return map[string]any{"route": "/movie"}, nil
	})
	if err != nil {
		t.Fatalf("register settings action failed: %v", err)
	}
	cleanup()

	if _, err := reg.DispatchSettings(context.Background(), "settings.index_resolve.resolve", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "MODULE_DISABLED" {
		t.Fatalf("expected disabled action after cleanup, got %v", err)
	}

	recleanup, err := reg.RegisterSettingsHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		return map[string]any{"route": "/movie", "seed_hash": strings.Repeat("ef", 32)}, nil
	})
	if err != nil {
		t.Fatalf("re-register settings action failed: %v", err)
	}
	recleanup()
}

func TestModuleRegistryOBSActionDispatchesThroughActionTable(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	called := false
	cleanup, err := reg.RegisterOBSControlHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
		called = true
		if ctx == nil {
			return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "ctx is required")
		}
		if strings.TrimSpace(action) != "settings.index_resolve.resolve" {
			return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "unexpected action")
		}
		if got := strings.TrimSpace(fmt.Sprint(payload["route"])); got != "movie" {
			return OBSActionResponse{}, newModuleHookError("BAD_REQUEST", "unexpected route")
		}
		return OBSActionResponse{
			OK:     true,
			Result: "resolved",
			Payload: map[string]any{
				"route":     "/movie",
				"seed_hash": strings.Repeat("ab", 32),
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("register obs action failed: %v", err)
	}
	defer cleanup()

	resp, err := reg.DispatchOBSControl(context.Background(), "settings.index_resolve.resolve", map[string]any{"route": "movie"})
	if err != nil {
		t.Fatalf("call obs action failed: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
	if !resp.OK || resp.Result != "resolved" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if got := strings.TrimSpace(resp.Payload["route"].(string)); got != "/movie" {
		t.Fatalf("unexpected payload route: %q", got)
	}
}

func TestModuleRegistryOBSActionReportsDisabledAndCanReRegisterAfterCleanup(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	if _, err := reg.DispatchOBSControl(context.Background(), "pricing.unknown", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "UNSUPPORTED_CONTROL_ACTION" {
		t.Fatalf("expected unsupported control action, got %v", err)
	}

	cleanup, err := reg.RegisterOBSControlHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
		return OBSActionResponse{OK: true, Result: "resolved"}, nil
	})
	if err != nil {
		t.Fatalf("register resolve action failed: %v", err)
	}
	cleanup()

	if _, err := reg.DispatchOBSControl(context.Background(), "settings.index_resolve.resolve", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "MODULE_DISABLED" {
		t.Fatalf("expected disabled control action after cleanup, got %v", err)
	}

	recleanup, err := reg.RegisterOBSControlHook("settings.index_resolve.resolve", func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
		return OBSActionResponse{OK: true, Result: "resolved"}, nil
	})
	if err != nil {
		t.Fatalf("re-register obs action failed: %v", err)
	}
	recleanup()
}
