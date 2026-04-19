package clientapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestModuleRegistryOBSActionDispatchesThroughGenericTable(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()
	called := false
	cleanup, err := reg.RegisterOBSControlHook(func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
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

func TestModuleRegistryOBSActionReportsDisabledAndMissingHandlers(t *testing.T) {
	t.Parallel()

	reg := newModuleRegistry()

	if _, err := reg.DispatchOBSControl(context.Background(), "pricing.unknown", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "UNSUPPORTED_CONTROL_ACTION" {
		t.Fatalf("expected unsupported control action, got %v", err)
	}

	cleanup, err := reg.RegisterOBSControlHook(func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
		if strings.TrimSpace(action) != "settings.index_resolve.resolve" {
			return OBSActionResponse{}, newModuleHookError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
		}
		return OBSActionResponse{OK: true, Result: "resolved"}, nil
	})
	if err != nil {
		t.Fatalf("register resolve action failed: %v", err)
	}
	defer cleanup()

	if _, err := reg.DispatchOBSControl(context.Background(), "settings.index_resolve.upsert", map[string]any{"route": "movie"}); err == nil || moduleHookCode(err) != "UNSUPPORTED_CONTROL_ACTION" {
		t.Fatalf("expected unsupported control action, got %v", err)
	}
}
