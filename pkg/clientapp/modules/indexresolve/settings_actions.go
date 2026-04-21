package indexresolve

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type ResolveSettingsStore interface {
	ResolveStore
	SettingsStore
}

func indexResolveSettingsActions(store ResolveSettingsStore) []moduleapi.SettingsAction {
	actions := []string{
		"settings.index_resolve.list",
		"settings.index_resolve.upsert",
		"settings.index_resolve.delete",
	}
	out := make([]moduleapi.SettingsAction, 0, len(actions))
	for _, action := range actions {
		action := action
		out = append(out, moduleapi.SettingsAction{
			Action: action,
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
				if strings.TrimSpace(gotAction) != strings.TrimSpace(action) {
					return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
				}
				return indexResolveSettingsActionResult(ctx, store, action, payload)
			},
		})
	}
	return out
}

func indexResolveOBSActions(store ResolveSettingsStore) []moduleapi.OBSAction {
	actions := []string{
		"settings.index_resolve.list",
		"settings.index_resolve.upsert",
		"settings.index_resolve.delete",
	}
	out := make([]moduleapi.OBSAction, 0, len(actions))
	for _, action := range actions {
		action := action
		out = append(out, moduleapi.OBSAction{
			Action: action,
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
				if strings.TrimSpace(gotAction) != strings.TrimSpace(action) {
					return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
				}
				return indexResolveOBSActionResult(ctx, store, action, payload)
			},
		})
	}
	return out
}

func indexResolveSettingsActionResult(ctx context.Context, store ResolveSettingsStore, action string, payload map[string]any) (map[string]any, error) {
	switch strings.TrimSpace(action) {
	case "settings.index_resolve.list":
		items, err := BizSettingsList(ctx, store)
		if err != nil {
			return nil, toModuleAPIError(err)
		}
		return map[string]any{
			"total": len(items),
			"items": items,
		}, nil
	case "settings.index_resolve.upsert":
		route := strings.TrimSpace(fmt.Sprint(payload["route"]))
		seedHash := strings.TrimSpace(fmt.Sprint(payload["seed_hash"]))
		item, err := BizSettingsUpsert(ctx, store, route, seedHash)
		if err != nil {
			return nil, toModuleAPIError(err)
		}
		return map[string]any{
			"route":           item.Route,
			"seed_hash":       item.SeedHash,
			"updated_at_unix": item.UpdatedAtUnix,
		}, nil
	case "settings.index_resolve.delete":
		route := strings.TrimSpace(fmt.Sprint(payload["route"]))
		if err := BizSettingsDelete(ctx, store, route); err != nil {
			return nil, toModuleAPIError(err)
		}
		return map[string]any{
			"deleted": true,
			"route":   route,
		}, nil
	default:
		return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
	}
}

func indexResolveOBSActionResult(ctx context.Context, store ResolveSettingsStore, action string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
	out, err := indexResolveSettingsActionResult(ctx, store, action, payload)
	if err != nil {
		return moduleapi.OBSActionResponse{}, err
	}
	result := map[string]string{
		"settings.index_resolve.list":   "listed",
		"settings.index_resolve.upsert": "upserted",
		"settings.index_resolve.delete": "deleted",
	}[strings.TrimSpace(action)]
	if result == "" {
		return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
	}
	return moduleapi.OBSActionResponse{OK: true, Result: result, Payload: out}, nil
}
