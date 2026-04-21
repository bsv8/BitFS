package indexresolve

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/modulekit"
)

func handleIndexResolveSettings(store ResolveSettingsStore) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		action, payload, status, err := indexResolveSettingsActionRequest(r)
		if err != nil {
			modulekit.WriteError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
			return
		}
		out, err := indexResolveSettingsActionResult(r.Context(), store, action, payload)
		if err != nil {
			modulekit.WriteError(w, indexResolveSettingsStatusFromError(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
			return
		}
		modulekit.WriteOK(w, indexResolveSettingsPayloadForHTTP(action, out))
	}
}

func indexResolveSettingsActionRequest(r *http.Request) (string, map[string]any, int, error) {
	switch r.Method {
	case http.MethodGet:
		return "settings.index_resolve.list", map[string]any{}, 0, nil
	case http.MethodPost:
		var req struct {
			Route    string `json:"route"`
			SeedHash string `json:"seed_hash"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return "", nil, http.StatusBadRequest, moduleapi.NewError("BAD_REQUEST", "invalid json")
		}
		return "settings.index_resolve.upsert", map[string]any{
			"route":     strings.TrimSpace(req.Route),
			"seed_hash": strings.TrimSpace(req.SeedHash),
		}, 0, nil
	case http.MethodDelete:
		return "settings.index_resolve.delete", map[string]any{
			"route": strings.TrimSpace(r.URL.Query().Get("route")),
		}, 0, nil
	default:
		return "", nil, http.StatusMethodNotAllowed, moduleapi.NewError("METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func indexResolveSettingsPayloadForHTTP(action string, payload map[string]any) any {
	switch action {
	case "settings.index_resolve.list":
		items, _ := payload["items"].([]RouteItem)
		if items == nil {
			items = []RouteItem{}
		}
		out := make([]settingsIndexResolveRouteItem, 0, len(items))
		for _, item := range items {
			out = append(out, settingsIndexResolveRouteItem{
				Route:         item.Route,
				SeedHash:      item.SeedHash,
				UpdatedAtUnix: item.UpdatedAtUnix,
			})
		}
		return map[string]any{"total": payload["total"], "items": out}
	default:
		return payload
	}
}

type settingsIndexResolveRouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

func indexResolveSettingsStatusFromError(err error) int {
	switch strings.ToUpper(strings.TrimSpace(moduleapi.CodeOf(err))) {
	case "BAD_REQUEST", "ROUTE_INVALID", "SEED_HASH_INVALID":
		return http.StatusBadRequest
	case "SEED_NOT_FOUND", "ROUTE_NOT_FOUND":
		return http.StatusNotFound
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	case "REQUEST_CANCELED":
		return 499
	default:
		return http.StatusInternalServerError
	}
}
