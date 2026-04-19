package indexresolve

import (
	"encoding/json"
	"net/http"
	"strings"
)

type settingsIndexResolveRouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

func NewHTTPSettingsIndexResolveHandler(lister SettingsLister, upserter SettingsUpserter, deleter SettingsDeleter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		switch r.Method {
		case http.MethodGet:
			items, err := BizSettingsList(r.Context(), lister)
			if err != nil {
				writeModuleSettingsError(w, moduleSettingsStatusFromErr(err), CodeOf(err), MessageOf(err))
				return
			}
			out := make([]settingsIndexResolveRouteItem, 0, len(items))
			for _, item := range items {
				out = append(out, settingsIndexResolveRouteItem{
					Route:         strings.TrimSpace(item.Route),
					SeedHash:      strings.TrimSpace(item.SeedHash),
					UpdatedAtUnix: item.UpdatedAtUnix,
				})
			}
			writeModuleSettingsOK(w, map[string]any{
				"total": len(out),
				"items": out,
			})
		case http.MethodPost:
			var req struct {
				Route    string `json:"route"`
				SeedHash string `json:"seed_hash"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeModuleSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
				return
			}
			item, err := BizSettingsUpsert(r.Context(), upserter, req.Route, req.SeedHash)
			if err != nil {
				writeModuleSettingsError(w, moduleSettingsStatusFromErr(err), CodeOf(err), MessageOf(err))
				return
			}
			writeModuleSettingsOK(w, settingsIndexResolveRouteItem{
				Route:         strings.TrimSpace(item.Route),
				SeedHash:      strings.TrimSpace(item.SeedHash),
				UpdatedAtUnix: item.UpdatedAtUnix,
			})
		case http.MethodDelete:
			route := r.URL.Query().Get("route")
			normalizedRoute, err := NormalizeRoute(route)
			if err != nil {
				writeModuleSettingsError(w, moduleSettingsStatusFromErr(err), CodeOf(err), MessageOf(err))
				return
			}
			if err := BizSettingsDelete(r.Context(), deleter, normalizedRoute); err != nil {
				writeModuleSettingsError(w, moduleSettingsStatusFromErr(err), CodeOf(err), MessageOf(err))
				return
			}
			writeModuleSettingsOK(w, map[string]any{
				"deleted": true,
				"route":   strings.TrimSpace(normalizedRoute),
			})
		default:
			writeModuleSettingsError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	}
}

func writeModuleSettingsOK(w http.ResponseWriter, data any) {
	writeModuleJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func writeModuleSettingsError(w http.ResponseWriter, status int, code, message string) {
	writeModuleJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    strings.TrimSpace(code),
			"message": strings.TrimSpace(message),
		},
	})
}

func writeModuleJSON(w http.ResponseWriter, status int, payload any) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func moduleSettingsStatusFromErr(err error) int {
	switch CodeOf(err) {
	case "ROUTE_INVALID", "SEED_HASH_INVALID", "SEED_NOT_FOUND", "BAD_REQUEST":
		return http.StatusBadRequest
	case "REQUEST_CANCELED":
		return 499
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	case "ROUTE_NOT_FOUND":
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
