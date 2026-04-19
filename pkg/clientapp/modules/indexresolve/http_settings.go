package indexresolve

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type settingsIndexResolveRouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

func NewHTTPSettingsIndexResolveHandler(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if svc == nil || !svc.Enabled() {
			writeModuleSettingsError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "index_resolve module is disabled")
			return
		}
		switch r.Method {
		case http.MethodGet:
			items, err := svc.List(r.Context())
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
			item, err := svc.Upsert(r.Context(), req.Route, req.SeedHash, time.Now().Unix())
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
			if err := svc.Delete(r.Context(), route); err != nil {
				writeModuleSettingsError(w, moduleSettingsStatusFromErr(err), CodeOf(err), MessageOf(err))
				return
			}
			normRoute, err := NormalizeRoute(route)
			if err != nil {
				normRoute = strings.TrimSpace(route)
			}
			writeModuleSettingsOK(w, map[string]any{
				"deleted": true,
				"route":   normRoute,
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
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	case "ROUTE_NOT_FOUND":
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
