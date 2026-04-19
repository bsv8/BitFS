//go:build with_indexresolve

package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

type indexResolveBootstrapStore interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	SerialAccess() bool
}

type indexResolveSerialDoer interface {
	Do(context.Context, func(SQLConn) error) error
}

type indexResolveStoreAdapter struct {
	store  indexResolveBootstrapStore
	serial indexResolveSerialDoer
}

func (a indexResolveStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a indexResolveStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a indexResolveStoreAdapter) Do(ctx context.Context, fn func(indexresolve.Conn) error) error {
	if a.serial == nil {
		return fmt.Errorf("serial db is required")
	}
	return a.serial.Do(ctx, func(conn SQLConn) error {
		return fn(conn)
	})
}

const (
	obsActionIndexResolveResolve = "settings.index_resolve.resolve"
	obsActionIndexResolveList    = "settings.index_resolve.list"
	obsActionIndexResolveUpsert  = "settings.index_resolve.upsert"
	obsActionIndexResolveDelete  = "settings.index_resolve.delete"
)

type settingsIndexResolveRouteItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

type indexResolveSettingsDispatcher interface {
	DispatchSettings(context.Context, string, map[string]any) (map[string]any, error)
}

// registerOptionalModules 只负责把可选模块接到主框架钩子上。
//
// 设计说明：
// - 这里是唯一允许引用 indexresolve 模块实现包的地方；
// - 模块自己的 store、能力、HTTP 面、settings、obs 和生命周期都在这里接线；
// - cleanup 只负责解绑钩子，模块能力的生死由编译期开关决定。
func registerOptionalModules(ctx context.Context, rt *Runtime, store indexResolveBootstrapStore) (func(), error) {
	if rt == nil || store == nil {
		return func() {}, nil
	}
	if ctx == nil {
		return func() {}, fmt.Errorf("ctx is required")
	}

	serial, _ := store.(indexResolveSerialDoer)
	adapter := indexResolveStoreAdapter{store: store}
	var serialExec indexresolve.SerialExecutor
	if store.SerialAccess() && serial != nil {
		adapter.serial = serial
		serialExec = adapter
	}

	moduleStore, err := indexresolve.BootstrapStore(ctx, adapter, serialExec)
	if err != nil {
		return nil, err
	}
	reg := ensureModuleRegistry(rt)
	if reg == nil {
		return func() {}, nil
	}

	moduleCleanup, err := reg.registerModuleLockProvider(indexresolve.ModuleIdentity, indexresolve.FunctionLocks)
	if err != nil {
		return nil, err
	}

	capabilityCleanup, err := reg.registerCapabilityHook(indexresolve.CapabilityItem)
	if err != nil {
		moduleCleanup()
		return nil, err
	}
	p2pCleanup, err := reg.RegisterP2PCallHook(func(ctx context.Context, route string) (routeIndexManifest, error) {
		manifest, err := indexresolve.BizResolve(ctx, moduleStore, route)
		if err != nil {
			return routeIndexManifest{}, newModuleHookError(indexresolve.CodeOf(err), indexresolve.MessageOf(err))
		}
		return routeIndexManifest{
			Route:               manifest.Route,
			SeedHash:            manifest.SeedHash,
			RecommendedFileName: manifest.RecommendedFileName,
			MIMEHint:            manifest.MIMEHint,
			FileSize:            manifest.FileSize,
			UpdatedAtUnix:       manifest.UpdatedAtUnix,
		}, nil
	})
	if err != nil {
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	httpAPICleanup, err := reg.RegisterHTTPAPIHook(func(s *httpAPIServer, mux *http.ServeMux, prefix string) {
		if s == nil || mux == nil {
			return
		}
		mux.HandleFunc(prefix+"/v1/settings/index-resolve", s.withAuth(newIndexResolveSettingsHTTPHandler(reg)))
	})
	if err != nil {
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	settingsCleanup, err := reg.RegisterSettingsHook(func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		switch strings.TrimSpace(action) {
		case obsActionIndexResolveResolve:
			route := strings.TrimSpace(fmt.Sprint(payload["route"]))
			manifest, err := indexresolve.BizResolve(ctx, moduleStore, route)
			if err != nil {
				return nil, newModuleHookError(indexresolve.CodeOf(err), indexresolve.MessageOf(err))
			}
			return map[string]any{
				"route":                 manifest.Route,
				"seed_hash":             manifest.SeedHash,
				"recommended_file_name": manifest.RecommendedFileName,
				"mime_hint":             manifest.MIMEHint,
				"file_size":             manifest.FileSize,
				"updated_at_unix":       manifest.UpdatedAtUnix,
			}, nil
		case obsActionIndexResolveList:
			items, err := indexresolve.BizSettingsList(ctx, moduleStore)
			if err != nil {
				return nil, newModuleHookError(indexresolve.CodeOf(err), indexresolve.MessageOf(err))
			}
			return map[string]any{
				"total": len(items),
				"items": items,
			}, nil
		case obsActionIndexResolveUpsert:
			route := strings.TrimSpace(fmt.Sprint(payload["route"]))
			seedHash := strings.TrimSpace(fmt.Sprint(payload["seed_hash"]))
			item, err := indexresolve.BizSettingsUpsert(ctx, moduleStore, route, seedHash)
			if err != nil {
				return nil, newModuleHookError(indexresolve.CodeOf(err), indexresolve.MessageOf(err))
			}
			return map[string]any{
				"route":           item.Route,
				"seed_hash":       item.SeedHash,
				"updated_at_unix": item.UpdatedAtUnix,
			}, nil
		case obsActionIndexResolveDelete:
			route := strings.TrimSpace(fmt.Sprint(payload["route"]))
			if err := indexresolve.BizSettingsDelete(ctx, moduleStore, route); err != nil {
				return nil, newModuleHookError(indexresolve.CodeOf(err), indexresolve.MessageOf(err))
			}
			return map[string]any{
				"deleted": true,
				"route":   strings.TrimSpace(route),
			}, nil
		default:
			return nil, newModuleHookError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
		}
	})
	if err != nil {
		httpAPICleanup()
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	obsCleanup, err := reg.RegisterOBSControlHook(func(ctx context.Context, action string, payload map[string]any) (OBSActionResponse, error) {
		switch strings.TrimSpace(action) {
		case obsActionIndexResolveResolve:
			out, err := reg.DispatchSettings(ctx, obsActionIndexResolveResolve, payload)
			if err != nil {
				return OBSActionResponse{}, err
			}
			return OBSActionResponse{OK: true, Result: "resolved", Payload: out}, nil
		case obsActionIndexResolveList:
			out, err := reg.DispatchSettings(ctx, obsActionIndexResolveList, payload)
			if err != nil {
				return OBSActionResponse{}, err
			}
			return OBSActionResponse{OK: true, Result: "listed", Payload: out}, nil
		case obsActionIndexResolveUpsert:
			out, err := reg.DispatchSettings(ctx, obsActionIndexResolveUpsert, payload)
			if err != nil {
				return OBSActionResponse{}, err
			}
			return OBSActionResponse{OK: true, Result: "upserted", Payload: out}, nil
		case obsActionIndexResolveDelete:
			out, err := reg.DispatchSettings(ctx, obsActionIndexResolveDelete, payload)
			if err != nil {
				return OBSActionResponse{}, err
			}
			return OBSActionResponse{OK: true, Result: "deleted", Payload: out}, nil
		default:
			return OBSActionResponse{}, newModuleHookError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
		}
	})
	if err != nil {
		settingsCleanup()
		httpAPICleanup()
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	openCleanup, err := reg.RegisterOpenHook(func(context.Context) error { return nil })
	if err != nil {
		obsCleanup()
		settingsCleanup()
		httpAPICleanup()
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	closeCleanup, err := reg.RegisterCloseHook(func(context.Context) error { return nil })
	if err != nil {
		openCleanup()
		obsCleanup()
		settingsCleanup()
		httpAPICleanup()
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
		return nil, err
	}
	return func() {
		closeCleanup()
		openCleanup()
		obsCleanup()
		settingsCleanup()
		httpAPICleanup()
		p2pCleanup()
		capabilityCleanup()
		moduleCleanup()
	}, nil
}

func newIndexResolveSettingsHTTPHandler(reg indexResolveSettingsDispatcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleSettingsError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		action, payload, status, err := indexResolveSettingsActionRequest(r)
		if err != nil {
			writeModuleSettingsError(w, status, ModuleHookCodeOf(err), ModuleHookMessageOf(err))
			return
		}
		if reg == nil {
			writeModuleSettingsError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "module is disabled")
			return
		}
		out, err := reg.DispatchSettings(r.Context(), action, payload)
		if err != nil {
			writeModuleSettingsError(w, moduleSettingsStatusFromHookErr(err), ModuleHookCodeOf(err), ModuleHookMessageOf(err))
			return
		}
		writeModuleSettingsOK(w, indexResolveSettingsPayloadForHTTP(action, out))
	}
}

func writeModuleSettingsOK(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func writeModuleSettingsError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    strings.TrimSpace(code),
			"message": strings.TrimSpace(message),
		},
	})
}

func indexResolveSettingsActionRequest(r *http.Request) (string, map[string]any, int, error) {
	switch r.Method {
	case http.MethodGet:
		return obsActionIndexResolveList, map[string]any{}, 0, nil
	case http.MethodPost:
		var req struct {
			Route    string `json:"route"`
			SeedHash string `json:"seed_hash"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return "", nil, http.StatusBadRequest, newModuleHookError("BAD_REQUEST", "invalid json")
		}
		return obsActionIndexResolveUpsert, map[string]any{
			"route":     strings.TrimSpace(req.Route),
			"seed_hash": strings.TrimSpace(req.SeedHash),
		}, 0, nil
	case http.MethodDelete:
		return obsActionIndexResolveDelete, map[string]any{
			"route": strings.TrimSpace(r.URL.Query().Get("route")),
		}, 0, nil
	default:
		return "", nil, http.StatusMethodNotAllowed, newModuleHookError("METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func indexResolveSettingsPayloadForHTTP(action string, payload map[string]any) any {
	switch action {
	case obsActionIndexResolveList:
		items, _ := payload["items"].([]indexresolve.RouteItem)
		out := make([]settingsIndexResolveRouteItem, 0, len(items))
		for _, item := range items {
			out = append(out, settingsIndexResolveRouteItem{
				Route:         strings.TrimSpace(item.Route),
				SeedHash:      strings.TrimSpace(item.SeedHash),
				UpdatedAtUnix: item.UpdatedAtUnix,
			})
		}
		return map[string]any{
			"total": len(out),
			"items": out,
		}
	case obsActionIndexResolveUpsert:
		return settingsIndexResolveRouteItem{
			Route:         strings.TrimSpace(fmt.Sprint(payload["route"])),
			SeedHash:      strings.TrimSpace(fmt.Sprint(payload["seed_hash"])),
			UpdatedAtUnix: obsPayloadInt64(payload, "updated_at_unix"),
		}
	case obsActionIndexResolveDelete:
		return map[string]any{
			"deleted": obsPayloadBool(payload, "deleted"),
			"route":   strings.TrimSpace(fmt.Sprint(payload["route"])),
		}
	default:
		return payload
	}
}

func moduleSettingsStatusFromHookErr(err error) int {
	switch ModuleHookCodeOf(err) {
	case "ROUTE_INVALID", "SEED_HASH_INVALID", "SEED_NOT_FOUND", "BAD_REQUEST":
		return http.StatusBadRequest
	case "REQUEST_CANCELED":
		return 499
	case "MODULE_DISABLED":
		return http.StatusServiceUnavailable
	case "HANDLER_NOT_REGISTERED":
		return http.StatusServiceUnavailable
	case "ROUTE_NOT_FOUND":
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

func obsPayloadInt64(payload map[string]any, key string) int64 {
	value, ok := payload[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func obsPayloadBool(payload map[string]any, key string) bool {
	value, ok := payload[key]
	if !ok {
		return false
	}
	v, _ := value.(bool)
	return v
}
