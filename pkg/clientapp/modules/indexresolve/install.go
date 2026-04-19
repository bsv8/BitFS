package indexresolve

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

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

type indexResolveSettingsState struct {
	handlers map[string]func(context.Context, map[string]any) (map[string]any, error)
}

type indexResolveModuleStore interface {
	ListIndexResolveRoutes(context.Context) ([]RouteItem, error)
	ResolveIndexRoute(context.Context, string) (Manifest, error)
	UpsertIndexResolveRoute(context.Context, string, string, int64) (RouteItem, error)
	DeleteIndexResolveRoute(context.Context, string) error
}

type indexResolveStoreAdapter struct {
	store moduleapi.Store
}

func (a indexResolveStoreAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.store.ExecContext(ctx, query, args...)
}

func (a indexResolveStoreAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.store.QueryContext(ctx, query, args...)
}

func (a indexResolveStoreAdapter) Do(ctx context.Context, fn func(Conn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("store fn is required")
	}
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

type indexResolveSerialAdapter struct {
	store moduleapi.Store
}

func (a indexResolveSerialAdapter) Do(ctx context.Context, fn func(Conn) error) error {
	if a.store == nil {
		return fmt.Errorf("store is required")
	}
	if fn == nil {
		return fmt.Errorf("store fn is required")
	}
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

// Install 把 indexresolve 模块接到主干能力上。
//
// 设计说明：
// - 这里只做接线，不碰 Runtime 和 *sql.DB；
// - settings/OBS/HTTP/resolve 都从同一份模块 store 走；
// - cleanup 先回收本模块已注册内容，避免留下半装状态。
func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}
	store := host.Store()
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	db := indexResolveStoreAdapter{store: store}
	var serialExec SerialExecutor
	if store.SerialAccess() {
		serialExec = indexResolveSerialAdapter{store: store}
	}

	moduleStore, err := BootstrapStore(ctx, db, serialExec)
	if err != nil {
		return nil, err
	}

	settingsState := &indexResolveSettingsState{}
	var obsHandlers map[string]func(context.Context, map[string]any) (moduleapi.OBSActionResponse, error)
	cleanups := make([]func(), 0, 10)
	pushCleanup := func(cleanup func()) {
		if cleanup != nil {
			cleanups = append(cleanups, cleanup)
		}
	}
	closeAll := func() {
		settingsState.handlers = nil
		obsHandlers = nil
		for i := len(cleanups) - 1; i >= 0; i-- {
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}
	fail := func(err error) (func(), error) {
		closeAll()
		return nil, err
	}

	cleanup, err := host.RegisterCapability(ModuleID, CapabilityVersion)
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterModuleLockProvider(ModuleIdentity, FunctionLocks)
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterLibP2P(moduleapi.LibP2PProtocolNodeResolve, "", func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		manifest, err := BizResolve(ctx, moduleStore, ev.Request.Route)
		if err != nil {
			return moduleapi.LibP2PResult{}, toModuleAPIError(err)
		}
		return moduleapi.LibP2PResult{
			ResolveManifest: moduleapi.ResolveManifest{
				Route:               manifest.Route,
				SeedHash:            manifest.SeedHash,
				RecommendedFileName: manifest.RecommendedFileName,
				MIMEHint:            manifest.MIMEHint,
				FileSize:            manifest.FileSize,
				UpdatedAtUnix:       manifest.UpdatedAtUnix,
			},
		}, nil
	})
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	settingsState.handlers = map[string]func(context.Context, map[string]any) (map[string]any, error){
		obsActionIndexResolveResolve: func(ctx context.Context, payload map[string]any) (map[string]any, error) {
			return indexResolveSettingsActionResult(ctx, moduleStore, obsActionIndexResolveResolve, payload)
		},
		obsActionIndexResolveList: func(ctx context.Context, payload map[string]any) (map[string]any, error) {
			return indexResolveSettingsActionResult(ctx, moduleStore, obsActionIndexResolveList, payload)
		},
		obsActionIndexResolveUpsert: func(ctx context.Context, payload map[string]any) (map[string]any, error) {
			return indexResolveSettingsActionResult(ctx, moduleStore, obsActionIndexResolveUpsert, payload)
		},
		obsActionIndexResolveDelete: func(ctx context.Context, payload map[string]any) (map[string]any, error) {
			return indexResolveSettingsActionResult(ctx, moduleStore, obsActionIndexResolveDelete, payload)
		},
	}

	for action := range settingsState.handlers {
		action := action
		cleanup, err = host.RegisterSettingsAction(action, func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
			if strings.TrimSpace(gotAction) != strings.TrimSpace(action) {
				return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
			}
			handler := settingsState.handlers[action]
			if handler == nil {
				return nil, moduleapi.NewError("MODULE_DISABLED", "module is disabled")
			}
			return handler(ctx, payload)
		})
		if err != nil {
			return fail(err)
		}
		pushCleanup(cleanup)
	}

	obsHandlers = map[string]func(context.Context, map[string]any) (moduleapi.OBSActionResponse, error){
		obsActionIndexResolveResolve: func(ctx context.Context, payload map[string]any) (moduleapi.OBSActionResponse, error) {
			return indexResolveOBSActionResult(ctx, moduleStore, obsActionIndexResolveResolve, payload)
		},
		obsActionIndexResolveList: func(ctx context.Context, payload map[string]any) (moduleapi.OBSActionResponse, error) {
			return indexResolveOBSActionResult(ctx, moduleStore, obsActionIndexResolveList, payload)
		},
		obsActionIndexResolveUpsert: func(ctx context.Context, payload map[string]any) (moduleapi.OBSActionResponse, error) {
			return indexResolveOBSActionResult(ctx, moduleStore, obsActionIndexResolveUpsert, payload)
		},
		obsActionIndexResolveDelete: func(ctx context.Context, payload map[string]any) (moduleapi.OBSActionResponse, error) {
			return indexResolveOBSActionResult(ctx, moduleStore, obsActionIndexResolveDelete, payload)
		},
	}

	for action := range obsHandlers {
		action := action
		cleanup, err = host.RegisterOBSAction(action, func(ctx context.Context, gotAction string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
			if strings.TrimSpace(gotAction) != strings.TrimSpace(action) {
				return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
			}
			handler := obsHandlers[action]
			if handler == nil {
				return moduleapi.OBSActionResponse{}, moduleapi.NewError("MODULE_DISABLED", "module is disabled")
			}
			return handler(ctx, payload)
		})
		if err != nil {
			return fail(err)
		}
		pushCleanup(cleanup)
	}

	cleanup, err = host.RegisterHTTPRoute("/v1/settings/index-resolve", newIndexResolveSettingsHTTPHandler(settingsState))
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterOpenHook(func(context.Context) error { return nil })
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	cleanup, err = host.RegisterCloseHook(func(context.Context) error { return nil })
	if err != nil {
		return fail(err)
	}
	pushCleanup(cleanup)

	return func() {
		closeAll()
	}, nil
}

func indexResolveSettingsActionResult(ctx context.Context, store indexResolveModuleStore, action string, payload map[string]any) (map[string]any, error) {
	switch strings.TrimSpace(action) {
	case obsActionIndexResolveResolve:
		route := strings.TrimSpace(fmt.Sprint(payload["route"]))
		manifest, err := BizResolve(ctx, store, route)
		if err != nil {
			return nil, toModuleAPIError(err)
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
		items, err := BizSettingsList(ctx, store)
		if err != nil {
			return nil, toModuleAPIError(err)
		}
		return map[string]any{
			"total": len(items),
			"items": items,
		}, nil
	case obsActionIndexResolveUpsert:
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
	case obsActionIndexResolveDelete:
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

func indexResolveOBSActionResult(ctx context.Context, store indexResolveModuleStore, action string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
	out, err := indexResolveSettingsActionResult(ctx, store, action, payload)
	if err != nil {
		return moduleapi.OBSActionResponse{}, err
	}
	result := map[string]string{
		obsActionIndexResolveResolve: "resolved",
		obsActionIndexResolveList:    "listed",
		obsActionIndexResolveUpsert:  "upserted",
		obsActionIndexResolveDelete:  "deleted",
	}[strings.TrimSpace(action)]
	if result == "" {
		return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
	}
	return moduleapi.OBSActionResponse{OK: true, Result: result, Payload: out}, nil
}

func newIndexResolveSettingsHTTPHandler(state *indexResolveSettingsState) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeIndexResolveJSON(w, http.StatusBadRequest, map[string]any{
				"status": "error",
				"error": map[string]any{
					"code":    "BAD_REQUEST",
					"message": "request is required",
				},
			})
			return
		}
		action, payload, status, err := indexResolveSettingsActionRequest(r)
		if err != nil {
			writeIndexResolveError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
			return
		}
		if state == nil || state.handlers == nil {
			writeIndexResolveError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "module is disabled")
			return
		}
		handler := state.handlers[strings.TrimSpace(action)]
		if handler == nil {
			writeIndexResolveError(w, http.StatusServiceUnavailable, "MODULE_DISABLED", "module is disabled")
			return
		}
		out, err := handler(r.Context(), payload)
		if err != nil {
			writeIndexResolveError(w, indexResolveSettingsStatusFromError(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
			return
		}
		writeIndexResolveOK(w, indexResolveSettingsPayloadForHTTP(action, out))
	}
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
			return "", nil, http.StatusBadRequest, moduleapi.NewError("BAD_REQUEST", "invalid json")
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
		return "", nil, http.StatusMethodNotAllowed, moduleapi.NewError("METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func indexResolveSettingsPayloadForHTTP(action string, payload map[string]any) any {
	switch action {
	case obsActionIndexResolveList:
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

func writeIndexResolveOK(w http.ResponseWriter, data any) {
	writeIndexResolveJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func writeIndexResolveError(w http.ResponseWriter, status int, code, message string) {
	writeIndexResolveJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    strings.TrimSpace(code),
			"message": strings.TrimSpace(message),
		},
	})
}

func writeIndexResolveJSON(w http.ResponseWriter, status int, payload any) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
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

func toModuleAPIError(err error) error {
	if err == nil {
		return nil
	}
	if code := CodeOf(err); code != "" {
		return moduleapi.NewError(code, MessageOf(err))
	}
	if code := moduleapi.CodeOf(err); code != "" {
		return err
	}
	return moduleapi.NewError("INTERNAL_ERROR", err.Error())
}
