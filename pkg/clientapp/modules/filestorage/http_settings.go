package filestorage

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func handleWorkspaces(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		switch r.Method {
		case http.MethodGet:
			items, err := svc.listWorkspaces(r.Context())
			if err != nil {
				writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
				return
			}
			writeModuleOK(w, map[string]any{"total": len(items), "items": items})
		case http.MethodPost:
			var req struct {
				WorkspacePath string `json:"workspace_path"`
				MaxBytes      uint64 `json:"max_bytes"`
				Enabled       *bool  `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
				return
			}
			enabled := true
			if req.Enabled != nil {
				enabled = *req.Enabled
			}
			item, err := dbUpsertWorkspace(r.Context(), svc.host.Store(), req.WorkspacePath, req.MaxBytes, enabled)
			if err != nil {
				writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
				return
			}
			_ = svc.reloadWatcher(r.Context())
			writeModuleOK(w, map[string]any{"workspace": item})
		default:
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	}
}

func handleWorkspaceDetail(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		path := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if path == "" {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "workspace_path is required")
			return
		}
		switch r.Method {
		case http.MethodPut:
			var req struct {
				MaxBytes *uint64 `json:"max_bytes,omitempty"`
				Enabled  *bool   `json:"enabled,omitempty"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
				return
			}
			item, err := dbUpdateWorkspace(r.Context(), svc.host.Store(), path, req.MaxBytes, req.Enabled)
			if err != nil {
				writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
				return
			}
			_ = svc.reloadWatcher(r.Context())
			writeModuleOK(w, map[string]any{"workspace": item})
		case http.MethodDelete:
			if err := dbDeleteWorkspace(r.Context(), svc.host.Store(), path); err != nil {
				writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
				return
			}
			_ = svc.reloadWatcher(r.Context())
			writeModuleOK(w, map[string]any{"workspace_path": path})
		default:
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	}
}

func handleRegisterDownloadedFile(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		var req struct {
			FilePath            string `json:"file_path"`
			SeedHash            string `json:"seed_hash"`
			SeedLocked          bool   `json:"seed_locked"`
			RecommendedFileName string `json:"recommended_file_name"`
			MimeHint            string `json:"mime_hint"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
			return
		}
		if err := svc.registerDownloadedFile(r.Context(), req.FilePath, req.SeedHash, req.SeedLocked, req.RecommendedFileName, req.MimeHint); err != nil {
			writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
			return
		}
		writeModuleOK(w, map[string]any{"ok": true})
	}
}

func handleSettingsList(svc *service) moduleapi.SettingsHook {
	return func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		items, err := svc.listWorkspaces(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"items": items, "total": len(items)}, nil
	}
}

func handleSettingsAdd(svc *service) moduleapi.SettingsHook {
	return func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		path, _ := payload["workspace_path"].(string)
		maxBytes := uint64(0)
		if v, ok := payload["max_bytes"]; ok {
			switch t := v.(type) {
			case int:
				maxBytes = uint64(t)
			case int64:
				maxBytes = uint64(t)
			case float64:
				maxBytes = uint64(t)
			}
		}
		item, err := dbUpsertWorkspace(ctx, svc.host.Store(), path, maxBytes, true)
		if err != nil {
			return nil, err
		}
		return map[string]any{"workspace": item}, nil
	}
}

func handleSettingsUpdate(svc *service) moduleapi.SettingsHook {
	return func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		path, _ := payload["workspace_path"].(string)
		var maxBytes *uint64
		if v, ok := payload["max_bytes"]; ok {
			switch t := v.(type) {
			case int:
				val := uint64(t)
				maxBytes = &val
			case int64:
				val := uint64(t)
				maxBytes = &val
			case float64:
				val := uint64(t)
				maxBytes = &val
			}
		}
		var enabled *bool
		if v, ok := payload["enabled"].(bool); ok {
			enabled = &v
		}
		item, err := dbUpdateWorkspace(ctx, svc.host.Store(), path, maxBytes, enabled)
		if err != nil {
			return nil, err
		}
		return map[string]any{"workspace": item}, nil
	}
}

func handleSettingsDelete(svc *service) moduleapi.SettingsHook {
	return func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		path, _ := payload["workspace_path"].(string)
		if err := dbDeleteWorkspace(ctx, svc.host.Store(), path); err != nil {
			return nil, err
		}
		return map[string]any{"workspace_path": strings.TrimSpace(path)}, nil
	}
}

func handleSettingsSyncOnce(svc *service) moduleapi.SettingsHook {
	return func(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
		if err := svc.syncOnce(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, nil
	}
}
