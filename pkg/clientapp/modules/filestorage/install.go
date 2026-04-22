package filestorage

import (
	"context"
	"fmt"
	"net/http"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	svc, err := newService(host)
	if err != nil {
		return nil, err
	}
	svc.ensureWatcher()

	cleanup, err := host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		HTTP: []moduleapi.HTTPRoute{
			{Path: "/v1/settings/file-storage/workspaces", Handler: handleWorkspaces(svc)},
			{Path: "/v1/settings/file-storage/workspaces/detail", Handler: handleWorkspaceDetail(svc)},
			{Path: "/v1/file-storage/files", Handler: handleFiles(svc)},
			{Path: "/v1/file-storage/seeds", Handler: handleSeeds(svc)},
			{Path: "/v1/admin/static/tree", Handler: handleAdminStaticTree(svc)},
			{Path: "/v1/admin/static/mkdir", Handler: handleAdminStaticMkdir(svc)},
			{Path: "/v1/admin/static/upload", Handler: handleAdminStaticUpload(svc)},
			{Path: "/v1/admin/static/move", Handler: handleAdminStaticMove(svc)},
			{Path: "/v1/admin/static/entry", Handler: handleAdminStaticEntry(svc)},
			{Path: "/v1/admin/static/price/set", Handler: handleAdminStaticPriceSet(svc)},
			{Path: "/v1/admin/static/price", Handler: handleAdminStaticPriceGet(svc)},
			{Path: "/v1/admin/file-storage/scan-once", Handler: handleScanOnce(svc)},
			{Path: "/v1/admin/file-storage/register-downloaded-file", Handler: handleRegisterDownloadedFile(svc)},
		},
		Settings: []moduleapi.SettingsAction{
			{Action: "file_storage.list", Handler: handleSettingsList(svc)},
			{Action: "file_storage.add", Handler: handleSettingsAdd(svc)},
			{Action: "file_storage.update", Handler: handleSettingsUpdate(svc)},
			{Action: "file_storage.delete", Handler: handleSettingsDelete(svc)},
			{Action: "file_storage.sync_once", Handler: handleSettingsSyncOnce(svc)},
		},
		OpenHooks: []moduleapi.OpenHook{
			func(ctx context.Context) error {
				return svc.open(ctx)
			},
		},
		CloseHooks: []moduleapi.CloseHook{
			func(ctx context.Context) error {
				return svc.close(ctx)
			},
		},
	})
	return cleanup, err
}

func handleScanOnce(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodPost {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		if err := svc.syncOnce(r.Context()); err != nil {
			writeModuleError(w, httpStatusFromErr(err), "INTERNAL_ERROR", err.Error())
			return
		}
		writeModuleOK(w, map[string]any{"ok": true})
	}
}
