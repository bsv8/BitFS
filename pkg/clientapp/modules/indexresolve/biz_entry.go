package indexresolve

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

const (
	bizObsLevelBusiness = "business"
	bizObsLevelError    = "error"
)

// BizResolve 是 indexresolve 的业务入口之一。
//
// 设计说明：
// - 这里统一做参数校验、归一化、错误码映射和 obs 事件；
// - 上层接线只负责把能力和参数送进来，不再写业务分支；
// - route 归一化留在业务入口层，避免 adapter 和 store 重复判断。
func BizResolve(ctx context.Context, state ModuleState, resolver ResolveReader, emitter ObsEmitter, rawRoute string) (Manifest, error) {
	if ctx == nil {
		return emitBizResolveError(emitter, Manifest{}, NewError("BAD_REQUEST", "ctx is required"))
	}
	if ctx.Err() != nil {
		return emitBizResolveError(emitter, Manifest{}, NewError("REQUEST_CANCELED", ctx.Err().Error()))
	}
	if state == nil || !state.Enabled() || resolver == nil {
		return emitBizResolveError(emitter, Manifest{}, moduleDisabledErr())
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return emitBizResolveError(emitter, Manifest{}, err)
	}

	manifest, err := resolver.ResolveIndexRoute(ctx, route)
	if err != nil {
		err = mapBizStoreError(err, "resolve")
		return emitBizResolveError(emitter, Manifest{}, err, "route", route)
	}

	emitBizResolveOK(emitter, route, manifest)
	return manifest, nil
}

// BizSettingsList 是 settings 列表的业务入口。
func BizSettingsList(ctx context.Context, state ModuleState, lister SettingsLister, emitter ObsEmitter) ([]RouteItem, error) {
	if ctx == nil {
		return nil, emitBizSettingsListError(emitter, NewError("BAD_REQUEST", "ctx is required"))
	}
	if ctx.Err() != nil {
		return nil, emitBizSettingsListError(emitter, NewError("REQUEST_CANCELED", ctx.Err().Error()))
	}
	if state == nil || !state.Enabled() || lister == nil {
		return nil, emitBizSettingsListError(emitter, moduleDisabledErr())
	}

	items, err := lister.ListIndexResolveRoutes(ctx)
	if err != nil {
		err = mapBizStoreError(err, "list")
		return nil, emitBizSettingsListError(emitter, err)
	}
	emitBizSettingsListOK(emitter, len(items))
	return items, nil
}

// BizSettingsUpsert 是 settings 写入的业务入口。
func BizSettingsUpsert(ctx context.Context, state ModuleState, upserter SettingsUpserter, emitter ObsEmitter, rawRoute string, rawSeedHash string) (RouteItem, error) {
	if ctx == nil {
		return emitBizSettingsUpsertError(emitter, RouteItem{}, NewError("BAD_REQUEST", "ctx is required"))
	}
	if ctx.Err() != nil {
		return emitBizSettingsUpsertError(emitter, RouteItem{}, NewError("REQUEST_CANCELED", ctx.Err().Error()))
	}
	if state == nil || !state.Enabled() || upserter == nil {
		return emitBizSettingsUpsertError(emitter, RouteItem{}, moduleDisabledErr())
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return emitBizSettingsUpsertError(emitter, RouteItem{}, err)
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return emitBizSettingsUpsertError(emitter, RouteItem{}, err, "route", route)
	}

	item, err := upserter.UpsertIndexResolveRoute(ctx, route, seedHash, time.Now().Unix())
	if err != nil {
		err = mapBizStoreError(err, "upsert")
		return emitBizSettingsUpsertError(emitter, RouteItem{}, err, "route", route, "seed_hash", seedHash)
	}

	emitBizSettingsUpsertOK(emitter, item)
	return item, nil
}

// BizSettingsDelete 是 settings 删除的业务入口。
func BizSettingsDelete(ctx context.Context, state ModuleState, deleter SettingsDeleter, emitter ObsEmitter, rawRoute string) (string, error) {
	if ctx == nil {
		return "", emitBizSettingsDeleteError(emitter, NewError("BAD_REQUEST", "ctx is required"))
	}
	if ctx.Err() != nil {
		return "", emitBizSettingsDeleteError(emitter, NewError("REQUEST_CANCELED", ctx.Err().Error()))
	}
	if state == nil || !state.Enabled() || deleter == nil {
		return "", emitBizSettingsDeleteError(emitter, moduleDisabledErr())
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return "", emitBizSettingsDeleteError(emitter, err)
	}
	if err := deleter.DeleteIndexResolveRoute(ctx, route); err != nil {
		err = mapBizStoreError(err, "delete")
		return "", emitBizSettingsDeleteError(emitter, err, "route", route)
	}

	emitBizSettingsDeleteOK(emitter, route)
	return route, nil
}

func mapBizStoreError(err error, op string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError("REQUEST_CANCELED", err.Error())
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		switch op {
		case "resolve", "delete":
			return routeNotFoundErr()
		case "upsert":
			return seedNotFoundErr()
		}
	}
	return err
}

func emitBizResolveOK(emitter ObsEmitter, route string, manifest Manifest) {
	if emitter == nil {
		return
	}
	emitter.Emit(bizObsLevelBusiness, "indexresolve.resolve", map[string]any{
		"route":                 strings.TrimSpace(route),
		"seed_hash":             strings.TrimSpace(manifest.SeedHash),
		"recommended_file_name": strings.TrimSpace(manifest.RecommendedFileName),
		"mime_hint":             strings.TrimSpace(manifest.MIMEHint),
		"file_size":             manifest.FileSize,
		"updated_at_unix":       manifest.UpdatedAtUnix,
	})
}

func emitBizResolveError(emitter ObsEmitter, fallback Manifest, err error, fields ...any) (Manifest, error) {
	emitBizError(emitter, "indexresolve.resolve", err, fields...)
	return fallback, err
}

func emitBizSettingsListOK(emitter ObsEmitter, total int) {
	if emitter == nil {
		return
	}
	emitter.Emit(bizObsLevelBusiness, "indexresolve.settings_list", map[string]any{
		"total": total,
	})
}

func emitBizSettingsListError(emitter ObsEmitter, err error, fields ...any) error {
	emitBizError(emitter, "indexresolve.settings_list", err, fields...)
	return err
}

func emitBizSettingsUpsertOK(emitter ObsEmitter, item RouteItem) {
	if emitter == nil {
		return
	}
	emitter.Emit(bizObsLevelBusiness, "indexresolve.settings_upsert", map[string]any{
		"route":           strings.TrimSpace(item.Route),
		"seed_hash":       strings.TrimSpace(item.SeedHash),
		"updated_at_unix": item.UpdatedAtUnix,
	})
}

func emitBizSettingsUpsertError(emitter ObsEmitter, fallback RouteItem, err error, fields ...any) (RouteItem, error) {
	emitBizError(emitter, "indexresolve.settings_upsert", err, fields...)
	return fallback, err
}

func emitBizSettingsDeleteOK(emitter ObsEmitter, route string) {
	if emitter == nil {
		return
	}
	emitter.Emit(bizObsLevelBusiness, "indexresolve.settings_delete", map[string]any{
		"route":   strings.TrimSpace(route),
		"deleted": true,
	})
}

func emitBizSettingsDeleteError(emitter ObsEmitter, err error, fields ...any) error {
	emitBizError(emitter, "indexresolve.settings_delete", err, fields...)
	return err
}

func emitBizError(emitter ObsEmitter, name string, err error, fields ...any) {
	if emitter == nil {
		return
	}
	payload := map[string]any{
		"code":    CodeOf(err),
		"message": MessageOf(err),
	}
	if len(fields) > 0 {
		for i := 0; i+1 < len(fields); i += 2 {
			key, _ := fields[i].(string)
			if strings.TrimSpace(key) == "" {
				continue
			}
			payload[key] = fields[i+1]
		}
	}
	emitter.Emit(bizObsLevelError, name, payload)
}
