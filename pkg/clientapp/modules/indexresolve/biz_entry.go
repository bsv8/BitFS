package indexresolve

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// BizResolve 是 indexresolve 的业务入口之一。
//
// 设计说明：
// - 这里只做参数校验、归一化和错误码映射；
// - 上层接线只负责把能力和参数送进来，不再写业务分支；
// - route 归一化留在业务入口层，避免 adapter 和 store 重复判断。
func BizResolve(ctx context.Context, resolver ResolveReader, rawRoute string) (Manifest, error) {
	if ctx == nil {
		return Manifest{}, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return Manifest{}, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if resolver == nil {
		return Manifest{}, moduleDisabledErr()
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return Manifest{}, err
	}

	manifest, err := resolver.ResolveIndexRoute(ctx, route)
	if err != nil {
		err = mapBizStoreError(err, "resolve")
		return Manifest{}, err
	}

	return manifest, nil
}

// BizSettingsList 是 settings 列表的业务入口。
func BizSettingsList(ctx context.Context, lister SettingsLister) ([]RouteItem, error) {
	if ctx == nil {
		return nil, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if lister == nil {
		return nil, moduleDisabledErr()
	}

	items, err := lister.ListIndexResolveRoutes(ctx)
	if err != nil {
		err = mapBizStoreError(err, "list")
		return nil, err
	}
	return items, nil
}

// BizSettingsUpsert 是 settings 写入的业务入口。
func BizSettingsUpsert(ctx context.Context, upserter SettingsUpserter, rawRoute string, rawSeedHash string) (RouteItem, error) {
	if ctx == nil {
		return RouteItem{}, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return RouteItem{}, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if upserter == nil {
		return RouteItem{}, moduleDisabledErr()
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return RouteItem{}, err
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return RouteItem{}, err
	}

	item, err := upserter.UpsertIndexResolveRoute(ctx, route, seedHash, time.Now().Unix())
	if err != nil {
		err = mapBizStoreError(err, "upsert")
		return RouteItem{}, err
	}

	return item, nil
}

// BizSettingsDelete 是 settings 删除的业务入口。
func BizSettingsDelete(ctx context.Context, deleter SettingsDeleter, rawRoute string) error {
	if ctx == nil {
		return NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if deleter == nil {
		return moduleDisabledErr()
	}

	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return err
	}
	if err := deleter.DeleteIndexResolveRoute(ctx, route); err != nil {
		err = mapBizStoreError(err, "delete")
		return err
	}

	return nil
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
