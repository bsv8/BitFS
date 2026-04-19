package indexresolve

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

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
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

type indexResolveSerialAdapter struct {
	store moduleapi.Store
}

func (a indexResolveSerialAdapter) Do(ctx context.Context, fn func(Conn) error) error {
	return a.store.Do(ctx, func(conn moduleapi.Conn) error {
		return fn(conn)
	})
}

func openIndexResolveStore(ctx context.Context, host moduleapi.Host) (indexResolveModuleStore, error) {
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

	return BootstrapStore(ctx, db, serialExec)
}

func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	moduleStore, err := openIndexResolveStore(ctx, host)
	if err != nil {
		return nil, err
	}

	return host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		LibP2P: []moduleapi.LibP2PRoute{
			{Protocol: moduleapi.LibP2PProtocolNodeResolve, Handler: resolveIndexRoute(moduleStore)},
		},
		Settings: indexResolveSettingsActions(moduleStore),
		OBS:      indexResolveOBSActions(moduleStore),
		HTTP: []moduleapi.HTTPRoute{
			{Path: "/v1/settings/index-resolve", Handler: handleIndexResolveSettings(moduleStore)},
		},
	})
}
