//go:build !indexresolve_disabled

package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
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

// registerIndexResolveModule 只负责把模块接到主框架钩子上。
// 设计说明：
// - 模块包只允许在这里被引用；
// - 这里先建模块自己的 store/schema，再把能力和路由挂进注册表；
// - 返回的 cleanup 会同时解绑钩子并把模块服务打失效，避免关闭后继续可用。
func registerIndexResolveModule(ctx context.Context, rt *Runtime, store indexResolveBootstrapStore) (func(), error) {
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
	svc := indexresolve.NewService(moduleStore, rt)
	reg := ensureModuleRegistry(rt)
	if reg == nil {
		return func() {}, nil
	}
	moduleCleanup, err := reg.registerModuleLockProvider(indexresolve.ModuleIdentity, indexresolve.FunctionLocks)
	if err != nil {
		svc.Close()
		return nil, err
	}
	cleanup, err := reg.registerIndexResolve(
		func() *contractmessage.CapabilityItem {
			return svc.Capability()
		},
		func(ctx context.Context, route string) (routeIndexManifest, error) {
			// 这里只做查询映射，不给任何 settings 写入口留旁路。
			manifest, err := svc.Resolve(ctx, route)
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
		},
		func(s *httpAPIServer, mux *http.ServeMux, prefix string) {
			if s == nil || mux == nil {
				return
			}
			mux.HandleFunc(prefix+"/v1/settings/index-resolve", s.withAuth(indexresolve.NewHTTPSettingsIndexResolveHandler(svc)))
		},
		func() {
			svc.Close()
		},
	)
	if err != nil {
		moduleCleanup()
		svc.Close()
		return nil, err
	}
	return func() {
		cleanup()
		moduleCleanup()
	}, nil
}
