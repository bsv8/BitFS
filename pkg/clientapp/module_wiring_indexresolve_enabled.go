//go:build with_indexresolve

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

// registerOptionalModules 只负责把可选模块接到主框架钩子上。
//
// 设计说明：
// - 这里是唯一允许引用 indexresolve 模块实现包的地方；
// - 模块自己的 store、能力、settings 路由、白名单都在这里接线；
// - cleanup 必须同时解绑钩子和关闭模块服务，避免关闭后还能继续读写。
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
	svc := indexresolve.NewService(moduleStore, rt)
	reg := ensureModuleRegistry(rt)
	if reg == nil {
		svc.Close()
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
			// 这里只做查询映射，不给 settings 写入口留旁路。
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
