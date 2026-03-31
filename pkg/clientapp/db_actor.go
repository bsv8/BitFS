package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

// 设计说明：
// - 这些 helper 只做一件事：把 BitFS 运行时访问统一压回 sqliteactor；
// - 新的运行时代码优先通过这些入口取值或执行事务；
// - 不允许把 `Rows / Tx / Stmt` 逃逸到 helper 外，否则会重新引入单连接重入问题。
func runtimeDBDo(rt *Runtime, ctx context.Context, fn func(*sql.DB) error) error {
	store := runtimeStore(rt)
	if store == nil {
		return fmt.Errorf("runtime db actor is nil")
	}
	return store.Do(ctx, fn)
}

func runtimeDBValue[T any](rt *Runtime, ctx context.Context, fn func(*sql.DB) (T, error)) (T, error) {
	store := runtimeStore(rt)
	if store == nil {
		var zero T
		return zero, fmt.Errorf("runtime db actor is nil")
	}
	return clientDBValue(ctx, store, fn)
}

func httpDBValue[T any](ctx context.Context, s *httpAPIServer, fn func(*sql.DB) (T, error)) (T, error) {
	store := httpStore(s)
	if store == nil {
		var zero T
		return zero, fmt.Errorf("http db is nil")
	}
	return clientDBValue(ctx, store, fn)
}

func schedulerDBDo(s *taskScheduler, ctx context.Context, fn func(*sql.DB) error) error {
	store := schedulerStore(s)
	if store == nil {
		return nil
	}
	return store.Do(ctx, fn)
}

func runtimeStore(rt *Runtime) *clientDB {
	if rt == nil {
		return nil
	}
	return rt.store
}

func httpStore(s *httpAPIServer) *clientDB {
	if s == nil {
		return nil
	}
	if s.store != nil {
		return s.store
	}
	return newClientDB(s.db, s.dbActor)
}

func schedulerStore(s *taskScheduler) *clientDB {
	if s == nil {
		return nil
	}
	if s.store != nil {
		return s.store
	}
	return newClientDB(s.db, s.dbActor)
}
