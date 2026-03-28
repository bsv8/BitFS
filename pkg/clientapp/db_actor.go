package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

// 设计说明：
// - 这些 helper 只做一件事：把 BitFS 运行时访问统一压回 sqliteactor；
// - 新的运行时代码优先通过这些入口取值或执行事务；
// - 不允许把 `Rows / Tx / Stmt` 逃逸到 helper 外，否则会重新引入单连接重入问题。
func runtimeDBDo(rt *Runtime, ctx context.Context, fn func(*sql.DB) error) error {
	if rt == nil || rt.DBActor == nil {
		return fmt.Errorf("runtime db actor is nil")
	}
	return rt.DBActor.Do(ctx, fn)
}

func runtimeDBValue[T any](rt *Runtime, ctx context.Context, fn func(*sql.DB) (T, error)) (T, error) {
	if rt == nil || rt.DBActor == nil {
		var zero T
		return zero, fmt.Errorf("runtime db actor is nil")
	}
	return sqliteactor.DoValue(ctx, rt.DBActor, fn)
}

func httpDBValue[T any](ctx context.Context, s *httpAPIServer, fn func(*sql.DB) (T, error)) (T, error) {
	if s == nil {
		var zero T
		return zero, fmt.Errorf("http api server is nil")
	}
	if s.dbActor != nil {
		return sqliteactor.DoValue(ctx, s.dbActor, fn)
	}
	if s.db != nil {
		return fn(s.db)
	}
	var zero T
	return zero, fmt.Errorf("http db is nil")
}

func schedulerDBDo(s *taskScheduler, ctx context.Context, fn func(*sql.DB) error) error {
	if s == nil || s.dbActor == nil {
		return nil
	}
	return s.dbActor.Do(ctx, fn)
}
