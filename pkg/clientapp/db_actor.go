package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

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
	if s == nil || s.dbActor == nil {
		var zero T
		return zero, fmt.Errorf("http db actor is nil")
	}
	return sqliteactor.DoValue(ctx, s.dbActor, fn)
}

func schedulerDBDo(s *taskScheduler, ctx context.Context, fn func(*sql.DB) error) error {
	if s == nil || s.dbActor == nil {
		return nil
	}
	return s.dbActor.Do(ctx, fn)
}
