package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

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
	return s.store
}
