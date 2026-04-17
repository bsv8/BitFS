package clientapp

import (
	"context"
	"fmt"
)

func httpDBValue[T any](ctx context.Context, s *httpAPIServer, fn func(*clientDB) (T, error)) (T, error) {
	store := httpStore(s)
	if store == nil {
		var zero T
		return zero, fmt.Errorf("http db is nil")
	}
	if fn == nil {
		var zero T
		return zero, fmt.Errorf("http db fn is nil")
	}
	return fn(store)
}

func httpStore(s *httpAPIServer) *clientDB {
	if s == nil {
		return nil
	}
	return s.store
}
