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
	if s.store != nil {
		return s.store
	}
	// 仅兼容旧测试夹具：很多单测直接构造 httpAPIServer{db:...}。
	// 运行时主路径已在 run.go 注入 store，这里不走 newClientDB，避免新增构造入口。
	if s.db != nil {
		return clientDBFromDB(s.db)
	}
	return nil
}
