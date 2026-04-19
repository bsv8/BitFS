//go:build !with_indexresolve

package clientapp

import "context"

// registerOptionalModules 在默认构建下只挂 inbox；不引入 indexresolve。
//
// 设计说明：
// - 默认构建必须保留 inbox.message；
// - 不启用 indexresolve 时，只接 inbox，不接索引解析；
// - cleanup 仍然统一由调用方管理。
func registerOptionalModules(ctx context.Context, rt *Runtime, store moduleBootstrapStore) (func(), error) {
	if ctx == nil || rt == nil || store == nil {
		return func() {}, nil
	}
	return registerInboxMessageModule(ctx, rt, store)
}
