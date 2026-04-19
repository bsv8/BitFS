//go:build indexresolve_disabled

package clientapp

import "context"

// registerIndexResolveModule 在禁用构建下保持空实现。
// 设计说明：这里彻底不引入模块实现，编译层面就把能力切掉。
func registerIndexResolveModule(ctx context.Context, rt *Runtime, store any) (func(), error) {
	return func() {}, nil
}
