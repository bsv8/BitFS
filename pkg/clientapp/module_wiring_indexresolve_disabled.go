//go:build !with_indexresolve

package clientapp

import "context"

// registerOptionalModules 在无模块构建下保持空实现。
//
// 设计说明：
// - 编译期开关直接把模块接线切掉；
// - 这里不引入模块实现包，保证无 tag 构建是干净的；
// - 返回 no-op cleanup，调用方不需要分支处理。
func registerOptionalModules(_ context.Context, _ *Runtime, _ any) (func(), error) {
	return func() {}, nil
}
