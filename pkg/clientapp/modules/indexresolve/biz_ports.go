package indexresolve

import "context"

// ResolveReader 只提供解析能力。
//
// 设计说明：
// - 业务入口只拿最小能力，不拿 Service 或聚合 store；
// - 这样 HTTP、libp2p、测试都能共用同一层业务入口。
type ResolveReader interface {
	ResolveIndexRoute(ctx context.Context, route string) (Manifest, error)
}

// SettingsLister 只提供列表能力。
type SettingsLister interface {
	ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error)
}

// SettingsUpserter 只提供写入能力。
type SettingsUpserter interface {
	UpsertIndexResolveRoute(ctx context.Context, route string, seedHash string, updatedAtUnix int64) (RouteItem, error)
}

// SettingsDeleter 只提供删除能力。
type SettingsDeleter interface {
	DeleteIndexResolveRoute(ctx context.Context, route string) error
}

// ModuleState 只表示模块当前是否还活着。
//
// 设计说明：
// - 这是一个很小的状态能力，不是依赖聚合；
// - 关闭后业务入口直接返回 MODULE_DISABLED，避免继续穿透到 store；
// - 这样 with_indexresolve 的同生共死语义可以保住。
type ModuleState interface {
	Enabled() bool
}

// ObsEmitter 是可选的观察事件输出能力。
//
// 设计说明：
// - 业务入口只负责决定何时发事件；
// - emitter 可以为空，空值不影响主流程；
// - 适配层只负责把事件送到进程级 obs 系统。
type ObsEmitter interface {
	Emit(level string, name string, fields map[string]any)
}

// ObsEmitterFunc 让普通函数也能作为观察事件输出。
type ObsEmitterFunc func(level string, name string, fields map[string]any)

func (f ObsEmitterFunc) Emit(level string, name string, fields map[string]any) {
	if f == nil {
		return
	}
	f(level, name, fields)
}
