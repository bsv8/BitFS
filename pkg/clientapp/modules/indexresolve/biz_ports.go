package indexresolve

import "context"

// Store 是模块对外的完整存储能力。
//
// 设计说明：
// - 业务入口只依赖最小能力，避免把 DB 细节往上抛；
// - 这里保留完整 store 口径，方便测试和运行侧接线统一；
// - 业务流程仍然只走 Biz*，这里不承载任何行为。
type Store interface {
	ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error)
	ResolveIndexRoute(ctx context.Context, route string) (Manifest, error)
	UpsertIndexResolveRoute(ctx context.Context, route string, seedHash string, updatedAtUnix int64) (RouteItem, error)
	DeleteIndexResolveRoute(ctx context.Context, route string) error
	GetIndexResolveSeed(ctx context.Context, seedHash string) (SeedItem, error)
}

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
