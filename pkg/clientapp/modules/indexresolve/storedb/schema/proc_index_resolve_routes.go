package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcIndexResolveRoutes 对齐表 proc_index_resolve_routes。
// 设计说明：
// - 这是 index-resolve 模块的单点路由表；
// - 路由唯一，seed_hash 是可追踪来源。
type ProcIndexResolveRoutes struct {
	ent.Schema
}

func (ProcIndexResolveRoutes) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_index_resolve_routes"}}
}

func (ProcIndexResolveRoutes) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("route").Unique().Immutable(),
		field.String("seed_hash"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcIndexResolveRoutes) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("seed_hash").StorageKey("idx_proc_index_resolve_routes_seed_hash"),
		index.Fields("updated_at_unix", "route").StorageKey("idx_proc_index_resolve_routes_updated_at"),
	}
}
