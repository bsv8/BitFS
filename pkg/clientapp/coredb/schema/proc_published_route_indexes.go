package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcPublishedRouteIndexes 对齐表 proc_published_route_indexes。
type ProcPublishedRouteIndexes struct {
	ent.Schema
}

func (ProcPublishedRouteIndexes) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_published_route_indexes"}}
}

func (ProcPublishedRouteIndexes) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("route").Unique().Immutable(),
		field.String("seed_hash"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcPublishedRouteIndexes) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("updated_at_unix", "route"),
	}
}
