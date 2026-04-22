package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizDemands 对齐表 biz_demands。
type BizDemands struct {
	ent.Schema
}

func (BizDemands) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_demands"}}
}

func (BizDemands) Fields() []ent.Field {
	return []ent.Field{
		field.String("demand_id"),
		field.String("seed_hash"),
		field.Int64("created_at_unix"),
	}
}

func (BizDemands) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("demand_id").Unique(),
		index.Fields("created_at_unix", "id"),
	}
}
