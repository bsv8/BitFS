package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Pool2of3V1Arbitrations 对齐表 pool_2of3_v1_arbitrations。
// 记录 pool_2of3_v1 模块的仲裁案件。
type Pool2of3V1Arbitrations struct {
	ent.Schema
}

func (Pool2of3V1Arbitrations) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pool_2of3_v1_arbitrations"}}
}

func (Pool2of3V1Arbitrations) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_ref"),
		field.Int("chunk_index"),
		field.String("case_id").Unique(),
		field.String("state"), // opened, decided, appealed
		field.String("evidence"),
		field.String("award_txid"),
		field.Int64("decided_at_unix"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (Pool2of3V1Arbitrations) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("session_ref", "chunk_index").Unique(),
		index.Fields("state", "created_at_unix"),
	}
}
