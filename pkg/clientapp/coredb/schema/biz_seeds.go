package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// BizSeeds 对齐表 biz_seeds。
type BizSeeds struct {
	ent.Schema
}

func (BizSeeds) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_seeds"}}
}

func (BizSeeds) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("seed_hash").Unique().Immutable(),
		field.Int64("chunk_count"),
		field.Int64("file_size"),
		field.String("seed_file_path"),
		field.String("recommended_file_name").Default(""),
		field.String("mime_hint").Default(""),
	}
}
