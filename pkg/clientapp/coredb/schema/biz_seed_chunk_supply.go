package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizSeedChunkSupply 对齐表 biz_seed_chunk_supply。
type BizSeedChunkSupply struct {
	ent.Schema
}

func (BizSeedChunkSupply) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_seed_chunk_supply"}}
}

func (BizSeedChunkSupply) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("seed_hash"),
		field.Int64("chunk_index"),
	}
}

func (BizSeedChunkSupply) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("seed_hash", "chunk_index").Unique(),
	}
}
