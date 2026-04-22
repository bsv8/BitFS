package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcGetFileByHashChunks 对齐表 proc_getfilebyhash_chunks（getfilebyhash 模块的 chunk 上报表）。
type ProcGetFileByHashChunks struct {
	ent.Schema
}

func (ProcGetFileByHashChunks) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_getfilebyhash_chunks"}}
}

func (ProcGetFileByHashChunks) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("job_id"),
		field.String("seed_hash"),
		field.Int64("chunk_index"),
		field.String("state"),
		field.String("seller_pubkey_hex"),
		field.Int64("chunk_price_sat"),
		field.Int64("speed_bps"),
		field.Bool("selected"),
		field.String("reject_reason"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcGetFileByHashChunks) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("job_id", "chunk_index").Unique(),
		index.Fields("seed_hash"),
	}
}