package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcGetFileByHashQuotes 对齐表 proc_getfilebyhash_quotes（getfilebyhash 模块的 quote 上报表）。
type ProcGetFileByHashQuotes struct {
	ent.Schema
}

func (ProcGetFileByHashQuotes) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_getfilebyhash_quotes"}}
}

func (ProcGetFileByHashQuotes) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("job_id"),
		field.String("seed_hash"),
		field.String("seller_pubkey_hex"),
		field.Int64("seed_price_sat"),
		field.Int64("chunk_price_sat"),
		field.Int64("chunk_count"),
		field.String("available_chunks_json"),
		field.String("recommended_file_name"),
		field.String("mime_type"),
		field.Int64("file_size_bytes"),
		field.Int64("quote_timestamp"),
		field.Int64("expires_at_unix"),
		field.Bool("selected"),
		field.String("reject_reason"),
	}
}

func (ProcGetFileByHashQuotes) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("job_id", "seller_pubkey_hex").Unique(),
		index.Fields("seed_hash"),
	}
}