package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcFileDownloadChunks 对齐表 proc_file_download_chunks。
type ProcFileDownloadChunks struct {
	ent.Schema
}

func (ProcFileDownloadChunks) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_file_download_chunks"}}
}

func (ProcFileDownloadChunks) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("seed_hash"),
		field.Int64("chunk_index"),
		field.String("status"),
		field.String("seller_pubkey_hex"),
		field.Int64("price_sats"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcFileDownloadChunks) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("seed_hash", "chunk_index").Unique(),
	}
}
