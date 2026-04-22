package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcFileDownloads 对齐表 proc_file_downloads。
type ProcFileDownloads struct {
	ent.Schema
}

func (ProcFileDownloads) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_file_downloads"}}
}

func (ProcFileDownloads) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("seed_hash").Unique().Immutable(),
		field.String("file_path"),
		field.Int64("file_size"),
		field.Int64("chunk_count"),
		field.Int64("completed_chunks"),
		field.Int64("paid_sats"),
		field.String("status"),
		field.String("demand_id"),
		field.String("last_error"),
		field.String("status_json"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcFileDownloads) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("updated_at_unix"),
	}
}
