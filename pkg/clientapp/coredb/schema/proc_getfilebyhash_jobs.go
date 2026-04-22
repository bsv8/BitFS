package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcGetFileByHashJobs 对齐表 proc_getfilebyhash_jobs（getfilebyhash 模块的 job 主表）。
type ProcGetFileByHashJobs struct {
	ent.Schema
}

func (ProcGetFileByHashJobs) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_getfilebyhash_jobs"}}
}

func (ProcGetFileByHashJobs) Fields() []ent.Field {
	return []ent.Field{
		field.String("job_id").Unique(),
		field.String("seed_hash").Unique(),
		field.String("front_order_id").Optional().Nillable(),
		field.String("demand_id"),
		field.String("state"),
		field.Int64("chunk_count"),
		field.Int64("completed_chunks"),
		field.Int64("paid_total_sat"),
		field.String("output_file_path"),
		field.String("part_file_path"),
		field.String("error"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcGetFileByHashJobs) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("front_order_id"),
		index.Fields("state"),
		index.Fields("updated_at_unix"),
	}
}
