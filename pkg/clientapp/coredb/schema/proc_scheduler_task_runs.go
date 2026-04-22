package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcSchedulerTaskRuns 对齐表 proc_scheduler_task_runs。
type ProcSchedulerTaskRuns struct {
	ent.Schema
}

func (ProcSchedulerTaskRuns) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_scheduler_task_runs"}}
}

func (ProcSchedulerTaskRuns) Fields() []ent.Field {
	return []ent.Field{
		field.String("task_name"),
		field.String("owner"),
		field.String("mode"),
		field.String("trigger"),
		field.Int64("started_at_unix"),
		field.Int64("ended_at_unix"),
		field.Int64("duration_ms"),
		field.String("status"),
		field.String("error_message"),
		field.String("summary_json"),
		field.Int64("created_at_unix"),
	}
}

func (ProcSchedulerTaskRuns) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("started_at_unix", "id"),
		index.Fields("status", "id"),
		index.Fields("task_name", "id"),
	}
}
