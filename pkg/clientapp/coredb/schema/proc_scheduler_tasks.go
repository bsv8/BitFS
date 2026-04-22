package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcSchedulerTasks 对齐表 proc_scheduler_tasks。
type ProcSchedulerTasks struct {
	ent.Schema
}

func (ProcSchedulerTasks) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_scheduler_tasks"}}
}

func (ProcSchedulerTasks) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("task_name").Unique().Immutable(),
		field.String("owner"),
		field.String("mode"),
		field.String("status"),
		field.Int64("interval_seconds"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("closed_at_unix"),
		field.String("last_trigger"),
		field.Int64("last_started_at_unix"),
		field.Int64("last_ended_at_unix"),
		field.Int64("last_duration_ms"),
		field.String("last_error"),
		field.Int64("in_flight"),
		field.Int64("run_count"),
		field.Int64("success_count"),
		field.Int64("failure_count"),
		field.String("last_summary_json"),
		field.String("meta_json"),
	}
}

func (ProcSchedulerTasks) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("owner", "mode", "task_name"),
		index.Fields("status", "updated_at_unix", "task_name"),
	}
}
