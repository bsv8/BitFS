package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcOrchestratorLogs 对齐表 proc_orchestrator_logs。
type ProcOrchestratorLogs struct {
	ent.Schema
}

func (ProcOrchestratorLogs) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_orchestrator_logs"}}
}

func (ProcOrchestratorLogs) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("created_at_unix"),
		field.String("event_type"),
		field.String("source"),
		field.String("signal_type"),
		field.String("aggregate_key"),
		field.String("command_type"),
		field.String("gateway_pubkey_hex"),
		field.String("task_status"),
		field.Int64("retry_count"),
		field.Int64("queue_length"),
		field.String("error_message"),
		field.String("payload_json"),
	}
}

func (ProcOrchestratorLogs) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("created_at_unix"),
		index.Fields("event_type", "id"),
		index.Fields("gateway_pubkey_hex", "id"),
		index.Fields("signal_type", "id"),
	}
}
