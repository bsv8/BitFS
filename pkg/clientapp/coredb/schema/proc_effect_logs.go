package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcEffectLogs 对齐表 proc_effect_logs。
type ProcEffectLogs struct {
	ent.Schema
}

func (ProcEffectLogs) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table: "proc_effect_logs",
			Check: "trim(command_id) <> ''",
		},
	}
}

func (ProcEffectLogs) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("created_at_unix"),
		field.String("command_id"),
		field.String("gateway_pubkey_hex"),
		field.String("effect_type"),
		field.String("stage"),
		field.String("status"),
		field.String("error_message"),
		field.String("payload_json"),
	}
}

func (ProcEffectLogs) Edges() []ent.Edge {
	return nil
}

func (ProcEffectLogs) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("command_id", "id").StorageKey("idx_proc_effect_logs_cmd_id"),
		index.Fields("created_at_unix"),
		index.Fields("gateway_pubkey_hex", "id"),
	}
}
