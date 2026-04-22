package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcCommandJournal 对齐表 proc_command_journal。
type ProcCommandJournal struct {
	ent.Schema
}

func (ProcCommandJournal) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table: "proc_command_journal",
			Check: "trim(command_id) <> ''",
		},
	}
}

func (ProcCommandJournal) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.Int64("created_at_unix"),
		field.String("command_id").Unique().Immutable(),
		field.String("command_type"),
		field.String("gateway_pubkey_hex"),
		field.String("aggregate_id"),
		field.String("requested_by"),
		field.Int64("requested_at_unix"),
		field.Int64("accepted"),
		field.String("status"),
		field.String("error_code"),
		field.String("error_message"),
		field.String("state_before"),
		field.String("state_after"),
		field.Int64("duration_ms"),
		field.String("trigger_key").Default(""),
		field.String("payload_json"),
		field.String("result_json"),
	}
}

func (ProcCommandJournal) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("created_at_unix"),
		index.Fields("gateway_pubkey_hex", "id"),
		index.Fields("trigger_key", "id").StorageKey("idx_proc_command_journal_trigger_key"),
	}
}
