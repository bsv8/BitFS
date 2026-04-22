package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcStateSnapshots 对齐表 proc_state_snapshots。
type ProcStateSnapshots struct {
	ent.Schema
}

func (ProcStateSnapshots) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table: "proc_state_snapshots",
			Check: "trim(command_id) <> ''",
		},
	}
}

func (ProcStateSnapshots) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("created_at_unix"),
		field.String("command_id"),
		field.String("gateway_pubkey_hex"),
		field.String("state"),
		field.String("pause_reason"),
		field.Int64("pause_need_satoshi"),
		field.Int64("pause_have_satoshi"),
		field.String("last_error"),
		field.String("payload_json"),
	}
}

func (ProcStateSnapshots) Edges() []ent.Edge {
	return nil
}

func (ProcStateSnapshots) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("command_id", "id").StorageKey("idx_proc_state_snapshots_cmd_id"),
		index.Fields("created_at_unix"),
		index.Fields("gateway_pubkey_hex", "id"),
	}
}
