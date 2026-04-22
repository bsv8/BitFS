package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// ProcChainTipState 对齐表 proc_chain_tip_state。
type ProcChainTipState struct {
	ent.Schema
}

func (ProcChainTipState) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_chain_tip_state"}}
}

func (ProcChainTipState) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("tip_height"),
		field.Int64("updated_at_unix"),
		field.String("last_error"),
		field.String("last_updated_by"),
		field.String("last_trigger"),
		field.Int64("last_duration_ms"),
	}
}
