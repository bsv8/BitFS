package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// ProcSelfNodeReachabilityState 对齐表 proc_self_node_reachability_state。
type ProcSelfNodeReachabilityState struct {
	ent.Schema
}

func (ProcSelfNodeReachabilityState) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_self_node_reachability_state"}}
}

func (ProcSelfNodeReachabilityState) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("node_pubkey_hex").Unique().Immutable(),
		field.Int64("head_height"),
		field.Int64("seq"),
		field.Int64("updated_at_unix"),
	}
}
