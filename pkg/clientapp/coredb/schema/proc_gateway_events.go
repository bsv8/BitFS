package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcGatewayEvents 对齐表 proc_gateway_events。
type ProcGatewayEvents struct {
	ent.Schema
}

func (ProcGatewayEvents) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_gateway_events"}}
}

func (ProcGatewayEvents) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("created_at_unix"),
		field.String("gateway_pubkey_hex"),
		field.String("command_id"),
		field.String("action"),
		field.String("msg_id"),
		field.Int64("sequence_num"),
		field.String("pool_id"),
		field.Int64("amount_satoshi"),
		field.String("payload_json"),
	}
}

func (ProcGatewayEvents) Edges() []ent.Edge {
	return nil
}

func (ProcGatewayEvents) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("command_id").StorageKey("idx_proc_gateway_events_cmd_id"),
		index.Fields("created_at_unix"),
	}
}
