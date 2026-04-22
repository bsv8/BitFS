package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactPoolSessionEvents 对齐表 fact_pool_session_events。
type FactPoolSessionEvents struct {
	ent.Schema
}

func (FactPoolSessionEvents) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_pool_session_events"}}
}

func (FactPoolSessionEvents) Fields() []ent.Field {
	return []ent.Field{
		field.String("allocation_id"),
		field.String("pool_session_id").Default(""),
		field.Int64("allocation_no").Default(0),
		field.String("allocation_kind").Default(""),
		field.String("event_kind").Default("pool_event"),
		field.Int64("sequence_num").Default(0),
		field.String("state").Default("confirmed"),
		field.String("direction").Default(""),
		field.Int64("amount_satoshi").Default(0),
		field.String("purpose").Default(""),
		field.String("note").Default(""),
		field.String("msg_id").Default(""),
		field.Int64("cycle_index").Default(0),
		field.Int64("payee_amount_after").Default(0),
		field.Int64("payer_amount_after").Default(0),
		field.String("txid").Default(""),
		field.String("tx_hex").Default(""),
		field.String("gateway_pubkey_hex").Default(""),
		field.Int64("created_at_unix"),
		field.String("payload_json").Default("{}"),
	}
}

func (FactPoolSessionEvents) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("allocation_id").Unique(),
		index.Fields("created_at_unix", "id"),
		index.Fields("created_at_unix"),
		index.Fields("pool_session_id", "event_kind", "sequence_num"),
		index.Fields("pool_session_id", "allocation_no"),
		index.Fields("txid"),
	}
}
