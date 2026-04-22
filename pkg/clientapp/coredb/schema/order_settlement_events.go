package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// OrderSettlementEvents 对齐表 order_settlement_events。
type OrderSettlementEvents struct {
	ent.Schema
}

func (OrderSettlementEvents) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "order_settlement_events"}}
}

func (OrderSettlementEvents) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id"),
		field.String("process_id"),
		field.String("settlement_id"),
		field.String("order_id"),
		field.String("source_type"),
		field.String("source_id"),
		field.String("accounting_scene"),
		field.String("accounting_subtype"),
		field.String("event_type"),
		field.String("status"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
		field.Int64("occurred_at_unix"),
	}
}

func (OrderSettlementEvents) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("settlement_id", "occurred_at_unix").StorageKey("idx_order_settlement_events_settlement"),
		index.Fields("event_type", "occurred_at_unix").StorageKey("idx_order_settlement_events_type"),
		index.Fields("settlement_id", "event_type", "order_id").Unique(),
	}
}
