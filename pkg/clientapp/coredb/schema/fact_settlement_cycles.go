package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactSettlementCycles 对齐表 fact_settlement_cycles。
type FactSettlementCycles struct {
	ent.Schema
}

func (FactSettlementCycles) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_settlement_cycles"}}
}

func (FactSettlementCycles) Fields() []ent.Field {
	return []ent.Field{
		field.String("cycle_id").Unique(),
		field.String("source_type"),
		field.String("source_id"),
		field.String("state").Default("confirmed"),
		field.Int64("gross_amount_satoshi").Default(0),
		field.Int64("gate_fee_satoshi").Default(0),
		field.Int64("net_amount_satoshi").Default(0),
		field.Int64("cycle_index").Default(0),
		field.Int64("occurred_at_unix"),
		field.Int64("confirmed_at_unix").Default(0),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactSettlementCycles) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("source_type", "source_id").Unique(),
		index.Fields("source_type", "state", "occurred_at_unix"),
	}
}
