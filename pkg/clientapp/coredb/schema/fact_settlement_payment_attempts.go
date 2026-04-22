package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactSettlementPaymentAttempts 对齐表 fact_settlement_payment_attempts。
type FactSettlementPaymentAttempts struct {
	ent.Schema
}

func (FactSettlementPaymentAttempts) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{
			Table: "fact_settlement_payment_attempts",
			Checks: map[string]string{
				"valid_source_type": "source_type IN ('pool_session_quote_pay', 'chain_quote_pay', 'chain_direct_pay', 'chain_asset_create')",
				"valid_state":       "state IN ('pending', 'confirmed', 'failed')",
			},
		},
	}
}

func (FactSettlementPaymentAttempts) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id"),
		field.String("payment_attempt_id").Unique().Immutable(),
		field.String("source_type"),
		field.String("source_id"),
		field.String("state"),
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

func (FactSettlementPaymentAttempts) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("source_type", "source_id").Unique(),
		index.Fields("source_type", "state", "occurred_at_unix"),
	}
}
