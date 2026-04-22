package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactSettlementChannelPoolSessionQuotePay 对齐表 fact_settlement_channel_pool_session_quote_pay。
type FactSettlementChannelPoolSessionQuotePay struct {
	ent.Schema
}

func (FactSettlementChannelPoolSessionQuotePay) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_settlement_channel_pool_session_quote_pay"}}
}

func (FactSettlementChannelPoolSessionQuotePay) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("settlement_payment_attempt_id").Unique(),
		field.String("pool_session_id").Unique(),
		field.String("txid").Default(""),
		field.String("pool_scheme"),
		field.String("counterparty_pubkey_hex").Default(""),
		field.String("seller_pubkey_hex").Default(""),
		field.String("arbiter_pubkey_hex").Default(""),
		field.String("gateway_pubkey_hex").Default(""),
		field.Int64("pool_amount_satoshi"),
		field.Int64("spend_tx_fee_satoshi"),
		field.Float("fee_rate_sat_byte").Default(0),
		field.Int64("lock_blocks").Default(0),
		field.String("open_base_txid").Default(""),
		field.String("status"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (FactSettlementChannelPoolSessionQuotePay) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("settlement_payment_attempt", FactSettlementPaymentAttempts.Type).
			Field("settlement_payment_attempt_id").
			Required().Unique(),
	}
}

func (FactSettlementChannelPoolSessionQuotePay) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("counterparty_pubkey_hex", "status"),
		index.Fields("pool_scheme", "status", "updated_at_unix"),
		index.Fields("txid"),
	}
}
