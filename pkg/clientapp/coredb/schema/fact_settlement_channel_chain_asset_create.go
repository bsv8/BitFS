package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactSettlementChannelChainAssetCreate 对齐表 fact_settlement_channel_chain_asset_create。
type FactSettlementChannelChainAssetCreate struct {
	ent.Schema
}

func (FactSettlementChannelChainAssetCreate) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_settlement_channel_chain_asset_create"}}
}

func (FactSettlementChannelChainAssetCreate) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("settlement_payment_attempt_id").Unique(),
		field.String("txid"),
		field.String("payment_subtype"),
		field.String("status"),
		field.Int64("wallet_input_satoshi"),
		field.Int64("wallet_output_satoshi"),
		field.Int64("net_amount_satoshi"),
		field.Int64("block_height"),
		field.Int64("occurred_at_unix"),
		field.Int64("submitted_at_unix").Default(0),
		field.Int64("wallet_observed_at_unix").Default(0),
		field.String("from_party_id"),
		field.String("to_party_id"),
		field.String("payload_json"),
		field.Int64("updated_at_unix"),
	}
}

func (FactSettlementChannelChainAssetCreate) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("settlement_payment_attempt", FactSettlementPaymentAttempts.Type).
			Field("settlement_payment_attempt_id").
			Required().Unique(),
	}
}

func (FactSettlementChannelChainAssetCreate) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("txid").Unique(),
		index.Fields("occurred_at_unix", "id"),
		index.Fields("status", "occurred_at_unix"),
		index.Fields("payment_subtype", "occurred_at_unix"),
	}
}
