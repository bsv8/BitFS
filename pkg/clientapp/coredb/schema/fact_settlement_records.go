package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactSettlementRecords 对齐表 fact_settlement_records。
type FactSettlementRecords struct {
	ent.Schema
}

func (FactSettlementRecords) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{
		Table: "fact_settlement_records",
		Check: "asset_type IN ('BSV','TOKEN') AND state IN ('pending','confirmed','reverted')",
	}}
}

func (FactSettlementRecords) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("record_id").Unique().Immutable(),
		field.Int64("settlement_payment_attempt_id"),
		field.String("asset_type"),
		field.String("owner_pubkey_hex"),
		field.String("source_utxo_id").Default(""),
		field.String("source_lot_id").Default(""),
		field.Int64("used_satoshi").Default(0),
		field.String("used_quantity_text").Default(""),
		field.String("state"),
		field.Int64("occurred_at_unix"),
		field.Int64("confirmed_at_unix").Default(0),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactSettlementRecords) Edges() []ent.Edge {
	return []ent.Edge{}
}

func (FactSettlementRecords) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("settlement_payment_attempt_id", "asset_type", "source_utxo_id", "source_lot_id").Unique(),
		index.Fields("settlement_payment_attempt_id", "asset_type", "occurred_at_unix"),
		index.Fields("owner_pubkey_hex", "state", "occurred_at_unix"),
		index.Fields("source_lot_id", "occurred_at_unix"),
		index.Fields("source_utxo_id", "occurred_at_unix"),
		index.Fields("state", "occurred_at_unix"),
	}
}
