package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactTokenCarrierLinks 对齐表 fact_token_carrier_links。
type FactTokenCarrierLinks struct {
	ent.Schema
}

func (FactTokenCarrierLinks) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{
		Table: "fact_token_carrier_links",
		Check: "link_state IN ('active','released','moved')",
	}}
}

func (FactTokenCarrierLinks) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("link_id").Unique().Immutable(),
		field.String("lot_id"),
		field.String("carrier_utxo_id"),
		field.String("owner_pubkey_hex"),
		field.String("link_state"),
		field.String("bind_txid").Default(""),
		field.String("unbind_txid").Default(""),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactTokenCarrierLinks) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("lot_id", "link_state").Unique(),
		index.Fields("carrier_utxo_id", "link_state").Unique(),
		index.Fields("bind_txid"),
		index.Fields("carrier_utxo_id", "link_state", "updated_at_unix"),
		index.Fields("lot_id", "link_state", "updated_at_unix"),
		index.Fields("owner_pubkey_hex", "link_state", "updated_at_unix"),
		index.Fields("unbind_txid"),
	}
}
