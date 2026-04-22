package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactBsvUtxos 对齐表 fact_bsv_utxos。
type FactBsvUtxos struct {
	ent.Schema
}

func (FactBsvUtxos) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{
		Table: "fact_bsv_utxos",
		Check: "utxo_state IN ('unspent','spent') AND carrier_type IN ('plain_bsv','token_carrier','fee_change','unknown')",
	}}
}

func (FactBsvUtxos) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("utxo_id").Unique().Immutable(),
		field.String("owner_pubkey_hex"),
		field.String("address"),
		field.String("txid"),
		field.Int64("vout"),
		field.Int64("value_satoshi"),
		field.String("utxo_state"),
		field.String("carrier_type"),
		field.String("spent_by_txid").Default(""),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("spent_at_unix").Default(0),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactBsvUtxos) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("txid", "vout").Unique(),
		index.Fields("created_at_unix"),
		index.Fields("owner_pubkey_hex", "carrier_type", "utxo_state", "updated_at_unix"),
		index.Fields("owner_pubkey_hex", "utxo_state", "updated_at_unix"),
		index.Fields("spent_by_txid", "spent_at_unix"),
	}
}
