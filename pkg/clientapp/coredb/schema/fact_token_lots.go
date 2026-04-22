package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactTokenLots 对齐表 fact_token_lots。
type FactTokenLots struct {
	ent.Schema
}

func (FactTokenLots) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{
		Table: "fact_token_lots",
		Check: "token_standard IN ('BSV20','BSV21') AND lot_state IN ('unspent','spent','locked')",
	}}
}

func (FactTokenLots) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("lot_id").Unique().Immutable(),
		field.String("owner_pubkey_hex"),
		field.String("token_id"),
		field.String("token_standard"),
		field.String("quantity_text"),
		field.String("used_quantity_text").Default("0"),
		field.String("lot_state"),
		field.String("mint_txid").Default(""),
		field.String("last_spend_txid").Default(""),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactTokenLots) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("last_spend_txid"),
		index.Fields("mint_txid"),
		index.Fields("owner_pubkey_hex", "lot_state", "updated_at_unix"),
		index.Fields("owner_pubkey_hex", "token_standard", "token_id", "lot_state", "updated_at_unix"),
		index.Fields("token_standard", "token_id", "updated_at_unix"),
	}
}
