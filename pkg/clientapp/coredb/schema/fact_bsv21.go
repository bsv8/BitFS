package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// FactBsv21 对齐表 fact_bsv21。
type FactBsv21 struct {
	ent.Schema
}

func (FactBsv21) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_bsv21"}}
}

func (FactBsv21) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("token_id").Unique().Immutable(),
		field.String("create_txid"),
		field.String("wallet_id"),
		field.String("address"),
		field.String("token_standard"),
		field.String("symbol"),
		field.String("max_supply"),
		field.Int64("decimals"),
		field.String("icon"),
		field.Int64("created_at_unix"),
		field.Int64("submitted_at_unix"),
		field.Int64("updated_at_unix"),
		field.String("payload_json").Default("{}"),
	}
}
