package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// WalletUtxo 对齐表 wallet_utxo。
type WalletUtxo struct {
	ent.Schema
}

func (WalletUtxo) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wallet_utxo"}}
}

func (WalletUtxo) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("utxo_id").Unique().Immutable(),
		field.String("wallet_id"),
		field.String("address"),
		field.String("txid"),
		field.Int64("vout"),
		field.Int64("value_satoshi"),
		field.String("state"),
		field.String("script_type").Default("unknown"),
		field.String("script_type_reason").Default(""),
		field.Int64("script_type_updated_at_unix").Default(0),
		field.String("allocation_class").Default("plain_bsv"),
		field.String("allocation_reason").Default(""),
		field.String("created_txid"),
		field.String("spent_txid"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("spent_at_unix"),
	}
}

func (WalletUtxo) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("wallet_id", "state", "value_satoshi", "txid", "vout"),
		index.Fields("txid", "vout"),
		index.Fields("address", "txid", "vout").Unique(),
	}
}
