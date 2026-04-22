package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// WalletUtxoTokenVerification 对齐表 wallet_utxo_token_verification。
type WalletUtxoTokenVerification struct {
	ent.Schema
}

func (WalletUtxoTokenVerification) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wallet_utxo_token_verification"}}
}

func (WalletUtxoTokenVerification) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("utxo_id").Unique().Immutable(),
		field.String("wallet_id"),
		field.String("address"),
		field.String("txid"),
		field.Int64("vout"),
		field.Int64("value_satoshi"),
		field.String("status").Default("pending"),
		field.String("woc_response_json").Default("{}"),
		field.Int64("last_check_at_unix").Default(0),
		field.Int64("next_retry_at_unix").Default(0),
		field.Int64("retry_count").Default(0),
		field.String("error_message").Default(""),
		field.Int64("updated_at_unix"),
	}
}

func (WalletUtxoTokenVerification) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("status", "next_retry_at_unix"),
		index.Fields("wallet_id", "status"),
	}
}
