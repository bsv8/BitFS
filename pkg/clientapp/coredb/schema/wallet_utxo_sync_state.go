package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// WalletUtxoSyncState 对齐表 wallet_utxo_sync_state。
type WalletUtxoSyncState struct {
	ent.Schema
}

func (WalletUtxoSyncState) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wallet_utxo_sync_state"}}
}

func (WalletUtxoSyncState) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("address").Unique().Immutable(),
		field.String("wallet_id"),
		field.Int64("utxo_count"),
		field.Int64("balance_satoshi"),
		field.Int64("plain_bsv_utxo_count").Default(0),
		field.Int64("plain_bsv_balance_satoshi").Default(0),
		field.Int64("protected_utxo_count").Default(0),
		field.Int64("protected_balance_satoshi").Default(0),
		field.Int64("unknown_utxo_count").Default(0),
		field.Int64("unknown_balance_satoshi").Default(0),
		field.Int64("updated_at_unix"),
		field.String("last_error"),
		field.String("last_updated_by"),
		field.String("last_trigger"),
		field.Int64("last_duration_ms"),
		field.String("last_sync_round_id").Default(""),
		field.String("last_failed_step").Default(""),
		field.String("last_upstream_path").Default(""),
		field.Int64("last_http_status").Default(0),
	}
}
