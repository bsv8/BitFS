package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// WalletLocalBroadcastTxs 对齐表 wallet_local_broadcast_txs。
type WalletLocalBroadcastTxs struct {
	ent.Schema
}

func (WalletLocalBroadcastTxs) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wallet_local_broadcast_txs"}}
}

func (WalletLocalBroadcastTxs) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("txid").Unique().Immutable(),
		field.String("wallet_id"),
		field.String("address"),
		field.String("tx_hex"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("observed_at_unix").Default(0),
	}
}

func (WalletLocalBroadcastTxs) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("wallet_id", "observed_at_unix", "created_at_unix").StorageKey("idx_wallet_local_broadcast_txs_wallet_observed"),
	}
}
