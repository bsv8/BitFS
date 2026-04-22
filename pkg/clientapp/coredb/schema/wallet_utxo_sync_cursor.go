package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// WalletUtxoSyncCursor 对齐表 wallet_utxo_sync_cursor。
type WalletUtxoSyncCursor struct {
	ent.Schema
}

func (WalletUtxoSyncCursor) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wallet_utxo_sync_cursor"}}
}

func (WalletUtxoSyncCursor) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("address").Unique().Immutable(),
		field.String("wallet_id"),
		field.Int64("next_confirmed_height"),
		field.String("next_page_token"),
		field.Int64("anchor_height"),
		field.Int64("round_tip_height"),
		field.Int64("updated_at_unix"),
		field.String("last_error"),
	}
}

func (WalletUtxoSyncCursor) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("round_tip_height", "updated_at_unix"),
	}
}
