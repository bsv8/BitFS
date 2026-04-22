package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// ProcDirectTransferPools 对齐表 proc_direct_transfer_pools。
type ProcDirectTransferPools struct {
	ent.Schema
}

func (ProcDirectTransferPools) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_direct_transfer_pools"}}
}

func (ProcDirectTransferPools) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_id").Unique().Immutable(),
		field.String("deal_id"),
		field.String("buyer_pubkey_hex"),
		field.String("seller_pubkey_hex"),
		field.String("arbiter_pubkey_hex"),
		field.Int64("pool_amount"),
		field.Int64("spend_tx_fee"),
		field.Int64("sequence_num"),
		field.Int64("seller_amount"),
		field.Int64("buyer_amount"),
		field.String("current_tx_hex"),
		field.String("base_tx_hex"),
		field.String("base_txid"),
		field.String("status"),
		field.Float("fee_rate_sat_byte"),
		field.Int64("lock_blocks"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}
