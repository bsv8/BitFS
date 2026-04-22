package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// BizPool 对齐表 biz_pool。
type BizPool struct {
	ent.Schema
}

func (BizPool) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_pool"}}
}

func (BizPool) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("pool_session_id").Unique().Immutable(),
		field.String("pool_scheme"),
		field.String("counterparty_pubkey_hex").Default(""),
		field.String("seller_pubkey_hex").Default(""),
		field.String("arbiter_pubkey_hex").Default(""),
		field.String("gateway_pubkey_hex").Default(""),
		field.Int64("pool_amount_satoshi").Default(0),
		field.Int64("spend_tx_fee_satoshi").Default(0),
		field.Int64("allocated_satoshi").Default(0),
		field.Int64("cycle_fee_satoshi").Default(0),
		field.Int64("available_satoshi").Default(0),
		field.Int64("next_sequence_num").Default(1),
		field.String("status"),
		field.String("open_base_txid").Default(""),
		field.String("open_allocation_id").Default(""),
		field.String("close_allocation_id").Default(""),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}
