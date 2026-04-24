package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ChainTxV1Payments 对齐表 chain_tx_v1_payments。
// 记录 chain_tx_v1 模块的支付请求。
type ChainTxV1Payments struct {
	ent.Schema
}

func (ChainTxV1Payments) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "chain_tx_v1_payments"}}
}

func (ChainTxV1Payments) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("payment_ref_id").Unique(),
		field.String("target_peer_hex"),
		field.Int("amount_satoshi"),
		field.String("state"), // quoted, submitted, confirmed, failed
		field.String("txid"),
		field.String("tx_hex"),
		field.Int("miner_fee_satoshi"),
		field.String("route"),
		field.String("quote_hash"),
		field.String("receipt_scheme"),
		field.String("receipt_hash"),
		field.Int64("confirmed_at_unix"),
		field.String("error_message"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (ChainTxV1Payments) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("target_peer_hex", "created_at_unix"),
		index.Fields("state", "created_at_unix"),
	}
}
