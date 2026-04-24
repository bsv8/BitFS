package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ChainTxV1Facts 对齐表 chain_tx_v1_facts。
// 记录 chain_tx_v1 模块的链上支付事实，作为全局支付凭证。
type ChainTxV1Facts struct {
	ent.Schema
}

func (ChainTxV1Facts) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "chain_tx_v1_facts"}}
}

func (ChainTxV1Facts) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("source_type"), // payment, refund, dispute_resolve
		field.String("source_id"),
		field.Int("gross_amount_satoshi"),
		field.Int("fee_amount_satoshi"),
		field.Int("net_amount_satoshi"),
		field.String("txid"),
		field.Int64("confirmed_at_unix"),
		field.Int64("occurred_at_unix"),
	}
}

func (ChainTxV1Facts) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("source_type", "source_id").Unique(),
		index.Fields("txid"),
		index.Fields("occurred_at_unix"),
	}
}
