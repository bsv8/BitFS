package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Pool2of3V1Entries 对齐表 pool_2of3_v1_entries。
// 记录 pool_2of3_v1 模块的 chunk 支付明细。
type Pool2of3V1Entries struct {
	ent.Schema
}

func (Pool2of3V1Entries) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pool_2of3_v1_entries"}}
}

func (Pool2of3V1Entries) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_ref"),
		field.Int("chunk_index"),
		field.Int("amount_satoshi"),
		field.String("state"), // pending, paid, arbitrating, awarded, refunded
		field.String("payment_txid"),
		field.Int64("paid_at_unix"),
		field.String("arbitration_case_id"),
		field.Int64("arbitrated_at_unix"),
		field.String("award_txid"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (Pool2of3V1Entries) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("session_ref", "chunk_index").Unique(),
		index.Fields("session_ref", "state"),
	}
}
