package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Pool2of3V1Sessions 对齐表 pool_2of3_v1_sessions。
// 记录 pool_2of3_v1 模块的三边池会话。
type Pool2of3V1Sessions struct {
	ent.Schema
}

func (Pool2of3V1Sessions) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pool_2of3_v1_sessions"}}
}

func (Pool2of3V1Sessions) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_ref").Unique(),
		field.String("trade_id"),
		field.String("buyer_hex"),
		field.String("seller_hex"),
		field.String("arbiter_hex"),
		field.Int("total_amount_satoshi"),
		field.Int("paid_amount_satoshi"),
		field.Int("refunded_amount_satoshi"),
		field.Int("chunk_count"),
		field.String("state"), // open, in_progress, arbitrating, completed, aborted
		field.String("current_txid"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("closed_at_unix"),
	}
}

func (Pool2of3V1Sessions) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("trade_id").Unique(),
		index.Fields("buyer_hex", "state"),
		index.Fields("seller_hex", "state"),
		index.Fields("state", "updated_at_unix"),
	}
}
