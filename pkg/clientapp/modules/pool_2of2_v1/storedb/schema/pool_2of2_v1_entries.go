package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Pool2of2V1Entries 对齐表 pool_2of2_v1_entries。
// 记录 pool_2of2_v1 模块的会话扣费明细。
type Pool2of2V1Entries struct {
	ent.Schema
}

func (Pool2of2V1Entries) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pool_2of2_v1_entries"}}
}

func (Pool2of2V1Entries) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_ref"),
		field.String("charge_reason"), // periodic_fee, service_use, rotation, close
		field.Int("charge_amount_satoshi"),
		field.Int("balance_after_satoshi"),
		field.Int("sequence_num"),
		field.String("proof_txid"),
		field.Int64("proof_at_unix"),
		field.Int64("created_at_unix"),
	}
}

func (Pool2of2V1Entries) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("session_ref", "created_at_unix"),
		index.Fields("charge_reason", "created_at_unix"),
	}
}
