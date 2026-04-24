package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Pool2of2V1Sessions 对齐表 pool_2of2_v1_sessions。
// 记录 pool_2of2_v1 模块的双边池会话。
type Pool2of2V1Sessions struct {
	ent.Schema
}

func (Pool2of2V1Sessions) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "pool_2of2_v1_sessions"}}
}

func (Pool2of2V1Sessions) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("session_ref").Unique(),
		field.String("counterparty_hex"),
		field.String("spend_txid"),
		field.Int("client_amount_satoshi"),
		field.Int("server_amount_satoshi"),
		field.Int("sequence_num"),
		field.String("state"), // active, rotating, closing, closed
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.Int64("closed_at_unix"),
	}
}

func (Pool2of2V1Sessions) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("counterparty_hex", "state"),
		index.Fields("state", "updated_at_unix"),
	}
}
