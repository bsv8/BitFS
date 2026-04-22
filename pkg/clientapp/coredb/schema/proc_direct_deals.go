package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// ProcDirectDeals 对齐表 proc_direct_deals。
type ProcDirectDeals struct {
	ent.Schema
}

func (ProcDirectDeals) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_direct_deals"}}
}

func (ProcDirectDeals) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("deal_id").Unique().Immutable(),
		field.String("demand_id"),
		field.String("buyer_pubkey_hex"),
		field.String("seller_pubkey_hex"),
		field.String("seed_hash"),
		field.Int64("seed_price"),
		field.Int64("chunk_price"),
		field.String("arbiter_pubkey_hex"),
		field.String("status"),
		field.Int64("created_at_unix"),
	}
}
