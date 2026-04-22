package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizPoolAllocations 对齐表 biz_pool_allocations。
type BizPoolAllocations struct {
	ent.Schema
}

func (BizPoolAllocations) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_pool_allocations"}}
}

func (BizPoolAllocations) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("allocation_id").Unique().Immutable(),
		field.String("pool_session_id"),
		field.Int64("allocation_no"),
		field.String("allocation_kind"),
		field.Int64("sequence_num"),
		field.Int64("payee_amount_after").Default(0),
		field.Int64("payer_amount_after").Default(0),
		field.String("txid"),
		field.String("tx_hex"),
		field.Int64("created_at_unix"),
	}
}

func (BizPoolAllocations) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("pool_session_id", "allocation_kind", "sequence_num").Unique(),
	}
}
