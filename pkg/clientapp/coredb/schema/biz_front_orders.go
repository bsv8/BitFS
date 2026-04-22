package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizFrontOrders 对齐表 biz_front_orders。
type BizFrontOrders struct {
	ent.Schema
}

func (BizFrontOrders) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_front_orders"}}
}

func (BizFrontOrders) Fields() []ent.Field {
	return []ent.Field{
		field.String("front_order_id").Unique().Immutable(),
		field.String("front_type"),
		field.String("front_subtype"),
		field.String("owner_pubkey_hex"),
		field.String("target_object_type"),
		field.String("target_object_id"),
		field.String("status"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (BizFrontOrders) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("owner_pubkey_hex", "created_at_unix"),
		index.Fields("target_object_type", "target_object_id"),
		index.Fields("front_type", "status", "updated_at_unix"),
	}
}
