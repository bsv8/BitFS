package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Orders 对齐表 orders。
// 约束约定：
// - id 由 ent 默认主键提供，作为内部行键；
// - order_id 是业务唯一键，不再作为物理主键。
type Orders struct {
	ent.Schema
}

func (Orders) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "orders"}}
}

func (Orders) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("order_id").Unique().Immutable(),
		field.String("order_type"),
		field.String("order_subtype"),
		field.String("owner_pubkey_hex"),
		field.String("target_object_type"),
		field.String("target_object_id"),
		field.String("status"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (Orders) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("order_type", "status").StorageKey("idx_orders_type_status"),
		index.Fields("target_object_type", "target_object_id").StorageKey("idx_orders_target"),
		index.Fields("owner_pubkey_hex", "created_at_unix").StorageKey("idx_orders_owner"),
		index.Fields("status", "updated_at_unix").StorageKey("idx_orders_status"),
	}
}
