package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// OrderSettlements 对齐表 order_settlements。
// 约束约定：
// - id 由 ent 默认主键提供，作为内部行键；
// - settlement_id 是业务唯一键，不再作为物理主键。
type OrderSettlements struct {
	ent.Schema
}

func (OrderSettlements) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "order_settlements"}}
}

func (OrderSettlements) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("settlement_id").Unique().Immutable(),
		field.String("order_id"),
		field.Int64("settlement_no"),
		field.String("business_role").Default(""),
		field.String("source_type").Default(""),
		field.String("source_id").Default(""),
		field.String("accounting_scene").Default(""),
		field.String("accounting_subtype").Default(""),
		field.String("settlement_method"),
		field.String("status"),
		field.String("settlement_status").Default(""),
		field.Int64("amount_satoshi").Default(0),
		field.String("from_party_id"),
		field.String("to_party_id"),
		field.String("target_type"),
		field.String("target_id"),
		field.String("note").Default(""),
		field.String("error_message").Default(""),
		field.String("payload_json").Default("{}"),
		field.String("settlement_payload_json").Default("{}"),
		field.Int64("created_at_unix"),
		field.Int64("updated_at_unix"),
	}
}

func (OrderSettlements) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("order_id", "settlement_no").Unique(),
		index.Fields("order_id", "created_at_unix").StorageKey("idx_order_settlements_order"),
		index.Fields("status", "updated_at_unix").StorageKey("idx_order_settlements_status"),
		index.Fields("settlement_method", "status", "updated_at_unix").StorageKey("idx_order_settlements_method"),
		index.Fields("target_type", "target_id").StorageKey("idx_order_settlements_target"),
	}
}
