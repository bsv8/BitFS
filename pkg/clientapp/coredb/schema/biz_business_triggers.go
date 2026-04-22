package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizBusinessTriggers 对齐表 biz_business_triggers。
type BizBusinessTriggers struct {
	ent.Schema
}

func (BizBusinessTriggers) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_business_triggers"}}
}

func (BizBusinessTriggers) Fields() []ent.Field {
	return []ent.Field{
		field.String("trigger_id").Unique().Immutable(),
		field.String("business_id"),
		field.String("trigger_type"),
		field.String("trigger_id_value"),
		field.String("trigger_role"),
		field.Int64("created_at_unix"),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (BizBusinessTriggers) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("business_id", "trigger_type", "trigger_id_value", "trigger_role").Unique(),
		index.Fields("business_id", "created_at_unix"),
		index.Fields("trigger_type", "trigger_id_value"),
		index.Fields("trigger_type", "trigger_id_value", "trigger_role"),
	}
}
