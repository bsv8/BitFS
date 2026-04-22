package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// BizPricingAutopilotState 对齐表 biz_pricing_autopilot_state。
// 设计说明：
// - 这是按 seed_hash 维度保存的运行态快照；
// - 由 contract 统一建表，避免业务仓里再手写 DDL。
type BizPricingAutopilotState struct {
	ent.Schema
}

func (BizPricingAutopilotState) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_pricing_autopilot_state"}}
}

func (BizPricingAutopilotState) Fields() []ent.Field {
	return []ent.Field{
		field.String("seed_hash").Unique().Immutable(),
		field.String("payload_json"),
		field.Int64("updated_at_unix"),
	}
}
