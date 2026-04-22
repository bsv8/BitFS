package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// BizPricingAutopilotConfig 对齐表 biz_pricing_autopilot_config。
// 设计说明：
// - 这是运行时定价配置快照，不单独做业务表分层；
// - 由 contract 统一建表，clientapp 只负责一次性初始化。
type BizPricingAutopilotConfig struct {
	ent.Schema
}

func (BizPricingAutopilotConfig) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_pricing_autopilot_config"}}
}

func (BizPricingAutopilotConfig) Fields() []ent.Field {
	return []ent.Field{
		field.String("config_key").Unique().Immutable(),
		field.String("payload_json"),
		field.Int64("updated_at_unix"),
	}
}
