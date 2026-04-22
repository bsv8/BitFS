package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizPricingAutopilotAudit 对齐表 biz_pricing_autopilot_audit。
// 设计说明：
// - 只追加审计，不做更新；
// - 按 seed_hash + ticked_at_unix 做检索。
type BizPricingAutopilotAudit struct {
	ent.Schema
}

func (BizPricingAutopilotAudit) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_pricing_autopilot_audit"}}
}

func (BizPricingAutopilotAudit) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id"),
		field.String("seed_hash"),
		field.String("payload_json"),
		field.Int64("ticked_at_unix"),
	}
}

func (BizPricingAutopilotAudit) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("seed_hash", "ticked_at_unix", "id").StorageKey("idx_biz_pricing_autopilot_audit_seed_time"),
	}
}
