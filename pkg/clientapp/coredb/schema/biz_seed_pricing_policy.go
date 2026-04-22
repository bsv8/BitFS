package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizSeedPricingPolicy 对齐表 biz_seed_pricing_policy。
type BizSeedPricingPolicy struct {
	ent.Schema
}

func (BizSeedPricingPolicy) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_seed_pricing_policy"}}
}

func (BizSeedPricingPolicy) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("seed_hash").Unique().Immutable(),
		field.Int64("floor_unit_price_sat_per_64k"),
		field.Int64("resale_discount_bps"),
		field.String("pricing_source"),
		field.Int64("updated_at_unix"),
	}
}

func (BizSeedPricingPolicy) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("updated_at_unix"),
	}
}
