package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizDemandQuoteArbiters 对齐表 biz_demand_quote_arbiters。
type BizDemandQuoteArbiters struct {
	ent.Schema
}

func (BizDemandQuoteArbiters) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_demand_quote_arbiters"}}
}

func (BizDemandQuoteArbiters) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("quote_id"),
		field.String("arbiter_pub_hex"),
	}
}

func (BizDemandQuoteArbiters) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("quote_id", "arbiter_pub_hex").Unique(),
		index.Fields("arbiter_pub_hex", "quote_id"),
	}
}
