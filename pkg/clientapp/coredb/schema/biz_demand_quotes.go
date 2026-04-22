package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizDemandQuotes 对齐表 biz_demand_quotes。
type BizDemandQuotes struct {
	ent.Schema
}

func (BizDemandQuotes) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_demand_quotes"}}
}

func (BizDemandQuotes) Fields() []ent.Field {
	return []ent.Field{
		field.String("demand_id"),
		field.String("seller_pub_hex"),
		field.Int64("seed_price_satoshi"),
		field.Int64("chunk_price_satoshi"),
		field.Int64("chunk_count"),
		field.Int64("file_size_bytes"),
		field.String("recommended_file_name"),
		field.String("mime_type"),
		field.String("available_chunk_bitmap_hex"),
		field.Int64("expires_at_unix"),
		field.Int64("created_at_unix"),
	}
}

func (BizDemandQuotes) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("demand_id", "seller_pub_hex").Unique(),
		index.Fields("demand_id", "created_at_unix"),
	}
}
