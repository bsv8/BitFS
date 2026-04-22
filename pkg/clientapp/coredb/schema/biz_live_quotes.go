package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizLiveQuotes 对齐表 biz_live_quotes。
type BizLiveQuotes struct {
	ent.Schema
}

func (BizLiveQuotes) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_live_quotes"}}
}

func (BizLiveQuotes) Fields() []ent.Field {
	return []ent.Field{
		field.String("demand_id"),
		field.String("seller_pubkey_hex"),
		field.String("stream_id"),
		field.Int64("latest_segment_index"),
		field.String("recent_segments_json"),
		field.Int64("expires_at_unix"),
		field.Int64("created_at_unix"),
	}
}

func (BizLiveQuotes) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("demand_id", "seller_pubkey_hex").Unique(),
		index.Fields("demand_id", "created_at_unix"),
	}
}
