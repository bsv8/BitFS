package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizPurchases 对齐表 biz_purchases。
type BizPurchases struct {
	ent.Schema
}

func (BizPurchases) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_purchases"}}
}

func (BizPurchases) Fields() []ent.Field {
	return []ent.Field{
		field.String("demand_id"),
		field.String("seller_pub_hex"),
		field.String("arbiter_pub_hex"),
		field.Int64("chunk_index"),
		field.String("object_hash"),
		field.Int64("amount_satoshi"),
		field.String("status"),
		field.String("error_message"),
		field.Int64("created_at_unix"),
		field.Int64("finished_at_unix"),
	}
}

func (BizPurchases) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("created_at_unix", "id").StorageKey("idx_biz_purchases_created_at"),
		index.Fields("demand_id", "created_at_unix", "id").StorageKey("idx_biz_purchases_demand_created"),
		index.Fields("demand_id", "chunk_index", "seller_pub_hex", "arbiter_pub_hex", "created_at_unix", "id").StorageKey("idx_biz_purchases_history_lookup"),
		index.Fields("seller_pub_hex", "created_at_unix", "id").StorageKey("idx_biz_purchases_seller_created"),
		index.Fields("status", "created_at_unix", "id").StorageKey("idx_biz_purchases_status_created"),
	}
}
