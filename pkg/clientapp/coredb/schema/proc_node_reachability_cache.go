package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcNodeReachabilityCache 对齐表 proc_node_reachability_cache。
type ProcNodeReachabilityCache struct {
	ent.Schema
}

func (ProcNodeReachabilityCache) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_node_reachability_cache"}}
}

func (ProcNodeReachabilityCache) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("target_node_pubkey_hex").Unique().Immutable(),
		field.String("source_gateway_pubkey_hex"),
		field.Int64("head_height"),
		field.Int64("seq"),
		field.String("multiaddrs_json"),
		field.Int64("published_at_unix"),
		field.Int64("expires_at_unix"),
		field.Bytes("signature"),
		field.Int64("updated_at_unix"),
	}
}

func (ProcNodeReachabilityCache) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("expires_at_unix", "updated_at_unix"),
	}
}
