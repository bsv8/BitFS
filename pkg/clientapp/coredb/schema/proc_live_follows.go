package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

// ProcLiveFollows 对齐表 proc_live_follows。
type ProcLiveFollows struct {
	ent.Schema
}

func (ProcLiveFollows) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_live_follows"}}
}

func (ProcLiveFollows) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("stream_id").Unique().Immutable(),
		field.String("stream_uri"),
		field.String("publisher_pubkey"),
		field.Int64("have_segment_index"),
		field.Int64("last_bought_segment_index"),
		field.String("last_bought_seed_hash"),
		field.String("last_output_file_path"),
		field.String("last_quote_seller_pubkey_hex"),
		field.String("last_decision_json"),
		field.String("status"),
		field.String("last_error"),
		field.Int64("updated_at_unix"),
	}
}
