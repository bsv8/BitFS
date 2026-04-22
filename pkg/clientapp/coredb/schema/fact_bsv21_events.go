package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// FactBsv21Events 对齐表 fact_bsv21_events。
type FactBsv21Events struct {
	ent.Schema
}

func (FactBsv21Events) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "fact_bsv21_events"}}
}

func (FactBsv21Events) Fields() []ent.Field {
	return []ent.Field{
		field.String("token_id"),
		field.String("event_kind"),
		field.Int64("event_at_unix"),
		field.String("txid").Default(""),
		field.String("note").Default(""),
		field.String("payload_json").Default("{}"),
	}
}

func (FactBsv21Events) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("event_kind", "event_at_unix"),
		index.Fields("token_id", "id"),
	}
}
