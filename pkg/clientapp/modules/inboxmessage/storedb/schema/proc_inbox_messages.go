package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcInboxMessages 对齐表 proc_inbox_messages。
type ProcInboxMessages struct {
	ent.Schema
}

func (ProcInboxMessages) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_inbox_messages"}}
}

func (ProcInboxMessages) Fields() []ent.Field {
	return []ent.Field{
		field.String("message_id"),
		field.String("sender_pubkey_hex"),
		field.String("target_input"),
		field.String("route"),
		field.String("content_type"),
		field.Bytes("body_bytes"),
		field.Int64("body_size_bytes"),
		field.Int64("received_at_unix"),
	}
}

func (ProcInboxMessages) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("sender_pubkey_hex", "message_id").Unique(),
		index.Fields("received_at_unix", "id"),
	}
}
