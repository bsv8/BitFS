package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizWorkspaces 对齐表 biz_workspaces。
type BizWorkspaces struct {
	ent.Schema
}

func (BizWorkspaces) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_workspaces"}}
}

func (BizWorkspaces) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("workspace_path").Unique().Immutable(),
		field.Int64("enabled"),
		field.Int64("max_bytes"),
		field.Int64("created_at_unix"),
	}
}

func (BizWorkspaces) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("enabled", "workspace_path"),
	}
}
