package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// BizWorkspaceFiles 对齐表 biz_workspace_files。
type BizWorkspaceFiles struct {
	ent.Schema
}

func (BizWorkspaceFiles) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "biz_workspace_files"}}
}

func (BizWorkspaceFiles) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("workspace_path"),
		field.String("file_path"),
		field.String("seed_hash"),
		field.Int64("seed_locked").Default(0),
	}
}

func (BizWorkspaceFiles) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("workspace_path", "file_path").Unique(),
		index.Fields("seed_hash", "workspace_path", "file_path"),
	}
}
