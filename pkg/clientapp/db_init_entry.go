package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/migrate"
)

// ensureClientDBSchema 是客户端数据库结构就绪的唯一入口。
// 设计说明：
// - 这里只负责调度分层，不承载手写 DDL；
// - 统一交给 contract 的 ent schema 产物做建表和建约束。
func ensureClientDBSchema(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(ctx, func(db sqlConn) error {
		rawDB, ok := db.(*sql.DB)
		if !ok {
			return fmt.Errorf("db must be *sql.DB")
		}
		return ensureClientDBSchemaOnDB(ctx, rawDB)
	})
}

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)；
// - 这里不再执行老的手写建表和补丁逻辑，只接受 contract 的 schema 真源。
func ensureClientDBSchemaOnDB(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	schema := migrate.NewSchema(entsql.OpenDB(dialect.SQLite, db))
	if err := schema.Create(ctx); err != nil {
		return fmt.Errorf("create ent schema: %w", err)
	}
	return nil
}
