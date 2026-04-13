package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

// ensureClientDBSchema 是客户端数据库结构就绪的唯一入口。
// 设计说明：
// - 这里只负责调度分层，不承载具体迁移细节；
// - 具体 schema、迁移、约束、归一化都在下层文件里继续拆分。
func ensureClientDBSchema(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		return ensureClientDBSchemaOnDB(ctx, db)
	})
}

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)。
// - 初始化先建基础表，再做当前口径的约束和数据归一化。
func ensureClientDBSchemaOnDB(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 1. 基础表和索引（新库直接建）
	if err := ensureClientDBBaseSchemaCtx(ctx, db); err != nil {
		return fmt.Errorf("base schema: %w", err)
	}
	if err := ensureWalletUTXOSchema(ctx, db); err != nil {
		return fmt.Errorf("wallet utxo schema: %w", err)
	}
	if err := normalizeClientDBData(ctx, db); err != nil {
		return fmt.Errorf("normalize client db data: %w", err)
	}
	if err := ensureFinAccountingIndexes(ctx, db); err != nil {
		return fmt.Errorf("finance accounting indexes: %w", err)
	}

	return nil
}

// initIndexDB 是数据库初始化的测试入口。
// 设计说明：
// - 新代码应直接使用 ensureClientDBSchema 或 ensureClientDBSchemaOnDB；
// - 这里只是给现有测试保留的薄包装。
func initIndexDBCtx(ctx context.Context, db *sql.DB) error {
	return ensureClientDBSchemaOnDB(ctx, db)
}
