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
func ensureClientDBSchema(store *clientDB) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(context.Background(), func(db *sql.DB) error {
		return ensureClientDBSchemaOnDB(db)
	})
}

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)。
func ensureClientDBSchemaOnDB(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 1. 基础表和索引（不依赖迁移前置条件）
	if err := ensureClientDBBaseSchema(db); err != nil {
		return fmt.Errorf("base schema: %w", err)
	}

	// 2. 兼容迁移（历史列补齐、老表迁移、老数据搬运）
	if err := migrateClientDBLegacySchema(db); err != nil {
		return fmt.Errorf("legacy migration: %w", err)
	}
	if err := ensureGatewayEventAndCommandJournalConstraints(db); err != nil {
		return fmt.Errorf("gateway_events command constraints: %w", err)
	}
	if err := ensureCommandLinkedTableConstraints(db); err != nil {
		return fmt.Errorf("command linked table constraints: %w", err)
	}

	// 3. 数据规范化（历史脏数据口径纠偏）
	if err := normalizeClientDBData(db); err != nil {
		return fmt.Errorf("data normalization: %w", err)
	}

	return nil
}

// initIndexDB 是数据库初始化的旧入口，保留给测试代码兼容使用。
// 设计说明：
// - 新代码应直接使用 ensureClientDBSchema 或 ensureClientDBSchemaOnDB；
// - 该函数仅作为测试兼容层，内部包装新的分层初始化逻辑。
func initIndexDB(db *sql.DB) error {
	return ensureClientDBSchemaOnDB(db)
}
