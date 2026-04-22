package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// openResolveCallTestDB 提供给测试复用的一套真实 sqlite 库。
//
// 设计说明：
// - 这里只做测试准备，不碰业务逻辑；
// - 让需要建库的测试统一走同一段流程，避免每个文件自己复制；
// - 这个 helper 不属于模块本身，所以不能绑 build tag。
func openResolveCallTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}

// ensureClientSchemaOnDB 给测试统一做一次核心库和内置模块的建表。
// 设计说明：
// - 先建 core，再建 builtin module，顺序和运行入口保持一致；
// - 这里只服务测试入口，不承担业务路径上的 schema 初始化职责。
func ensureClientSchemaOnDB(ctx context.Context, target any) error {
	switch v := target.(type) {
	case *sql.DB:
		if err := ensureCoreSchema(ctx, v); err != nil {
			return err
		}
		return ensureBuiltinModuleSchemas(ctx, v)
	case *clientDB:
		if v == nil || v.db == nil {
			return fmt.Errorf("db is nil")
		}
		if err := ensureCoreSchema(ctx, v.db); err != nil {
			return err
		}
		return ensureBuiltinModuleSchemas(ctx, v.db)
	default:
		return fmt.Errorf("unsupported schema target")
	}
}
