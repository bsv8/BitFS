package clientapp

import (
	"database/sql"
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
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}
