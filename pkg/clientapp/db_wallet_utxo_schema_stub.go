package clientapp

import (
	"context"
	"database/sql"
)

// ensureWalletUTXOSchema 当前版本不再单独维护 wallet UTXO 迁移入口。
// 设计说明：
// - 保留这个薄桩，只是为了让旧的初始化调度先能跑通；
// - 真正的 wallet 相关表由各自模块的建表逻辑负责，不在这里重复做一次。
func ensureWalletUTXOSchema(ctx context.Context, db *sql.DB) error {
	_ = ctx
	_ = db
	return nil
}
