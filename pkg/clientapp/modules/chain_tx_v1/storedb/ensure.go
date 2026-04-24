package storedb

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/chain_tx_v1/storedb/gen"
)

// EnsureChainTxV1Schema 只做 ent Schema.Create，不打开 DB，不处理 PRAGMA。
// DB 参数由运行入口统一提供，已经过初始化处理。
func EnsureChainTxV1Schema(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
	return client.Schema.Create(ctx)
}