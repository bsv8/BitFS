package clientapp

import (
	"context"
	"database/sql"
)

// moduleBootstrapStore 是可选模块接线层共用的最小 store 能力。
// 设计说明：
// - 主干只认能力，不认具体实现；
// - 接线层在这里拿到统一的 SQL/串行能力后，再分别喂给各模块 bootstrap；
// - 这样 inbox 和 indexresolve 不会再各写一套 store 约束。
type moduleBootstrapStore interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	Do(context.Context, func(SQLConn) error) error
	SerialAccess() bool
}
