package clientapp

import (
	"context"
	"database/sql"
	"fmt"
)

// dbexec.go 统一收口业务运行期的 SQL 能力。
// 设计说明：
// - 这里不承载业务语义，只负责把 Exec / Query / QueryRow / Tx 的入口压成一套；
// - *sql.DB 和 *sql.Tx 都走同一个 helper，避免上层分叉；
// - 所有参数都交给 sqliteactor 侧做类型收口，业务层不再自己处理 driver 细节。

type sqlQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type sqlQueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlTxStarter interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// ExecContext / QueryContext / QueryRowContext 统一收口运行层 SQL 能力。
// 设计说明：
// - 这里不补默认 ctx，也不吞掉调用错误；
// - 所有调用方都必须显式传入可取消 ctx；
// - QueryRowContext 保持和 database/sql 一样的返回形态，调用方仍然自己 Scan。
func ExecContext(ctx context.Context, execer sqlExecer, query string, args ...any) (sql.Result, error) {
	if execer == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	return execer.ExecContext(ctx, query, args...)
}

func QueryContext(ctx context.Context, queryer sqlQuerier, query string, args ...any) (*sql.Rows, error) {
	if queryer == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	return queryer.QueryContext(ctx, query, args...)
}

func QueryRowContext(ctx context.Context, queryer sqlQueryRower, query string, args ...any) *sql.Row {
	if queryer == nil || ctx == nil {
		return nil
	}
	return queryer.QueryRowContext(ctx, query, args...)
}

func dbExecContext(ctx context.Context, q sqlExecer, query string, args ...any) (sql.Result, error) {
	if q == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	return q.ExecContext(ctx, query, args...)
}

func dbQueryContext(ctx context.Context, q sqlQuerier, query string, args ...any) (*sql.Rows, error) {
	if q == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	return q.QueryContext(ctx, query, args...)
}

func dbQueryRowContext(ctx context.Context, q sqlQueryRower, query string, args ...any) *sql.Row {
	if q == nil {
		return nil
	}
	if ctx == nil {
		return nil
	}
	return q.QueryRowContext(ctx, query, args...)
}
