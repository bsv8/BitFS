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

type sqlQueryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlTxStarter interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func dbExecContext(ctx context.Context, q sqlQueryer, query string, args ...any) (sql.Result, error) {
	if q == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return q.ExecContext(ctx, query, args...)
}

func dbQueryContext(ctx context.Context, q sqlQueryer, query string, args ...any) (*sql.Rows, error) {
	if q == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return q.QueryContext(ctx, query, args...)
}

func dbQueryRowContext(ctx context.Context, q sqlQueryer, query string, args ...any) *sql.Row {
	if q == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return q.QueryRowContext(ctx, query, args...)
}

func dbInTx(ctx context.Context, db sqlTxStarter, fn func(*sql.Tx) error) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if fn == nil {
		return fmt.Errorf("tx func is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
