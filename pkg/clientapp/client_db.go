package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
)

// clientDB 是客户端唯一的 db 总入口。
// 设计说明：
// - 对外只保留这一个总模块，不再鼓励业务代码自己持有 sql 细节；
// - actor 存在时优先走 actor，把运行期访问压回单 owner 串行模型；
// - 迁移期仍保留底层 *sql.DB，仅给 db 模块内部闭包使用。
type clientDB struct {
	db    *sql.DB
	actor *sqliteactor.Actor
}

func newClientDB(db *sql.DB, actor *sqliteactor.Actor) *clientDB {
	if db == nil && actor == nil {
		return nil
	}
	return &clientDB{
		db:    db,
		actor: actor,
	}
}

func (d *clientDB) Do(ctx context.Context, fn func(*sql.DB) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("client db do func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
		if !sqlTraceScopeActive(scope) {
			return d.actor.Do(ctx, fn)
		}
		return sqliteactor.DoTrace(ctx, d.actor, scope, fn)
	}
	if d.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
	if sqlTraceScopeActive(scope) {
		var runErr error
		sqliteactor.WithTraceScope(scope, func() {
			runErr = fn(d.db)
		})
		return runErr
	}
	return fn(d.db)
}

// ExecContext / QueryContext / QueryRowContext 统一给运行时代码做 SQL 收口。
// 设计说明：
// - 新代码尽量不要直接碰 *sql.DB / *sql.Tx 的方法；
// - 这里保留同名能力，方便把业务代码从裸调用搬到统一 helper；
// - Query/QueryRow 这类会泄露生命周期的结果，调用方仍然必须在同一闭包里完成 Scan/Close。
func (d *clientDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if d == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		return clientDBValue(ctx, d, func(db *sql.DB) (sql.Result, error) {
			return dbExecContext(ctx, db, query, args...)
		})
	}
	if d.db == nil {
		return nil, fmt.Errorf("client db raw db is nil")
	}
	return dbExecContext(ctx, d.db, query, args...)
}

func (d *clientDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if d == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		return clientDBValue(ctx, d, func(db *sql.DB) (*sql.Rows, error) {
			return dbQueryContext(ctx, db, query, args...)
		})
	}
	if d.db == nil {
		return nil, fmt.Errorf("client db raw db is nil")
	}
	return dbQueryContext(ctx, d.db, query, args...)
}

func (d *clientDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if d == nil {
		return nil
	}
	if ctx == nil {
		return nil
	}
	if d.actor != nil {
		row, err := clientDBValue(ctx, d, func(db *sql.DB) (*sql.Row, error) {
			return dbQueryRowContext(ctx, db, query, args...), nil
		})
		if err != nil {
			return nil
		}
		return row
	}
	if d.db == nil {
		return nil
	}
	return dbQueryRowContext(ctx, d.db, query, args...)
}

func (d *clientDB) InTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("client db tx func is nil")
	}
	return d.Tx(ctx, fn)
}

func clientDBValue[T any](ctx context.Context, d *clientDB, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if d == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("client db value func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
		if !sqlTraceScopeActive(scope) {
			return sqliteactor.DoValue(ctx, d.actor, fn)
		}
		return sqliteactor.DoValueTrace(ctx, d.actor, scope, fn)
	}
	if d.db == nil {
		return zero, fmt.Errorf("client db raw db is nil")
	}
	scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
	if sqlTraceScopeActive(scope) {
		var out T
		var runErr error
		sqliteactor.WithTraceScope(scope, func() {
			out, runErr = fn(d.db)
		})
		return out, runErr
	}
	return fn(d.db)
}

func (d *clientDB) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("client db tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
		if !sqlTraceScopeActive(scope) {
			return d.actor.Tx(ctx, fn)
		}
		return sqliteactor.TxTrace(ctx, d.actor, scope, fn)
	}
	if d.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func clientDBTxValue[T any](ctx context.Context, d *clientDB, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	if d == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("client db tx value func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
		if !sqlTraceScopeActive(scope) {
			return sqliteactor.TxValue(ctx, d.actor, fn)
		}
		return sqliteactor.TxValueTrace(ctx, d.actor, scope, fn)
	}
	if d.db == nil {
		return zero, fmt.Errorf("client db raw db is nil")
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, err
	}
	defer func() { _ = tx.Rollback() }()
	value, err := fn(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	return value, nil
}
