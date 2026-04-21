package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
)

// clientDBEntTxValue 只在 store 层把 ent 事务包起来。
// 设计说明：
// - 业务层只接收“已准备好的能力”，不接触 *ent.Client 的构造过程；
// - 这里沿用 sqlite actor 的串行入口做观测与互斥，不把 raw tx 往外传；
// - 事务闭包里只做 ent 读写，避免把 SQL 细节继续散出去。
func clientDBEntTxValue[T any](ctx context.Context, d *clientDB, fn func(tx *gen.Tx) (T, error)) (T, error) {
	var zero T
	if d == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if d.ent == nil {
		return zero, fmt.Errorf("client db ent client is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("client db ent tx func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}

	run := func() (T, error) {
		tx, err := d.ent.Tx(ctx)
		if err != nil {
			return zero, err
		}
		defer func() { _ = tx.Rollback() }()
		out, err := fn(tx)
		if err != nil {
			return zero, err
		}
		if err := tx.Commit(); err != nil {
			return zero, err
		}
		return out, nil
	}

	scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
	if d.actor != nil {
		if sqlTraceScopeActive(scope) {
			return sqliteactor.DoValueTrace(ctx, d.actor, scope, func(*sql.DB) (T, error) {
				return run()
			})
		}
		return sqliteactor.DoValue(ctx, d.actor, func(*sql.DB) (T, error) {
			return run()
		})
	}
	if sqlTraceScopeActive(scope) {
		var out T
		var runErr error
		sqliteactor.WithTraceScope(scope, func() {
			out, runErr = run()
		})
		return out, runErr
	}
	return run()
}

func clientDBEntTx(ctx context.Context, d *clientDB, fn func(tx *gen.Tx) error) error {
	_, err := clientDBEntTxValue(ctx, d, func(tx *gen.Tx) (struct{}, error) {
		return struct{}{}, fn(tx)
	})
	return err
}

// clientDBEntQuery 只读查询的统一入口（不走事务，直接透传到 ent client）。
// 设计说明：
// - 读路径只查 ent client，不开事务，避免读一致性问题；
// - 有 actor 时走串行入口，无 actor 时直接查；
// - 只给 clientapp 内部 store adapter 的只读 ent 查询使用，写操作统一走 clientDBEntTx。
func clientDBEntQuery[T any](ctx context.Context, d *clientDB, fn func(*gen.Client) (T, error)) (T, error) {
	var zero T
	if d == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if d.ent == nil {
		return zero, fmt.Errorf("client db ent client is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("client db ent query func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}

	scope := sqlTraceScopeFromContext(ctx, sqlTraceCaptureCallerChain())
	if d.actor != nil {
		if sqlTraceScopeActive(scope) {
			return sqliteactor.DoValueTrace(ctx, d.actor, scope, func(*sql.DB) (T, error) {
				return fn(d.ent)
			})
		}
		return sqliteactor.DoValue(ctx, d.actor, func(*sql.DB) (T, error) {
			return fn(d.ent)
		})
	}
	if sqlTraceScopeActive(scope) {
		var out T
		var runErr error
		sqliteactor.WithTraceScope(scope, func() {
			out, runErr = fn(d.ent)
		})
		return out, runErr
	}
	return fn(d.ent)
}
