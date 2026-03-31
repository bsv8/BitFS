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
	if d.actor != nil {
		return d.actor.Do(ctx, fn)
	}
	if d.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	return fn(d.db)
}

func clientDBValue[T any](ctx context.Context, d *clientDB, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if d == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("client db value func is nil")
	}
	if d.actor != nil {
		return sqliteactor.DoValue(ctx, d.actor, fn)
	}
	if d.db == nil {
		return zero, fmt.Errorf("client db raw db is nil")
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
	if d.actor != nil {
		return d.actor.Tx(ctx, fn)
	}
	if d.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	if ctx == nil {
		ctx = context.Background()
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
	if d.actor != nil {
		return sqliteactor.TxValue(ctx, d.actor, fn)
	}
	if d.db == nil {
		return zero, fmt.Errorf("client db raw db is nil")
	}
	if ctx == nil {
		ctx = context.Background()
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
