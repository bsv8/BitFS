package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
)

// clientDB 是 BitFS 客户端 store 的实现。
// 设计约束：
// - 业务主路只走 ReadEnt / WriteEntTx；
// - raw SQL 只留给少数基础设施桥接；
// - 不把 *sql.DB、*sql.Tx、*gen.Client 直接交给业务闭包。
type clientDB struct {
	db     *sql.DB
	actor  *sqliteactor.Actor
	closed atomic.Bool
	mu     sync.Mutex
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

func newEntClient(db *sql.DB) *gen.Client {
	if db == nil {
		return nil
	}
	return gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
}

// NewClientStore 保留给测试和入口组装用。
func NewClientStore(db *sql.DB, actor *sqliteactor.Actor) *clientDB {
	return newClientDB(db, actor)
}

// EnsureClientStoreSchema 只在运行入口做一次 schema 准备。
// 设计说明：
// - 这里直接接已准备好的 clientDB，不再额外开一层业务封装；
// - schema 创建由 contract ent 真源负责，业务层不碰手写 DDL；
// - 只保留这一条入口，避免初始化路径分叉。
func EnsureClientStoreSchema(ctx context.Context, store *clientDB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	if store.db == nil {
		return fmt.Errorf("client db raw db is nil")
	}
	return ensureClientDBSchemaOnDB(ctx, store.db)
}

func ensureClientDBSchemaOnDB(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := applySQLitePragmas(db); err != nil {
		return err
	}
	client := gen.NewClient(gen.Driver(entsql.OpenDB(dialect.SQLite, db)))
	if err := client.Schema.Create(ctx); err != nil {
		return err
	}
	return nil
}

// Read 执行只读查询，查询句柄禁止带出闭包。
func (d *clientDB) Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("read func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.closed.Load() {
		return fmt.Errorf("client db is closed")
	}
	return d.readSQLDB(ctx, func(db *sql.DB) error {
		readConn := &readOnlyConn{db: db}
		return fn(readConn)
	})
}

// WriteTx 把写请求投入写队列，由唯一 writer goroutine 顺序执行。
// 当前事务未结束时，下一个写请求只能排队。
func (d *clientDB) WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.closed.Load() {
		return fmt.Errorf("client db is closed")
	}
	return d.writeModuleTx(ctx, fn)
}

// ReadEnt 只暴露查询壳，不把事务入口交给业务。
func (d *clientDB) ReadEnt(ctx context.Context, fn func(EntReadRoot) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("read ent func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.closed.Load() {
		return fmt.Errorf("client db is closed")
	}
	return d.readSQLDB(ctx, func(db *sql.DB) error {
		return fn(newEntReadRoot(newEntClient(db)))
	})
}

// Do 直接透传 *sql.DB 回调，仅用于 managedDaemon 这类外部消费者的基础设施桥接。
func (d *clientDB) Do(ctx context.Context, fn func(SQLConn) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("do func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	return d.readSQLDB(ctx, func(db *sql.DB) error {
		return fn(db)
	})
}

// WriteEntTx 先进入 writer 队列，再把当前事务包成只暴露实体 client 的壳。
func (d *clientDB) WriteEntTx(ctx context.Context, fn func(EntWriteRoot) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write ent tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.closed.Load() {
		return fmt.Errorf("client db is closed")
	}
	return d.writeEntTx(ctx, func(root EntWriteRoot) error {
		// 这里把可写实体壳交给业务，外层事务仍由 writer goroutine 统一提交回滚。
		return fn(root)
	})
}

func writeEntValue[T any](ctx context.Context, store ClientStore, fn func(EntWriteRoot) (T, error)) (T, error) {
	var zero T
	if store == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("write ent value func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	var value T
	err := store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		next, err := fn(root)
		if err != nil {
			return err
		}
		value = next
		return nil
	})
	if err != nil {
		return zero, err
	}
	return value, nil
}

func readEntValue[T any](ctx context.Context, store ClientStore, fn func(EntReadRoot) (T, error)) (T, error) {
	var zero T
	if store == nil {
		return zero, fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("read ent value func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	var value T
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		next, err := fn(root)
		if err != nil {
			return err
		}
		value = next
		return nil
	})
	if err != nil {
		return zero, err
	}
	return value, nil
}

func (d *clientDB) readSQLDB(ctx context.Context, fn func(*sql.DB) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("read db func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		return d.actor.Read(ctx, fn)
	}
	if d.db != nil {
		return fn(d.db)
	}
	return fmt.Errorf("client db has no read connection")
}

func (d *clientDB) writeModuleTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		return d.actor.WriteTx(ctx, func(tx *sql.Tx) error {
			wtx := &writeTxConn{tx: tx}
			return fn(wtx)
		})
	}
	if d.db == nil {
		return fmt.Errorf("client db has no write connection")
	}
	return d.dbWriteModuleTx(ctx, fn)
}

func (d *clientDB) dbWriteModuleTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	type writeResult struct {
		err error
	}
	done := make(chan writeResult, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			done <- writeResult{err: err}
			return
		}
		defer func() { _ = tx.Rollback() }()
		wtx := &writeTxConn{tx: tx}
		if err := fn(wtx); err != nil {
			done <- writeResult{err: err}
			return
		}
		done <- writeResult{err: tx.Commit()}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-done:
		return r.err
	}
}

func traceDirectSQL(ctx context.Context, op string, query string, args []any, startedAt time.Time, rows int64, err error) {
	mgr := currentSQLTraceManager()
	if mgr == nil {
		return
	}
	roundID := ""
	trigger := ""
	intent := ""
	stage := ""
	if ctx != nil {
		roundID = strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey)))
		trigger = strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey)))
		intent = strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextIntentKey)))
		stage = strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextStageKey)))
	}
	ev := sqliteactor.TraceEvent{
		TS:             time.Now().UTC().Format(time.RFC3339Nano),
		Operation:      op,
		SQL:            query,
		SQLFingerprint: fingerprintSQL(query),
		Args:           append([]any(nil), args...),
		ElapsedMS:      time.Since(startedAt).Milliseconds(),
		RowsAffected:   rows,
		Err:            traceErrString(err),
	}
	if roundID != "" || trigger != "" || intent != "" || stage != "" {
		ev.RoundID = roundID
		ev.Trigger = trigger
		ev.Intent = intent
		ev.Stage = stage
		ev.CallerChain = sqlTraceCaptureCallerChain()
	}
	mgr.Handle(ev)
}

func fingerprintSQL(sqlText string) string {
	norm := strings.TrimSpace(strings.ToLower(strings.Join(strings.Fields(sqlText), " ")))
	h := fnv.New64a()
	_, _ = h.Write([]byte(norm))
	return fmt.Sprintf("%016x", h.Sum64())
}

func traceErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// readOnlyConn 是 Read 闭包内使用的只读连接能力。
type readOnlyConn struct {
	db *sql.DB
}

func (c *readOnlyConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if c.db == nil {
		return nil, fmt.Errorf("read only conn has no db")
	}
	startedAt := time.Now()
	rows, err := c.db.QueryContext(ctx, query, args...)
	traceDirectSQL(ctx, "query", query, args, startedAt, -1, err)
	return rows, err
}

func (c *readOnlyConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if c.db == nil {
		return nil
	}
	traceDirectSQL(ctx, "query", query, args, time.Now(), -1, nil)
	return c.db.QueryRowContext(ctx, query, args...)
}

// writeTxConn 是 WriteTx 闭包内使用的写事务连接能力。
type writeTxConn struct {
	tx *sql.Tx
}

func (c *writeTxConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if c.tx == nil {
		return nil, fmt.Errorf("write tx conn has no tx")
	}
	startedAt := time.Now()
	rows, err := c.tx.QueryContext(ctx, query, args...)
	traceDirectSQL(ctx, "query", query, args, startedAt, -1, err)
	return rows, err
}

func (c *writeTxConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if c.tx == nil {
		return nil
	}
	traceDirectSQL(ctx, "query", query, args, time.Now(), -1, nil)
	return c.tx.QueryRowContext(ctx, query, args...)
}

func (c *writeTxConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if c.tx == nil {
		return nil, fmt.Errorf("write tx conn has no tx")
	}
	startedAt := time.Now()
	result, err := c.tx.ExecContext(ctx, query, args...)
	rows := int64(-1)
	if result != nil {
		if affected, rowsErr := result.RowsAffected(); rowsErr == nil {
			rows = affected
		}
	}
	traceDirectSQL(ctx, "exec", query, args, startedAt, rows, err)
	return result, err
}

func (d *clientDB) writeSQLTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write raw tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if d.actor != nil {
		return d.actor.WriteTx(ctx, fn)
	}
	if d.db == nil {
		return fmt.Errorf("client db has no write connection")
	}
	return d.dbWriteTx(ctx, fn)
}

func (d *clientDB) dbWriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	type writeResult struct {
		err error
	}
	done := make(chan writeResult, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			done <- writeResult{err: err}
			return
		}
		defer func() { _ = tx.Rollback() }()
		if err := fn(tx); err != nil {
			done <- writeResult{err: err}
			return
		}
		done <- writeResult{err: tx.Commit()}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-done:
		return r.err
	}
}

func (d *clientDB) writeEntTx(ctx context.Context, fn func(EntWriteRoot) error) error {
	if d == nil {
		return fmt.Errorf("client db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write ent tx func is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	return d.writeSQLTx(ctx, func(tx *sql.Tx) error {
		return fn(newEntWriteRoot(tx))
	})
}

// boundEntTxDriver 把现有 *sql.Tx 包成 ent 事务壳，禁止业务再自己开一层事务。
type boundEntTxDriver struct {
	base *entsql.Driver
}

func (d *boundEntTxDriver) Exec(ctx context.Context, query string, args, v any) error {
	return d.base.Exec(ctx, query, args, v)
}

func (d *boundEntTxDriver) Query(ctx context.Context, query string, args, v any) error {
	return d.base.Query(ctx, query, args, v)
}

func (d *boundEntTxDriver) Tx(context.Context) (dialect.Tx, error) {
	return d, nil
}

func (d *boundEntTxDriver) Close() error {
	return nil
}

func (d *boundEntTxDriver) Dialect() string {
	return d.base.Dialect()
}

func (d *boundEntTxDriver) Commit() error {
	return nil
}

func (d *boundEntTxDriver) Rollback() error {
	return nil
}

// Close 关闭所有连接。
func (d *clientDB) Close() error {
	if d == nil {
		return nil
	}
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d.actor != nil {
		return d.actor.Close()
	}
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}
