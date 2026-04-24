package sqliteactor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

type Opened struct {
	DB    *sql.DB
	Actor *Actor
}

// Actor 是单 writer goroutine 排队写的执行器。
// 设计约束：
// - 写请求只能走 WriteTx，写连接只有一条
// - 读请求走 Read，使用独立只读连接
// - 所有查询句柄禁止带出闭包
type Actor struct {
	db        *sql.DB
	roDB      *sql.DB
	writeCh   chan writeRequest
	closedCh  chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	inWrite int32
}

type writeRequest struct {
	ctx context.Context
	fn  func(*sql.Tx) error
	out chan error
}

func Open(path string, debug bool) (*Opened, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	dsn := appendPragma(path, "journal_mode(WAL)")
	dsn = appendQueryValue(dsn, "_fk", "1")
	dsn = appendQueryValue(dsn, "_txlock", "immediate")
	registerTraceDriverOnce.Do(registerTraceDriver)
	driverName := traceDriverName

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign_keys pragma: %w", err)
	}

	readOnlyDSN := appendQueryValue("file:"+path, "mode", "ro")
	readOnlyDSN = appendQueryValue(readOnlyDSN, "_fk", "1")
	readOnlyDSN = appendPragma(readOnlyDSN, "query_only(ON)")
	readOnlyDB, err := sql.Open(driverName, readOnlyDSN)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	readOnlyDB.SetMaxOpenConns(4)
	readOnlyDB.SetMaxIdleConns(4)
	if err := readOnlyDB.Ping(); err != nil {
		_ = db.Close()
		_ = readOnlyDB.Close()
		return nil, err
	}
	if _, err := readOnlyDB.Exec("PRAGMA foreign_keys=ON"); err != nil {
		_ = db.Close()
		_ = readOnlyDB.Close()
		return nil, fmt.Errorf("enable foreign_keys pragma on ro db: %w", err)
	}

	actor, err := New(db, readOnlyDB)
	if err != nil {
		_ = db.Close()
		_ = readOnlyDB.Close()
		return nil, err
	}
	return &Opened{
		DB:    db,
		Actor: actor,
	}, nil
}

func New(db *sql.DB, readOnlyDB *sql.DB) (*Actor, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite db is nil")
	}
	if readOnlyDB == nil {
		return nil, fmt.Errorf("sqlite read only db is nil")
	}
	a := &Actor{
		db:       db,
		roDB:     readOnlyDB,
		writeCh:  make(chan writeRequest, 16),
		closedCh: make(chan struct{}),
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.writeLoop()
	}()
	return a, nil
}

// Read 执行只读查询，查询句柄必须在闭包内关闭。
func (a *Actor) Read(ctx context.Context, fn func(*sql.DB) error) error {
	if fn == nil {
		return fmt.Errorf("sqlite actor read func is nil")
	}
	if a == nil {
		return fmt.Errorf("sqlite actor is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	roDB, err := a.getReadOnlyDB()
	if err != nil {
		return err
	}
	return fn(roDB)
}

func (a *Actor) getReadOnlyDB() (*sql.DB, error) {
	if a == nil {
		return nil, fmt.Errorf("sqlite actor is nil")
	}
	if a.roDB == nil {
		return nil, fmt.Errorf("sqlite read only db is nil")
	}
	select {
	case <-a.closedCh:
		return nil, fmt.Errorf("sqlite actor is closed")
	default:
	}
	return a.roDB, nil
}

// WriteTx 把写请求投入写队列，由唯一 writer goroutine 顺序执行。
// 当前事务未结束时，下一个写请求只能排队。
func (a *Actor) WriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if fn == nil {
		return fmt.Errorf("sqlite actor write tx func is nil")
	}
	if a == nil {
		return fmt.Errorf("sqlite actor is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closedCh:
		return fmt.Errorf("sqlite actor is closed")
	default:
	}
	req := writeRequest{
		ctx: ctx,
		fn:  fn,
		out: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closedCh:
		return fmt.Errorf("sqlite actor is closed")
	case a.writeCh <- req:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closedCh:
		return fmt.Errorf("sqlite actor is closed")
	case err := <-req.out:
		return err
	}
}

func (a *Actor) Close() error {
	if a == nil {
		return nil
	}
	var err error
	a.closeOnce.Do(func() {
		close(a.closedCh)
		a.wg.Wait()
		if a.db != nil {
			err = a.db.Close()
		}
		if a.roDB != nil {
			_ = a.roDB.Close()
		}
	})
	return err
}

func DoValue[T any](ctx context.Context, a *Actor, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if a == nil {
		return zero, fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("sqlite actor do func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	var value T
	err := a.Read(ctx, func(db *sql.DB) error {
		var runErr error
		value, runErr = fn(db)
		return runErr
	})
	return value, err
}

func TxValue[T any](ctx context.Context, a *Actor, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	if a == nil {
		return zero, fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("sqlite actor tx func is nil")
	}
	if ctx == nil {
		return zero, fmt.Errorf("ctx is required")
	}
	var value T
	var runErr error
	err := a.WriteTx(ctx, func(tx *sql.Tx) error {
		value, runErr = fn(tx)
		return runErr
	})
	if err != nil {
		return zero, err
	}
	return value, nil
}

func (a *Actor) writeLoop() {
	for {
		select {
		case <-a.closedCh:
			return
		case req, ok := <-a.writeCh:
			if !ok {
				return
			}
			a.handleWrite(req)
		}
	}
}

func (a *Actor) handleWrite(req writeRequest) {
	if req.ctx != nil && req.ctx.Err() != nil {
		req.out <- req.ctx.Err()
		return
	}
	if !atomic.CompareAndSwapInt32(&a.inWrite, 0, 1) {
		req.out <- fmt.Errorf("write transaction reentry detected: nested write transaction is not allowed")
		return
	}
	defer atomic.StoreInt32(&a.inWrite, 0)

	tx, err := a.db.BeginTx(req.ctx, nil)
	if err != nil {
		req.out <- err
		return
	}
	defer func() { _ = tx.Rollback() }()

	if err := req.fn(tx); err != nil {
		req.out <- err
		return
	}
	req.out <- tx.Commit()
}

func appendPragma(dsn string, pragma string) string {
	return appendQueryValue(dsn, "_pragma", pragma)
}

func appendQueryValue(dsn string, key string, value string) string {
	dsn = strings.TrimSpace(dsn)
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if dsn == "" || key == "" || value == "" {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + key + "=" + value
}
