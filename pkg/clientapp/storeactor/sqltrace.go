package sqliteactor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqliteDriver "modernc.org/sqlite"
)

const traceDriverName = "sqlite_trace"

var registerTraceDriverOnce sync.Once

// TraceSink 接收 SQL 追踪事件。
type TraceSink interface {
	Handle(TraceEvent)
}

// TraceScope 是一次数据库调用的关联标签。
// 设计说明：
// - 这是纯运行时观测数据，不参与业务语义；
// - scope 由上层在进入 DB 能力入口前显式注入；
// - driver 层只负责把当前 goroutine 的 scope 贴到每条 SQL 上。
type TraceScope struct {
	RoundID     string
	Trigger     string
	Intent      string
	Stage       string
	CallerChain []string
}

// TraceEvent 是单条 SQL 的追踪记录。
type TraceEvent struct {
	TS             string   `json:"ts"`
	Operation      string   `json:"operation"`
	SQL            string   `json:"sql"`
	SQLFingerprint string   `json:"sql_fingerprint"`
	Args           []any    `json:"args,omitempty"`
	ElapsedMS      int64    `json:"elapsed_ms"`
	RowsAffected   int64    `json:"rows_affected"`
	Err            string   `json:"err,omitempty"`
	RoundID        string   `json:"round_id,omitempty"`
	Trigger        string   `json:"trigger,omitempty"`
	Intent         string   `json:"intent,omitempty"`
	Stage          string   `json:"stage,omitempty"`
	CallerChain    []string `json:"caller_chain,omitempty"`
}

type traceState struct {
	mu   sync.RWMutex
	sink TraceSink
}

var traceHub = &traceState{}

var traceScopes sync.Map

type traceScopeEntry struct {
	scope TraceScope
}

func DoTrace(ctx context.Context, a *Actor, scope TraceScope, fn func(*sql.DB) error) error {
	if a == nil {
		return fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return fmt.Errorf("sqlite actor do func is nil")
	}
	return a.Read(ctx, func(db *sql.DB) error {
		var runErr error
		WithTraceScope(scope, func() {
			runErr = fn(db)
		})
		return runErr
	})
}

func TxTrace(ctx context.Context, a *Actor, scope TraceScope, fn func(*sql.Tx) error) error {
	if a == nil {
		return fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return fmt.Errorf("sqlite actor tx func is nil")
	}
	return a.WriteTx(ctx, func(tx *sql.Tx) error {
		var runErr error
		WithTraceScope(scope, func() {
			runErr = fn(tx)
		})
		return runErr
	})
}

func DoValueTrace[T any](ctx context.Context, a *Actor, scope TraceScope, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if a == nil {
		return zero, fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("sqlite actor value func is nil")
	}
	var value T
	var runErr error
	err := a.Read(ctx, func(db *sql.DB) error {
		WithTraceScope(scope, func() {
			value, runErr = fn(db)
		})
		return runErr
	})
	if err != nil {
		return zero, err
	}
	return value, nil
}

func TxValueTrace[T any](ctx context.Context, a *Actor, scope TraceScope, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	if a == nil {
		return zero, fmt.Errorf("sqlite actor is nil")
	}
	if fn == nil {
		return zero, fmt.Errorf("sqlite actor tx value func is nil")
	}
	var value T
	var runErr error
	err := a.WriteTx(ctx, func(tx *sql.Tx) error {
		WithTraceScope(scope, func() {
			value, runErr = fn(tx)
		})
		return runErr
	})
	if err != nil {
		return zero, err
	}
	return value, nil
}

func SetTraceSink(sink TraceSink) {
	traceHub.mu.Lock()
	traceHub.sink = sink
	traceHub.mu.Unlock()
}

func traceSink() TraceSink {
	traceHub.mu.RLock()
	defer traceHub.mu.RUnlock()
	return traceHub.sink
}

func registerTraceDriver() {
	sql.Register(traceDriverName, traceDriver{base: &sqliteDriver.Driver{}})
}

type traceDriver struct {
	base driver.Driver
}

func (d traceDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &traceConn{base: conn}, nil
}

type traceConn struct {
	base driver.Conn
}

func (c *traceConn) Prepare(query string) (driver.Stmt, error) {
	return c.base.Prepare(query)
}

func (c *traceConn) Close() error {
	return c.base.Close()
}

func (c *traceConn) Begin() (driver.Tx, error) {
	if bt, ok := c.base.(driver.ConnBeginTx); ok {
		return bt.BeginTx(context.Background(), driver.TxOptions{})
	}
	return c.base.Begin()
}

func (c *traceConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.base.(driver.ConnBeginTx); ok {
		// 事务本身不单独作为 SQL 事件展示，避免把 BEGIN/COMMIT 噪声混进主视图。
		return bt.BeginTx(ctx, opts)
	}
	return c.base.Begin()
}

func (c *traceConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if pc, ok := c.base.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.base.Prepare(query)
}

func (c *traceConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	result, err := execOnConn(c.base, ctx, query, args)
	rows := int64(-1)
	if result != nil {
		if v, rowsErr := result.RowsAffected(); rowsErr == nil {
			rows = v
		}
	}
	emitTraceEvent(TraceEvent{
		TS:             nowRFC3339(),
		Operation:      "exec",
		SQL:            query,
		SQLFingerprint: fingerprintSQL(query),
		Args:           sanitizeTraceArgs(args),
		ElapsedMS:      time.Since(start).Milliseconds(),
		RowsAffected:   rows,
		Err:            traceErrString(err),
	}, currentTraceScope())
	return result, err
}

func (c *traceConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	rows, err := queryOnConn(c.base, ctx, query, args)
	scope := currentTraceScope()
	if err != nil {
		emitTraceEvent(TraceEvent{
			TS:             nowRFC3339(),
			Operation:      "query",
			SQL:            query,
			SQLFingerprint: fingerprintSQL(query),
			Args:           sanitizeTraceArgs(args),
			ElapsedMS:      time.Since(start).Milliseconds(),
			RowsAffected:   -1,
			Err:            traceErrString(err),
		}, scope)
		return nil, err
	}
	return &traceRows{
		base:      rows,
		startedAt: start,
		query:     query,
		args:      sanitizeTraceArgs(args),
		scope:     scope,
	}, nil
}

func (c *traceConn) Ping(ctx context.Context) error {
	if p, ok := c.base.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *traceConn) ResetSession(ctx context.Context) error {
	if r, ok := c.base.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (c *traceConn) CheckNamedValue(nv *driver.NamedValue) error {
	return normalizeNamedValue(nv)
}

func execOnConn(base driver.Conn, ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := base.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	if execer, ok := base.(driver.Execer); ok {
		values, err := normalizeNamedValues(args)
		if err != nil {
			return nil, err
		}
		return execer.Exec(query, values)
	}
	return nil, driver.ErrSkip
}

func queryOnConn(base driver.Conn, ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if q, ok := base.(driver.QueryerContext); ok {
		return q.QueryContext(ctx, query, args)
	}
	if q, ok := base.(driver.Queryer); ok {
		values, err := normalizeNamedValues(args)
		if err != nil {
			return nil, err
		}
		return q.Query(query, values)
	}
	return nil, driver.ErrSkip
}

type traceRows struct {
	base      driver.Rows
	startedAt time.Time
	query     string
	args      []any
	scope     TraceScope
	rows      int64
	closed    atomic.Bool
}

func (r *traceRows) Columns() []string {
	return r.base.Columns()
}

func (r *traceRows) Close() error {
	err := r.base.Close()
	r.flush(err)
	return err
}

func (r *traceRows) Next(dest []driver.Value) error {
	err := r.base.Next(dest)
	if err == nil {
		r.rows++
		return nil
	}
	if err == driver.ErrBadConn {
		r.flush(err)
		return err
	}
	if err == io.EOF {
		r.flush(nil)
	}
	return err
}

func (r *traceRows) HasNextResultSet() bool {
	if x, ok := r.base.(driver.RowsNextResultSet); ok {
		return x.HasNextResultSet()
	}
	return false
}

func (r *traceRows) NextResultSet() error {
	if x, ok := r.base.(driver.RowsNextResultSet); ok {
		return x.NextResultSet()
	}
	return io.EOF
}

func (r *traceRows) flush(err error) {
	if r == nil {
		return
	}
	if !r.closed.CompareAndSwap(false, true) {
		return
	}
	emitTraceEvent(TraceEvent{
		TS:             nowRFC3339(),
		Operation:      "query",
		SQL:            r.query,
		SQLFingerprint: fingerprintSQL(r.query),
		Args:           r.args,
		ElapsedMS:      time.Since(r.startedAt).Milliseconds(),
		RowsAffected:   r.rows,
		Err:            traceErrString(err),
	}, r.scope)
}

func emitTraceEvent(ev TraceEvent, scope TraceScope) {
	sink := traceSink()
	if sink == nil {
		return
	}
	ev.RoundID = scope.RoundID
	ev.Trigger = scope.Trigger
	ev.Intent = scope.Intent
	ev.Stage = scope.Stage
	if len(scope.CallerChain) > 0 {
		ev.CallerChain = append([]string(nil), scope.CallerChain...)
	}
	sink.Handle(ev)
}

func WithTraceScope(scope TraceScope, fn func()) {
	if fn == nil {
		return
	}
	if traceSink() == nil {
		fn()
		return
	}
	gid := goroutineID()
	if gid == 0 {
		fn()
		return
	}
	traceScopes.Store(gid, traceScopeEntry{scope: scope})
	defer traceScopes.Delete(gid)
	fn()
}

func currentTraceScope() TraceScope {
	gid := goroutineID()
	if gid == 0 {
		return TraceScope{}
	}
	if v, ok := traceScopes.Load(gid); ok {
		if ent, ok := v.(traceScopeEntry); ok {
			return ent.scope
		}
	}
	return TraceScope{}
}

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[0], 10, 64)
	return id
}

func fingerprintSQL(sqlText string) string {
	norm := strings.TrimSpace(strings.ToLower(strings.Join(strings.Fields(sqlText), " ")))
	h := fnv.New64a()
	_, _ = h.Write([]byte(norm))
	return fmt.Sprintf("%016x", h.Sum64())
}

func sanitizeTraceArgs(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, 0, len(args))
	for _, arg := range args {
		out = append(out, sanitizeTraceValue(arg.Value))
	}
	return out
}

func sanitizeTraceValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return hex.EncodeToString(x)
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	case bool:
		return x
	case int, int8, int16, int32, int64:
		return x
	case uint, uint8, uint16, uint32, uint64:
		return x
	case float32, float64:
		return x
	default:
		return fmt.Sprint(v)
	}
}

func traceErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
