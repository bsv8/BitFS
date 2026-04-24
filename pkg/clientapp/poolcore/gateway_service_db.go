package poolcore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/storeactor"
)

// 设计说明：
// - GatewayService 对外还是业务入口，但库访问必须优先回到这里；
// - 这样 fee pool 的状态推进、时间线维护、报价落库都能固定在串行 owner 内；
// - 迁移期间允许 Actor 为空，便于测试和旧调用点逐步接入。
func gatewayServiceDBValue[T any](s *GatewayService, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if s == nil || s.DB == nil {
		return zero, fmt.Errorf("db not initialized")
	}
	if fn == nil {
		return zero, fmt.Errorf("gateway service db func is nil")
	}
	if s.Actor != nil {
		return sqliteactor.DoValue(context.Background(), s.Actor, fn)
	}
	return fn(s.DB)
}

// 设计说明：
// - 事务必须在这里完整结束，不能把 tx 往外漏；
// - 上层只表达“这段状态怎么推进”，不自己决定 sqlite 的执行方式。
func gatewayServiceDBTxValue[T any](s *GatewayService, fn func(*sql.Tx) (T, error)) (T, error) {
	var zero T
	if s == nil || s.DB == nil {
		return zero, fmt.Errorf("db not initialized")
	}
	if fn == nil {
		return zero, fmt.Errorf("gateway service tx func is nil")
	}
	if s.Actor != nil {
		return sqliteactor.TxValue(context.Background(), s.Actor, fn)
	}
	tx, err := s.DB.Begin()
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
