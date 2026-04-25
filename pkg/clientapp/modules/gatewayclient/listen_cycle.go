package gatewayclient

import (
	"context"
)

const (
	ListenCycleIntervalSeconds = 30
)

// StartListenCycle 开始监听循环。
func (s *service) StartListenCycle(ctx context.Context) error {
	if s == nil || s.host == nil {
		return nil
	}
	// 占位实现
	return nil
}