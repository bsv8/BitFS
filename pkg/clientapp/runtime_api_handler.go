package clientapp

import (
	"fmt"
	"net/http"
)

// NewRuntimeAPIHandler 构建 runtime API 的进程内 handler。
// 设计说明：
// - managed 模式下统一由单一入口接收请求；
// - runtime 仅提供业务处理函数，不再依赖内部 HTTP 反向代理链路。
func NewRuntimeAPIHandler(rt *Runtime) (http.Handler, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime is nil")
	}
	if rt.DB == nil {
		return nil, fmt.Errorf("runtime db is nil")
	}
	if rt.DBActor == nil {
		return nil, fmt.Errorf("runtime db actor is nil")
	}
	if rt.Host == nil {
		return nil, fmt.Errorf("runtime host is nil")
	}
	if rt.Workspace == nil {
		return nil, fmt.Errorf("runtime workspace is nil")
	}
	cfg := rt.runIn.toConfig()
	srv := newHTTPAPIServer(rt, &cfg, rt.DB, rt.DBActor, rt.Host, rt.HealthyGWs, rt.Workspace, rt.rpcTrace)
	return srv.Handler()
}
