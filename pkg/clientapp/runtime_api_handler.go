package clientapp

import (
	"database/sql"
	"fmt"
	"net/http"
)

// NewRuntimeAPIHandler 构建 runtime API 的进程内 handler。
// 设计说明：
// - managed 模式直接拿纯 handler，不依赖独立监听器是否启动；
// - runtime 只提供当前能力和快照，不再让 handler 反向依赖 rt.HTTP。
func NewRuntimeAPIHandler(rt *Runtime) (http.Handler, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime is nil")
	}
	if rt.Host == nil {
		return nil, fmt.Errorf("runtime host is nil")
	}
	if rt.Workspace == nil {
		return nil, fmt.Errorf("runtime workspace is nil")
	}
	var db *sql.DB
	if rt.store != nil {
		db = rt.store.db
	}
	srv := newHTTPAPIServer(rt, rt, db, rt.store, rt.Host, rt.HealthyGWs, rt.Workspace, rt.rpcTrace)
	return srv.Handler()
}
