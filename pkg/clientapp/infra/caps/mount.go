package caps

import (
	"context"
	"net/http"

	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/pproto"
	"github.com/libp2p/go-libp2p/core/host"
	coreprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

// MountContext 是当前这套静态装配层的最小挂载上下文。
// 设计说明：
// - 这里只统一宿主、默认安全域和本地 HTTP mux；
// - 不把角色私有运行时、业务依赖、生命周期控制揉进来；
// - 目标是减少重复挂载代码，而不是引入重型插件系统。
type MountContext struct {
	Host          host.Host
	NodeSecurity  pproto.SecurityConfig
	ProtoSecurity pproto.SecurityConfig
	AdminMux      *http.ServeMux
}

func (m MountContext) WithProtoSecurity(sec pproto.SecurityConfig) MountContext {
	m.ProtoSecurity = sec
	return m
}

func (m MountContext) RegisterCapabilitiesShow(showHandler func() ncall.CapabilitiesShowBody) {
	if m.Host == nil {
		return
	}
	ncall.RegisterCapabilitiesShow(m.Host, m.NodeSecurity, showHandler)
}

func (m MountContext) HandleHTTP(path string, fn http.HandlerFunc) {
	if m.AdminMux == nil || fn == nil {
		return
	}
	m.AdminMux.HandleFunc(path, fn)
}

func HandleProto[TReq any, TResp any](m MountContext, protoID coreprotocol.ID, fn func(context.Context, TReq) (TResp, error)) {
	if m.Host == nil {
		return
	}
	pproto.HandleProto[TReq, TResp](m.Host, protoID, m.ProtoSecurity, fn)
}
