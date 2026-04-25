package gatewayclient

import (
	"context"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// bizHost 是 gatewayclient 模块的业务入口能力。
// 设计说明：模块只通过这个面貌接入主干，不暴露 Runtime、*sql.DB 等内部结构。
type bizHost interface {
	moduleapi.Host
	PeerCallRaw(context.Context, string, string, moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error)
	GetGatewayPubkey(peer.ID) (*ec.PublicKey, error)
}
