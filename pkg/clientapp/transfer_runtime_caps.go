package clientapp

import (
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/libp2p/go-libp2p/core/host"
	"sync"
)

// transferRuntimeCaps 只暴露正式下载链路需要的运行时能力。
// 设计约束：
// - 这里不允许把整颗 Runtime 传进来；
// - 只给下载转运需要的 host / 链 / 会话锁 / 身份能力；
// - 这样下载编排层只能看见它真正要用的东西。
type transferRuntimeCaps interface {
	TransferHost() host.Host
	TransferActionChain() poolcore.ChainClient
	TransferRPCTrace() pproto.TraceSink
	ClientID() string
	WalletChainClient() walletChainClient
	ConfigSnapshot() Config
	runtimeIdentity() (*clientIdentityCaps, error)
	walletAllocMutex() *sync.Mutex
	transferPoolOpenMutex() *sync.Mutex
	transferPoolSessionMutex(sessionID string) *sync.Mutex
	getTriplePool(sessionID string) (*triplePoolSession, bool)
	setTriplePool(s *triplePoolSession)
	deleteTriplePool(sessionID string)
	releaseTransferPoolSessionMutex(sessionID string)
}
