package clientapp

import (
	"sync"
)

// feePoolSession 是 client 侧维护的“2-of-2 费用池通道会话”状态（按 gateway 维度）。
// 注意：这是 KeymasterMultisigPool 的通道状态，不是“数据库记账余额”。
type feePoolSession struct {
	GatewayPeerID string

	SpendTxID string
	BaseTxID  string
	FinalTxID string
	Status    string

	PoolAmountSat uint64
	SpendTxFeeSat uint64

	Sequence     uint32
	ServerAmount uint64
	ClientAmount uint64

	CurrentTxHex string // client 侧可重建下一次更新的 spend tx（不要求包含 server 签名）

	// 握手参数快照（用于 client 本地逻辑与 UI 观测）
	BillingCycleSeconds      uint32
	SingleCycleFeeSatoshi    uint64
	SinglePublishFeeSatoshi  uint64
	RenewNotifyBeforeSeconds uint32

	MinimumPoolAmountSatoshi uint64
	LockBlocks               uint32
	FeeRateSatPerByte        float64
}

func (r *Runtime) getFeePool(gatewayPeerID string) (*feePoolSession, bool) {
	if r == nil {
		return nil, false
	}
	r.feePoolsMu.RLock()
	defer r.feePoolsMu.RUnlock()
	s, ok := r.feePools[gatewayPeerID]
	return s, ok
}

func (r *Runtime) setFeePool(gatewayPeerID string, s *feePoolSession) {
	if r == nil {
		return
	}
	r.feePoolsMu.Lock()
	defer r.feePoolsMu.Unlock()
	if r.feePools == nil {
		r.feePools = map[string]*feePoolSession{}
	}
	r.feePools[gatewayPeerID] = s
}

func (r *Runtime) feePoolMutex() *sync.RWMutex {
	if r == nil {
		return &sync.RWMutex{}
	}
	return &r.feePoolsMu
}

func (r *Runtime) walletAllocMutex() *sync.Mutex {
	if r == nil {
		return &sync.Mutex{}
	}
	return &r.walletAllocMu
}
