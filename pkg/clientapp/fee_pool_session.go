package clientapp

import (
	"fmt"
	"strings"
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

	SuspiciousReason string
	SuspiciousAtUnix int64

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
	SingleQueryFeeSatoshi    uint64
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

// FeePoolGatewayPeerID 返回当前唯一活跃费用池会话对应的网关公钥 hex。
// 设计说明：
// - 这里不做“默认第一个网关”兜底；
// - 只在运行态已经明确存在唯一会话，且能映射到配置里的 gateway_pubkey_hex 时返回；
// - 用于 e2e / 运维显式把 gateway_pubkey_hex 传回触发入口。
func (r *Runtime) FeePoolGatewayPeerID() (string, error) {
	if r == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	r.feePoolsMu.RLock()
	defer r.feePoolsMu.RUnlock()
	var peerIDs []string
	for gatewayPeerID, sess := range r.feePools {
		if sess == nil {
			continue
		}
		if strings.TrimSpace(sess.SpendTxID) == "" {
			continue
		}
		peerIDs = append(peerIDs, strings.TrimSpace(gatewayPeerID))
	}
	switch len(peerIDs) {
	case 0:
		return "", fmt.Errorf("fee pool session missing")
	case 1:
		peerID := strings.TrimSpace(peerIDs[0])
		if peerID == "" {
			return "", fmt.Errorf("fee pool session missing")
		}
		for _, g := range r.ConfigSnapshot().Network.Gateways {
			ai, err := parseAddr(g.Addr)
			if err != nil || ai == nil || strings.TrimSpace(ai.ID.String()) != peerID {
				continue
			}
			pubkey := strings.ToLower(strings.TrimSpace(g.Pubkey))
			if pubkey == "" {
				return "", fmt.Errorf("gateway_pubkey_hex missing for fee pool gateway=%s", peerID)
			}
			return pubkey, nil
		}
		return "", fmt.Errorf("gateway_pubkey_hex not found for fee pool gateway=%s", peerID)
	default:
		return "", fmt.Errorf("multiple fee pool sessions found")
	}
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

// feePoolPayMutex 返回“按网关维度”的费用池扣费串行锁。
// 设计约束：所有会推进 sequence/server_amount 的扣费路径必须走同一把锁。
func (r *Runtime) feePoolPayMutex(gatewayPeerID string) *sync.Mutex {
	if r == nil {
		return &sync.Mutex{}
	}
	key := strings.TrimSpace(gatewayPeerID)
	if key == "" {
		key = "__default__"
	}
	r.feePoolPayLocksMu.Lock()
	defer r.feePoolPayLocksMu.Unlock()
	if r.feePoolPayLocks == nil {
		r.feePoolPayLocks = map[string]*sync.Mutex{}
	}
	if mu, ok := r.feePoolPayLocks[key]; ok && mu != nil {
		return mu
	}
	mu := &sync.Mutex{}
	r.feePoolPayLocks[key] = mu
	return mu
}
