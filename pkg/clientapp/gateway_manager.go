package clientapp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"

	"github.com/libp2p/go-libp2p/core/host"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// gatewayManager 管理运行时网关连接和主网关选举
type gatewayManager struct {
	rt *Runtime
	h  host.Host
	mu sync.RWMutex
	// connectedGWs 存储已连接的网关 peer.ID
	connectedGWs map[peer.ID]peer.AddrInfo
	// states 存储网关运行时连接状态（按 peer.ID 维度）。
	states map[peer.ID]gatewayConnState
}

type gatewayConnState struct {
	LastError              string
	LastConnectedAtUnix    int64
	LastRuntimeError       string
	LastRuntimeErrorStage  string
	LastRuntimeErrorAtUnix int64
}

// newGatewayManager 创建网关管理器
func newGatewayManager(rt *Runtime, h host.Host) *gatewayManager {
	return &gatewayManager{
		rt:           rt,
		h:            h,
		connectedGWs: make(map[peer.ID]peer.AddrInfo),
		states:       make(map[peer.ID]gatewayConnState),
	}
}

// InitFromConfig 从配置初始化已启用的网关连接
func (gm *gatewayManager) InitFromConfig(ctx context.Context, gateways []PeerNode) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	for _, g := range gateways {
		if !g.Enabled {
			continue
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			obs.Error("bitcast-client", "gateway_init_addr_invalid", map[string]any{"addr": g.Addr, "error": err.Error()})
			continue
		}
		if err := gm.h.Connect(ctx, *ai); err != nil {
			gm.setConnectErrorLocked(ai.ID, err)
			obs.Error("bitcast-client", "gateway_init_connect_failed", map[string]any{"transport_peer_id": ai.ID.String(), "error": err.Error()})
			continue
		}
		gm.connectedGWs[ai.ID] = *ai
		gm.setConnectedLocked(ai.ID)
		obs.Business("bitcast-client", "gateway_init_connected", map[string]any{"transport_peer_id": ai.ID.String()})
	}

	// 选举主网关
	gm.electMasterLocked()
	return nil
}

// AddGateway 添加新网关（HTTP API 调用）
func (gm *gatewayManager) AddGateway(ctx context.Context, node PeerNode) (int, error) {
	// 解析新网关地址
	newAI, err := parseAddr(node.Addr)
	if err != nil {
		return -1, fmt.Errorf("invalid addr: %w", err)
	}

	cfgSvc := gm.rt.RuntimeConfigService()
	if cfgSvc == nil {
		return -1, fmt.Errorf("runtime config service not initialized")
	}
	snapshot := gm.rt.ConfigSnapshot()
	mandatorySet, err := mandatoryGatewayPubkeySet(snapshot.BSV.Network)
	if err != nil {
		return -1, err
	}
	if isMandatoryPeer(node.Pubkey, mandatorySet) {
		return -1, errCannotModifyBuiltInGateway
	}
	// 检查是否已存在相同地址（peer ID）或 pubkey
	for i, g := range snapshot.Network.Gateways {
		// 检查地址重复（通过解析地址后比较 peer ID）
		if gAI, parseErr := parseAddr(g.Addr); parseErr == nil {
			if gAI.ID == newAI.ID {
				return -1, fmt.Errorf("gateway with addr %s already exists at index %d", node.Addr, i)
			}
		}
		// 检查 pubkey 重复
		if strings.EqualFold(g.Pubkey, node.Pubkey) {
			return -1, fmt.Errorf("gateway with pubkey %s already exists at index %d", node.Pubkey, i)
		}
	}

	// 添加到配置，先落盘再切换内存。
	next := snapshot
	next.Network.Gateways = append(next.Network.Gateways, node)
	idx := len(next.Network.Gateways) - 1
	if err := cfgSvc.UpdateAndPersist(next); err != nil {
		return -1, err
	}

	// 如果启用，立即连接
	if node.Enabled {
		if err := gm.h.Connect(ctx, *newAI); err != nil {
			gm.mu.Lock()
			gm.setConnectErrorLocked(newAI.ID, err)
			gm.mu.Unlock()
			obs.Error("bitcast-client", "gateway_add_connect_failed", map[string]any{"transport_peer_id": newAI.ID.String(), "error": err.Error()})
			return idx, nil // 返回索引，但连接失败
		}
		gm.mu.Lock()
		gm.connectedGWs[newAI.ID] = *newAI
		gm.setConnectedLocked(newAI.ID)
		gm.electMasterLocked()
		gm.mu.Unlock()
		obs.Business("bitcast-client", "gateway_added_and_connected", map[string]any{"transport_peer_id": newAI.ID.String(), "index": idx})
	}

	return idx, nil
}

// UpdateGateway 更新网关配置
func (gm *gatewayManager) UpdateGateway(ctx context.Context, index int, node PeerNode) error {
	cfgSvc := gm.rt.RuntimeConfigService()
	if cfgSvc == nil {
		return fmt.Errorf("runtime config service not initialized")
	}
	snapshot := gm.rt.ConfigSnapshot()
	mandatorySet, err := mandatoryGatewayPubkeySet(snapshot.BSV.Network)
	if err != nil {
		return err
	}
	if index < 0 || index >= len(snapshot.Network.Gateways) {
		return fmt.Errorf("gateway index %d out of range", index)
	}

	oldNode := snapshot.Network.Gateways[index]
	oldAI, _ := parseAddr(oldNode.Addr)
	if isMandatoryPeer(oldNode.Pubkey, mandatorySet) {
		if !node.Enabled {
			return errCannotDisableBuiltInGateway
		}
		return errCannotModifyBuiltInGateway
	}
	if isMandatoryPeer(node.Pubkey, mandatorySet) {
		return errCannotModifyBuiltInGateway
	}

	// 检查新 pubkey 是否与其他网关冲突（如果不是同一个索引）
	for i, g := range snapshot.Network.Gateways {
		if i != index && strings.EqualFold(g.Pubkey, node.Pubkey) {
			return fmt.Errorf("gateway with pubkey %s already exists at index %d", node.Pubkey, i)
		}
	}

	next := snapshot
	next.Network.Gateways[index] = node
	if err := cfgSvc.UpdateAndPersist(next); err != nil {
		return err
	}

	// 处理连接状态变化
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if oldAI != nil {
		if oldNode.Enabled && !node.Enabled {
			// 从 enable -> disable，断开连接
			delete(gm.connectedGWs, oldAI.ID)
			_ = gm.h.Network().ClosePeer(oldAI.ID)
			obs.Business("bitcast-client", "gateway_disabled", map[string]any{"transport_peer_id": oldAI.ID.String()})
			gm.electMasterLocked()
		} else if !oldNode.Enabled && node.Enabled {
			// 从 disable -> enable，建立连接
			newAI, err := parseAddr(node.Addr)
			if err == nil {
				if err := gm.h.Connect(ctx, *newAI); err != nil {
					gm.setConnectErrorLocked(newAI.ID, err)
					obs.Error("bitcast-client", "gateway_enable_connect_failed", map[string]any{"transport_peer_id": newAI.ID.String(), "error": err.Error()})
				} else {
					gm.connectedGWs[newAI.ID] = *newAI
					gm.setConnectedLocked(newAI.ID)
					obs.Business("bitcast-client", "gateway_enabled_and_connected", map[string]any{"transport_peer_id": newAI.ID.String()})
					gm.electMasterLocked()
				}
			}
		} else if oldNode.Enabled && node.Enabled && oldNode.Addr != node.Addr {
			// 地址变更，重新连接
			delete(gm.connectedGWs, oldAI.ID)
			_ = gm.h.Network().ClosePeer(oldAI.ID)
			newAI, err := parseAddr(node.Addr)
			if err == nil {
				if err := gm.h.Connect(ctx, *newAI); err != nil {
					gm.setConnectErrorLocked(newAI.ID, err)
					obs.Error("bitcast-client", "gateway_addr_change_connect_failed", map[string]any{"transport_peer_id": newAI.ID.String(), "error": err.Error()})
				} else {
					gm.connectedGWs[newAI.ID] = *newAI
					gm.setConnectedLocked(newAI.ID)
					obs.Business("bitcast-client", "gateway_addr_changed_and_connected", map[string]any{"transport_peer_id": newAI.ID.String()})
					gm.electMasterLocked()
				}
			}
		}
	}

	return nil
}

// DeleteGateway 删除网关
func (gm *gatewayManager) DeleteGateway(index int) error {
	cfgSvc := gm.rt.RuntimeConfigService()
	if cfgSvc == nil {
		return fmt.Errorf("runtime config service not initialized")
	}
	snapshot := gm.rt.ConfigSnapshot()
	mandatorySet, err := mandatoryGatewayPubkeySet(snapshot.BSV.Network)
	if err != nil {
		return err
	}
	if index < 0 || index >= len(snapshot.Network.Gateways) {
		return fmt.Errorf("gateway index %d out of range", index)
	}

	node := snapshot.Network.Gateways[index]
	if isMandatoryPeer(node.Pubkey, mandatorySet) {
		return errCannotDeleteBuiltInGateway
	}
	if node.Enabled {
		return fmt.Errorf("cannot delete enabled gateway, please disable it first")
	}

	// 从列表中移除
	next := snapshot
	next.Network.Gateways = append(next.Network.Gateways[:index], next.Network.Gateways[index+1:]...)
	if err := cfgSvc.UpdateAndPersist(next); err != nil {
		return err
	}
	obs.Business("bitcast-client", "gateway_deleted", map[string]any{"index": index})
	return nil
}

// GetGateway 获取指定索引的网关
func (gm *gatewayManager) GetGateway(index int) (PeerNode, error) {
	nodes := gm.rt.ConfigSnapshot().Network.Gateways
	if index < 0 || index >= len(nodes) {
		return PeerNode{}, fmt.Errorf("gateway index %d out of range", index)
	}
	return nodes[index], nil
}

// ListGateways 列出所有网关
func (gm *gatewayManager) ListGateways() []PeerNode {
	nodes := gm.rt.ConfigSnapshot().Network.Gateways
	out := make([]PeerNode, len(nodes))
	copy(out, nodes)
	return out
}

// GetConnectedGateways 获取已连接的网关列表
func (gm *gatewayManager) GetConnectedGateways() []peer.AddrInfo {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	out := make([]peer.AddrInfo, 0, len(gm.connectedGWs))
	for _, ai := range gm.connectedGWs {
		out = append(out, ai)
	}
	return out
}

// electMasterLocked 选举主网关（必须在持有写锁时调用）
// 策略：选择第一个已连接的 enabled 网关
func (gm *gatewayManager) electMasterLocked() {
	oldMaster := gm.rt.MasterGateway()
	var newMaster peer.ID

	for _, g := range gm.rt.ConfigSnapshot().Network.Gateways {
		if !g.Enabled {
			continue
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			continue
		}
		if _, ok := gm.connectedGWs[ai.ID]; ok {
			newMaster = ai.ID
			break
		}
	}

	gm.rt.SetMasterGateway(newMaster)
	if newMaster != oldMaster {
		if newMaster != "" {
			obs.Business("bitcast-client", "master_gateway_elected", map[string]any{"transport_peer_id": newMaster.String()})
		} else {
			obs.Business("bitcast-client", "master_gateway_cleared", map[string]any{"reason": "no_enabled_connected_gateway"})
		}
	}
}

// GetMasterGateway 获取当前主网关
func (gm *gatewayManager) GetMasterGateway() peer.ID {
	if gm == nil || gm.rt == nil {
		return ""
	}
	return gm.rt.MasterGateway()
}

// RefreshConnections 刷新所有已启用网关的连接
func (gm *gatewayManager) RefreshConnections(ctx context.Context) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// 清理已断开的连接
	for id := range gm.connectedGWs {
		if gm.h.Network().Connectedness(id) == libnetwork.NotConnected {
			delete(gm.connectedGWs, id)
			gm.setConnectErrorLocked(id, fmt.Errorf("connection lost"))
			obs.Business("bitcast-client", "gateway_connection_lost", map[string]any{"transport_peer_id": id.String()})
		}
	}

	// 尝试连接新启用的网关
	for _, g := range gm.rt.ConfigSnapshot().Network.Gateways {
		if !g.Enabled {
			continue
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			continue
		}
		if _, ok := gm.connectedGWs[ai.ID]; ok {
			continue
		}
		if err := gm.h.Connect(ctx, *ai); err != nil {
			gm.setConnectErrorLocked(ai.ID, err)
			obs.Error("bitcast-client", "gateway_refresh_connect_failed", map[string]any{"transport_peer_id": ai.ID.String(), "error": err.Error()})
		} else {
			gm.connectedGWs[ai.ID] = *ai
			gm.setConnectedLocked(ai.ID)
			obs.Business("bitcast-client", "gateway_refreshed_connected", map[string]any{"transport_peer_id": ai.ID.String()})
		}
	}

	gm.electMasterLocked()
}

func (gm *gatewayManager) setConnectErrorLocked(id peer.ID, err error) {
	if id == "" {
		return
	}
	s := gm.states[id]
	if err != nil {
		s.LastError = strings.TrimSpace(err.Error())
	}
	gm.states[id] = s
}

func (gm *gatewayManager) setConnectedLocked(id peer.ID) {
	if id == "" {
		return
	}
	s := gm.states[id]
	s.LastError = ""
	s.LastConnectedAtUnix = time.Now().Unix()
	gm.states[id] = s
}

func (gm *gatewayManager) setRuntimeErrorLocked(id peer.ID, stage string, err error) {
	if id == "" || err == nil {
		return
	}
	s := gm.states[id]
	s.LastRuntimeError = strings.TrimSpace(err.Error())
	s.LastRuntimeErrorStage = strings.TrimSpace(stage)
	s.LastRuntimeErrorAtUnix = time.Now().Unix()
	gm.states[id] = s
}

func (gm *gatewayManager) clearRuntimeErrorLocked(id peer.ID) {
	if id == "" {
		return
	}
	s := gm.states[id]
	s.LastRuntimeError = ""
	s.LastRuntimeErrorStage = ""
	s.LastRuntimeErrorAtUnix = 0
	gm.states[id] = s
}

// GetGatewayState 返回网关运行时状态快照。
func (gm *gatewayManager) GetGatewayState(id peer.ID) gatewayConnState {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return gm.states[id]
}

// SetRuntimeError 记录网关业务运行时错误（例如费用池 info/open/pay 阶段失败）。
func (gm *gatewayManager) SetRuntimeError(id peer.ID, stage string, err error) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.setRuntimeErrorLocked(id, stage, err)
}

// ClearRuntimeError 清理网关业务运行时错误（例如后续重试成功后）。
func (gm *gatewayManager) ClearRuntimeError(id peer.ID) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.clearRuntimeErrorLocked(id)
}
