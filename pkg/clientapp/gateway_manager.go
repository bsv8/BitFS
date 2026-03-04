package clientapp

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bsv8/BFTP/pkg/obs"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// gatewayManager 管理运行时网关连接和主网关选举
type gatewayManager struct {
	rt *Runtime
	h  host.Host
	mu sync.RWMutex
	// connectedGWs 存储已连接的网关 peer.ID
	connectedGWs map[peer.ID]peer.AddrInfo
}

// newGatewayManager 创建网关管理器
func newGatewayManager(rt *Runtime, h host.Host) *gatewayManager {
	return &gatewayManager{
		rt:           rt,
		h:            h,
		connectedGWs: make(map[peer.ID]peer.AddrInfo),
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
			obs.Error("bitcast-client", "gateway_init_connect_failed", map[string]any{"peer_id": ai.ID.String(), "error": err.Error()})
			continue
		}
		gm.connectedGWs[ai.ID] = *ai
		obs.Business("bitcast-client", "gateway_init_connected", map[string]any{"peer_id": ai.ID.String()})
	}

	// 选举主网关
	gm.electMasterLocked()
	return nil
}

// AddGateway 添加新网关（HTTP API 调用）
func (gm *gatewayManager) AddGateway(ctx context.Context, node PeerNode) (int, error) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// 解析新网关地址
	newAI, err := parseAddr(node.Addr)
	if err != nil {
		return -1, fmt.Errorf("invalid addr: %w", err)
	}

	// 检查是否已存在相同地址（peer ID）或 pubkey
	for i, g := range gm.rt.Config.Network.Gateways {
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

	// 添加到配置
	gm.rt.Config.Network.Gateways = append(gm.rt.Config.Network.Gateways, node)
	idx := len(gm.rt.Config.Network.Gateways) - 1

	// 如果启用，立即连接
	if node.Enabled {
		if err := gm.h.Connect(ctx, *newAI); err != nil {
			obs.Error("bitcast-client", "gateway_add_connect_failed", map[string]any{"peer_id": newAI.ID.String(), "error": err.Error()})
			return idx, nil // 返回索引，但连接失败
		}
		gm.connectedGWs[newAI.ID] = *newAI
		obs.Business("bitcast-client", "gateway_added_and_connected", map[string]any{"peer_id": newAI.ID.String(), "index": idx})
		gm.electMasterLocked()
	}

	return idx, nil
}

// UpdateGateway 更新网关配置
func (gm *gatewayManager) UpdateGateway(ctx context.Context, index int, node PeerNode) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if index < 0 || index >= len(gm.rt.Config.Network.Gateways) {
		return fmt.Errorf("gateway index %d out of range", index)
	}

	oldNode := gm.rt.Config.Network.Gateways[index]
	oldAI, _ := parseAddr(oldNode.Addr)

	// 检查新 pubkey 是否与其他网关冲突（如果不是同一个索引）
	for i, g := range gm.rt.Config.Network.Gateways {
		if i != index && strings.EqualFold(g.Pubkey, node.Pubkey) {
			return fmt.Errorf("gateway with pubkey %s already exists at index %d", node.Pubkey, i)
		}
	}

	gm.rt.Config.Network.Gateways[index] = node

	// 处理连接状态变化
	if oldAI != nil {
		if oldNode.Enabled && !node.Enabled {
			// 从 enable -> disable，断开连接
			delete(gm.connectedGWs, oldAI.ID)
			_ = gm.h.Network().ClosePeer(oldAI.ID)
			obs.Business("bitcast-client", "gateway_disabled", map[string]any{"peer_id": oldAI.ID.String()})
			gm.electMasterLocked()
		} else if !oldNode.Enabled && node.Enabled {
			// 从 disable -> enable，建立连接
			newAI, err := parseAddr(node.Addr)
			if err == nil {
				if err := gm.h.Connect(ctx, *newAI); err != nil {
					obs.Error("bitcast-client", "gateway_enable_connect_failed", map[string]any{"peer_id": newAI.ID.String(), "error": err.Error()})
				} else {
					gm.connectedGWs[newAI.ID] = *newAI
					obs.Business("bitcast-client", "gateway_enabled_and_connected", map[string]any{"peer_id": newAI.ID.String()})
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
					obs.Error("bitcast-client", "gateway_addr_change_connect_failed", map[string]any{"peer_id": newAI.ID.String(), "error": err.Error()})
				} else {
					gm.connectedGWs[newAI.ID] = *newAI
					obs.Business("bitcast-client", "gateway_addr_changed_and_connected", map[string]any{"peer_id": newAI.ID.String()})
					gm.electMasterLocked()
				}
			}
		}
	}

	return nil
}

// DeleteGateway 删除网关
func (gm *gatewayManager) DeleteGateway(index int) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if index < 0 || index >= len(gm.rt.Config.Network.Gateways) {
		return fmt.Errorf("gateway index %d out of range", index)
	}

	node := gm.rt.Config.Network.Gateways[index]
	if node.Enabled {
		return fmt.Errorf("cannot delete enabled gateway, please disable it first")
	}

	// 从列表中移除
	gm.rt.Config.Network.Gateways = append(gm.rt.Config.Network.Gateways[:index], gm.rt.Config.Network.Gateways[index+1:]...)
	obs.Business("bitcast-client", "gateway_deleted", map[string]any{"index": index})
	return nil
}

// GetGateway 获取指定索引的网关
func (gm *gatewayManager) GetGateway(index int) (PeerNode, error) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	if index < 0 || index >= len(gm.rt.Config.Network.Gateways) {
		return PeerNode{}, fmt.Errorf("gateway index %d out of range", index)
	}
	return gm.rt.Config.Network.Gateways[index], nil
}

// ListGateways 列出所有网关
func (gm *gatewayManager) ListGateways() []PeerNode {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	out := make([]PeerNode, len(gm.rt.Config.Network.Gateways))
	copy(out, gm.rt.Config.Network.Gateways)
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
	oldMaster := gm.rt.masterGW
	var newMaster peer.ID

	for _, g := range gm.rt.Config.Network.Gateways {
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

	gm.rt.masterGW = newMaster
	if newMaster != oldMaster {
		if newMaster != "" {
			obs.Business("bitcast-client", "master_gateway_elected", map[string]any{"peer_id": newMaster.String()})
		} else {
			obs.Business("bitcast-client", "master_gateway_cleared", map[string]any{"reason": "no_enabled_connected_gateway"})
		}
	}
}

// GetMasterGateway 获取当前主网关
func (gm *gatewayManager) GetMasterGateway() peer.ID {
	gm.rt.masterGWMu.RLock()
	defer gm.rt.masterGWMu.RUnlock()
	return gm.rt.masterGW
}

// SaveConfig 保存配置到 DB（不落地业务配置文件）。
func (gm *gatewayManager) SaveConfig() error {
	gm.mu.RLock()
	cfg := gm.rt.Config
	gm.mu.RUnlock()

	if err := SaveConfigInDB(gm.rt.DB, cfg); err != nil {
		return fmt.Errorf("save config to db failed: %w", err)
	}
	obs.Business("bitcast-client", "config_saved_to_db", map[string]any{})
	return nil
}

// RefreshConnections 刷新所有已启用网关的连接
func (gm *gatewayManager) RefreshConnections(ctx context.Context) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// 清理已断开的连接
	for id := range gm.connectedGWs {
		if gm.h.Network().Connectedness(id) == 0 { // NotConnected
			delete(gm.connectedGWs, id)
			obs.Business("bitcast-client", "gateway_connection_lost", map[string]any{"peer_id": id.String()})
		}
	}

	// 尝试连接新启用的网关
	for _, g := range gm.rt.Config.Network.Gateways {
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
			obs.Error("bitcast-client", "gateway_refresh_connect_failed", map[string]any{"peer_id": ai.ID.String(), "error": err.Error()})
		} else {
			gm.connectedGWs[ai.ID] = *ai
			obs.Business("bitcast-client", "gateway_refreshed_connected", map[string]any{"peer_id": ai.ID.String()})
		}
	}

	gm.electMasterLocked()
}
