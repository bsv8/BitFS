package clientapp

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// runtimeConfigService 是运行期配置的唯一真源。
// 设计约束：
// - 业务层只读快照；
// - 管理入口统一写这里；
// - 更新顺序固定为“校验 -> 落盘 -> 切换内存”，避免内存先改导致双真源。
type runtimeConfigService struct {
	mu          sync.RWMutex
	cfg         Config
	configPath  string
	startupMode StartupMode
}

func newRuntimeConfigService(cfg Config, configPath string, mode StartupMode) (*runtimeConfigService, error) {
	startupMode, err := normalizeStartupMode(mode)
	if err != nil {
		return nil, err
	}
	trimmedPath := strings.TrimSpace(configPath)
	cleanedPath := ""
	if trimmedPath != "" {
		cleanedPath = filepath.Clean(trimmedPath)
	}
	return &runtimeConfigService{
		cfg:         cloneConfig(cfg),
		configPath:  cleanedPath,
		startupMode: startupMode,
	}, nil
}

func (s *runtimeConfigService) Snapshot() Config {
	if s == nil {
		return Config{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneConfig(s.cfg)
}

// ConfigSnapshot 让运行期配置服务直接满足只读快照接口。
// 这样启动装配和运行态 handler 都能拿到同一份快照能力，而不是把 *Config 传来传去。
func (s *runtimeConfigService) ConfigSnapshot() Config {
	return s.Snapshot()
}

func (s *runtimeConfigService) ConfigPath() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.TrimSpace(s.configPath)
}

func (s *runtimeConfigService) StartupMode() StartupMode {
	if s == nil {
		return StartupModeProduct
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startupMode
}

func (s *runtimeConfigService) UpdateAndPersist(next Config) error {
	return s.update(next, true)
}

func (s *runtimeConfigService) UpdateMemoryOnly(next Config) error {
	return s.update(next, false)
}

// SnapshotWithSellerLiveBasePrice 只生成“改了全局 base”的新快照。
// 设计说明：
// - 这是 set_base 的最小辅助，不扩散成通用配置编辑器；
// - 调用方仍然要自己决定是落盘还是只改内存；
// - 这样控制面读起来更直，不用在上层手工复制整份 Config。
func (s *runtimeConfigService) SnapshotWithSellerLiveBasePrice(base uint64) Config {
	next := s.Snapshot()
	next.Seller.Pricing.LiveBasePriceSatPer64K = base
	return next
}

func (s *runtimeConfigService) update(next Config, persist bool) error {
	if s == nil {
		return fmt.Errorf("runtime config service is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	mode := s.startupMode
	if err := validateConfigForMode(&next, mode); err != nil {
		return err
	}
	if persist {
		if strings.TrimSpace(s.configPath) == "" {
			return fmt.Errorf("config path is required for persisted runtime config updates")
		}
		if err := SaveConfigFileForMode(s.configPath, next, mode); err != nil {
			return fmt.Errorf("save config file failed: %w", err)
		}
	}
	s.cfg = cloneConfig(next)
	return nil
}

func (s *runtimeConfigService) GatewaySnapshot() []PeerNode {
	return s.Snapshot().Network.Gateways
}

func (s *runtimeConfigService) ArbiterSnapshot() []PeerNode {
	return s.Snapshot().Network.Arbiters
}

func (s *runtimeConfigService) ListenEnabled() bool {
	cfg := s.Snapshot()
	return cfgBool(cfg.Listen.Enabled, true)
}

func (s *runtimeConfigService) ListenRenewThresholdSeconds() uint32 {
	return s.Snapshot().Listen.RenewThresholdSeconds
}

func (s *runtimeConfigService) ListenAutoRenewRounds() uint64 {
	return s.Snapshot().Listen.AutoRenewRounds
}

func (s *runtimeConfigService) ListenOfferPaymentSatoshi() uint64 {
	return s.Snapshot().Listen.OfferPaymentSatoshi
}

func (s *runtimeConfigService) ListenTickSeconds() uint32 {
	return s.Snapshot().Listen.TickSeconds
}

func (s *runtimeConfigService) LiveCacheMaxBytes() uint64 {
	return s.Snapshot().Live.CacheMaxBytes
}

func (s *runtimeConfigService) LiveBuyerTargetLagSegments() uint32 {
	return s.Snapshot().Live.Buyer.TargetLagSegments
}

func (s *runtimeConfigService) LiveBuyerMaxBudgetPerMinute() uint64 {
	return s.Snapshot().Live.Buyer.MaxBudgetPerMinute
}

func (s *runtimeConfigService) LiveBuyerPreferOlderSegments() bool {
	return s.Snapshot().Live.Buyer.PreferOlderSegments
}

func (s *runtimeConfigService) LivePublishBroadcastWindow() uint32 {
	return s.Snapshot().Live.Publish.BroadcastWindow
}

func (s *runtimeConfigService) LivePublishBroadcastIntervalSec() uint32 {
	return s.Snapshot().Live.Publish.BroadcastIntervalSec
}

func (s *runtimeConfigService) LiveBuyerPricing() LiveBuyerStrategy {
	cfg := s.Snapshot()
	return LiveBuyerStrategy{
		TargetLagSegments:   cfg.Live.Buyer.TargetLagSegments,
		MaxBudgetPerMinute:  cfg.Live.Buyer.MaxBudgetPerMinute,
		PreferOlderSegments: cfg.Live.Buyer.PreferOlderSegments,
	}
}

func (s *runtimeConfigService) LiveSellerPricing() LiveSellerPricing {
	cfg := s.Snapshot()
	return LiveSellerPricing{
		BasePriceSatPer64K:  cfg.Seller.Pricing.LiveBasePriceSatPer64K,
		FloorPriceSatPer64K: cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
		DecayPerMinuteBPS:   cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
	}
}

func (s *runtimeConfigService) FSWatchEnabled() bool {
	return s.Snapshot().Scan.FSWatchEnabled
}

func (s *runtimeConfigService) FSRescanIntervalSeconds() uint32 {
	return s.Snapshot().Scan.RescanIntervalSeconds
}

func (s *runtimeConfigService) StartupFullScan() bool {
	return s.Snapshot().Scan.StartupFullScan
}

func (s *runtimeConfigService) SellerFloorPriceSatPer64K() uint64 {
	return s.Snapshot().Seller.Pricing.FloorPriceSatPer64K
}

func (s *runtimeConfigService) SellerResaleDiscountBPS() uint64 {
	return s.Snapshot().Seller.Pricing.ResaleDiscountBPS
}

func (s *runtimeConfigService) WOCAPIKey() string {
	return strings.TrimSpace(s.Snapshot().ExternalAPI.WOC.APIKey)
}

func (s *runtimeConfigService) WOCMinIntervalMS() uint32 {
	return s.Snapshot().ExternalAPI.WOC.MinIntervalMS
}

func (s *runtimeConfigService) PreferredPaymentScheme() string {
	return strings.TrimSpace(s.Snapshot().Payment.PreferredScheme)
}

func (s *runtimeConfigService) DomainResolveOrder() []string {
	cfg := s.Snapshot()
	return append([]string(nil), cfg.Domain.ResolveOrder...)
}

func (s *runtimeConfigService) ReachabilityAutoAnnounceEnabled() bool {
	cfg := s.Snapshot()
	return cfgBool(cfg.Reachability.AutoAnnounceEnabled, true)
}

func (s *runtimeConfigService) ReachabilityAnnounceTTLSeconds() uint32 {
	return s.Snapshot().Reachability.AnnounceTTLSeconds
}

func (s *runtimeConfigService) HTTPEnabled() bool {
	return s.Snapshot().HTTP.Enabled
}

func (s *runtimeConfigService) HTTPListenAddr() string {
	return strings.TrimSpace(s.Snapshot().HTTP.ListenAddr)
}

func (s *runtimeConfigService) FSHTTPEnabled() bool {
	return s.Snapshot().FSHTTP.Enabled
}

func (s *runtimeConfigService) FSHTTPListenAddr() string {
	return strings.TrimSpace(s.Snapshot().FSHTTP.ListenAddr)
}

func (s *runtimeConfigService) cloneAndAssign(next Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cloneConfig(next)
}

func cloneConfig(in Config) Config {
	out := in
	out.Network.Gateways = append([]PeerNode(nil), in.Network.Gateways...)
	out.Network.Arbiters = append([]PeerNode(nil), in.Network.Arbiters...)
	out.Domain.ResolveOrder = append([]string(nil), in.Domain.ResolveOrder...)
	if in.Listen.Enabled != nil {
		v := *in.Listen.Enabled
		out.Listen.Enabled = &v
	}
	if in.Reachability.AutoAnnounceEnabled != nil {
		v := *in.Reachability.AutoAnnounceEnabled
		out.Reachability.AutoAnnounceEnabled = &v
	}
	return out
}
