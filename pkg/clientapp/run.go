package clientapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bftpv1 "github.com/bsv8/BFTP-contract/gen/go/v1"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BFTP/pkg/dealprod"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
	bitfsv1 "github.com/bsv8/bitfs-contract/gen/go/v1"
	bitfsprotoid "github.com/bsv8/bitfs-contract/pkg/v1/protoid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/pelletier/go-toml/v2"
)

const (
	BBroadcastSuiteVersion             = "BBroadcast/1.0"
	BBroadcastProtocolName             = "Bitcast Broadcast Protocol"
	ProtoGatewayHealth     protocol.ID = contractprotoid.ProtoGatewayHealth
	ProtoArbiterHealth     protocol.ID = contractprotoid.ProtoArbiterHealth
	ProtoSeedGet           protocol.ID = bitfsprotoid.ProtoSeedGet
	ProtoQuoteDirectSubmit protocol.ID = bitfsprotoid.ProtoQuoteDirectSubmit
	ProtoLiveQuoteSubmit   protocol.ID = bitfsprotoid.ProtoLiveQuoteSubmit
	ProtoDirectDealAccept  protocol.ID = bitfsprotoid.ProtoDirectDealAccept
	ProtoTransferPoolOpen  protocol.ID = bitfsprotoid.ProtoTransferPoolOpen
	ProtoTransferChunkGet  protocol.ID = bitfsprotoid.ProtoTransferChunkGet
	ProtoTransferPoolPay   protocol.ID = bitfsprotoid.ProtoTransferPoolPay
	ProtoTransferArbitrate protocol.ID = bitfsprotoid.ProtoTransferArbitrate
	ProtoTransferPoolClose protocol.ID = bitfsprotoid.ProtoTransferPoolClose
	ProtoLiveSubscribe     protocol.ID = bitfsprotoid.ProtoLiveSubscribe
	ProtoLiveHeadPush      protocol.ID = bitfsprotoid.ProtoLiveHeadPush

	bootPeerConnectTimeout = 5 * time.Second
	bootPeerHealthTimeout  = 5 * time.Second

	defaultIndexRelPath = "data/client-index.sqlite"
	seedBlockSize       = 65536
)

// StartupMode 表示本次启动采用的默认补齐策略。
// 设计约束：
// - product：按产品启动语义补齐基础网关和仲裁；
// - test：保留空配置，允许最小化 e2e 环境直接起跑。
type StartupMode string

const (
	StartupModeProduct StartupMode = "product"
	StartupModeTest    StartupMode = "test"
)

type seedGetReq = bitfsv1.SeedGetReq
type seedGetResp = bitfsv1.SeedGetResp
type directQuoteSubmitReq = bitfsv1.DirectQuoteSubmitReq
type directQuoteSubmitResp = bitfsv1.DirectQuoteSubmitResp
type directDealAcceptReq = bitfsv1.DirectDealAcceptReq
type directDealAcceptResp = bitfsv1.DirectDealAcceptResp
type directTransferPoolOpenReq = bitfsv1.DirectTransferPoolOpenReq
type directTransferPoolOpenResp = bitfsv1.DirectTransferPoolOpenResp
type directTransferChunkGetReq = bitfsv1.DirectTransferChunkGetReq
type directTransferChunkGetResp = bitfsv1.DirectTransferChunkGetResp
type directTransferPoolPayReq = bitfsv1.DirectTransferPoolPayReq
type directTransferPoolPayResp = bitfsv1.DirectTransferPoolPayResp
type directTransferArbitrateReq = bitfsv1.DirectTransferArbitrateReq
type directTransferArbitrateResp = bitfsv1.DirectTransferArbitrateResp
type directTransferPoolCloseReq = bitfsv1.DirectTransferPoolCloseReq
type directTransferPoolCloseResp = bitfsv1.DirectTransferPoolCloseResp
type liveSegmentDataPB = bitfsv1.LiveSegmentData
type liveSegmentPB = bitfsv1.LiveSegment
type liveSegmentRefPB = bitfsv1.LiveSegmentRef
type liveSubscribeReq = bitfsv1.LiveSubscribeReq
type liveSubscribeResp = bitfsv1.LiveSubscribeResp
type liveHeadPushReq = bitfsv1.LiveHeadPushReq
type liveHeadPushResp = bitfsv1.LiveHeadPushResp
type liveQuoteSegmentPB = bitfsv1.LiveSegmentRef
type liveQuoteSubmitReq = bitfsv1.LiveQuoteSubmitReq
type liveQuoteSubmitResp = bitfsv1.LiveQuoteSubmitResp

type Config struct {
	ClientID string `yaml:"-" toml:"-"`
	// Debug 打开后，client 会把 pproto 收发原文落盘到本地 raw 抓包目录。
	// 注意：这里只抓 pproto 控制层原文，不抓更底层 libp2p/TCP 线包。
	Debug bool `yaml:"debug" toml:"debug"`
	Keys  struct {
		PrivkeyHex string `yaml:"-" toml:"-"`
	} `yaml:"keys" toml:"keys"`
	BSV struct {
		Network string `yaml:"network" toml:"network"` // "test" 或 "main"（默认 "test"）
	} `yaml:"bsv" toml:"bsv"`
	Network struct {
		Gateways []PeerNode `yaml:"gateways" toml:"gateways"`
		Arbiters []PeerNode `yaml:"arbiters" toml:"arbiters"`
	} `yaml:"network" toml:"network"`
	Storage struct {
		WorkspaceDir string `yaml:"workspace_dir" toml:"workspace_dir"`
		DataDir      string `yaml:"data_dir" toml:"data_dir"`
		MinFreeBytes uint64 `yaml:"min_free_bytes" toml:"min_free_bytes"`
	} `yaml:"storage" toml:"storage"`
	Seller struct {
		Enabled bool `yaml:"enabled" toml:"enabled"`
		Pricing struct {
			FloorPriceSatPer64K     uint64 `yaml:"floor_price_sat_per_64k" toml:"floor_price_sat_per_64k"`
			ResaleDiscountBPS       uint64 `yaml:"resale_discount_bps" toml:"resale_discount_bps"`
			LiveBasePriceSatPer64K  uint64 `yaml:"live_base_price_sat_per_64k" toml:"live_base_price_sat_per_64k"`
			LiveFloorPriceSatPer64K uint64 `yaml:"live_floor_price_sat_per_64k" toml:"live_floor_price_sat_per_64k"`
			LiveDecayPerMinuteBPS   uint64 `yaml:"live_decay_per_minute_bps" toml:"live_decay_per_minute_bps"`
		} `yaml:"pricing" toml:"pricing"`
	} `yaml:"seller" toml:"seller"`
	Live struct {
		CacheMaxBytes uint64 `yaml:"cache_max_bytes" toml:"cache_max_bytes"`
		Buyer         struct {
			TargetLagSegments   uint32 `yaml:"target_lag_segments" toml:"target_lag_segments"`
			MaxBudgetPerMinute  uint64 `yaml:"max_budget_per_minute" toml:"max_budget_per_minute"`
			PreferOlderSegments bool   `yaml:"prefer_older_segments" toml:"prefer_older_segments"`
		} `yaml:"buyer" toml:"buyer"`
		Publish struct {
			BroadcastWindow      uint32 `yaml:"broadcast_window" toml:"broadcast_window"`
			BroadcastIntervalSec uint32 `yaml:"broadcast_interval_seconds" toml:"broadcast_interval_seconds"`
		} `yaml:"publish" toml:"publish"`
	} `yaml:"live" toml:"live"`
	Listen struct {
		Enabled               *bool  `yaml:"enabled" toml:"enabled"`
		RenewThresholdSeconds uint32 `yaml:"renew_threshold_seconds" toml:"renew_threshold_seconds"`
		AutoRenewRounds       uint64 `yaml:"auto_renew_rounds" toml:"auto_renew_rounds"`
		OfferPaymentSatoshi   uint64 `yaml:"offer_payment_satoshi" toml:"offer_payment_satoshi"`
		TickSeconds           uint32 `yaml:"tick_seconds" toml:"tick_seconds"`
	} `yaml:"listen" toml:"listen"`
	Payment struct {
		// PreferredScheme 是“系统默认优先支付通道”。
		// 设计说明：
		// - 这里只表达默认优先级，不引入“强制只走某一条”的第二层语义；
		// - 显式 quote/pay 仍可带 payment_scheme 覆盖；
		// - 缺省保持旧行为，优先走 pool_2of2_v1。
		PreferredScheme string `yaml:"preferred_scheme" toml:"preferred_scheme"`
	} `yaml:"payment" toml:"payment"`
	Domain struct {
		ResolveOrder []string `yaml:"resolve_order" toml:"resolve_order"`
	} `yaml:"domain" toml:"domain"`
	Reachability struct {
		// AutoAnnounceEnabled 控制“客户端自动把自己当前可达地址发布到 gateway 目录”。
		// 设计说明：
		// - 这是节点侧行为开关，不是 gateway 侧策略；
		// - 缺省开启，让 node locator 在大多数情况下开箱即用；
		// - 关闭后仍保留手动触发口子，方便后续 e2e 与精细调试。
		AutoAnnounceEnabled *bool  `yaml:"auto_announce_enabled" toml:"auto_announce_enabled"`
		AnnounceTTLSeconds  uint32 `yaml:"announce_ttl_seconds" toml:"announce_ttl_seconds"`
	} `yaml:"reachability" toml:"reachability"`
	Scan struct {
		StartupFullScan       bool   `yaml:"startup_full_scan" toml:"startup_full_scan"`
		FSWatchEnabled        bool   `yaml:"fs_watch_enabled" toml:"fs_watch_enabled"`
		RescanIntervalSeconds uint32 `yaml:"rescan_interval_seconds" toml:"rescan_interval_seconds"`
	} `yaml:"scan" toml:"scan"`
	Index struct {
		Backend    string `yaml:"backend" toml:"backend"`
		SQLitePath string `yaml:"sqlite_path" toml:"sqlite_path"`
	} `yaml:"index" toml:"index"`
	HTTP struct {
		Enabled    bool   `yaml:"enabled" toml:"enabled"`
		ListenAddr string `yaml:"listen_addr" toml:"listen_addr"`
	} `yaml:"http" toml:"http"`
	FSHTTP struct {
		Enabled                    bool   `yaml:"enabled" toml:"enabled"`
		ListenAddr                 string `yaml:"listen_addr" toml:"listen_addr"`
		DownloadWaitTimeoutSeconds uint32 `yaml:"download_wait_timeout_seconds" toml:"download_wait_timeout_seconds"`
		MaxConcurrentSessions      uint32 `yaml:"max_concurrent_sessions" toml:"max_concurrent_sessions"`
		MaxChunkPriceSatPer64K     uint64 `yaml:"max_chunk_price_sat_per_64k" toml:"max_chunk_price_sat_per_64k"`
		QuoteWaitSeconds           uint32 `yaml:"quote_wait_seconds" toml:"quote_wait_seconds"`
		QuotePollSeconds           uint32 `yaml:"quote_poll_seconds" toml:"quote_poll_seconds"`
		PrefetchDistanceChunks     uint32 `yaml:"prefetch_distance_chunks" toml:"prefetch_distance_chunks"`
		StrategyDebugLogEnabled    bool   `yaml:"strategy_debug_log_enabled" toml:"strategy_debug_log_enabled"`
	} `yaml:"fs_http" toml:"fs_http"`
	ExternalAPI struct {
		WOC struct {
			APIKey        string `yaml:"api_key" toml:"api_key"`
			MinIntervalMS uint32 `yaml:"min_interval_ms" toml:"min_interval_ms"`
		} `yaml:"woc" toml:"woc"`
	} `yaml:"external_api" toml:"external_api"`
	// WOCAPIKey 只保留作旧配置读入迁移。
	// 运行态和新配置统一走 external_api.woc.api_key。
	WOCAPIKey string `yaml:"woc_api_key,omitempty" toml:"woc_api_key"`
	Log       struct {
		File            string `yaml:"file" toml:"file"`
		ConsoleMinLevel string `yaml:"console_min_level" toml:"console_min_level"`
	} `yaml:"log" toml:"log"`
}

type PeerNode struct {
	Enabled                   bool   `yaml:"enabled" toml:"enabled"`
	Addr                      string `yaml:"addr" toml:"addr"`
	Pubkey                    string `yaml:"pubkey" toml:"pubkey"`
	ListenOfferPaymentSatoshi uint64 `yaml:"listen_offer_payment_satoshi" toml:"listen_offer_payment_satoshi"`
}

type sellerSeed struct {
	SeedHash            string
	FileSize            uint64
	ChunkCount          uint32
	ChunkHashes         []string
	SeedPrice           uint64
	ChunkPrice          uint64
	RecommendedFileName string
	MIMEHint            string
}

type sellerCatalog struct {
	snapshot  atomic.Pointer[seedCatalogSnapshot]
	cacheHits atomic.Uint64
	cacheMiss atomic.Uint64
	dbQueries atomic.Uint64
	stale     atomic.Bool
}

// ClientStore 是业务主路可见的最小数据库能力。
// 设计约束：
// - 业务主路只拿 ent 读写能力；
// - raw SQL 只留给模块边界和极少数基础设施桥接；
// - 真正的实现仍然是 clientDB。
type ClientStore interface {
	ReadEnt(ctx context.Context, fn func(EntReadRoot) error) error
	WriteEntTx(ctx context.Context, fn func(EntWriteRoot) error) error
}

// RunDeps 由最外层装配好后显式传给 Run。
// 设计约束：
// - Run 不再自己开 DB；
// - 这里必须显式提供已经准备好的 store 和 raw DB；
// - OwnsDB 只决定本次 Run 是否回收外部注入的 DB 资源。
type RunDeps struct {
	Store   *clientDB
	RawDB   *sql.DB
	DBActor *sqliteactor.Actor
	OwnsDB  bool
}

// RunOptions 只承载本次启动专属的能力和开关。
// 设计约束：
// - 这里放的是“启动参数”，不是运行期配置；
// - 运行期配置统一进入 runtimeConfigService；
// - 桌面托管模式需要的 listener、bootstrap 钩子等继续走这里，不进快照。
type RunOptions struct {
	ConfigPath  string
	StartupMode StartupMode

	DisableHTTPServer bool
	FSHTTPListener    net.Listener

	EffectivePrivKeyHex string
	ObsSink             obs.Sink

	ActionChain poolcore.ChainClient
	WalletChain walletChainClient
	RPCTrace    pproto.TraceSink

	PostWorkspaceBootstrap func(ctx context.Context, store ClientStore) error
}

type Runtime struct {
	Host host.Host
	ctx  context.Context
	// DB              *sql.DB
	// DBActor         *sqliteactor.Actor
	store           *clientDB
	modules         *moduleRegistry
	identity        *clientIdentityCaps
	config          *runtimeConfigService
	StartedAtUnix   int64
	HealthyGWs      []peer.AddrInfo
	HealthyArbiters []peer.AddrInfo
	Workspace       *workspaceManager
	Catalog         *sellerCatalog
	HTTP            *httpAPIServer
	FSHTTP          *fileHTTPServer

	ActionChain poolcore.ChainClient
	WalletChain walletChainClient
	feePoolsMu  sync.RWMutex
	feePools    map[string]*feePoolSession
	// feePoolPayLocks 按 gateway 串行化费用池扣费路径（listen cycle / publish demand / publish live demand）。
	// 设计约束：同一 gateway 只能有一个扣费请求在飞，避免 sequence/server_amount 并发竞争。
	feePoolPayLocksMu sync.Mutex
	feePoolPayLocks   map[string]*sync.Mutex
	tripleMu          sync.RWMutex
	triplePool        map[string]*triplePoolSession

	// 设计说明：
	// - open 阶段涉及 deal/session 建立与钱包输入准备，仍用全局锁保证顺序；
	// - pay/close 阶段按 session 串行，不同 session 允许并发，支撑多卖家并行下载。
	transferPoolOpenMu         sync.Mutex
	transferPoolSessionLocksMu sync.Mutex
	transferPoolSessionLocks   map[string]*sync.Mutex
	// walletAllocMu 保证“钱包 UTXO 分配”串行执行，避免并发选中同一输入导致冲突。
	// 分配完成后，基于专属 UTXO 的后续池内操作可并行。
	walletAllocMu sync.Mutex

	rpcTrace            pproto.TraceSink
	live                *liveRuntime
	sqlTraceDir         string
	sqlTraceSummaryPath string
	// 运行时状态
	gwManager   *gatewayManager
	masterGW    peer.ID
	masterGWMu  sync.RWMutex
	kernel      *clientKernel
	orch        *orchestrator
	chainMaint  *chainMaintainer
	taskSched   *taskScheduler
	taskSchedMu sync.Mutex

	bgCancel context.CancelFunc

	closeOnce sync.Once
	closeFn   func() error
}

// NewPricingTestRuntime 只给测试和桥接夹具用。
// 设计说明：
// - 它放在运行入口文件，避免把 newClientDB 带到控制/业务文件里；
// - 这里只装最小 runtime，不走完整启动流程。
func NewPricingTestRuntime(ctx context.Context, db *sql.DB, cfg Config) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if err := ApplyConfigDefaultsForMode(&cfg, StartupModeTest); err != nil {
		return nil, err
	}
	cfgSvc, err := newRuntimeConfigService(cfg, "", StartupModeTest)
	if err != nil {
		return nil, err
	}
	var store *clientDB
	if db != nil {
		store = newClientDB(db, nil)
	}
	rt := &Runtime{
		ctx:     ctx,
		store:   store,
		modules: newModuleRegistry(),
		config:  cfgSvc,
	}
	rt.Catalog = newSellerCatalog()
	rt.kernel = newClientKernel(rt, store, nil)
	if store != nil {
		if seeds, err := dbLoadSeedCatalogSnapshot(ctx, store); err == nil {
			rt.Catalog.Replace(seeds)
		}
	}
	return rt, nil
}

// Store 返回已经准备好的客户端 store 能力。
// 说明：
// - 这里只暴露能力接口，不暴露底层 *sql.DB；
// - 上层触发入口只拿到这个能力即可，不需要也不应该自己重建 DB。
func (r *Runtime) Store() ClientStore {
	if r == nil {
		return nil
	}
	return r.store
}

// ClientKernel 返回运行时内核能力。
// 说明：跨包调用只拿 kernel，不再复制一层 workspace 控制壳。
func (r *Runtime) ClientKernel() *Kernel {
	if r == nil {
		return nil
	}
	return r.kernel
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			err = r.closeFn()
		}
	})
	return err
}

func (r *Runtime) ClientID() string {
	if r == nil {
		return ""
	}
	identity, err := r.runtimeIdentity()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(identity.ClientID)
}

func (r *Runtime) TransferHost() host.Host {
	if r == nil {
		return nil
	}
	return r.Host
}

func (r *Runtime) TransferActionChain() poolcore.ChainClient {
	if r == nil {
		return nil
	}
	return r.ActionChain
}

func (r *Runtime) WalletChainClient() walletChainClient {
	if r == nil {
		return nil
	}
	return r.WalletChain
}

func (r *Runtime) TransferRPCTrace() pproto.TraceSink {
	if r == nil {
		return nil
	}
	return r.rpcTrace
}

// runtimeIdentity 返回运行时只读身份能力。
// 设计约束：身份只在启动装配一次，业务侧只读，不再回看旧启动配置。
func (r *Runtime) runtimeIdentity() (*clientIdentityCaps, error) {
	if r == nil || r.identity == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return r.identity, nil
}

// mustIdentity 是内部快捷入口，调用方仍然要判空并在需要时返回业务错误。
func (r *Runtime) mustIdentity() *clientIdentityCaps {
	identity, _ := r.runtimeIdentity()
	return identity
}

// DB 返回运行时底层 clientDB 指针，供 e2e 测试和内部桥接函数访问。
// 设计约束：只读场景可直接用，写入场景优先走显式 store 参数传递。
func (r *Runtime) DB() *clientDB {
	if r == nil {
		return nil
	}
	return r.store
}

// ConfigSnapshot 返回运行期配置的不可变快照。
// 设计约束：业务层只读快照，不直接碰可变内存对象。
func (r *Runtime) ConfigSnapshot() Config {
	if r == nil || r.config == nil {
		return Config{}
	}
	return r.config.Snapshot()
}

// RuntimeConfigService 返回运行期配置服务。
// 设计说明：管理面写配置时使用它，业务读取优先用 Snapshot 或更小的能力接口。
func (r *Runtime) RuntimeConfigService() *runtimeConfigService {
	if r == nil {
		return nil
	}
	return r.config
}

// MasterGateway 返回当前主网关。
// 设计说明：主网关单独加锁，避免和配置快照锁混用。
func (r *Runtime) MasterGateway() peer.ID {
	if r == nil {
		return ""
	}
	r.masterGWMu.RLock()
	defer r.masterGWMu.RUnlock()
	return r.masterGW
}

// SetMasterGateway 切换主网关并返回旧值。
// 设计说明：统一由 Runtime 自己持有主网关锁，外部不再直接摸字段。
func (r *Runtime) SetMasterGateway(next peer.ID) peer.ID {
	if r == nil {
		return ""
	}
	r.masterGWMu.Lock()
	defer r.masterGWMu.Unlock()
	old := r.masterGW
	r.masterGW = next
	return old
}

func (r *Runtime) HTTPListenAddr() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.ConfigSnapshot().HTTP.ListenAddr)
}

func (r *Runtime) FSHTTPListenAddr() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.ConfigSnapshot().FSHTTP.ListenAddr)
}

func (r *Runtime) WorkspaceDir() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.ConfigSnapshot().Storage.WorkspaceDir)
}

func (r *Runtime) BSVNetwork() string {
	if r == nil {
		return ""
	}
	identity, err := r.runtimeIdentity()
	if err != nil || identity == nil {
		return "test"
	}
	if identity.IsMainnet {
		return "main"
	}
	return "test"
}

func (r *Runtime) NetworkArbiters() []PeerNode {
	if r == nil {
		return nil
	}
	cfg := r.ConfigSnapshot()
	out := make([]PeerNode, len(cfg.Network.Arbiters))
	copy(out, cfg.Network.Arbiters)
	return out
}

func (r *Runtime) SellerEnabled() bool {
	if r == nil {
		return false
	}
	return r.ConfigSnapshot().Seller.Enabled
}

func (r *Runtime) SQLTraceDir() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.sqlTraceDir)
}

func (r *Runtime) SQLTraceSummaryPath() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.sqlTraceSummaryPath)
}

func Run(ctx context.Context, cfg Config, deps RunDeps, opt RunOptions) (*Runtime, error) {
	phasePlan := newRunStartupPhases(ctx, cfg, deps, opt)
	success := false
	defer func() {
		if !success {
			phasePlan.Cleanup()
		}
	}()
	if err := phasePlan.Preflight(); err != nil {
		return nil, err
	}
	if err := phasePlan.BuildCore(); err != nil {
		return nil, err
	}
	if err := phasePlan.ConnectExternal(); err != nil {
		return nil, err
	}
	if err := phasePlan.StartServices(); err != nil {
		return nil, err
	}
	if err := phasePlan.BindShutdown(); err != nil {
		return nil, err
	}
	success = true
	return phasePlan.Runtime(), nil
}

func ApplyConfigDefaults(cfg *Config) error {
	return ApplyConfigDefaultsForMode(cfg, StartupModeProduct)
}

// ApplyConfigDefaultsForMode 根据启动模式补齐配置。
// 设计说明：
// - 这里不再让测试态自动补齐基础网关/仲裁；
// - 产品态继续保留原有缺省注入，少配时自动补。
func ApplyConfigDefaultsForMode(cfg *Config, mode StartupMode) error {
	return applyConfigDefaultsForMode(cfg, mode)
}

func applyConfigDefaultsForMode(cfg *Config, mode StartupMode) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	startupMode, err := normalizeStartupMode(mode)
	if err != nil {
		return err
	}
	// BSV：仅支持 test/main 两种网络；默认 test。
	{
		n, err := NormalizeBSVNetwork(cfg.BSV.Network)
		if err != nil {
			return err
		}
		cfg.BSV.Network = n
	}
	networkDefaults, err := networkInitDefaults(cfg.BSV.Network)
	if err != nil {
		return err
	}
	if startupMode == StartupModeProduct {
		// 内置网关/仲裁是运行底座，必须强制并入运行态，且不可被文件覆盖。
		warnMandatoryPeerOverrides("gateway", cfg.Network.Gateways, networkDefaults.DefaultGateways)
		warnMandatoryPeerOverrides("arbiter", cfg.Network.Arbiters, networkDefaults.DefaultArbiters)
		cfg.Network.Gateways = mergeMandatoryPeerNodes(cfg.Network.Gateways, networkDefaults.DefaultGateways)
		cfg.Network.Arbiters = mergeMandatoryPeerNodes(cfg.Network.Arbiters, networkDefaults.DefaultArbiters)
	}
	if cfg.Index.Backend == "" {
		cfg.Index.Backend = networkDefaults.IndexBackend
	}
	if cfg.Index.SQLitePath == "" {
		cfg.Index.SQLitePath = networkDefaults.IndexSQLitePath
	}
	if cfg.Seller.Pricing.FloorPriceSatPer64K == 0 {
		cfg.Seller.Pricing.FloorPriceSatPer64K = networkDefaults.SellerFloorPriceSatPer64K
	}
	if cfg.Seller.Pricing.ResaleDiscountBPS == 0 {
		cfg.Seller.Pricing.ResaleDiscountBPS = networkDefaults.SellerResaleDiscountBPS
	}
	if cfg.Seller.Pricing.LiveBasePriceSatPer64K == 0 {
		cfg.Seller.Pricing.LiveBasePriceSatPer64K = cfg.Seller.Pricing.FloorPriceSatPer64K * networkDefaults.SellerLiveBaseMultiplier
		if cfg.Seller.Pricing.LiveBasePriceSatPer64K == 0 {
			cfg.Seller.Pricing.LiveBasePriceSatPer64K = networkDefaults.SellerFloorPriceSatPer64K * networkDefaults.SellerLiveBaseMultiplier
		}
	}
	if cfg.Seller.Pricing.LiveFloorPriceSatPer64K == 0 {
		cfg.Seller.Pricing.LiveFloorPriceSatPer64K = cfg.Seller.Pricing.FloorPriceSatPer64K
	}
	if cfg.Seller.Pricing.LiveDecayPerMinuteBPS == 0 {
		cfg.Seller.Pricing.LiveDecayPerMinuteBPS = networkDefaults.SellerLiveDecayPerMinuteBPS
	}
	if cfg.Live.Buyer.TargetLagSegments == 0 {
		cfg.Live.Buyer.TargetLagSegments = networkDefaults.LiveBuyerTargetLagSegments
	}
	if cfg.Live.Publish.BroadcastWindow == 0 {
		cfg.Live.Publish.BroadcastWindow = networkDefaults.LivePublishBroadcastWindow
	}
	if cfg.Live.Publish.BroadcastIntervalSec == 0 {
		cfg.Live.Publish.BroadcastIntervalSec = networkDefaults.LivePublishIntervalSeconds
	}
	if cfg.Listen.Enabled == nil {
		v := networkDefaults.ListenEnabled
		cfg.Listen.Enabled = &v
	}
	if cfg.Listen.RenewThresholdSeconds == 0 {
		cfg.Listen.RenewThresholdSeconds = networkDefaults.ListenRenewThresholdSeconds
	}
	if cfg.Listen.AutoRenewRounds == 0 {
		cfg.Listen.AutoRenewRounds = networkDefaults.ListenAutoRenewRounds
	}
	if cfg.Listen.TickSeconds == 0 {
		cfg.Listen.TickSeconds = networkDefaults.ListenTickSeconds
	}
	scheme, err := normalizePreferredPaymentScheme(cfg.Payment.PreferredScheme)
	if err != nil {
		return err
	}
	cfg.Payment.PreferredScheme = scheme
	cfg.Domain.ResolveOrder = normalizeStringList(cfg.Domain.ResolveOrder)
	if cfg.Reachability.AutoAnnounceEnabled == nil {
		v := true
		cfg.Reachability.AutoAnnounceEnabled = &v
	}
	if cfg.Reachability.AnnounceTTLSeconds == 0 {
		cfg.Reachability.AnnounceTTLSeconds = 3600
	}
	if cfg.Scan.RescanIntervalSeconds == 0 {
		cfg.Scan.RescanIntervalSeconds = networkDefaults.ScanRescanIntervalSeconds
	}
	if cfg.Storage.MinFreeBytes == 0 {
		cfg.Storage.MinFreeBytes = networkDefaults.StorageMinFreeBytes
	}
	if strings.TrimSpace(cfg.HTTP.ListenAddr) == "" {
		cfg.HTTP.ListenAddr = networkDefaults.HTTPListenAddr
	}
	if strings.TrimSpace(cfg.FSHTTP.ListenAddr) == "" {
		cfg.FSHTTP.ListenAddr = networkDefaults.FSHTTPListenAddr
	}
	if cfg.FSHTTP.DownloadWaitTimeoutSeconds == 0 {
		cfg.FSHTTP.DownloadWaitTimeoutSeconds = networkDefaults.FSHTTPDownloadWaitSeconds
	}
	if cfg.FSHTTP.MaxConcurrentSessions == 0 {
		cfg.FSHTTP.MaxConcurrentSessions = networkDefaults.FSHTTPMaxConcurrentSessions
	}
	if cfg.FSHTTP.QuoteWaitSeconds == 0 {
		cfg.FSHTTP.QuoteWaitSeconds = networkDefaults.FSHTTPQuoteWaitSeconds
	}
	if cfg.FSHTTP.QuotePollSeconds == 0 {
		cfg.FSHTTP.QuotePollSeconds = networkDefaults.FSHTTPQuotePollSeconds
	}
	if cfg.FSHTTP.PrefetchDistanceChunks == 0 {
		cfg.FSHTTP.PrefetchDistanceChunks = networkDefaults.FSHTTPPrefetchDistanceChunks
	}
	// 设计说明：
	// - 外部 API 保护统一放在 external_api 下做透明管理；
	// - 但每个 provider 的频率策略独立，不能共用一个桶；
	// - 当前默认让 WOC 更保守，1sat 兼容资产索引稍快一些。
	if strings.TrimSpace(cfg.ExternalAPI.WOC.APIKey) == "" && strings.TrimSpace(cfg.WOCAPIKey) != "" {
		cfg.ExternalAPI.WOC.APIKey = strings.TrimSpace(cfg.WOCAPIKey)
		cfg.WOCAPIKey = ""
	}
	if cfg.ExternalAPI.WOC.MinIntervalMS == 0 {
		cfg.ExternalAPI.WOC.MinIntervalMS = 1000
	}
	if strings.TrimSpace(cfg.Log.ConsoleMinLevel) == "" {
		cfg.Log.ConsoleMinLevel = networkDefaults.LogConsoleMinLevel
	}
	return nil
}

func ParseConfigTOML(data []byte) (Config, error) {
	// 历史字段迁移：金额语义已下线，统一收敛到“续费轮数”。
	// 迁移后仍走严格模式，避免把未知字段静默吞掉。
	normalized := strings.ReplaceAll(string(data), "max_auto_renew_amount", "auto_renew_rounds")
	// 历史字段迁移：管理 API token 语义已移除。
	// 这里仅做读时清理，避免旧 DB 配置因严格解析失败。
	normalized = stripDeprecatedTOMLKeyLines(normalized, map[string]struct{}{
		"auth_token": {},
	})
	// 历史字段迁移：旧资产索引能力已整体下线。
	// 旧配置文件即使还留着该 section，也不应继续影响当前启动。
	normalized = stripTOMLSections(normalized, map[string]struct{}{
		"external_api.asset_index": {},
	})
	var cfg Config
	dec := toml.NewDecoder(strings.NewReader(normalized))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func stripDeprecatedTOMLKeyLines(src string, keys map[string]struct{}) string {
	if strings.TrimSpace(src) == "" || len(keys) == 0 {
		return src
	}
	lines := strings.Split(src, "\n")
	dst := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			dst = append(dst, line)
			continue
		}
		candidate := trimmed
		if i := strings.Index(candidate, "="); i >= 0 {
			candidate = strings.TrimSpace(candidate[:i])
		}
		if _, deprecated := keys[candidate]; deprecated {
			continue
		}
		dst = append(dst, line)
	}
	return strings.Join(dst, "\n")
}

func stripTOMLSections(src string, sections map[string]struct{}) string {
	if strings.TrimSpace(src) == "" || len(sections) == 0 {
		return src
	}
	lines := strings.Split(src, "\n")
	dst := make([]string, 0, len(lines))
	skip := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
			_, shouldSkip := sections[name]
			skip = shouldSkip
			if shouldSkip {
				continue
			}
		}
		if skip {
			continue
		}
		dst = append(dst, line)
	}
	return strings.Join(dst, "\n")
}

func EncodeConfigTOML(cfg Config) ([]byte, error) {
	return toml.Marshal(cfg)
}

func ResolveLogConfig(cfg *Config) (string, string) {
	logFile := strings.TrimSpace(cfg.Log.File)
	if logFile == "" {
		logFile = filepath.Join("logs", "bitfs.log")
		if strings.TrimSpace(cfg.Storage.DataDir) != "" {
			logFile = filepath.Join(cfg.Storage.DataDir, "logs", "bitfs.log")
		}
	}
	consoleMin := strings.TrimSpace(cfg.Log.ConsoleMinLevel)
	if consoleMin == "" {
		consoleMin = obs.LevelNone
	}
	return filepath.Clean(logFile), consoleMin
}

func normalizeStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func validateConfig(cfg *Config) error {
	return validateConfigForMode(cfg, StartupModeProduct)
}

// validateConfigForMode 明确区分“允许为空”和“必须补齐”。
// product 模式下，空网关/空仲裁都会被拦住；test 模式允许保留最小配置。
func validateConfigForMode(cfg *Config, mode StartupMode) error {
	startupMode, err := normalizeStartupMode(mode)
	if err != nil {
		return err
	}
	n, err := NormalizeBSVNetwork(cfg.BSV.Network)
	if err != nil {
		return err
	}
	cfg.BSV.Network = n

	workspaceDir := strings.TrimSpace(cfg.Storage.WorkspaceDir)
	if workspaceDir == "" {
		cfg.Storage.WorkspaceDir = ""
	} else {
		cfg.Storage.WorkspaceDir = filepath.Clean(workspaceDir)
	}
	cfg.Storage.DataDir = filepath.Clean(strings.TrimSpace(cfg.Storage.DataDir))
	if cfg.Storage.DataDir == "" {
		return errors.New("storage.data_dir is required")
	}
	if cfg.Storage.WorkspaceDir != "" {
		if cfg.Storage.WorkspaceDir == cfg.Storage.DataDir {
			return errors.New("workspace_dir and data_dir must be different")
		}
		if overlaps(cfg.Storage.WorkspaceDir, cfg.Storage.DataDir) {
			return errors.New("workspace_dir and data_dir must not overlap")
		}
	}
	if len(cfg.Network.Gateways) == 0 {
		if startupMode == StartupModeProduct {
			return errors.New("network.gateways is required")
		}
	} else {
		if err := validateNetworkPeers(cfg.Network.Gateways, true); err != nil {
			return err
		}
	}
	if len(cfg.Network.Arbiters) == 0 {
		if startupMode == StartupModeProduct {
			return errors.New("network.arbiters is required")
		}
	} else {
		if err := validateNetworkPeers(cfg.Network.Arbiters, false); err != nil {
			return err
		}
	}
	if cfg.Index.Backend != "sqlite" {
		return errors.New("index.backend must be sqlite in phase1")
	}
	cfg.Index.SQLitePath = filepath.Clean(strings.TrimSpace(cfg.Index.SQLitePath))
	if cfg.Index.SQLitePath == "" {
		return errors.New("index.sqlite_path is required")
	}
	if cfg.Seller.Pricing.ResaleDiscountBPS > 10000 {
		return errors.New("seller.pricing.resale_discount_bps must be <= 10000")
	}
	if cfg.Seller.Pricing.LiveFloorPriceSatPer64K > cfg.Seller.Pricing.LiveBasePriceSatPer64K {
		return errors.New("seller.pricing.live_floor_price_sat_per_64k must be <= seller.pricing.live_base_price_sat_per_64k")
	}
	if cfg.Seller.Pricing.LiveDecayPerMinuteBPS > 10000 {
		return errors.New("seller.pricing.live_decay_per_minute_bps must be <= 10000")
	}
	if cfg.Live.Publish.BroadcastWindow == 0 || cfg.Live.Publish.BroadcastWindow > maxLiveWindowSize {
		return fmt.Errorf("live.publish.broadcast_window must be between 1 and %d", maxLiveWindowSize)
	}
	if cfg.Live.Publish.BroadcastIntervalSec == 0 {
		return errors.New("live.publish.broadcast_interval_seconds must be > 0")
	}
	if strings.TrimSpace(cfg.FSHTTP.ListenAddr) == "" {
		return errors.New("fs_http.listen_addr is required")
	}
	if cfg.FSHTTP.MaxConcurrentSessions == 0 {
		return errors.New("fs_http.max_concurrent_sessions must be > 0")
	}
	if cfg.FSHTTP.DownloadWaitTimeoutSeconds == 0 {
		return errors.New("fs_http.download_wait_timeout_seconds must be > 0")
	}
	if strings.TrimSpace(cfg.HTTP.ListenAddr) == "" {
		return errors.New("http.listen_addr is required")
	}
	if cfg.Reachability.AnnounceTTLSeconds == 0 {
		return errors.New("reachability.announce_ttl_seconds must be > 0")
	}
	cfg.ExternalAPI.WOC.APIKey = strings.TrimSpace(cfg.ExternalAPI.WOC.APIKey)
	return nil
}

// ValidateConfig 对外提供启动前配置校验，失败即中止启动。
func ValidateConfig(cfg *Config) error {
	return validateConfig(cfg)
}

func normalizeStartupMode(mode StartupMode) (StartupMode, error) {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "", string(StartupModeProduct):
		return StartupModeProduct, nil
	case string(StartupModeTest):
		return StartupModeTest, nil
	default:
		return "", fmt.Errorf("unsupported startup mode: %s", strings.TrimSpace(string(mode)))
	}
}

func validateNetworkPeers(items []PeerNode, requireEnabled bool) error {
	seenPub := map[string]struct{}{}
	for i, p := range items {
		if requireEnabled && strings.TrimSpace(p.Addr) == "" {
			return fmt.Errorf("network entry[%d] addr required", i)
		}
		if strings.TrimSpace(p.Addr) == "" || strings.TrimSpace(p.Pubkey) == "" {
			return fmt.Errorf("network entry[%d] addr/pubkey required", i)
		}
		pk := strings.ToLower(strings.TrimSpace(p.Pubkey))
		if _, ok := seenPub[pk]; ok {
			return fmt.Errorf("duplicate pubkey in network config: %s", pk)
		}
		seenPub[pk] = struct{}{}
		peerIDFromCfg, err := peerIDFromSecp256k1PubHex(pk)
		if err != nil {
			return err
		}
		addrInfo, err := parseAddr(p.Addr)
		if err != nil {
			return err
		}
		if addrInfo.ID != peerIDFromCfg {
			return fmt.Errorf("addr transport_peer_id mismatch for pubkey=%s", pk)
		}
	}
	return nil
}

func initDataDirs(cfg *Config) error {
	dirs := []string{
		cfg.Storage.DataDir,
		filepath.Join(cfg.Storage.DataDir, "config"),
		filepath.Join(cfg.Storage.DataDir, "biz_seeds"),
		filepath.Join(cfg.Storage.DataDir, "db"),
		filepath.Join(cfg.Storage.DataDir, "keys"),
		filepath.Join(cfg.Storage.DataDir, "logs"),
	}
	if strings.TrimSpace(cfg.Storage.WorkspaceDir) != "" {
		dirs = append(dirs, cfg.Storage.WorkspaceDir)
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	if strings.TrimSpace(cfg.Storage.WorkspaceDir) != "" {
		if err := ensureReadableDir(cfg.Storage.WorkspaceDir); err != nil {
			return err
		}
	}
	if err := ensureWritableDir(cfg.Storage.DataDir); err != nil {
		return err
	}
	if err := ensureMinFreeSpace(cfg.Storage.DataDir, cfg.Storage.MinFreeBytes); err != nil {
		return err
	}
	return nil
}

func ensureReadableDir(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !s.IsDir() {
		return fmt.Errorf("not a directory: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, _ = f.Readdirnames(1)
	return nil
}

func ensureWritableDir(path string) error {
	f, err := os.CreateTemp(path, ".write-check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	return os.Remove(name)
}

func ensureMinFreeSpace(path string, minBytes uint64) error {
	free, err := freeBytesUnderPath(path)
	if err != nil {
		return err
	}
	if free < minBytes {
		return fmt.Errorf("insufficient free space under %s: have=%d need=%d", path, free, minBytes)
	}
	return nil
}

// 设计说明：
// - 运行时正式入口统一走 infra/sqliteactor.Open；
// - 这里保留给直接 sql.Open("sqlite", ...) 的测试库做最小 WAL 初始化；
// - 不再在这里叠加 busy_timeout 之类并发补丁，避免测试口径和正式口径分裂。

// 启用外键约束（硬切版 schema 依赖外键约束）

// 注意：数据库结构由 contract 的 ent schema 管理，通过 ensureClientDBSchema 入口调用。
// 该文件不再包含数据库初始化逻辑，只保留启动顺序。

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func cfgBool(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// registerDirectQuoteSubmitHandler 注册买方接收报价入口。
// 设计说明：
// - direct quote 是“卖方 -> 买方”回推路径，买方即便不是 seller 模式也必须可接收；
// - 该入口只负责落库 biz_demand_quotes 和 biz_demand_quote_arbiters，不涉及卖方资源读取，因此可全端默认启用。
func registerDirectQuoteSubmitHandler(h host.Host, store *clientDB, trace pproto.TraceSink) {
	pproto.HandleProto[directQuoteSubmitReq, directQuoteSubmitResp](h, ProtoQuoteDirectSubmit, clientSec(trace), func(ctx context.Context, req directQuoteSubmitReq) (directQuoteSubmitResp, error) {
		if strings.TrimSpace(req.DemandId) == "" || strings.TrimSpace(req.SellerPubkeyHex) == "" || req.SeedPrice == 0 || req.ChunkPrice == 0 {
			return directQuoteSubmitResp{}, fmt.Errorf("invalid direct quote")
		}
		sellerPubHex, err := normalizeCompressedPubKeyHex(req.SellerPubkeyHex)
		if err != nil {
			return directQuoteSubmitResp{}, fmt.Errorf("invalid seller pubkey")
		}
		if req.ExpiresAtUnix > 0 && req.ExpiresAtUnix < time.Now().Unix() {
			return directQuoteSubmitResp{}, fmt.Errorf("direct quote expired")
		}
		if err := dbUpsertDirectQuote(ctx, store, req, sellerPubHex); err != nil {
			return directQuoteSubmitResp{}, err
		}
		return directQuoteSubmitResp{Status: "stored"}, nil
	})
}

func registerSellerHandlers(rt *Runtime, h host.Host, store *clientDB, live *liveRuntime, trace pproto.TraceSink, cfg Config) {
	if store == nil || rt == nil || rt.Catalog == nil {
		return
	}
	pproto.HandleProto[dealprod.DemandAnnounceReq, dealprod.DemandAnnounceResp](h, protocol.ID(dealprod.ProtoDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.DemandAnnounceReq) (dealprod.DemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || seedHash == "" || buyerPeerID == "" || req.ChunkCount == 0 {
			return dealprod.DemandAnnounceResp{}, fmt.Errorf("invalid demand announce")
		}
		if err := dbRecordDemand(ctx, store, demandID, seedHash); err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		seed, ok := rt.Catalog.Get(seedHash)
		if !ok {
			obs.Business(ServiceName, "demand_announce_ignored_no_seed", map[string]any{
				"demand_id":   demandID,
				"seed_hash":   seedHash,
				"buyer_peer":  buyerPeerID,
				"chunk_count": req.ChunkCount,
			})
			return dealprod.DemandAnnounceResp{Status: "ignored_no_seed"}, nil
		}
		pricing, pricingErr := currentSeedLiveSellerPricing(ctx, store, cfg, seedHash)
		if pricingErr != nil {
			return dealprod.DemandAnnounceResp{}, pricingErr
		}
		seed = ComputeLiveQuotePrices(seed, liveSegmentMeta{}, pricing, time.Now())
		if live != nil {
			if liveMeta, ok := live.segment(seedHash); ok {
				seed = ComputeLiveQuotePrices(seed, liveMeta, pricing, time.Now())
			}
		}
		availableChunks, err := dbListSeedChunkSupply(ctx, store, seedHash)
		if err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		if len(availableChunks) == 0 {
			obs.Business(ServiceName, "demand_announce_ignored_no_chunks", map[string]any{
				"demand_id":  demandID,
				"seed_hash":  seedHash,
				"buyer_peer": buyerPeerID,
			})
			return dealprod.DemandAnnounceResp{Status: "ignored_no_chunks"}, nil
		}
		if err := submitDirectQuote(ctx, h, trace, DirectQuoteParams{
			DemandID:            demandID,
			BuyerPubHex:         buyerPeerID,
			BuyerAddrs:          req.BuyerAddrs,
			SeedPrice:           seed.SeedPrice,
			ChunkPrice:          seed.ChunkPrice,
			ChunkCount:          seed.ChunkCount,
			FileSizeBytes:       seed.FileSize,
			ExpiresAtUnix:       time.Now().Add(10 * time.Minute).Unix(),
			RecommendedFileName: seed.RecommendedFileName,
			MimeType:            seed.MIMEHint,
			ArbiterPubHexes:     configuredArbiterPubHexes(cfg),
			AvailableChunkBitmapHex: chunkBitmapHexFromIndexes(
				availableChunks,
				seed.ChunkCount,
			),
		}); err != nil {
			return dealprod.DemandAnnounceResp{}, err
		}
		obs.Business(ServiceName, "demand_announce_quote_submitted", map[string]any{
			"demand_id":   demandID,
			"seed_hash":   seedHash,
			"buyer_peer":  buyerPeerID,
			"seed_price":  seed.SeedPrice,
			"chunk_price": seed.ChunkPrice,
			"chunk_have":  len(availableChunks),
		})
		return dealprod.DemandAnnounceResp{Status: "quoted"}, nil
	})
	pproto.HandleProto[dealprod.LiveDemandAnnounceReq, dealprod.LiveDemandAnnounceResp](h, protocol.ID(dealprod.ProtoLiveDemandAnnounce), clientSec(trace), func(ctx context.Context, req dealprod.LiveDemandAnnounceReq) (dealprod.LiveDemandAnnounceResp, error) {
		demandID := strings.TrimSpace(req.DemandID)
		streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
		buyerPeerID := strings.TrimSpace(req.BuyerPeerID)
		if demandID == "" || !isSeedHashHex(streamID) || buyerPeerID == "" || req.Window == 0 {
			return dealprod.LiveDemandAnnounceResp{}, fmt.Errorf("invalid live demand announce")
		}
		recentSegments, latestIndex, err := listLocalLiveQuoteSegments(ctx, store, streamID, int(req.Window))
		if err != nil {
			return dealprod.LiveDemandAnnounceResp{}, err
		}
		if len(recentSegments) == 0 {
			obs.Business(ServiceName, "live_demand_announce_ignored_no_stream", map[string]any{
				"demand_id":  demandID,
				"stream_id":  streamID,
				"buyer_peer": buyerPeerID,
			})
			return dealprod.LiveDemandAnnounceResp{Status: "ignored_no_stream"}, nil
		}
		if err := submitLiveQuote(ctx, h, trace, LiveQuoteParams{
			DemandID:           demandID,
			BuyerPeerID:        buyerPeerID,
			BuyerAddrs:         req.BuyerAddrs,
			StreamID:           streamID,
			LatestSegmentIndex: latestIndex,
			RecentSegments:     recentSegments,
			ExpiresAtUnix:      time.Now().Add(2 * time.Minute).Unix(),
		}); err != nil {
			return dealprod.LiveDemandAnnounceResp{}, err
		}
		obs.Business(ServiceName, "live_demand_announce_quote_submitted", map[string]any{
			"demand_id":            demandID,
			"stream_id":            streamID,
			"buyer_peer":           buyerPeerID,
			"latest_segment_index": latestIndex,
			"segment_count":        len(recentSegments),
		})
		return dealprod.LiveDemandAnnounceResp{Status: "quoted"}, nil
	})

	pproto.HandleProto[seedGetReq, seedGetResp](h, ProtoSeedGet, clientSec(trace), func(ctx context.Context, req seedGetReq) (seedGetResp, error) {
		seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
		if strings.TrimSpace(req.SessionId) == "" || seedHash == "" {
			return seedGetResp{}, fmt.Errorf("invalid params")
		}
		row, err := dbLoadDirectTransferPoolRow(ctx, store, strings.TrimSpace(req.SessionId))
		if err != nil {
			return seedGetResp{}, fmt.Errorf("session not found")
		}
		dealSeedHash, err := dbLoadDirectDealSeedHash(ctx, store, strings.TrimSpace(row.DealID))
		if err != nil {
			return seedGetResp{}, fmt.Errorf("deal not found")
		}
		if seedHash != strings.ToLower(strings.TrimSpace(dealSeedHash)) {
			return seedGetResp{}, fmt.Errorf("seed_hash mismatch")
		}
		seedBytes, err := dbLoadSeedBytesBySeedHash(ctx, store, seedHash)
		if err != nil {
			return seedGetResp{}, err
		}
		return seedGetResp{Seed: append([]byte(nil), seedBytes...)}, nil
	})
	pproto.HandleProto[directDealAcceptReq, directDealAcceptResp](h, ProtoDirectDealAccept, clientSec(trace), func(ctx context.Context, req directDealAcceptReq) (directDealAcceptResp, error) {
		if strings.TrimSpace(req.DemandId) == "" || strings.TrimSpace(req.BuyerPubkeyHex) == "" || strings.TrimSpace(req.SeedHash) == "" || req.SeedPrice == 0 || req.ChunkPrice == 0 {
			return directDealAcceptResp{}, fmt.Errorf("invalid direct deal accept")
		}
		buyerPubHex, err := normalizeCompressedPubKeyHex(req.BuyerPubkeyHex)
		if err != nil {
			return directDealAcceptResp{}, fmt.Errorf("invalid buyer pubkey")
		}
		sellerPubHex, err := normalizeCompressedPubKeyHex(localPubHex(h))
		if err != nil {
			return directDealAcceptResp{}, fmt.Errorf("invalid seller pubkey")
		}
		if req.ExpiresAtUnix > 0 && req.ExpiresAtUnix < time.Now().Unix() {
			return directDealAcceptResp{}, fmt.Errorf("direct quote expired")
		}
		dealID := "ddeal_" + randHex(8)
		if err := dbInsertDirectDeal(ctx, store, dealID, req, buyerPubHex, sellerPubHex); err != nil {
			return directDealAcceptResp{}, err
		}
		return directDealAcceptResp{
			DealId:          dealID,
			SellerPubkeyHex: sellerPubHex,
			ChunkPrice:      req.ChunkPrice,
			Status:          "accepted",
		}, nil
	})
	pproto.HandleProto[directTransferPoolOpenReq, directTransferPoolOpenResp](h, ProtoTransferPoolOpen, clientSec(trace), func(ctx context.Context, req directTransferPoolOpenReq) (directTransferPoolOpenResp, error) {
		return handleDirectTransferPoolOpen(ctx, h, store, cfg, req)
	})
	pproto.HandleProto[directTransferChunkGetReq, directTransferChunkGetResp](h, ProtoTransferChunkGet, clientSec(trace), func(ctx context.Context, req directTransferChunkGetReq) (directTransferChunkGetResp, error) {
		return handleDirectTransferChunkGet(ctx, h, store, cfg, req)
	})
	pproto.HandleProto[directTransferPoolPayReq, directTransferPoolPayResp](h, ProtoTransferPoolPay, clientSec(trace), func(ctx context.Context, req directTransferPoolPayReq) (directTransferPoolPayResp, error) {
		return handleDirectTransferPoolPay(ctx, rt, h, store, cfg, req)
	})
	pproto.HandleProto[directTransferArbitrateReq, directTransferArbitrateResp](h, ProtoTransferArbitrate, clientSec(trace), func(ctx context.Context, req directTransferArbitrateReq) (directTransferArbitrateResp, error) {
		return handleDirectTransferArbitrate(ctx, rt, h, store, cfg, req)
	})
	pproto.HandleProto[directTransferPoolCloseReq, directTransferPoolCloseResp](h, ProtoTransferPoolClose, clientSec(trace), func(ctx context.Context, req directTransferPoolCloseReq) (directTransferPoolCloseResp, error) {
		return handleDirectTransferPoolClose(ctx, h, store, cfg, req)
	})
}

func submitDirectQuote(ctx context.Context, h host.Host, trace pproto.TraceSink, p DirectQuoteParams) error {
	if h == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(p.DemandID) == "" || strings.TrimSpace(p.BuyerPubHex) == "" || p.SeedPrice == 0 || p.ChunkPrice == 0 {
		return fmt.Errorf("invalid params")
	}
	if p.ExpiresAtUnix == 0 {
		p.ExpiresAtUnix = time.Now().Add(10 * time.Minute).Unix()
	}
	buyerID, err := peerIDFromClientID(strings.TrimSpace(p.BuyerPubHex))
	if err != nil {
		return err
	}
	for _, raw := range p.BuyerAddrs {
		ai, err := parseAddr(strings.TrimSpace(raw))
		if err != nil || ai == nil {
			continue
		}
		h.Peerstore().AddAddrs(ai.ID, ai.Addrs, 10*time.Minute)
	}
	if err := h.Connect(ctx, peer.AddrInfo{ID: buyerID}); err != nil {
		return err
	}
	sellerClientID, err := localPubKeyHex(h)
	if err != nil {
		return err
	}
	var resp directQuoteSubmitResp
	bitmapHex, err := normalizeChunkBitmapHex(p.AvailableChunkBitmapHex)
	if err != nil {
		return err
	}
	var bitmapBytes []byte
	if bitmapHex != "" {
		bitmapBytes, err = hex.DecodeString(bitmapHex)
		if err != nil {
			return fmt.Errorf("invalid available_chunk_bitmap_hex")
		}
	}
	arbiterPubHexes, err := normalizePubHexList(p.ArbiterPubHexes)
	if err != nil {
		return err
	}
	if err := pproto.CallProto(ctx, h, buyerID, ProtoQuoteDirectSubmit, clientSec(trace), directQuoteSubmitReq{
		DemandId:             strings.TrimSpace(p.DemandID),
		SellerPubkeyHex:      strings.ToLower(strings.TrimSpace(sellerClientID)),
		SeedPrice:            p.SeedPrice,
		ChunkPrice:           p.ChunkPrice,
		ChunkCount:           p.ChunkCount,
		FileSize:             p.FileSizeBytes,
		ExpiresAtUnix:        p.ExpiresAtUnix,
		RecommendedFileName:  sanitizeRecommendedFileName(p.RecommendedFileName),
		MimeHint:             sanitizeMIMEHint(p.MimeType),
		ArbiterPubkeyHexes:   arbiterPubHexes,
		AvailableChunkBitmap: bitmapBytes,
	}, &resp); err != nil {
		return err
	}
	if strings.TrimSpace(resp.Status) != "stored" {
		return fmt.Errorf("direct quote not stored")
	}
	return nil
}

func submitLiveQuote(ctx context.Context, h host.Host, trace pproto.TraceSink, p LiveQuoteParams) error {
	if h == nil {
		return fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(p.DemandID) == "" || strings.TrimSpace(p.BuyerPeerID) == "" || !isSeedHashHex(strings.ToLower(strings.TrimSpace(p.StreamID))) || len(p.RecentSegments) == 0 {
		return fmt.Errorf("invalid params")
	}
	if p.ExpiresAtUnix == 0 {
		p.ExpiresAtUnix = time.Now().Add(2 * time.Minute).Unix()
	}
	buyerID, err := peerIDFromClientID(strings.TrimSpace(p.BuyerPeerID))
	if err != nil {
		return err
	}
	for _, raw := range p.BuyerAddrs {
		ai, err := parseAddr(strings.TrimSpace(raw))
		if err != nil || ai == nil {
			continue
		}
		h.Peerstore().AddAddrs(ai.ID, ai.Addrs, 10*time.Minute)
	}
	if err := h.Connect(ctx, peer.AddrInfo{ID: buyerID}); err != nil {
		return err
	}
	sellerClientID, err := localPubKeyHex(h)
	if err != nil {
		return err
	}
	recent := make([]*liveQuoteSegmentPB, 0, len(p.RecentSegments))
	for _, seg := range p.RecentSegments {
		seedHash := strings.ToLower(strings.TrimSpace(seg.SeedHash))
		if !isSeedHashHex(seedHash) {
			continue
		}
		recent = append(recent, &liveQuoteSegmentPB{SegmentIndex: seg.SegmentIndex, SeedHash: seedHash})
	}
	if len(recent) == 0 {
		return fmt.Errorf("empty recent segments")
	}
	var resp liveQuoteSubmitResp
	if err := pproto.CallProto(ctx, h, buyerID, ProtoLiveQuoteSubmit, clientSec(trace), liveQuoteSubmitReq{
		DemandId:           strings.TrimSpace(p.DemandID),
		SellerPubkeyHex:    strings.ToLower(strings.TrimSpace(sellerClientID)),
		StreamId:           strings.ToLower(strings.TrimSpace(p.StreamID)),
		LatestSegmentIndex: p.LatestSegmentIndex,
		RecentSegments:     recent,
		ExpiresAtUnix:      p.ExpiresAtUnix,
	}, &resp); err != nil {
		return err
	}
	if strings.TrimSpace(resp.Status) != "stored" {
		return fmt.Errorf("live quote not stored")
	}
	return nil
}

func sanitizeRecommendedFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = filepath.Base(name)
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

func sanitizeMIMEHint(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return ""
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return ""
	}
	mediaType = strings.TrimSpace(strings.ToLower(mediaType))
	if mediaType == "" || !strings.Contains(mediaType, "/") {
		return ""
	}
	return mediaType
}

func mimeHintBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	return dbMimeHintBySeedHash(ctx, store, seedHash)
}

func configuredArbiterPubHexes(cfg Config) []string {
	out := make([]string, 0, len(cfg.Network.Arbiters))
	for _, a := range cfg.Network.Arbiters {
		if !a.Enabled {
			continue
		}
		pubHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(a.Pubkey))
		if err != nil {
			continue
		}
		out = append(out, pubHex)
	}
	normalized, err := normalizePubHexList(out)
	if err != nil {
		return nil
	}
	return normalized
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func localPubHex(h host.Host) string {
	if h == nil {
		return ""
	}
	pub := h.Peerstore().PubKey(h.ID())
	if pub == nil {
		return ""
	}
	raw, err := pub.Raw()
	if err != nil {
		return ""
	}
	out, err := normalizeCompressedPubKeyHex(hex.EncodeToString(raw))
	if err != nil {
		return ""
	}
	return out
}

func connectGateways(ctx context.Context, h host.Host, gateways []PeerNode) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0)
	for _, g := range gateways {
		if !g.Enabled {
			continue
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			obs.Error(ServiceName, "gateway_addr_invalid", map[string]any{"addr": g.Addr, "error": err.Error()})
			continue
		}
		if err := h.Connect(ctx, *ai); err != nil {
			obs.Error(ServiceName, "gateway_connect_failed", map[string]any{"transport_peer_id": ai.ID.String(), "error": err.Error()})
			continue
		}
		obs.Business(ServiceName, "gateway_connected", map[string]any{"transport_peer_id": ai.ID.String(), "addr_count": len(ai.Addrs)})
		out = append(out, *ai)
	}
	// 允许零网关，返回空列表
	return out, nil
}

func connectArbiters(ctx context.Context, h host.Host, arbiters []PeerNode) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(arbiters))
	for i, a := range arbiters {
		if strings.TrimSpace(a.Addr) == "" {
			continue
		}
		ai, err := parseAddr(a.Addr)
		if err != nil {
			obs.Error(ServiceName, "arbiter_addr_invalid", map[string]any{
				"index": i,
				"addr":  a.Addr,
				"error": err.Error(),
			})
			continue
		}
		if err := h.Connect(ctx, *ai); err != nil {
			obs.Error(ServiceName, "arbiter_connect_failed", map[string]any{
				"index":             i,
				"transport_peer_id": ai.ID.String(),
				"error":             err.Error(),
			})
			continue
		}
		obs.Business(ServiceName, "arbiter_connected", map[string]any{"transport_peer_id": ai.ID.String(), "addr_count": len(ai.Addrs)})
		out = append(out, *ai)
	}
	return out, nil
}

func checkPeerHealth(ctx context.Context, h host.Host, peers []peer.AddrInfo, protoID protocol.ID, sec pproto.SecurityConfig, kind string) []peer.AddrInfo {
	const maxAttempts = 3
	out := make([]peer.AddrInfo, 0, len(peers))
	for _, p := range peers {
		var lastErr error
		ok := false
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			var health bftpv1.HealthResp
			err := pproto.CallProto(ctx, h, p.ID, protoID, sec, bftpv1.HealthReq{}, &health)
			if err == nil {
				obs.Business(ServiceName, kind+"_health_ok", map[string]any{
					"transport_peer_id": p.ID.String(),
					"status":            health.GetStatus(),
					"attempt":           attempt,
				})
				ok = true
				break
			}
			lastErr = err
			obs.Error(ServiceName, kind+"_health_failed", map[string]any{
				"transport_peer_id": p.ID.String(),
				"attempt":           attempt,
				"error":             err.Error(),
			})
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if ok {
			out = append(out, p)
			continue
		}
		obs.Error(ServiceName, kind+"_unhealthy", map[string]any{
			"transport_peer_id": p.ID.String(),
			"error":             errString(lastErr),
		})
	}
	return out
}

func buildSeedV1(path string) ([]byte, string, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, "", 0, err
	}
	fileSize := st.Size()
	if fileSize < 0 {
		return nil, "", 0, fmt.Errorf("negative file size")
	}
	chunkCount := uint32(ceilDiv(uint64(fileSize), seedBlockSize))

	buf := &bytes.Buffer{}
	buf.WriteString("BSE1")
	buf.WriteByte(0x01)
	buf.WriteByte(0x01)
	_ = binary.Write(buf, binary.BigEndian, uint32(seedBlockSize))
	_ = binary.Write(buf, binary.BigEndian, uint64(fileSize))
	_ = binary.Write(buf, binary.BigEndian, chunkCount)

	chunk := make([]byte, seedBlockSize)
	for i := uint32(0); i < chunkCount; i++ {
		n, err := io.ReadFull(f, chunk)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, "", 0, err
			}
		}
		if n < seedBlockSize {
			for j := n; j < seedBlockSize; j++ {
				chunk[j] = 0
			}
		}
		h := sha256.Sum256(chunk)
		buf.Write(h[:])
	}

	seedBytes := buf.Bytes()
	h := sha256.Sum256(seedBytes)
	return seedBytes, hex.EncodeToString(h[:]), chunkCount, nil
}

func noEnabledGatewayMessage(httpAPIEnabled bool, fsHTTPEnabled bool) string {
	if httpAPIEnabled {
		return "no enabled gateway, waiting for HTTP API configuration"
	}
	if fsHTTPEnabled {
		return "no enabled gateway, HTTP API disabled; waiting external gateway config or runner injection"
	}
	return "no enabled gateway, HTTP API and FS HTTP disabled; waiting external gateway config or runner injection"
}

func writeIfChanged(path string, data []byte) error {
	old, err := os.ReadFile(path)
	if err == nil && bytes.Equal(old, data) {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func ceilDiv(v uint64, d uint64) uint64 {
	if v == 0 {
		return 0
	}
	return (v + d - 1) / d
}

func overlaps(a, b string) bool {
	aAbs, _ := filepath.Abs(a)
	bAbs, _ := filepath.Abs(b)
	return isParentOrSame(aAbs, bAbs) || isParentOrSame(bAbs, aAbs)
}

func isParentOrSame(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != "" && !strings.HasPrefix(rel, "..") && rel != ".."
}

func peerIDFromSecp256k1PubHex(pubHex string) (peer.ID, error) {
	b, err := hex.DecodeString(strings.TrimSpace(pubHex))
	if err != nil {
		return "", err
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(b)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(pub)
}

func normalizeChunkIndexes(in []uint32, maxExclusive uint32) []uint32 {
	if len(in) == 0 {
		return nil
	}
	tmp := make([]uint32, 0, len(in))
	seen := make(map[uint32]struct{}, len(in))
	for _, idx := range in {
		if maxExclusive > 0 && idx >= maxExclusive {
			continue
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		tmp = append(tmp, idx)
	}
	if len(tmp) == 0 {
		return nil
	}
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	return tmp
}

func normalizeChunkBitmapHex(bitmapHex string) (string, error) {
	bitmapHex = strings.ToLower(strings.TrimSpace(bitmapHex))
	if bitmapHex == "" {
		return "", nil
	}
	raw, err := hex.DecodeString(bitmapHex)
	if err != nil {
		return "", fmt.Errorf("invalid available_chunk_bitmap_hex")
	}
	return hex.EncodeToString(raw), nil
}

func normalizeChunkBitmapBytes(bitmap []byte) string {
	if len(bitmap) == 0 {
		return ""
	}
	return strings.ToLower(hex.EncodeToString(bitmap))
}

func chunkBitmapHexFromIndexes(indexes []uint32, chunkCount uint32) string {
	indexes = normalizeChunkIndexes(indexes, chunkCount)
	if len(indexes) == 0 {
		return ""
	}
	if chunkCount == 0 {
		chunkCount = indexes[len(indexes)-1] + 1
	}
	byteLen := int((chunkCount + 7) / 8)
	bits := make([]byte, byteLen)
	for _, idx := range indexes {
		if idx >= chunkCount {
			continue
		}
		byteIdx := idx / 8
		// 采用 BT 风格位序：块 0 对应字节最高位(bit7)。
		bit := 7 - (idx % 8)
		bits[byteIdx] |= byte(1 << bit)
	}
	return hex.EncodeToString(bits)
}

func chunkIndexesFromBitmapHex(bitmapHex string, maxExclusive uint32) ([]uint32, error) {
	bitmapHex, err := normalizeChunkBitmapHex(bitmapHex)
	if err != nil {
		return nil, err
	}
	if bitmapHex == "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(bitmapHex)
	if err != nil {
		return nil, err
	}
	out := make([]uint32, 0, len(raw)*4)
	for bi, b := range raw {
		if b == 0 {
			continue
		}
		for bit := uint32(0); bit < 8; bit++ {
			mask := byte(1 << (7 - bit))
			if b&mask == 0 {
				continue
			}
			idx := uint32(bi)*8 + bit
			if maxExclusive > 0 && idx >= maxExclusive {
				continue
			}
			out = append(out, idx)
		}
	}
	return normalizeChunkIndexes(out, maxExclusive), nil
}

func contiguousChunkIndexes(chunkCount uint32) []uint32 {
	if chunkCount == 0 {
		return nil
	}
	out := make([]uint32, 0, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		out = append(out, i)
	}
	return out
}

func gwSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "bitcast-gateway", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func arbSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: "arbiter-mr", Network: "test", TTL: 30 * time.Second, Trace: trace}
}
func clientSec(trace pproto.TraceSink) pproto.SecurityConfig {
	return pproto.SecurityConfig{Domain: ServiceName, Network: "test", TTL: 30 * time.Second, Trace: trace}
}

func nodeSecForRuntime(rt *Runtime) pproto.SecurityConfig {
	network := "test"
	trace := pproto.TraceSink(nil)
	if rt != nil {
		trace = rt.rpcTrace
		if identity, err := rt.runtimeIdentity(); err == nil && identity != nil {
			if identity.IsMainnet {
				network = "main"
			}
		}
	}
	return pproto.SecurityConfig{Domain: "bitfs-node", Network: network, TTL: 30 * time.Second, Trace: trace}
}

func parseAddr(full string) (*peer.AddrInfo, error) {
	ma, err := multiaddr.NewMultiaddr(full)
	if err != nil {
		return nil, err
	}
	return peer.AddrInfoFromP2pAddr(ma)
}

func resolvePrivKeyHex(cfg Config, cliPrivHex string) (string, error) {
	if s := strings.TrimSpace(cliPrivHex); s != "" {
		return normalizeRawSecp256k1PrivKeyHex(s)
	}
	if s := strings.TrimSpace(cfg.Keys.PrivkeyHex); s != "" {
		return normalizeRawSecp256k1PrivKeyHex(s)
	}
	return "", nil
}

// ResolveEffectivePrivKeyHex 在启动前统一解析“唯一运行时私钥”。
// 调用方应在进入 Run 之前完成该解析，再通过 RunOptions.EffectivePrivKeyHex 传入。
func ResolveEffectivePrivKeyHex(cfg Config, overridePrivHex string) (string, error) {
	return resolvePrivKeyHex(cfg, overridePrivHex)
}

func normalizeRawSecp256k1PrivKeyHex(s string) (string, error) {
	hexKey := strings.ToLower(strings.TrimSpace(s))
	if len(hexKey) != 64 {
		return "", fmt.Errorf("invalid private key format: expect 32-byte secp256k1 hex (len=64)")
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return "", fmt.Errorf("invalid private key hex: %w", err)
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(b)
	if err != nil {
		return "", fmt.Errorf("invalid secp256k1 private key: %w", err)
	}
	raw, err := priv.Raw()
	if err != nil {
		return "", fmt.Errorf("read private key raw bytes: %w", err)
	}
	if len(raw) != 32 {
		return "", fmt.Errorf("invalid secp256k1 private key length: got=%d want=32", len(raw))
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func clientIDFromPrivHex(privHex string) (string, error) {
	priv, err := parsePrivHex(privHex)
	if err != nil {
		return "", err
	}
	pubRaw, err := priv.GetPublic().Raw()
	if err != nil {
		return "", fmt.Errorf("read public key raw bytes: %w", err)
	}
	return normalizeCompressedPubKeyHex(hex.EncodeToString(pubRaw))
}

func parsePrivHex(s string) (crypto.PrivKey, error) {
	hexKey, err := normalizeRawSecp256k1PrivKeyHex(s)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalSecp256k1PrivateKey(b)
}

func localPubKeyHex(h host.Host) (string, error) {
	pub := h.Peerstore().PubKey(h.ID())
	if pub == nil {
		return "", fmt.Errorf("missing host public key")
	}
	raw, err := pub.Raw()
	if err != nil {
		return "", fmt.Errorf("read host public key raw bytes: %w", err)
	}
	return normalizeCompressedPubKeyHex(hex.EncodeToString(raw))
}

// must 已移除：库代码不应 panic。
