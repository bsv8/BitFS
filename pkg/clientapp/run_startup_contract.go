package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// StartupPhase 表示 Run 启动生命周期里的阶段名。
// 设计说明：
// - 阶段名稳定后可直接用于日志、测试断言和回归定位；
// - 这里只定义“阶段边界”，不定义业务细节。
type StartupPhase string

const (
	StartupPhasePreflight       StartupPhase = "preflight"
	StartupPhaseBuildCore       StartupPhase = "build_core"
	StartupPhaseConnectExternal StartupPhase = "connect_external"
	StartupPhaseStartServices   StartupPhase = "start_services"
	StartupPhaseBindShutdown    StartupPhase = "bind_shutdown"
)

// StartupContractItem 描述一条启动依赖契约。
// 设计说明：
// - 依赖是否必需由 Required 明确表达；
// - MissingError 固定英文错误文案，便于 e2e 和单测稳定断言。
type StartupContractItem struct {
	Name         string
	Required     bool
	Owner        string
	Phase        StartupPhase
	Usage        string
	MissingError string
}

// StartupContractTable 返回当前 Run 的启动契约表快照。
// 说明：
// - 这是“代码内契约”，不是独立文档；
// - 先把依赖边界说清，再逐步把 Run 逻辑挪进对应 phase。
func StartupContractTable() []StartupContractItem {
	return []StartupContractItem{
		{
			Name:         "ctx",
			Required:     true,
			Owner:        "process_entry",
			Phase:        StartupPhasePreflight,
			Usage:        "root cancellation and lifecycle propagation",
			MissingError: "ctx is required",
		},
		{
			Name:         "store",
			Required:     true,
			Owner:        "runtime_assembler",
			Phase:        StartupPhasePreflight,
			Usage:        "client business store capability",
			MissingError: "store is required",
		},
		{
			Name:         "raw_db",
			Required:     true,
			Owner:        "runtime_assembler",
			Phase:        StartupPhasePreflight,
			Usage:        "db handle for runtime wiring",
			MissingError: "raw db is required",
		},
		{
			Name:         "effective_privkey_hex",
			Required:     true,
			Owner:        "key_workflow",
			Phase:        StartupPhaseBuildCore,
			Usage:        "build libp2p identity and onchain signer actor",
			MissingError: "effective private key is required",
		},
		{
			Name:         "config_path_when_http_enabled",
			Required:     true,
			Owner:        "managed_client",
			Phase:        StartupPhasePreflight,
			Usage:        "runtime config persistence path for HTTP management",
			MissingError: "config path is required when HTTP management is enabled",
		},
		{
			Name:         "action_chain",
			Required:     true,
			Owner:        "runtime_assembler",
			Phase:        StartupPhaseConnectExternal,
			Usage:        "fee pool chain querying and broadcast",
			MissingError: "action chain is required",
		},
		{
			Name:         "wallet_chain",
			Required:     true,
			Owner:        "runtime_assembler",
			Phase:        StartupPhaseConnectExternal,
			Usage:        "wallet read-side chain API",
			MissingError: "wallet chain is required",
		},
	}
}

// runStartupPhases 是 Run 的阶段骨架。
// 设计说明：
// - 当前把 preflight 真正落地，其余阶段先给边界；
// - Run 签名硬显式后，preflight 负责统一 fail-fast。
type runStartupPhases struct {
	ctx                 context.Context
	cfg                 Config
	store               ClientStore
	rawDB               *sql.DB
	configPath          string
	startupMode         StartupMode
	effectivePrivKeyHex string
	actionChain         poolcore.ChainClient
	walletChain         WalletChainClient

	normalizedMode       StartupMode
	runtimeCfg           Config
	normalizedPrivKeyHex string
}

func newRunStartupPhases(
	ctx context.Context,
	cfg Config,
	store ClientStore,
	rawDB *sql.DB,
	configPath string,
	startupMode StartupMode,
	effectivePrivKeyHex string,
	actionChain poolcore.ChainClient,
	walletChain WalletChainClient,
) *runStartupPhases {
	return &runStartupPhases{
		ctx:                 ctx,
		cfg:                 cfg,
		store:               store,
		rawDB:               rawDB,
		configPath:          configPath,
		startupMode:         startupMode,
		effectivePrivKeyHex: effectivePrivKeyHex,
		actionChain:         actionChain,
		walletChain:         walletChain,
	}
}

// Preflight 执行启动前预检。
// 设计说明：
// - 这里只做“能否继续启动”的必要条件判断；
// - 失败尽量在最前面抛出，避免后面才暴雷。
func (p *runStartupPhases) Preflight() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	if p.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if p.store == nil {
		return fmt.Errorf("store is required")
	}
	if p.rawDB == nil {
		return fmt.Errorf("raw db is required")
	}
	mode, err := normalizeStartupMode(p.startupMode)
	if err != nil {
		return err
	}
	runtimeCfg := cloneConfig(p.cfg)
	if err := applyConfigDefaultsForMode(&runtimeCfg, mode); err != nil {
		return err
	}
	if err := validateConfigForMode(&runtimeCfg, mode); err != nil {
		return err
	}
	if runtimeCfg.HTTP.Enabled && strings.TrimSpace(p.configPath) == "" {
		return fmt.Errorf("config path is required when HTTP management is enabled")
	}
	if strings.TrimSpace(p.effectivePrivKeyHex) == "" {
		return fmt.Errorf("effective private key is required")
	}
	privHex, err := normalizeRawSecp256k1PrivKeyHex(p.effectivePrivKeyHex)
	if err != nil {
		return err
	}
	if p.actionChain == nil {
		return fmt.Errorf("action chain is required")
	}
	if p.walletChain == nil {
		return fmt.Errorf("wallet chain is required")
	}
	p.normalizedMode = mode
	p.runtimeCfg = runtimeCfg
	p.normalizedPrivKeyHex = privHex
	return nil
}

// BuildCore 代表“本地核心能力组装”阶段。
// 当前先保留骨架，后续把 Run 内对应逻辑迁入这里。
func (p *runStartupPhases) BuildCore() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	return nil
}

// ConnectExternal 代表“外部连接与探活”阶段。
func (p *runStartupPhases) ConnectExternal() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	return nil
}

// StartServices 代表“后台 loop 与服务启动”阶段。
func (p *runStartupPhases) StartServices() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	return nil
}

// BindShutdown 代表“统一收尾路径绑定”阶段。
func (p *runStartupPhases) BindShutdown() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	return nil
}

func (p *runStartupPhases) RuntimeConfig() Config {
	if p == nil {
		return Config{}
	}
	return cloneConfig(p.runtimeCfg)
}

func (p *runStartupPhases) StartupMode() StartupMode {
	if p == nil {
		return StartupModeProduct
	}
	return p.normalizedMode
}

func (p *runStartupPhases) EffectivePrivKeyHex() string {
	if p == nil {
		return ""
	}
	return strings.TrimSpace(p.normalizedPrivKeyHex)
}
