package clientapp

import (
	"context"
	"fmt"
	"strings"
)

// StartupPhase 表示 Run 启动生命周期里的阶段名。
// 设计说明：
// - 阶段名稳定后可直接用于日志、测试断言和回归定位；
// - 这里只定义“阶段边界”，不定义业务细节。
type StartupPhase string

const (
	StartupPhasePreflight      StartupPhase = "preflight"
	StartupPhaseBuildCore      StartupPhase = "build_core"
	StartupPhaseConnectExternal StartupPhase = "connect_external"
	StartupPhaseStartServices  StartupPhase = "start_services"
	StartupPhaseBindShutdown   StartupPhase = "bind_shutdown"
)

// StartupContractItem 描述一条启动依赖契约。
// 设计说明：
// - 依赖是否必需由 Required 明确表达；
// - MissingError 固定英文错误文案，便于 e2e 和单测稳定断言；
// - AllowFallback=true 只表示“允许在 Run 内兜底创建”，不是推荐做法。
type StartupContractItem struct {
	Name          string
	Required      bool
	Owner         string
	Phase         StartupPhase
	Usage         string
	MissingError  string
	AllowFallback bool
}

// StartupContractTable 返回当前 Run 的启动契约表快照。
// 说明：
// - 这是“代码内契约”，不是独立文档；
// - 先把依赖边界说清，再逐步把 Run 逻辑挪进对应 phase。
func StartupContractTable() []StartupContractItem {
	return []StartupContractItem{
		{
			Name:          "ctx",
			Required:      true,
			Owner:         "process_entry",
			Phase:         StartupPhasePreflight,
			Usage:         "root cancellation and lifecycle propagation",
			MissingError:  "ctx is required",
			AllowFallback: false,
		},
		{
			Name:          "deps.store",
			Required:      true,
			Owner:         "runtime_assembler",
			Phase:         StartupPhasePreflight,
			Usage:         "client business store capability",
			MissingError:  "runtime deps are required",
			AllowFallback: false,
		},
		{
			Name:          "deps.raw_db",
			Required:      true,
			Owner:         "runtime_assembler",
			Phase:         StartupPhasePreflight,
			Usage:         "db lifecycle close ownership",
			MissingError:  "runtime deps are required",
			AllowFallback: false,
		},
		{
			Name:          "opt.effective_privkey_hex",
			Required:      true,
			Owner:         "key_workflow",
			Phase:         StartupPhaseBuildCore,
			Usage:         "build libp2p identity and onchain signer actor",
			MissingError:  "effective private key is required",
			AllowFallback: false,
		},
		{
			Name:          "opt.config_path_when_http_enabled",
			Required:      true,
			Owner:         "managed_client",
			Phase:         StartupPhasePreflight,
			Usage:         "runtime config persistence path for HTTP management",
			MissingError:  "config path is required when HTTP management is enabled",
			AllowFallback: false,
		},
		{
			Name:          "opt.action_chain",
			Required:      false,
			Owner:         "runtime_assembler",
			Phase:         StartupPhaseConnectExternal,
			Usage:         "fee pool chain querying and broadcast",
			MissingError:  "action chain is required",
			AllowFallback: true,
		},
		{
			Name:          "opt.wallet_chain",
			Required:      false,
			Owner:         "runtime_assembler",
			Phase:         StartupPhaseConnectExternal,
			Usage:         "wallet read-side chain API",
			MissingError:  "wallet chain is required",
			AllowFallback: true,
		},
	}
}

// runStartupPhases 是 Run 的阶段骨架。
// 设计说明：
// - 当前只把 preflight 真正落地，其余阶段先给边界；
// - 这样可以在不改业务行为前提下，先把“入口结构”固定下来。
type runStartupPhases struct {
	ctx context.Context
	cfg Config
	deps RunDeps
	opt RunOptions

	startupMode StartupMode
	runtimeCfg  Config
}

func newRunStartupPhases(ctx context.Context, cfg Config, deps RunDeps, opt RunOptions) *runStartupPhases {
	return &runStartupPhases{
		ctx:  ctx,
		cfg:  cfg,
		deps: deps,
		opt:  opt,
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
	if p.deps.Store == nil || p.deps.RawDB == nil {
		return fmt.Errorf("runtime deps are required")
	}
	mode, err := normalizeStartupMode(p.opt.StartupMode)
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
	if runtimeCfg.HTTP.Enabled && strings.TrimSpace(p.opt.ConfigPath) == "" {
		return fmt.Errorf("config path is required when HTTP management is enabled")
	}
	p.startupMode = mode
	p.runtimeCfg = runtimeCfg
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
	return p.startupMode
}
