package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// StartupPhase 表示 Run 启动契约里的阶段分类。
// 说明：
// - 这里只用于契约项分组，不再承载启动骨架逻辑；
// - Run 真正执行流程统一留在 run.go。
type StartupPhase string

const (
	StartupPhasePreflight       StartupPhase = "preflight"
	StartupPhaseBuildCore       StartupPhase = "build_core"
	StartupPhaseConnectExternal StartupPhase = "connect_external"
)

// StartupContractItem 描述一条启动依赖契约。
// 设计说明：
// - Required 明确依赖是否必传；
// - MissingError 固定英文错误文案，便于 e2e/单测断言。
type StartupContractItem struct {
	Name         string
	Required     bool
	Owner        string
	Phase        StartupPhase
	Usage        string
	MissingError string
}

// StartupContractTable 返回当前 Run 的启动契约快照。
// 说明：这是代码内契约，不是额外文档。
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

type runPreflightResult struct {
	startupMode         StartupMode
	runtimeCfg          Config
	effectivePrivKeyHex string
	store               *clientDB
}

// preflightRunArgs 把 Run 的启动前校验集中到一个入口。
// 设计说明：
// - 这里只做 fail-fast，不做副作用初始化；
// - 返回已规范化的数据，Run 主流程不再重复解析。
func preflightRunArgs(
	ctx context.Context,
	cfg Config,
	storeCap ClientStore,
	rawDB *sql.DB,
	configPath string,
	startupMode StartupMode,
	effectivePrivKeyHex string,
	actionChain poolcore.ChainClient,
	walletChain WalletChainClient,
) (runPreflightResult, error) {
	if ctx == nil {
		return runPreflightResult{}, fmt.Errorf("ctx is required")
	}
	if storeCap == nil {
		return runPreflightResult{}, fmt.Errorf("store is required")
	}
	store, ok := storeCap.(*clientDB)
	if !ok {
		return runPreflightResult{}, fmt.Errorf("store must come from clientapp.NewClientStore")
	}
	if rawDB == nil {
		return runPreflightResult{}, fmt.Errorf("raw db is required")
	}
	mode, err := normalizeStartupMode(startupMode)
	if err != nil {
		return runPreflightResult{}, err
	}
	runtimeCfg := cloneConfig(cfg)
	if err := applyConfigDefaultsForMode(&runtimeCfg, mode); err != nil {
		return runPreflightResult{}, err
	}
	if err := validateConfigForMode(&runtimeCfg, mode); err != nil {
		return runPreflightResult{}, err
	}
	if runtimeCfg.HTTP.Enabled && strings.TrimSpace(configPath) == "" {
		return runPreflightResult{}, fmt.Errorf("config path is required when HTTP management is enabled")
	}
	if strings.TrimSpace(effectivePrivKeyHex) == "" {
		return runPreflightResult{}, fmt.Errorf("effective private key is required")
	}
	privHex, err := normalizeRawSecp256k1PrivKeyHex(effectivePrivKeyHex)
	if err != nil {
		return runPreflightResult{}, err
	}
	if actionChain == nil {
		return runPreflightResult{}, fmt.Errorf("action chain is required")
	}
	if walletChain == nil {
		return runPreflightResult{}, fmt.Errorf("wallet chain is required")
	}
	return runPreflightResult{
		startupMode:         mode,
		runtimeCfg:          runtimeCfg,
		effectivePrivKeyHex: privHex,
		store:               store,
	}, nil
}
