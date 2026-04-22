package clientapp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFileResult 表示配置文件的加载结果。
type ConfigFileResult struct {
	Config  Config
	Created bool
}

// ResolveConfigRoot 解析客户端配置根目录。
// 约定：
// - 空值回退到系统默认目录（Linux/macOS: ~/.config/bitfs）；
// - 传入值始终视为目录，不是文件。
func ResolveConfigRoot(path string) string {
	p := strings.TrimSpace(path)
	if p == "" {
		base, err := os.UserConfigDir()
		if err != nil || strings.TrimSpace(base) == "" {
			return filepath.Clean(filepath.Join(".config", "bitfs"))
		}
		return filepath.Clean(filepath.Join(base, "bitfs"))
	}
	return filepath.Clean(p)
}

// ResolveConfigPath 解析配置文件路径。
func ResolveConfigPath(configRoot string) string {
	return filepath.Join(ResolveConfigRoot(configRoot), "config.yaml")
}

// ResolveKeyFilePath 解析 key.json 路径。
func ResolveKeyFilePath(configRoot string) string {
	return filepath.Join(ResolveConfigRoot(configRoot), "key.json")
}

// LoadConfig 读取 YAML 配置文件。
// 设计约束：
// - 只处理文件态配置；
// - 身份材料不应由配置文件承载，因此不会从 YAML 中读取。
func LoadConfig(path string) (Config, []byte, error) {
	return LoadConfigWithSeed(path, defaultConfigSeed())
}

// LoadConfigWithSeed 以 seed 作为文件层初值读取 YAML 配置文件。
// 这里不做运行时默认补齐，只负责把文件内容还原出来，再把相对路径展开成运行态路径。
func LoadConfigWithSeed(path string, seed Config) (Config, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, err
	}
	cfg := seed
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, nil, err
	}
	if err := resolveConfigPathsForRuntime(&cfg, path); err != nil {
		return Config{}, nil, err
	}
	return cfg, raw, nil
}

// LoadOrInitConfigFile 保证配置文件存在。
// 默认按产品模式执行：首次创建时会先补默认值再落盘。
func LoadOrInitConfigFile(path string, seed Config) (ConfigFileResult, error) {
	return LoadOrInitConfigFileForMode(path, seed, StartupModeProduct)
}

// LoadOrInitConfigFileForMode 加载配置文件，并把运行态默认值补齐到最终快照。
// 设计说明：
// - 差异化配置下，“无差异=无文件”是常态，因此这里不再强制创建文件；
// - product 模式会补齐基础默认值（包含内置网关/仲裁）；
// - test 模式保持最小 seed，不注入默认网关/仲裁。
func LoadOrInitConfigFileForMode(path string, seed Config, mode StartupMode) (ConfigFileResult, error) {
	resolved := filepath.Clean(strings.TrimSpace(path))
	if resolved == "" {
		return ConfigFileResult{}, fmt.Errorf("config path is empty")
	}
	startupMode, err := normalizeStartupMode(mode)
	if err != nil {
		return ConfigFileResult{}, err
	}
	if st, err := os.Stat(resolved); err == nil {
		if st.IsDir() {
			return ConfigFileResult{}, fmt.Errorf("config path is a directory: %s", resolved)
		}
		cfg, _, err := LoadConfigWithSeed(resolved, seed)
		if err != nil {
			return ConfigFileResult{}, err
		}
		if err := ApplyConfigDefaultsForMode(&cfg, startupMode); err != nil {
			return ConfigFileResult{}, err
		}
		return ConfigFileResult{Config: cfg}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ConfigFileResult{}, err
	}

	cfg := seed
	if err := ApplyConfigDefaultsForMode(&cfg, startupMode); err != nil {
		return ConfigFileResult{}, err
	}
	if err := resolveConfigPathsForRuntime(&cfg, resolved); err != nil {
		return ConfigFileResult{}, err
	}
	return ConfigFileResult{Config: cfg, Created: false}, nil
}

// SaveConfigFile 将配置原样写回 YAML。
// 设计约束：
// - 仅保存业务配置；
// - 路径优先折叠为 vault 目录下的相对路径，避免把配置写死在机器绝对路径上。
func SaveConfigFile(path string, cfg Config) error {
	return SaveConfigFileForMode(path, cfg, StartupModeProduct)
}

// SaveConfigFileForMode 按启动模式保存配置。
// 差异化写盘规则：
// - 文件只保存“与默认值不同”的字段；
// - 当没有任何差异时删除配置文件；
// - product 模式下，内置网关/仲裁不进文件（读取时会自动并入）。
func SaveConfigFileForMode(path string, cfg Config, mode StartupMode) error {
	resolved := filepath.Clean(strings.TrimSpace(path))
	if resolved == "" {
		return fmt.Errorf("config path is empty")
	}
	startupMode, err := normalizeStartupMode(mode)
	if err != nil {
		return err
	}

	fileCfg := cloneConfig(cfg)
	if err := ApplyConfigDefaultsForMode(&fileCfg, startupMode); err != nil {
		return err
	}
	if err := normalizeConfigForFile(&fileCfg, resolved); err != nil {
		return err
	}

	baseCfg := defaultConfigSeed()
	baseCfg.BSV.Network = fileCfg.BSV.Network
	if err := ApplyConfigDefaultsForMode(&baseCfg, startupMode); err != nil {
		return err
	}
	if err := normalizeConfigForFile(&baseCfg, resolved); err != nil {
		return err
	}

	if startupMode == StartupModeProduct {
		defaults, err := networkInitDefaults(fileCfg.BSV.Network)
		if err != nil {
			return err
		}
		fileCfg.Network.Gateways = stripMandatoryPeerNodesForFile(fileCfg.Network.Gateways, defaults.DefaultGateways)
		fileCfg.Network.Arbiters = stripMandatoryPeerNodesForFile(fileCfg.Network.Arbiters, defaults.DefaultArbiters)
		baseCfg.Network.Gateways = stripMandatoryPeerNodesForFile(baseCfg.Network.Gateways, defaults.DefaultGateways)
		baseCfg.Network.Arbiters = stripMandatoryPeerNodesForFile(baseCfg.Network.Arbiters, defaults.DefaultArbiters)
	}
	// 设计说明：
	// - startup_full_scan 是桌面托管启动时的临时技术开关；
	// - 只影响本次运行，不应污染用户配置文件；
	// - 写盘时统一按“未开启”视角比较差异。
	fileCfg.Scan.StartupFullScan = false
	baseCfg.Scan.StartupFullScan = false

	diffRaw, err := buildConfigDiffYAML(fileCfg, baseCfg)
	if err != nil {
		return err
	}
	if len(diffRaw) == 0 {
		if err := os.Remove(resolved); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return writeRawConfigFile(resolved, diffRaw)
}

func defaultConfigSeed() Config {
	cfg := Config{}
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	cfg.Index.Backend = "sqlite"
	cfg.Storage.DataDir = "data"
	cfg.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))
	return cfg
}

func writeRawConfigFile(path string, raw []byte) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, raw, 0o644)
}

func buildConfigDiffYAML(current Config, baseline Config) ([]byte, error) {
	currentMap, err := configToYAMLMap(current)
	if err != nil {
		return nil, err
	}
	baselineMap, err := configToYAMLMap(baseline)
	if err != nil {
		return nil, err
	}
	diff := diffYAMLValue(currentMap, baselineMap)
	diffMap, ok := diff.(map[string]any)
	if !ok || len(diffMap) == 0 {
		return nil, nil
	}
	raw, err := yaml.Marshal(diffMap)
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, nil
	}
	return append(raw, '\n'), nil
}

func configToYAMLMap(cfg Config) (map[string]any, error) {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func diffYAMLValue(current any, baseline any) any {
	switch c := current.(type) {
	case map[string]any:
		b, ok := baseline.(map[string]any)
		if !ok {
			return c
		}
		out := map[string]any{}
		for k, v := range c {
			d := diffYAMLValue(v, b[k])
			if d != nil {
				out[k] = d
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case []any:
		if b, ok := baseline.([]any); ok && reflect.DeepEqual(c, b) {
			return nil
		}
		return c
	default:
		if reflect.DeepEqual(current, baseline) {
			return nil
		}
		return current
	}
}

func resolveConfigPathsForRuntime(cfg *Config, configPath string) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	baseDir := filepath.Dir(filepath.Clean(strings.TrimSpace(configPath)))
	cfg.Storage.DataDir = resolveConfigPathValue(baseDir, cfg.Storage.DataDir, "data")
	cfg.Index.SQLitePath = resolveConfigPathValue(baseDir, cfg.Index.SQLitePath, filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = resolveConfigPathValue(baseDir, cfg.Log.File, filepath.Join("logs", "bitfs.log"))
	return nil
}

func normalizeConfigForFile(cfg *Config, configPath string) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	baseDir := filepath.Dir(filepath.Clean(strings.TrimSpace(configPath)))
	cfg.Storage.DataDir = saveConfigPathValue(baseDir, cfg.Storage.DataDir, "data")
	cfg.Index.SQLitePath = saveConfigPathValue(baseDir, cfg.Index.SQLitePath, filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = saveConfigPathValue(baseDir, cfg.Log.File, filepath.Join("logs", "bitfs.log"))
	return nil
}

func resolveConfigPathValue(baseDir, rawValue, fallback string) string {
	v := strings.TrimSpace(rawValue)
	fallback = strings.TrimSpace(fallback)
	if v == "" {
		if fallback == "" {
			return ""
		}
		v = fallback
	}
	if filepath.IsAbs(v) {
		return filepath.Clean(v)
	}
	if baseDir == "" || baseDir == "." {
		return filepath.Clean(v)
	}
	return filepath.Clean(filepath.Join(baseDir, v))
}

func saveConfigPathValue(baseDir, rawValue, fallback string) string {
	v := strings.TrimSpace(rawValue)
	fallback = strings.TrimSpace(fallback)
	if v == "" {
		if fallback == "" {
			return ""
		}
		v = fallback
	}
	clean := filepath.Clean(v)
	if baseDir != "" && baseDir != "." {
		if rel, err := filepath.Rel(baseDir, clean); err == nil && rel != "." && !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel) {
			return filepath.ToSlash(rel)
		}
	}
	return filepath.ToSlash(clean)
}
