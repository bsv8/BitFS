package clientapp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFileResult 表示配置文件的加载结果。
type ConfigFileResult struct {
	Config  Config
	Created bool
}

// ResolveVaultPath 解析客户端 vault 目录。
// 约定：
// - 空值回退到 `.vault`
// - 传入值始终视为目录，不是文件
func ResolveVaultPath(path string) string {
	p := strings.TrimSpace(path)
	if p == "" {
		return ".vault"
	}
	return p
}

// ResolveConfigPath 解析配置文件路径。
func ResolveConfigPath(path string) string {
	return filepath.Join(filepath.Clean(ResolveVaultPath(path)), "config.yaml")
}

// ResolveKeyFilePath 解析 key.json 路径。
func ResolveKeyFilePath(path string) string {
	return filepath.Join(filepath.Clean(ResolveVaultPath(path)), "key.json")
}

// LoadConfig 读取 YAML 配置文件。
// 设计约束：
// - 只处理文件态配置；
// - 身份材料不应由配置文件承载，因此不会从 YAML 中读取。
func LoadConfig(path string) (Config, []byte, error) {
	return LoadConfigWithSeed(path, defaultConfigSeed())
}

// LoadConfigWithSeed 以 seed 作为默认值读取 YAML 配置文件。
// seed 适合承载目录级默认值，例如首次创建时的网络、路径和开关。
func LoadConfigWithSeed(path string, seed Config) (Config, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, err
	}
	cfg := seed
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, nil, err
	}
	if err := ApplyConfigDefaults(&cfg); err != nil {
		return Config{}, nil, err
	}
	if err := resolveConfigPathsForRuntime(&cfg, path); err != nil {
		return Config{}, nil, err
	}
	return cfg, raw, nil
}

// LoadOrInitConfigFile 保证配置文件存在。
// - 文件不存在：使用 seed 创建；
// - 文件已存在：直接读取并转为运行态配置。
func LoadOrInitConfigFile(path string, seed Config) (ConfigFileResult, error) {
	resolved := filepath.Clean(strings.TrimSpace(path))
	if resolved == "" {
		return ConfigFileResult{}, fmt.Errorf("config path is empty")
	}
	if st, err := os.Stat(resolved); err == nil {
		if st.IsDir() {
			return ConfigFileResult{}, fmt.Errorf("config path is a directory: %s", resolved)
		}
		cfg, _, err := LoadConfigWithSeed(resolved, seed)
		if err != nil {
			return ConfigFileResult{}, err
		}
		return ConfigFileResult{Config: cfg}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ConfigFileResult{}, err
	}

	cfg := seed
	if err := ApplyConfigDefaults(&cfg); err != nil {
		return ConfigFileResult{}, err
	}
	if err := normalizeConfigForFile(&cfg, resolved); err != nil {
		return ConfigFileResult{}, err
	}
	if err := saveConfigFile(resolved, cfg); err != nil {
		return ConfigFileResult{}, err
	}
	loaded, _, err := LoadConfigWithSeed(resolved, seed)
	if err != nil {
		return ConfigFileResult{}, err
	}
	return ConfigFileResult{Config: loaded, Created: true}, nil
}

// SaveConfigFile 将运行态配置写回 YAML。
// 设计约束：
// - 仅保存业务配置；
// - 路径优先折叠为 vault 目录下的相对路径，避免把配置写死在机器绝对路径上。
func SaveConfigFile(path string, cfg Config) error {
	resolved := filepath.Clean(strings.TrimSpace(path))
	if resolved == "" {
		return fmt.Errorf("config path is empty")
	}
	if err := normalizeConfigForFile(&cfg, resolved); err != nil {
		return err
	}
	return saveConfigFile(resolved, cfg)
}

func defaultConfigSeed() Config {
	cfg := Config{}
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	cfg.Index.Backend = "sqlite"
	cfg.Storage.WorkspaceDir = "workspace"
	cfg.Storage.DataDir = "data"
	cfg.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))
	return cfg
}

func saveConfigFile(path string, cfg Config) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func resolveConfigPathsForRuntime(cfg *Config, configPath string) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	baseDir := filepath.Dir(filepath.Clean(strings.TrimSpace(configPath)))
	cfg.Storage.WorkspaceDir = resolveConfigPathValue(baseDir, cfg.Storage.WorkspaceDir, "workspace")
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
	cfg.Storage.WorkspaceDir = saveConfigPathValue(baseDir, cfg.Storage.WorkspaceDir, "workspace")
	cfg.Storage.DataDir = saveConfigPathValue(baseDir, cfg.Storage.DataDir, "data")
	cfg.Index.SQLitePath = saveConfigPathValue(baseDir, cfg.Index.SQLitePath, filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = saveConfigPathValue(baseDir, cfg.Log.File, filepath.Join("logs", "bitfs.log"))
	return nil
}

func resolveConfigPathValue(baseDir, rawValue, fallback string) string {
	v := strings.TrimSpace(rawValue)
	if v == "" {
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
	if v == "" {
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
