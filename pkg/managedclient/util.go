package managedclient

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/term"
)

// StartupSummary 描述托管后端启动时需要对外暴露的路径摘要。
// 这些字段会进入 managed key/status 接口和启动日志，便于壳层与 e2e 统一观察。
type StartupSummary struct {
	VaultPath           string
	ConfigPath          string
	KeyPath             string
	IndexDBPath         string
	RuntimeConfigStatus string
}

// RuntimeListenOverrides 描述“只在本次运行生效”的监听地址覆盖。
// 设计说明：
// - 托管模式下，端口属于宿主层分配的运行态资源；
// - 不应偷偷写回用户长期配置文件。
type RuntimeListenOverrides struct {
	HTTPListenAddr   string
	FSHTTPListenAddr string
}

func (o RuntimeListenOverrides) Apply(cfg *clientapp.Config) {
	if cfg == nil {
		return
	}
	if addr := strings.TrimSpace(o.HTTPListenAddr); addr != "" {
		cfg.HTTP.ListenAddr = addr
	}
	if addr := strings.TrimSpace(o.FSHTTPListenAddr); addr != "" {
		cfg.FSHTTP.ListenAddr = addr
	}
}

type DesktopBootstrapOptions struct {
	SystemHomepageBundle string
}

type DaemonOptions struct {
	Config               clientapp.Config
	Startup              StartupSummary
	InitNetwork          string
	Overrides            RuntimeListenOverrides
	Desktop              DesktopBootstrapOptions
	UnlockPasswordPrompt string
	ControlStream        ManagedControlStream
}

func ReadPassword(prompt string) (string, error) {
	if prompt == "" {
		prompt = "password"
	}
	fmt.Fprint(os.Stderr, prompt)
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		b, err := term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func NewDefaultConfig(network string) clientapp.Config {
	cfg := clientapp.Config{}
	cfg.BSV.Network = network
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	cfg.Index.Backend = "sqlite"
	cfg.Storage.WorkspaceDir = "workspace"
	cfg.Storage.DataDir = "data"
	cfg.Index.SQLitePath = filepath.ToSlash(filepath.Join("data", "client-index.sqlite"))
	cfg.Log.File = filepath.ToSlash(filepath.Join("logs", "bitfs.log"))
	return cfg
}

func LoadRuntimeConfigOrInit(configPath, initNetwork string) (clientapp.Config, bool, error) {
	configExisted := true
	if _, err := os.Stat(configPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			configExisted = false
		} else {
			return clientapp.Config{}, false, err
		}
	}
	defaultCfg := NewDefaultConfig(initNetwork)
	res, err := clientapp.LoadOrInitConfigFileForMode(configPath, defaultCfg, clientapp.StartupModeProduct)
	if err != nil {
		return clientapp.Config{}, false, err
	}
	cfg := res.Config
	if err := clientapp.ApplyConfigDefaultsForMode(&cfg, clientapp.StartupModeProduct); err != nil {
		return clientapp.Config{}, false, err
	}
	cfg.Index.Backend = "sqlite"
	if strings.TrimSpace(cfg.Index.SQLitePath) == "" {
		cfg.Index.SQLitePath = filepath.Clean(filepath.Join(filepath.Dir(configPath), "data", "client-index.sqlite"))
	}
	if err := clientapp.SaveConfigFileForMode(configPath, cfg, clientapp.StartupModeProduct); err != nil {
		return clientapp.Config{}, false, err
	}
	configExistsAfterSave := false
	if _, err := os.Stat(configPath); err == nil {
		configExistsAfterSave = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return clientapp.Config{}, false, err
	}
	return cfg, !configExisted && configExistsAfterSave && res.Created, nil
}

func GeneratePrivateKeyHex() (string, error) {
	k, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		return "", err
	}
	b, err := k.Raw()
	if err != nil {
		return "", err
	}
	if len(b) != 32 {
		return "", fmt.Errorf("invalid secp256k1 private key length: got=%d want=32", len(b))
	}
	return strings.ToLower(hex.EncodeToString(b)), nil
}

func NormalizeRawSecp256k1PrivKeyHex(in string) (string, error) {
	hexKey := strings.ToLower(strings.TrimSpace(in))
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

func PubHexFromPrivHex(privHex string) (string, error) {
	k, err := parsePrivHex(privHex)
	if err != nil {
		return "", err
	}
	pub := k.GetPublic()
	raw, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		return "", err
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func parsePrivHex(s string) (crypto.PrivKey, error) {
	hexKey, err := NormalizeRawSecp256k1PrivKeyHex(s)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalSecp256k1PrivateKey(b)
}
