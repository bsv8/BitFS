package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/term"
	_ "modernc.org/sqlite"
)

var version = "dev"
var cliLang = detectCLILanguage()

type cliOptions struct {
	vaultPath   string
	initNetwork string
	newKey      bool
	importPath  string
	exportPath  string
	showVer     bool
}

type startupSummary struct {
	VaultPath           string
	ConfigPath          string
	KeyPath             string
	IndexDBPath         string
	RuntimeConfigStatus string
}

type cliAction string

const (
	actionRun    cliAction = "run"
	actionNew    cliAction = "new"
	actionImport cliAction = "import"
	actionExport cliAction = "export"
)

func main() {
	opts := parseFlags()
	if opts.showVer {
		fmt.Printf(msg("version_line")+"\n", version)
		return
	}

	if len(flag.Args()) > 0 {
		log.Fatal(msg("err_unexpected_args"))
	}
	action, err := resolveCLIAction(opts)
	if err != nil {
		log.Fatal(err)
	}
	initNetwork, err := clientapp.NormalizeBSVNetwork(opts.initNetwork)
	if err != nil {
		log.Fatal(err)
	}

	vaultPath := clientapp.ResolveVaultPath(opts.vaultPath)
	configPath := clientapp.ResolveConfigPath(vaultPath)
	keyPath := clientapp.ResolveKeyFilePath(vaultPath)
	cfg, runtimeCfgCreated, err := loadRuntimeConfigOrInit(configPath, initNetwork)
	if err != nil {
		log.Fatal(err)
	}
	runtimeConfigStatus := "已加载"
	if runtimeCfgCreated {
		runtimeConfigStatus = "已创建（首次启动）"
	}
	startup := startupSummary{
		VaultPath:           vaultPath,
		ConfigPath:          configPath,
		KeyPath:             keyPath,
		IndexDBPath:         strings.TrimSpace(cfg.Index.SQLitePath),
		RuntimeConfigStatus: runtimeConfigStatus,
	}

	switch action {
	case actionNew:
		if err := runCLIKeyNew(keyPath); err != nil {
			log.Fatal(err)
		}
		return
	case actionImport:
		if err := runCLIKeyImport(keyPath, opts.importPath); err != nil {
			log.Fatal(err)
		}
		return
	case actionExport:
		if err := runCLIKeyExport(keyPath, opts.exportPath); err != nil {
			log.Fatal(err)
		}
		return
	case actionRun:
		if err := runManagedDaemon(cfg, startup, initNetwork); err != nil {
			log.Fatal(err)
		}
		return
	default:
		log.Fatalf("unknown cli action: %s", action)
	}
}

func parseFlags() cliOptions {
	var opts cliOptions
	flag.StringVar(&opts.vaultPath, "path", ".vault", msg("flag_path"))
	flag.StringVar(&opts.initNetwork, "network", "main", msg("flag_network"))
	flag.BoolVar(&opts.newKey, "new", false, msg("flag_new"))
	flag.StringVar(&opts.importPath, "import", "", msg("flag_import"))
	flag.StringVar(&opts.exportPath, "export", "", msg("flag_export"))
	flag.BoolVar(&opts.showVer, "version", false, msg("flag_version"))
	flag.Usage = func() {
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), msg("usage_line"))
		flag.PrintDefaults()
	}
	flag.Parse()
	return opts
}

func resolveCLIAction(opts cliOptions) (cliAction, error) {
	actions := 0
	if opts.newKey {
		actions++
	}
	if strings.TrimSpace(opts.importPath) != "" {
		actions++
	}
	if strings.TrimSpace(opts.exportPath) != "" {
		actions++
	}
	if actions > 1 {
		return "", fmt.Errorf("%s", msg("err_actions_mutually_exclusive"))
	}
	if opts.newKey {
		return actionNew, nil
	}
	if strings.TrimSpace(opts.importPath) != "" {
		return actionImport, nil
	}
	if strings.TrimSpace(opts.exportPath) != "" {
		return actionExport, nil
	}
	return actionRun, nil
}

func runCLIKeyNew(keyPath string) error {
	if _, exists, err := loadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("%s", msg("err_key_exists"))
	}
	p1, err := readPassword(msg("prompt_password_new"))
	if err != nil {
		return err
	}
	p2, err := readPassword(msg("prompt_password_confirm"))
	if err != nil {
		return err
	}
	if p1 != p2 {
		return fmt.Errorf("%s", msg("err_password_not_match"))
	}
	if strings.TrimSpace(p1) == "" {
		return fmt.Errorf("%s", msg("err_password_empty"))
	}
	privHex, err := generatePrivateKeyHex()
	if err != nil {
		return err
	}
	env, err := encryptPrivateKeyEnvelope(privHex, p1)
	if err != nil {
		return err
	}
	if err := saveEncryptedKeyEnvelope(keyPath, env); err != nil {
		return err
	}
	pubHex, _ := pubHexFromPrivHex(privHex)
	fmt.Printf("%s\nkey_path: %s\npubkey: %s\n", msg("new_done"), keyPath, pubHex)
	return nil
}

func runCLIKeyImport(keyPath, importPath string) error {
	importPath = strings.TrimSpace(importPath)
	if importPath == "" {
		return fmt.Errorf("%s", msg("err_import_path_required"))
	}
	raw, err := os.ReadFile(importPath)
	if err != nil {
		return err
	}
	var env encryptedKeyEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("invalid key envelope json: %w", err)
	}
	if _, exists, err := loadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("%s", msg("err_key_exists"))
	}
	if err := saveEncryptedKeyEnvelope(keyPath, env); err != nil {
		return err
	}
	fmt.Printf("%s\nfile: %s\n", msg("import_done"), importPath)
	return nil
}

func runCLIKeyExport(keyPath, exportPath string) error {
	exportPath = strings.TrimSpace(exportPath)
	if exportPath == "" {
		return fmt.Errorf("%s", msg("err_export_path_required"))
	}
	env, exists, err := loadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		return err
	}
	if !exists || env == nil {
		return fmt.Errorf("%s", msg("err_key_not_found"))
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(exportPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(exportPath, data, 0o600); err != nil {
		return err
	}
	fmt.Printf("%s\nfile: %s\n", msg("export_done"), exportPath)
	return nil
}

func readPassword(prompt string) (string, error) {
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

func newDefaultConfig(network string) clientapp.Config {
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

func loadRuntimeConfigOrInit(configPath, initNetwork string) (clientapp.Config, bool, error) {
	defaultCfg := newDefaultConfig(initNetwork)
	res, err := clientapp.LoadOrInitConfigFile(configPath, defaultCfg)
	if err != nil {
		return clientapp.Config{}, false, err
	}
	cfg := res.Config
	cfg.Index.Backend = "sqlite"
	if strings.TrimSpace(cfg.Index.SQLitePath) == "" {
		cfg.Index.SQLitePath = filepath.Clean(filepath.Join(filepath.Dir(configPath), "data", "client-index.sqlite"))
	}
	return cfg, res.Created, nil
}

func generatePrivateKeyHex() (string, error) {
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

func normalizeRawSecp256k1PrivKeyHex(in string) (string, error) {
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

func pubHexFromPrivHex(privHex string) (string, error) {
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

func detectCLILanguage() string {
	raw := detectSystemLocale()
	return normalizeLocale(raw)
}

func detectSystemLocale() string {
	if runtime.GOOS == "windows" {
		if v := strings.TrimSpace(detectWindowsLocale()); v != "" {
			return v
		}
	}
	for _, key := range []string{"LC_ALL", "LC_MESSAGES", "LANG"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func normalizeLocale(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return "en"
	}
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		s = s[:idx]
	}
	if idx := strings.IndexByte(s, '@'); idx >= 0 {
		s = s[:idx]
	}
	s = strings.ReplaceAll(s, "_", "-")

	if strings.HasPrefix(s, "ja") {
		return "ja"
	}
	if strings.HasPrefix(s, "zh") {
		if strings.Contains(s, "hant") || strings.Contains(s, "-tw") || strings.Contains(s, "-hk") || strings.Contains(s, "-mo") {
			return "zh-TW"
		}
		return "zh-CN"
	}
	if strings.HasPrefix(s, "en") {
		return "en"
	}
	return "en"
}

func msg(key string) string {
	langTable, ok := cliMessages[cliLang]
	if !ok {
		langTable = cliMessages["en"]
	}
	if v, ok := langTable[key]; ok {
		return v
	}
	if v, ok := cliMessages["en"][key]; ok {
		return v
	}
	return key
}

var cliMessages = map[string]map[string]string{
	"en": {
		"version_line":                   "bitfs version %s",
		"usage_line":                     "Usage: bitfs [flags]",
		"flag_path":                      "vault directory path",
		"flag_network":                   "initial bsv network for first run only: test/main",
		"flag_new":                       "create encrypted private key in key.json",
		"flag_import":                    "import encrypted key json file into key.json",
		"flag_export":                    "export encrypted key json file from key.json",
		"flag_version":                   "show version",
		"err_unexpected_args":            "unexpected positional arguments",
		"err_actions_mutually_exclusive": "action flags are mutually exclusive: choose one of -new/-import/-export",
		"err_key_exists":                 "encrypted key already exists",
		"err_key_not_found":              "encrypted key not found, run -new or -import first",
		"err_import_path_required":       "-import requires a file path",
		"err_export_path_required":       "-export requires a file path",
		"prompt_password_new":            "Enter password: ",
		"prompt_password_confirm":        "Confirm password: ",
		"prompt_password_unlock":         "Unlock password: ",
		"err_password_not_match":         "passwords do not match",
		"err_password_empty":             "password cannot be empty",
		"new_done":                       "Private key created",
		"import_done":                    "Encrypted key imported",
		"export_done":                    "Encrypted key exported",
	},
	"zh-CN": {
		"version_line":                   "bitfs 版本 %s",
		"usage_line":                     "用法: bitfs [flags]",
		"flag_path":                      "vault 目录路径",
		"flag_network":                   "首次初始化使用的 bsv 网络：test/main（仅首次生效）",
		"flag_new":                       "新建并加密私钥到 key.json",
		"flag_import":                    "从 json 文件导入密文私钥到 key.json",
		"flag_export":                    "从 key.json 导出密文私钥到 json 文件",
		"flag_version":                   "显示版本",
		"err_unexpected_args":            "不支持位置参数",
		"err_actions_mutually_exclusive": "动作命令互斥：-new/-import/-export 只能选一个",
		"err_key_exists":                 "key.json 中已存在密文私钥",
		"err_key_not_found":              "key.json 中没有密文私钥，请先执行 -new 或 -import",
		"err_import_path_required":       "-import 需要提供文件路径",
		"err_export_path_required":       "-export 需要提供文件路径",
		"prompt_password_new":            "输入密码: ",
		"prompt_password_confirm":        "再次输入密码: ",
		"prompt_password_unlock":         "输入解锁密码: ",
		"err_password_not_match":         "两次密码不一致",
		"err_password_empty":             "密码不能为空",
		"new_done":                       "已创建私钥",
		"import_done":                    "已导入密文私钥",
		"export_done":                    "已导出密文私钥",
	},
	"zh-TW": {
		"version_line":                   "bitfs 版本 %s",
		"usage_line":                     "用法: bitfs [flags]",
		"flag_path":                      "vault 目錄路徑",
		"flag_network":                   "首次初始化使用的 bsv 網路：test/main（僅首次生效）",
		"flag_new":                       "新建並加密私鑰到 key.json",
		"flag_import":                    "從 json 檔案匯入密文私鑰到 key.json",
		"flag_export":                    "從 key.json 匯出密文私鑰到 json 檔案",
		"flag_version":                   "顯示版本",
		"err_unexpected_args":            "不支援位置參數",
		"err_actions_mutually_exclusive": "動作命令互斥：-new/-import/-export 只能選一個",
		"err_key_exists":                 "key.json 中已存在密文私鑰",
		"err_key_not_found":              "key.json 中沒有密文私鑰，請先執行 -new 或 -import",
		"err_import_path_required":       "-import 需要提供檔案路徑",
		"err_export_path_required":       "-export 需要提供檔案路徑",
		"prompt_password_new":            "輸入密碼: ",
		"prompt_password_confirm":        "再次輸入密碼: ",
		"prompt_password_unlock":         "輸入解鎖密碼: ",
		"err_password_not_match":         "兩次密碼不一致",
		"err_password_empty":             "密碼不能為空",
		"new_done":                       "已建立私鑰",
		"import_done":                    "已匯入密文私鑰",
		"export_done":                    "已匯出密文私鑰",
	},
	"ja": {
		"version_line":                   "bitfs バージョン %s",
		"usage_line":                     "使い方: bitfs [flags]",
		"flag_path":                      "vault ディレクトリパス",
		"flag_network":                   "初回初期化のみで使う bsv ネットワーク: test/main",
		"flag_new":                       "key.json に暗号化秘密鍵を新規作成",
		"flag_import":                    "json から暗号化秘密鍵を key.json にインポート",
		"flag_export":                    "key.json から暗号化秘密鍵を json にエクスポート",
		"flag_version":                   "バージョンを表示",
		"err_unexpected_args":            "位置引数はサポートされていません",
		"err_actions_mutually_exclusive": "アクションフラグは排他です: -new/-import/-export のいずれか1つのみ",
		"err_key_exists":                 "key.json に暗号化秘密鍵は既に存在します",
		"err_key_not_found":              "key.json に暗号化秘密鍵が見つかりません。先に -new か -import を実行してください",
		"err_import_path_required":       "-import にはファイルパスが必要です",
		"err_export_path_required":       "-export にはファイルパスが必要です",
		"prompt_password_new":            "パスワード入力: ",
		"prompt_password_confirm":        "パスワード再入力: ",
		"prompt_password_unlock":         "アンロック用パスワード: ",
		"err_password_not_match":         "パスワードが一致しません",
		"err_password_empty":             "パスワードは空にできません",
		"new_done":                       "秘密鍵を作成しました",
		"import_done":                    "暗号化秘密鍵をインポートしました",
		"export_done":                    "暗号化秘密鍵をエクスポートしました",
	},
}
