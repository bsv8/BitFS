package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/BitFS/pkg/managedclient"
)

var version = "dev"
var cliLang = detectCLILanguage()

type cliOptions struct {
	vaultPath            string
	initNetwork          string
	httpListenAddr       string
	fsHTTPListen         string
	systemHomepageBundle string
	newKey               bool
	importPath           string
	exportPath           string
	showVer              bool
}

type cliAction string

type startupSummary = managedclient.StartupSummary
type runtimeListenOverrides = managedclient.RuntimeListenOverrides
type desktopBootstrapOptions = managedclient.DesktopBootstrapOptions
type encryptedKeyEnvelope = managedclient.EncryptedKeyEnvelope

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
	overrides := runtimeListenOverrides{
		HTTPListenAddr:   strings.TrimSpace(opts.httpListenAddr),
		FSHTTPListenAddr: strings.TrimSpace(opts.fsHTTPListen),
	}
	desktopOptions := desktopBootstrapOptions{
		SystemHomepageBundle: strings.TrimSpace(opts.systemHomepageBundle),
	}

	vaultPath := clientapp.ResolveVaultPath(opts.vaultPath)
	configPath := clientapp.ResolveConfigPath(vaultPath)
	keyPath := clientapp.ResolveKeyFilePath(vaultPath)
	cfg, runtimeCfgCreated, err := managedclient.LoadRuntimeConfigOrInit(configPath, initNetwork)
	if err != nil {
		log.Fatal(err)
	}
	overrides.Apply(&cfg)
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
		if err := managedclient.RunManagedDaemon(managedclient.DaemonOptions{
			Config:               cfg,
			Startup:              startup,
			InitNetwork:          initNetwork,
			Overrides:            overrides,
			Desktop:              desktopOptions,
			UnlockPasswordPrompt: msg("prompt_password_unlock"),
			ControlStream:        managedclient.NewManagedControlStreamFromEnv(),
		}); err != nil {
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
	flag.StringVar(&opts.httpListenAddr, "http-listen", "", msg("flag_http_listen"))
	flag.StringVar(&opts.fsHTTPListen, "fs-http-listen", "", msg("flag_fs_http_listen"))
	flag.StringVar(&opts.systemHomepageBundle, "system-homepage-bundle", "", msg("flag_system_homepage_bundle"))
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
	if _, exists, err := managedclient.LoadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("%s", msg("err_key_exists"))
	}
	p1, err := managedclient.ReadPassword(msg("prompt_password_new"))
	if err != nil {
		return err
	}
	p2, err := managedclient.ReadPassword(msg("prompt_password_confirm"))
	if err != nil {
		return err
	}
	if p1 != p2 {
		return fmt.Errorf("%s", msg("err_password_not_match"))
	}
	if strings.TrimSpace(p1) == "" {
		return fmt.Errorf("%s", msg("err_password_empty"))
	}
	privHex, err := managedclient.GeneratePrivateKeyHex()
	if err != nil {
		return err
	}
	env, err := managedclient.EncryptPrivateKeyEnvelope(privHex, p1)
	if err != nil {
		return err
	}
	if err := managedclient.SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		return err
	}
	pubHex, _ := managedclient.PubHexFromPrivHex(privHex)
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
	if _, exists, err := managedclient.LoadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("%s", msg("err_key_exists"))
	}
	if err := managedclient.SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
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
	env, exists, err := managedclient.LoadEncryptedKeyEnvelope(keyPath)
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
		"flag_http_listen":               "override managed api listen address for current run only",
		"flag_fs_http_listen":            "override fs_http listen address for current run only",
		"flag_system_homepage_bundle":    "install system homepage bundle into workspace for current desktop product run",
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
		"flag_http_listen":               "仅本次运行覆盖 managed api 监听地址",
		"flag_fs_http_listen":            "仅本次运行覆盖 fs_http 监听地址",
		"flag_system_homepage_bundle":    "仅桌面产品本次运行使用的系统首页 bundle 目录",
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
		"flag_http_listen":               "僅本次執行覆蓋 managed api 監聽位址",
		"flag_fs_http_listen":            "僅本次執行覆蓋 fs_http 監聽位址",
		"flag_system_homepage_bundle":    "僅桌面產品本次執行使用的系統首頁 bundle 目錄",
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
		"flag_http_listen":               "今回の起動だけ managed api の待受アドレスを上書き",
		"flag_fs_http_listen":            "今回の起動だけ fs_http の待受アドレスを上書き",
		"flag_system_homepage_bundle":    "今回のデスクトップ起動だけ使うシステムホームページ bundle ディレクトリ",
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
