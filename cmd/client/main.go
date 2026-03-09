package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
	"github.com/bsv8/BitFS/pkg/clientapp"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/pelletier/go-toml/v2"
	_ "modernc.org/sqlite"
)

var version = "dev"
var cliLang = detectCLILanguage()

type cliOptions struct {
	appName    string
	configPath string
	importHex  string
	newKey     bool
	delKey     bool
	status     bool
	daemon     bool
	showVer    bool
}

func main() {
	opts := parseFlags()
	if opts.showVer {
		fmt.Printf(msg("version_line")+"\n", version)
		return
	}

	appName := strings.TrimSpace(opts.appName)
	if appName == "" {
		log.Fatal(msg("err_appname_empty"))
	}

	cfgPath := strings.TrimSpace(opts.configPath)
	if cfgPath == "" {
		var err error
		cfgPath, err = defaultConfigPath(appName)
		if err != nil {
			log.Fatal(err)
		}
	}
	cfgPath = filepath.Clean(cfgPath)
	dbPath, err := defaultConfigDBPath(appName)
	if err != nil {
		log.Fatal(err)
	}

	act, seedHex, err := resolveAction(opts)
	if err != nil {
		log.Fatal(err)
	}

	var keyCfg keyFileConfig
	configFileStatus := "已加载"
	switch act {
	case "import", "new", "del":
		keyCfg, _, err = loadKeyConfigOrEmpty(cfgPath)
		if err != nil {
			log.Fatal(err)
		}
	default:
		keyRes, err := loadOrInitKeyConfig(cfgPath)
		if err != nil {
			log.Fatal(err)
		}
		keyCfg = keyRes.Config
		if keyRes.Created {
			configFileStatus = "已创建（首次启动，使用内置默认值）"
		} else if keyRes.Upgraded {
			configFileStatus = "已升级（缺失 privkey_hex 已自动补齐并回写）"
		}
	}
	cfg, runtimeCfgCreated, err := loadRuntimeConfigOrInit(appName, dbPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg.Keys.PrivkeyHex = keyCfg.Keys.PrivkeyHex
	runtimeConfigStatus := "已加载"
	if runtimeCfgCreated {
		runtimeConfigStatus = "已创建（首次启动）"
	}

	hasKey := strings.TrimSpace(cfg.Keys.PrivkeyHex) != ""
	switch act {
	case "import":
		if hasKey {
			log.Fatal(msg("err_key_exists_delete_first"))
		}
		norm, err := normalizePrivateKeyHex(opts.importHex)
		if err != nil {
			log.Fatalf(msg("err_import_invalid")+": %v", err)
		}
		keyCfg.Keys.PrivkeyHex = norm
		if err := writeKeyConfig(cfgPath, keyCfg); err != nil {
			log.Fatal(err)
		}
		pubHex, _ := pubHexFromPrivHex(norm)
		fmt.Printf(msg("import_done")+"\nappname: %s\nconfig: %s\npubkey: %s\n", appName, cfgPath, pubHex)
		return
	case "new":
		if hasKey {
			log.Fatal(msg("err_key_exists_delete_first"))
		}
		gen, err := generatePrivateKeyHex()
		if err != nil {
			log.Fatal(err)
		}
		keyCfg.Keys.PrivkeyHex = gen
		if err := writeKeyConfig(cfgPath, keyCfg); err != nil {
			log.Fatal(err)
		}
		pubHex, _ := pubHexFromPrivHex(gen)
		fmt.Printf(msg("new_done")+"\nappname: %s\nconfig: %s\npubkey: %s\n", appName, cfgPath, pubHex)
		return
	case "del":
		if !hasKey {
			log.Fatal(msg("err_no_key_to_delete"))
		}
		keyCfg.Keys.PrivkeyHex = ""
		if err := writeKeyConfig(cfgPath, keyCfg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf(msg("del_done")+"\nappname: %s\nconfig: %s\n", appName, cfgPath)
		return
	}

	if err := clientapp.ValidateConfig(&cfg); err != nil {
		log.Fatal(err)
	}

	startup := startupSummary{
		AppName:             appName,
		ConfigPath:          cfgPath,
		ConfigStatus:        configFileStatus,
		RuntimeConfigDBPath: dbPath,
		RuntimeConfigStatus: runtimeConfigStatus,
	}
	switch act {
	case "status":
		if err := printStatus(cfgPath, appName, cfg); err != nil {
			log.Fatal(err)
		}
	case "daemon":
		if err := runDaemon(cfg, startup); err != nil {
			log.Fatal(err)
		}
	case "download":
		if err := runDownload(cfg, startup, seedHex); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown action: %s", act)
	}
}

func parseFlags() cliOptions {
	var opts cliOptions
	flag.StringVar(&opts.appName, "appname", "bitfs", msg("flag_appname"))
	flag.StringVar(&opts.configPath, "config", "", msg("flag_config"))
	flag.StringVar(&opts.importHex, "import", "", msg("flag_import"))
	flag.BoolVar(&opts.newKey, "new", false, msg("flag_new"))
	flag.BoolVar(&opts.delKey, "del", false, msg("flag_del"))
	flag.BoolVar(&opts.status, "status", false, msg("flag_status"))
	flag.BoolVar(&opts.daemon, "d", false, msg("flag_daemon"))
	flag.BoolVar(&opts.showVer, "version", false, msg("flag_version"))
	flag.Usage = func() {
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), msg("usage_line"))
		flag.PrintDefaults()
	}
	flag.Parse()
	return opts
}

func resolveAction(opts cliOptions) (string, string, error) {
	args := flag.Args()
	actions := 0
	if strings.TrimSpace(opts.importHex) != "" {
		actions++
	}
	if opts.newKey {
		actions++
	}
	if opts.delKey {
		actions++
	}
	if opts.status {
		actions++
	}
	if opts.daemon {
		actions++
	}
	if actions > 1 {
		return "", "", errors.New(msg("err_actions_mutually_exclusive"))
	}
	if len(args) > 1 {
		return "", "", errors.New(msg("err_only_one_hex"))
	}
	if len(args) == 1 {
		if actions > 0 {
			return "", "", errors.New(msg("err_action_hex_conflict"))
		}
		h := strings.ToLower(strings.TrimSpace(args[0]))
		if h == "" {
			return "", "", errors.New(msg("err_hex_empty"))
		}
		if _, err := hex.DecodeString(h); err != nil {
			return "", "", fmt.Errorf(msg("err_hex_invalid")+": %w", err)
		}
		return "download", h, nil
	}
	if strings.TrimSpace(opts.importHex) != "" {
		return "import", "", nil
	}
	if opts.newKey {
		return "new", "", nil
	}
	if opts.delKey {
		return "del", "", nil
	}
	if opts.daemon {
		return "daemon", "", nil
	}
	return "daemon", "", nil
}

type keyFileConfig struct {
	Keys struct {
		PrivkeyHex string `toml:"privkey_hex"`
	} `toml:"keys"`
}

type keyConfigFileResult struct {
	Config   keyFileConfig
	Created  bool
	Upgraded bool
}

type startupSummary struct {
	AppName             string
	ConfigPath          string
	ConfigStatus        string
	RuntimeConfigDBPath string
	RuntimeConfigStatus string
}

func loadKeyConfigOrEmpty(configPath string) (keyFileConfig, bool, error) {
	if _, err := os.Stat(configPath); err == nil {
		b, err := os.ReadFile(configPath)
		if err != nil {
			return keyFileConfig{}, true, err
		}
		var cfg keyFileConfig
		dec := toml.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			return keyFileConfig{}, true, err
		}
		cfg.Keys.PrivkeyHex = strings.ToLower(strings.TrimSpace(cfg.Keys.PrivkeyHex))
		return cfg, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return keyFileConfig{}, false, err
	}
	return keyFileConfig{}, false, nil
}

func writeKeyConfig(path string, cfg keyFileConfig) error {
	cfg.Keys.PrivkeyHex = strings.ToLower(strings.TrimSpace(cfg.Keys.PrivkeyHex))
	data, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func loadOrInitKeyConfig(configPath string) (keyConfigFileResult, error) {
	cfg, exists, err := loadKeyConfigOrEmpty(configPath)
	if err != nil {
		return keyConfigFileResult{}, err
	}
	if !exists {
		gen, err := generatePrivateKeyHex()
		if err != nil {
			return keyConfigFileResult{}, err
		}
		cfg.Keys.PrivkeyHex = gen
		if err := writeKeyConfig(configPath, cfg); err != nil {
			return keyConfigFileResult{}, err
		}
		return keyConfigFileResult{Config: cfg, Created: true}, nil
	}
	if strings.TrimSpace(cfg.Keys.PrivkeyHex) == "" {
		gen, err := generatePrivateKeyHex()
		if err != nil {
			return keyConfigFileResult{}, err
		}
		cfg.Keys.PrivkeyHex = gen
		if err := writeKeyConfig(configPath, cfg); err != nil {
			return keyConfigFileResult{}, err
		}
		return keyConfigFileResult{Config: cfg, Upgraded: true}, nil
	}
	return keyConfigFileResult{Config: cfg}, nil
}

func newDefaultConfig(appName, network string) (clientapp.Config, error) {
	paths, err := defaultPaths(appName, network)
	if err != nil {
		return clientapp.Config{}, err
	}
	cfg := clientapp.Config{}
	cfg.BSV.Network = network
	cfg.Storage.WorkspaceDir = paths.WorkspaceDir
	cfg.Storage.DataDir = paths.DataDir
	cfg.HTTP.Enabled = true
	cfg.FSHTTP.Enabled = true
	cfg.Log.File = filepath.Join(paths.LogDir, "bitfs.log")
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		return clientapp.Config{}, err
	}
	return cfg, nil
}

func loadRuntimeConfigOrInit(appName, dbPath string) (clientapp.Config, bool, error) {
	defaultCfg, err := newDefaultConfig(appName, "test")
	if err != nil {
		return clientapp.Config{}, false, err
	}
	defaultCfg.Index.Backend = "sqlite"
	defaultCfg.Index.SQLitePath = dbPath

	cfg, created, err := clientapp.LoadOrInitConfigInDB(dbPath, defaultCfg)
	if err != nil {
		return clientapp.Config{}, false, err
	}
	// DB 路径按 appname 推导，配置项不可覆盖。
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = dbPath
	return cfg, created, nil
}

func printStatus(configPath, appName string, cfg clientapp.Config) error {
	pubHex, err := pubHexFromPrivHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		return err
	}
	runtimeCfgDB, runtimeDBErr := defaultConfigDBPath(appName)
	indexDB := effectiveIndexDBPath(cfg)
	addr, bal, balErr := queryAddressBalance(context.Background(), cfg)
	feeIn, feeOut, feeErr := queryFeePoolSummary(cfg)

	fmt.Printf("appname: %s\n", appName)
	fmt.Printf("config: %s\n", configPath)
	fmt.Printf("network: %s\n", cfg.BSV.Network)
	fmt.Printf("workspace_dir: %s\n", absOrRaw(cfg.Storage.WorkspaceDir))
	fmt.Printf("data_dir: %s\n", absOrRaw(cfg.Storage.DataDir))
	if runtimeDBErr == nil {
		fmt.Printf("runtime_config_db: %s\n", absOrRaw(runtimeCfgDB))
	}
	fmt.Printf("index_db: %s\n", absOrRaw(indexDB))
	fmt.Printf("pubkey: %s\n", pubHex)
	if balErr != nil {
		fmt.Printf(msg("status_balance_unavailable")+": %v\n", balErr)
	} else {
		fmt.Printf("address: %s\n", addr)
		fmt.Printf("balance_satoshi: %d\n", bal)
	}
	if feeErr != nil {
		fmt.Printf(msg("status_feepool_unavailable")+": %v\n", feeErr)
		if strings.Contains(strings.ToLower(feeErr.Error()), "no such table") {
			fmt.Println("hint: 先运行一次 `go run ./cmd/client -d` 初始化数据库表。")
		}
	} else {
		fmt.Printf("fee_pool_in_satoshi: %d\n", feeIn)
		fmt.Printf("fee_pool_out_satoshi: %d\n", feeOut)
	}
	fmt.Println("hint: 默认直接运行会进入 daemon；查看状态请使用 `-status`。")
	return nil
}

func runDaemon(cfg clientapp.Config, startup startupSummary) error {
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&cfg)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		return err
	}
	defer func() { _ = obs.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	guardURL, stopGuard, err := woc.EnsureGuardRunning(ctx, woc.GuardRuntimeOptions{Network: cfg.BSV.Network})
	if err != nil {
		return err
	}
	defer stopGuard()

	rt, err := clientapp.Run(ctx, cfg, clientapp.RunOptions{
		WebAssets: webAssets,
		Chain:     woc.NewGuardClient(guardURL),
	})
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	printClientStartupSummary("daemon", startup, guardURL, rt)
	<-ctx.Done()
	return nil
}

func runDownload(cfg clientapp.Config, startup startupSummary, seedHash string) error {
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&cfg)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		return err
	}
	defer func() { _ = obs.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	guardURL, stopGuard, err := woc.EnsureGuardRunning(ctx, woc.GuardRuntimeOptions{Network: cfg.BSV.Network})
	if err != nil {
		return err
	}
	defer stopGuard()

	// 一次性下载模式不需要 HTTP 服务和卖方监听。
	downloadCfg := cfg
	downloadCfg.HTTP.Enabled = false
	downloadCfg.Seller.Enabled = false
	downloadCfg.Scan.StartupFullScan = false

	rt, err := clientapp.Run(ctx, downloadCfg, clientapp.RunOptions{
		WebAssets: webAssets,
		Chain:     woc.NewGuardClient(guardURL),
	})
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	printClientStartupSummary("download", startup, guardURL, rt)
	arbiter := ""
	if len(rt.HealthyArbiters) > 0 {
		arbiter = rt.HealthyArbiters[0].ID.String()
	}
	download, err := clientapp.TriggerOneShotDirectDownload(ctx, rt, clientapp.OneShotDownloadParams{
		SeedHash:      seedHash,
		QuoteMaxRetry: 12,
		QuoteInterval: 2 * time.Second,
		ArbiterPeerID: arbiter,
	})
	if err != nil {
		return err
	}

	seedPath := seedHash + ".bitfs"
	if err := os.WriteFile(seedPath, download.Seed, 0o644); err != nil {
		return err
	}
	fileName := download.FileName
	if err := os.WriteFile(fileName, download.Transfer.Data, 0o644); err != nil {
		return err
	}
	fmt.Printf(msg("download_done")+"\ndemand_id: %s\nseller_count: %d\nchunk_count: %d\nseed_file: %s\nfile: %s\nbytes: %d\nsha256: %s\n",
		download.DemandID, len(download.Transfer.Sellers), download.ChunkCount, seedPath, fileName, len(download.Transfer.Data), download.Transfer.SHA256)
	return nil
}

// 启动摘要只给人看，不进入结构化日志。
func printClientStartupSummary(mode string, startup startupSummary, guardURL string, rt *clientapp.Runtime) {
	pubHex := strings.TrimSpace(rt.Config.ClientID)
	if strings.TrimSpace(rt.Config.Keys.PrivkeyHex) != "" {
		if v, err := pubHexFromPrivHex(rt.Config.Keys.PrivkeyHex); err == nil {
			pubHex = v
		}
	}
	dbPath := effectiveIndexDBPath(rt.Config)
	logFile, _ := clientapp.ResolveLogConfig(&rt.Config)

	fmt.Println("========== BitFS Client 启动信息 ==========")
	fmt.Printf("模式: %s\n", mode)
	fmt.Printf("appname: %s\n", startup.AppName)
	fmt.Printf("配置文件路径: %s\n", absOrRaw(startup.ConfigPath))
	fmt.Printf("配置文件状态: %s\n", startup.ConfigStatus)
	fmt.Printf("运行配置DB: %s\n", absOrRaw(startup.RuntimeConfigDBPath))
	fmt.Printf("运行配置DB状态: %s\n", startup.RuntimeConfigStatus)
	fmt.Printf("网络: %s\n", rt.Config.BSV.Network)
	fmt.Printf("索引数据库: %s\n", absOrRaw(dbPath))
	fmt.Printf("日志文件: %s\n", absOrRaw(logFile))
	fmt.Printf("Guard URL: %s\n", guardURL)
	fmt.Printf("Peer ID: %s\n", rt.Host.ID().String())
	fmt.Printf("公钥(hex): %s\n", pubHex)
	fmt.Printf("网关数量: %d\n", len(rt.HealthyGWs))
	fmt.Printf("仲裁数量: %d\n", len(rt.HealthyArbiters))
	fmt.Printf("HTTP API: enabled=%t listen_addr=%s\n", rt.Config.HTTP.Enabled, rt.Config.HTTP.ListenAddr)
	fmt.Printf("FS HTTP: enabled=%t listen_addr=%s\n", rt.Config.FSHTTP.Enabled, rt.Config.FSHTTP.ListenAddr)
	fmt.Println("监听地址:")
	for _, a := range rt.Host.Addrs() {
		fmt.Printf("  - %s/p2p/%s\n", a.String(), rt.Host.ID().String())
	}
	fmt.Println("===========================================")
}

func effectiveIndexDBPath(cfg clientapp.Config) string {
	dbPath := strings.TrimSpace(cfg.Index.SQLitePath)
	if dbPath == "" {
		dbPath = "db/client-index.sqlite"
	}
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(cfg.Storage.DataDir, dbPath)
	}
	return dbPath
}

func generatePrivateKeyHex() (string, error) {
	k, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, -1)
	if err != nil {
		return "", err
	}
	b, err := crypto.MarshalPrivateKey(k)
	if err != nil {
		return "", err
	}
	return strings.ToLower(hex.EncodeToString(b)), nil
}

func normalizePrivateKeyHex(in string) (string, error) {
	k, err := parsePrivHex(in)
	if err != nil {
		return "", err
	}
	b, err := crypto.MarshalPrivateKey(k)
	if err != nil {
		return "", err
	}
	return strings.ToLower(hex.EncodeToString(b)), nil
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
	b, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, err
	}
	if k, err := crypto.UnmarshalPrivateKey(b); err == nil {
		return k, nil
	}
	if len(b) == 32 {
		return crypto.UnmarshalSecp256k1PrivateKey(b)
	}
	return nil, fmt.Errorf("invalid private key format: expect libp2p MarshalPrivateKey bytes or raw 32-byte secp256k1 key")
}

func queryAddressBalance(ctx context.Context, cfg clientapp.Config) (string, uint64, error) {
	k, err := parsePrivHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		return "", 0, err
	}
	raw, err := k.Raw()
	if err != nil {
		return "", 0, err
	}
	isMain := strings.EqualFold(strings.TrimSpace(cfg.BSV.Network), "main")
	actor, err := dual2of2.BuildActor("client", hex.EncodeToString(raw), isMain)
	if err != nil {
		return "", 0, err
	}
	guardURL, stopGuard, err := woc.EnsureGuardRunning(ctx, woc.GuardRuntimeOptions{Network: cfg.BSV.Network})
	if err != nil {
		return actor.Addr, 0, err
	}
	defer stopGuard()
	utxos, err := woc.NewGuardClient(guardURL).GetUTXOsContext(ctx, actor.Addr)
	if err != nil {
		return actor.Addr, 0, err
	}
	var sum uint64
	for _, u := range utxos {
		sum += u.Value
	}
	return actor.Addr, sum, nil
}

func queryFeePoolSummary(cfg clientapp.Config) (uint64, uint64, error) {
	dbPath := strings.TrimSpace(cfg.Index.SQLitePath)
	if dbPath == "" {
		dbPath = "db/client-index.sqlite"
	}
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(cfg.Storage.DataDir, dbPath)
	}
	if _, err := os.Stat(dbPath); err != nil {
		return 0, 0, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()

	var inSat int64
	if err := db.QueryRow(`SELECT COALESCE(SUM(amount_satoshi),0) FROM wallet_fund_flows WHERE flow_type='fee_pool' AND amount_satoshi>0`).Scan(&inSat); err != nil {
		return 0, 0, err
	}
	var outSat int64
	if err := db.QueryRow(`SELECT COALESCE(SUM(amount_satoshi),0) FROM wallet_fund_flows WHERE flow_type='fee_pool' AND amount_satoshi<0`).Scan(&outSat); err != nil {
		return 0, 0, err
	}
	return uint64(inSat), uint64(-outSat), nil
}

type appPaths struct {
	ConfigPath   string
	ConfigDBPath string
	DataDir      string
	WorkspaceDir string
	LogDir       string
}

func defaultConfigPath(appName string) (string, error) {
	paths, err := defaultPaths(appName, "test")
	if err != nil {
		return "", err
	}
	return paths.ConfigPath, nil
}

func defaultConfigDBPath(appName string) (string, error) {
	paths, err := defaultPaths(appName, "test")
	if err != nil {
		return "", err
	}
	return paths.ConfigDBPath, nil
}

func defaultPaths(appName, network string) (appPaths, error) {
	appName = strings.TrimSpace(appName)
	network = strings.ToLower(strings.TrimSpace(network))
	if appName == "" {
		return appPaths{}, fmt.Errorf("appname cannot be empty")
	}
	if network == "" {
		network = "test"
	}

	switch runtime.GOOS {
	case "windows":
		appData := firstNonEmpty(os.Getenv("APPDATA"), os.Getenv("USERPROFILE"))
		if appData == "" {
			return appPaths{}, fmt.Errorf("APPDATA is not set")
		}
		local := firstNonEmpty(os.Getenv("LOCALAPPDATA"), appData)
		return appPaths{
			ConfigPath:   filepath.Join(appData, appName, "config.toml"),
			ConfigDBPath: filepath.Join(local, appName, "config", "runtime-config.sqlite"),
			DataDir:      filepath.Join(local, appName, network),
			WorkspaceDir: filepath.Join(local, appName, "workspace"),
			LogDir:       filepath.Join(local, appName, network, "Logs"),
		}, nil
	case "darwin":
		home, err := os.UserHomeDir()
		if err != nil {
			return appPaths{}, err
		}
		appSupport := filepath.Join(home, "Library", "Application Support")
		return appPaths{
			ConfigPath:   filepath.Join(appSupport, appName, "config.toml"),
			ConfigDBPath: filepath.Join(appSupport, appName, "config", "runtime-config.sqlite"),
			DataDir:      filepath.Join(appSupport, appName, network),
			WorkspaceDir: filepath.Join(appSupport, appName, "workspace"),
			LogDir:       filepath.Join(home, "Library", "Logs", appName, network),
		}, nil
	default:
		home, err := os.UserHomeDir()
		if err != nil {
			return appPaths{}, err
		}
		cfgBase, err := os.UserConfigDir()
		if err != nil {
			cfgBase = filepath.Join(home, ".config")
		}
		return appPaths{
			ConfigPath:   filepath.Join(cfgBase, appName, "config.toml"),
			ConfigDBPath: filepath.Join(home, ".local", "state", appName, "runtime-config.sqlite"),
			DataDir:      filepath.Join(home, ".local", "share", appName, network),
			WorkspaceDir: filepath.Join(home, ".local", "share", appName, "workspace"),
			LogDir:       filepath.Join(home, ".local", "state", appName, network),
		}, nil
	}
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
		"usage_line":                     "Usage: bitfs [flags] [seed_hex]",
		"flag_appname":                   "app instance name used to derive config/data/log directories",
		"flag_config":                    "private key config file path (derived from appname by default)",
		"flag_import":                    "import private key hex",
		"flag_new":                       "create new private key",
		"flag_del":                       "delete current private key",
		"flag_status":                    "show status (default action)",
		"flag_daemon":                    "run in daemon mode",
		"flag_version":                   "show version",
		"err_appname_empty":              "-appname cannot be empty",
		"err_key_exists_delete_first":    "private key already exists in current instance, run -del first",
		"err_import_invalid":             "invalid private key for -import",
		"err_no_key_to_delete":           "no private key to delete",
		"config_not_found":               "config file not found",
		"err_missing_key":                "private key is not configured, run -new or -import <hex> first",
		"err_actions_mutually_exclusive": "action flags are mutually exclusive: choose one of -import/-new/-del/-status/-d",
		"err_only_one_hex":               "only one hex argument is allowed",
		"err_action_hex_conflict":        "action flags and seed hex argument are mutually exclusive",
		"err_hex_empty":                  "hex argument cannot be empty",
		"err_hex_invalid":                "invalid hex argument",
		"import_done":                    "Private key imported",
		"new_done":                       "Private key created",
		"del_done":                       "Private key deleted",
		"status_balance_unavailable":     "balance unavailable",
		"status_feepool_unavailable":     "fee pool unavailable",
		"download_done":                  "Download completed",
	},
	"zh-CN": {
		"version_line":                   "bitfs 版本 %s",
		"usage_line":                     "用法: bitfs [flags] [seed_hex]",
		"flag_appname":                   "应用实例名，用于计算配置/数据/日志目录",
		"flag_config":                    "私钥配置文件路径（默认按 appname 计算）",
		"flag_import":                    "导入私钥 hex",
		"flag_new":                       "创建新私钥",
		"flag_del":                       "删除当前私钥",
		"flag_status":                    "显示状态（默认动作）",
		"flag_daemon":                    "后台服务模式",
		"flag_version":                   "显示版本",
		"err_appname_empty":              "-appname 不能为空",
		"err_key_exists_delete_first":    "当前实例已存在私钥，请先执行 -del",
		"err_import_invalid":             "-import 私钥非法",
		"err_no_key_to_delete":           "当前没有可删除的私钥",
		"config_not_found":               "未检测到配置文件",
		"err_missing_key":                "未配置私钥，请先执行 -new 或 -import <hex>",
		"err_actions_mutually_exclusive": "动作命令互斥：-import/-new/-del/-status/-d 只能选一个",
		"err_only_one_hex":               "只能提供一个 hex 参数",
		"err_action_hex_conflict":        "动作命令与 hex 参数互斥",
		"err_hex_empty":                  "hex 参数不能为空",
		"err_hex_invalid":                "hex 参数非法",
		"import_done":                    "已导入私钥",
		"new_done":                       "已新建私钥",
		"del_done":                       "已删除私钥",
		"status_balance_unavailable":     "余额不可用",
		"status_feepool_unavailable":     "费用池不可用",
		"download_done":                  "下载完成",
	},
	"zh-TW": {
		"version_line":                   "bitfs 版本 %s",
		"usage_line":                     "用法: bitfs [flags] [seed_hex]",
		"flag_appname":                   "應用實例名，用於計算配置/資料/日誌目錄",
		"flag_config":                    "私鑰配置檔路徑（預設按 appname 計算）",
		"flag_import":                    "匯入私鑰 hex",
		"flag_new":                       "建立新私鑰",
		"flag_del":                       "刪除目前私鑰",
		"flag_status":                    "顯示狀態（預設動作）",
		"flag_daemon":                    "背景服務模式",
		"flag_version":                   "顯示版本",
		"err_appname_empty":              "-appname 不能為空",
		"err_key_exists_delete_first":    "目前實例已存在私鑰，請先執行 -del",
		"err_import_invalid":             "-import 私鑰不合法",
		"err_no_key_to_delete":           "目前沒有可刪除的私鑰",
		"config_not_found":               "未偵測到配置檔",
		"err_missing_key":                "未配置私鑰，請先執行 -new 或 -import <hex>",
		"err_actions_mutually_exclusive": "動作命令互斥：-import/-new/-del/-status/-d 只能選一個",
		"err_only_one_hex":               "只能提供一個 hex 參數",
		"err_action_hex_conflict":        "動作命令與 hex 參數互斥",
		"err_hex_empty":                  "hex 參數不能為空",
		"err_hex_invalid":                "hex 參數不合法",
		"import_done":                    "已匯入私鑰",
		"new_done":                       "已建立私鑰",
		"del_done":                       "已刪除私鑰",
		"status_balance_unavailable":     "餘額不可用",
		"status_feepool_unavailable":     "費用池不可用",
		"download_done":                  "下載完成",
	},
	"ja": {
		"version_line":                   "bitfs バージョン %s",
		"usage_line":                     "使い方: bitfs [flags] [seed_hex]",
		"flag_appname":                   "設定/データ/ログ ディレクトリ計算に使うアプリ名",
		"flag_config":                    "秘密鍵設定ファイルのパス（既定は appname から計算）",
		"flag_import":                    "秘密鍵 hex をインポート",
		"flag_new":                       "新しい秘密鍵を作成",
		"flag_del":                       "現在の秘密鍵を削除",
		"flag_status":                    "状態を表示（デフォルト動作）",
		"flag_daemon":                    "デーモンモードで起動",
		"flag_version":                   "バージョンを表示",
		"err_appname_empty":              "-appname は空にできません",
		"err_key_exists_delete_first":    "このインスタンスには既に秘密鍵があります。先に -del を実行してください",
		"err_import_invalid":             "-import の秘密鍵が不正です",
		"err_no_key_to_delete":           "削除できる秘密鍵がありません",
		"config_not_found":               "設定ファイルが見つかりません",
		"err_missing_key":                "秘密鍵が未設定です。先に -new または -import <hex> を実行してください",
		"err_actions_mutually_exclusive": "アクションフラグは排他です: -import/-new/-del/-status/-d のいずれか1つのみ",
		"err_only_one_hex":               "hex 引数は1つだけ指定できます",
		"err_action_hex_conflict":        "アクションフラグと seed hex 引数は同時指定できません",
		"err_hex_empty":                  "hex 引数は空にできません",
		"err_hex_invalid":                "hex 引数が不正です",
		"import_done":                    "秘密鍵をインポートしました",
		"new_done":                       "秘密鍵を作成しました",
		"del_done":                       "秘密鍵を削除しました",
		"status_balance_unavailable":     "残高を取得できません",
		"status_feepool_unavailable":     "手数料プール情報を取得できません",
		"download_done":                  "ダウンロード完了",
	},
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func absOrRaw(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}
