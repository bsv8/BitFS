package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
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
	_ "modernc.org/sqlite"
)

var version = "dev"

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
		fmt.Printf("bitfs version %s\n", version)
		return
	}

	appName := strings.TrimSpace(opts.appName)
	if appName == "" {
		log.Fatal("-appname 不能为空")
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

	act, seedHex, err := resolveAction(opts)
	if err != nil {
		log.Fatal(err)
	}

	cfg, raw, cfgExists, err := loadConfigOrDefaults(cfgPath, appName)
	if err != nil {
		log.Fatal(err)
	}

	hasKey := strings.TrimSpace(cfg.Keys.PrivkeyHex) != ""
	switch act {
	case "import":
		if hasKey {
			log.Fatal("当前实例已存在私钥，请先执行 -del")
		}
		norm, err := normalizePrivateKeyHex(opts.importHex)
		if err != nil {
			log.Fatalf("-import 私钥非法: %v", err)
		}
		cfg.Keys.PrivkeyHex = norm
		if err := writeConfig(cfgPath, cfg); err != nil {
			log.Fatal(err)
		}
		pubHex, _ := pubHexFromPrivHex(norm)
		fmt.Printf("已导入私钥\nappname: %s\nconfig: %s\npubkey: %s\n", appName, cfgPath, pubHex)
		return
	case "new":
		if hasKey {
			log.Fatal("当前实例已存在私钥，请先执行 -del")
		}
		gen, err := generatePrivateKeyHex()
		if err != nil {
			log.Fatal(err)
		}
		cfg.Keys.PrivkeyHex = gen
		if err := writeConfig(cfgPath, cfg); err != nil {
			log.Fatal(err)
		}
		pubHex, _ := pubHexFromPrivHex(gen)
		fmt.Printf("已新建私钥\nappname: %s\nconfig: %s\npubkey: %s\n", appName, cfgPath, pubHex)
		return
	case "del":
		if !hasKey {
			log.Fatal("当前没有可删除的私钥")
		}
		cfg.Keys.PrivkeyHex = ""
		if err := writeConfig(cfgPath, cfg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("已删除私钥\nappname: %s\nconfig: %s\n", appName, cfgPath)
		return
	}

	if !hasKey {
		if !cfgExists {
			fmt.Printf("未检测到配置文件: %s\n", cfgPath)
		}
		log.Fatal("未配置私钥，请先执行 -new 或 -import <hex>")
	}

	switch act {
	case "status":
		if err := printStatus(cfgPath, appName, cfg); err != nil {
			log.Fatal(err)
		}
	case "daemon":
		if !cfgExists {
			if err := writeConfig(cfgPath, cfg); err != nil {
				log.Fatal(err)
			}
		}
		if err := runDaemon(cfgPath, cfg, raw); err != nil {
			log.Fatal(err)
		}
	case "download":
		if err := runDownload(cfgPath, cfg, raw, seedHex); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown action: %s", act)
	}
}

func parseFlags() cliOptions {
	var opts cliOptions
	flag.StringVar(&opts.appName, "appname", "bitfs", "应用实例名，用于计算配置/数据/日志目录")
	flag.StringVar(&opts.configPath, "config", "", "配置文件路径（默认按 appname 计算）")
	flag.StringVar(&opts.importHex, "import", "", "导入私钥 hex")
	flag.BoolVar(&opts.newKey, "new", false, "创建新私钥")
	flag.BoolVar(&opts.delKey, "del", false, "删除当前私钥")
	flag.BoolVar(&opts.status, "status", false, "显示状态（默认动作）")
	flag.BoolVar(&opts.daemon, "d", false, "后台服务模式")
	flag.BoolVar(&opts.showVer, "version", false, "显示版本")
	flag.Usage = func() {
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), "Usage: bitfs [flags] [seed_hex]")
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
		return "", "", fmt.Errorf("动作命令互斥：-import/-new/-del/-status/-d 只能选一个")
	}
	if len(args) > 1 {
		return "", "", fmt.Errorf("只能提供一个 hex 参数")
	}
	if len(args) == 1 {
		if actions > 0 {
			return "", "", fmt.Errorf("动作命令与 hex 参数互斥")
		}
		h := strings.ToLower(strings.TrimSpace(args[0]))
		if h == "" {
			return "", "", fmt.Errorf("hex 参数不能为空")
		}
		if _, err := hex.DecodeString(h); err != nil {
			return "", "", fmt.Errorf("hex 参数非法: %w", err)
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
	return "status", "", nil
}

func loadConfigOrDefaults(configPath, appName string) (clientapp.Config, []byte, bool, error) {
	if _, err := os.Stat(configPath); err == nil {
		cfg, raw, err := clientapp.LoadConfig(configPath)
		if err != nil {
			return clientapp.Config{}, nil, true, err
		}
		if err := fillPathDefaults(&cfg, appName); err != nil {
			return clientapp.Config{}, nil, true, err
		}
		if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
			return clientapp.Config{}, nil, true, err
		}
		return cfg, raw, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return clientapp.Config{}, nil, false, err
	}

	cfg, err := newDefaultConfig(appName, "test")
	if err != nil {
		return clientapp.Config{}, nil, false, err
	}
	return cfg, nil, false, nil
}

func fillPathDefaults(cfg *clientapp.Config, appName string) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	network := strings.ToLower(strings.TrimSpace(cfg.BSV.Network))
	if network == "" {
		network = "test"
	}
	paths, err := defaultPaths(appName, network)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Storage.DataDir) == "" {
		cfg.Storage.DataDir = paths.DataDir
	}
	if strings.TrimSpace(cfg.Storage.WorkspaceDir) == "" {
		cfg.Storage.WorkspaceDir = paths.WorkspaceDir
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = filepath.Join(paths.LogDir, "bitfs.log")
	}
	return nil
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
	cfg.Log.File = filepath.Join(paths.LogDir, "bitfs.log")
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		return clientapp.Config{}, err
	}
	return cfg, nil
}

func writeConfig(path string, cfg clientapp.Config) error {
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		return err
	}
	data, err := clientapp.EncodeConfigTOML(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func printStatus(configPath, appName string, cfg clientapp.Config) error {
	pubHex, err := pubHexFromPrivHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		return err
	}
	addr, bal, balErr := queryAddressBalance(context.Background(), cfg)
	feeIn, feeOut, feeErr := queryFeePoolSummary(cfg)

	fmt.Printf("appname: %s\n", appName)
	fmt.Printf("config: %s\n", configPath)
	fmt.Printf("network: %s\n", cfg.BSV.Network)
	fmt.Printf("pubkey: %s\n", pubHex)
	if balErr != nil {
		fmt.Printf("balance: unavailable (%v)\n", balErr)
	} else {
		fmt.Printf("address: %s\n", addr)
		fmt.Printf("balance_satoshi: %d\n", bal)
	}
	if feeErr != nil {
		fmt.Printf("fee_pool: unavailable (%v)\n", feeErr)
	} else {
		fmt.Printf("fee_pool_in_satoshi: %d\n", feeIn)
		fmt.Printf("fee_pool_out_satoshi: %d\n", feeOut)
	}
	return nil
}

func runDaemon(cfgPath string, cfg clientapp.Config, raw []byte) error {
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
		ConfigRaw:  raw,
		WebAssets:  webAssets,
		Chain:      woc.NewGuardClient(guardURL),
		ConfigPath: cfgPath,
	})
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	<-ctx.Done()
	return nil
}

func runDownload(cfgPath string, cfg clientapp.Config, raw []byte, seedHash string) error {
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
		ConfigRaw:  raw,
		WebAssets:  webAssets,
		Chain:      woc.NewGuardClient(guardURL),
		ConfigPath: cfgPath,
	})
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	pub, err := clientapp.TriggerGatewayPublishDemand(ctx, rt, clientapp.PublishDemandParams{
		SeedHash:   seedHash,
		ChunkCount: 1,
	})
	if err != nil {
		return fmt.Errorf("publish demand failed: %w", err)
	}
	quotes, err := waitDirectQuotes(ctx, rt, pub.DemandID, 12, 2*time.Second)
	if err != nil {
		return err
	}
	if len(quotes) == 0 {
		return fmt.Errorf("no direct quotes for demand_id=%s", pub.DemandID)
	}
	q := quotes[0]

	arbiter := ""
	if len(rt.HealthyArbiters) > 0 {
		arbiter = rt.HealthyArbiters[0].ID.String()
	}
	transfer, err := clientapp.TriggerTransferChunks(ctx, rt, clientapp.TransferChunksParams{
		SellerPeerID:  q.SellerPeerID,
		DemandID:      pub.DemandID,
		ArbiterPeerID: arbiter,
		SeedHash:      seedHash,
		SeedPrice:     q.SeedPrice,
		ChunkPrice:    q.ChunkPrice,
		ExpiresAtUnix: q.ExpiresAtUnix,
		ChunkCount:    0, // 由 seed metadata 决定实际块数（全量）
	})
	if err != nil {
		return fmt.Errorf("direct transfer failed: %w", err)
	}

	seedGet, err := clientapp.TriggerClientSeedGet(ctx, rt, clientapp.SeedGetParams{
		SellerPeerID: q.SellerPeerID,
		SessionID:    transfer.SessionID,
		SeedHash:     seedHash,
	})
	if err != nil {
		return fmt.Errorf("seed get failed: %w", err)
	}
	chunkCount, err := verifySeedHash(seedGet.Seed, seedHash)
	if err != nil {
		return err
	}

	seedPath := seedHash + ".bitfs"
	if err := os.WriteFile(seedPath, seedGet.Seed, 0o644); err != nil {
		return err
	}
	fileName := chooseDownloadFileName(q.RecommendedFileName, seedHash)
	if err := os.WriteFile(fileName, transfer.Data, 0o644); err != nil {
		return err
	}
	fmt.Printf("下载完成\ndemand_id: %s\nsession_id: %s\nchunk_count: %d\nseed_file: %s\nfile: %s\nbytes: %d\nsha256: %s\n",
		pub.DemandID, transfer.SessionID, chunkCount, seedPath, fileName, len(transfer.Data), transfer.SHA256)
	return nil
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

func defaultPaths(appName, network string) (appPaths, error) {
	appName = strings.TrimSpace(appName)
	network = strings.ToLower(strings.TrimSpace(network))
	if appName == "" {
		return appPaths{}, fmt.Errorf("appname 不能为空")
	}
	if network == "" {
		network = "test"
	}

	switch runtime.GOOS {
	case "windows":
		appData := firstNonEmpty(os.Getenv("APPDATA"), os.Getenv("USERPROFILE"))
		if appData == "" {
			return appPaths{}, fmt.Errorf("无法解析 APPDATA")
		}
		local := firstNonEmpty(os.Getenv("LOCALAPPDATA"), appData)
		return appPaths{
			ConfigPath:   filepath.Join(appData, appName, "config.toml"),
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
			DataDir:      filepath.Join(home, ".local", "share", appName, network),
			WorkspaceDir: filepath.Join(home, ".local", "share", appName, "workspace"),
			LogDir:       filepath.Join(home, ".local", "state", appName, network),
		}, nil
	}
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

func waitDirectQuotes(ctx context.Context, rt *clientapp.Runtime, demandID string, maxRetry int, interval time.Duration) ([]clientapp.DirectQuoteItem, error) {
	for i := 0; i < maxRetry; i++ {
		quotes, err := clientapp.TriggerClientListDirectQuotes(ctx, rt, demandID)
		if err != nil {
			return nil, err
		}
		if len(quotes) > 0 {
			return quotes, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
	return nil, nil
}

func verifySeedHash(seed []byte, expectedHash string) (uint32, error) {
	if len(seed) < 22 {
		return 0, fmt.Errorf("invalid seed bytes")
	}
	if string(seed[:4]) != "BSE1" {
		return 0, fmt.Errorf("invalid seed magic")
	}
	chunkCount := binary.BigEndian.Uint32(seed[18:22])
	expectLen := 22 + int(chunkCount)*32
	if len(seed) != expectLen {
		return 0, fmt.Errorf("invalid seed body length")
	}
	h := sha256.Sum256(seed)
	got := strings.ToLower(hex.EncodeToString(h[:]))
	if got != strings.ToLower(strings.TrimSpace(expectedHash)) {
		return 0, fmt.Errorf("seed hash mismatch: expect=%s got=%s", expectedHash, got)
	}
	return chunkCount, nil
}

func chooseDownloadFileName(recommended, seedHash string) string {
	name := strings.TrimSpace(recommended)
	name = strings.ReplaceAll(name, "\\", "/")
	name = filepath.Base(name)
	if name == "." || name == "/" || name == "" {
		return seedHash + ".bin"
	}
	return name
}
