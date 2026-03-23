package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
	chainapi "github.com/bsv8/BSVChainAPI"
	"github.com/bsv8/BitFS/pkg/clientapp"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type managedDaemon struct {
	initNetwork string
	cfg         clientapp.Config
	startup     startupSummary
	overrides   runtimeListenOverrides
	desktop     desktopBootstrapOptions

	rootCtx    context.Context
	rootCancel context.CancelFunc

	srv *http.Server

	mu        sync.RWMutex
	rt        *clientapp.Runtime
	rtCancel  context.CancelFunc
	rtAPI     http.Handler
	guardStop func()

	systemHomepage *systemHomepageState
}

func runManagedDaemon(cfg clientapp.Config, startup startupSummary, initNetwork string, overrides runtimeListenOverrides, desktop desktopBootstrapOptions) error {
	rootCtx, rootCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer rootCancel()
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&cfg)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		return err
	}
	defer func() { _ = obs.Close() }()

	d := &managedDaemon{
		initNetwork: initNetwork,
		cfg:         cfg,
		startup:     startup,
		overrides:   overrides,
		desktop:     desktop,
		rootCtx:     rootCtx,
		rootCancel:  rootCancel,
	}
	if err := d.prepareSystemHomepage(); err != nil {
		return err
	}
	if err := d.startHTTPServer(); err != nil {
		return err
	}
	// 控制台先打印“解锁前可见运行摘要”，再进入密码输入阶段。
	d.printLockedStartupSummary()
	// 缺省启动路径进入“命令行解锁循环”：
	// - locked + 有密钥：提示输入密码；
	// - API 若先解锁：命令行输入会自动取消并提示已解锁；
	// - API 再次 lock：会重新进入等待密码状态。
	go d.cliUnlockLoop()
	<-rootCtx.Done()
	return d.close()
}

func (d *managedDaemon) close() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	guardStop := d.guardStop
	d.rtCancel = nil
	d.rt = nil
	d.rtAPI = nil
	d.guardStop = nil
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if guardStop != nil {
		guardStop()
	}
	if d.srv != nil {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = d.srv.Shutdown(ctx)
		stop()
	}
	return nil
}

func (d *managedDaemon) startHTTPServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/key/status", d.handleKeyStatus)
	mux.HandleFunc("/api/v1/key/new", d.handleKeyNew)
	mux.HandleFunc("/api/v1/key/import", d.handleKeyImport)
	mux.HandleFunc("/api/v1/key/export", d.handleKeyExport)
	mux.HandleFunc("/api/v1/key/unlock", d.handleKeyUnlock)
	mux.HandleFunc("/api/v1/key/lock", d.handleKeyLock)
	mux.HandleFunc("/api", d.handleAPIProxyOrLocked)
	mux.HandleFunc("/api/", d.handleAPIProxyOrLocked)
	mux.HandleFunc("/", d.handleWebAsset)

	d.srv = &http.Server{
		Addr:              d.cfg.HTTP.ListenAddr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 启动阶段先完成端口预绑定，避免异步 ListenAndServe 把绑定失败吞进后台日志，
	// 导致前台仅看到“解锁提示后退出”而没有明确错误。
	ln, err := net.Listen("tcp", d.cfg.HTTP.ListenAddr)
	if err != nil {
		return fmt.Errorf("start managed api listen failed: %w", err)
	}
	obs.Important("bitcast-client", "managed_api_started", map[string]any{
		"listen_addr": d.cfg.HTTP.ListenAddr,
		"config_path": d.startup.ConfigPath,
		"locked":      true,
	})
	go func() {
		if err := d.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			obs.Error("bitcast-client", "managed_api_stopped", map[string]any{"error": err.Error()})
			d.rootCancel()
		}
	}()
	return nil
}

func (d *managedDaemon) handleWebAsset(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(webAssets, "web")
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
		return
	}
	name := strings.TrimPrefix(pathClean(r.URL.Path), "/")
	if name == "" {
		name = "index.html"
	}
	if strings.Contains(name, "..") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
		return
	}
	data, err := fs.ReadFile(sub, name)
	if err != nil {
		if name != "index.html" {
			data, err = fs.ReadFile(sub, "index.html")
		}
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
			return
		}
		name = "index.html"
	}
	if ct := mime.TypeByExtension(filepath.Ext(name)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (d *managedDaemon) handleAPIProxyOrLocked(w http.ResponseWriter, r *http.Request) {
	api := d.currentRuntimeAPI()
	if api == nil {
		writeJSON(w, http.StatusLocked, map[string]any{"error": "client is locked"})
		return
	}
	api.ServeHTTP(w, r)
}

func (d *managedDaemon) handleKeyStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	_, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	d.mu.RLock()
	unlocked := d.rt != nil
	d.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"vault_path":             d.startup.VaultPath,
		"config_path":            d.startup.ConfigPath,
		"key_path":               d.startup.KeyPath,
		"index_db_path":          d.startup.IndexDBPath,
		"has_key":                exists,
		"unlocked":               unlocked,
		"has_system_home_bundle": d.systemHomepage != nil && d.systemHomepage.HasBundle(),
		"default_home_seed_hash": d.defaultHomeSeedHash(),
	})
}

func (d *managedDaemon) handleKeyNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
		return
	}
	if _, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	} else if exists {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "encrypted key already exists"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.Password) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "password is required"})
		return
	}
	privHex, err := generatePrivateKeyHex()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	env, err := encryptPrivateKeyEnvelope(privHex, req.Password)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := saveEncryptedKeyEnvelope(d.startup.KeyPath, env); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	pubHex, _ := pubHexFromPrivHex(privHex)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pubkey_hex": pubHex})
}

func (d *managedDaemon) handleKeyImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
		return
	}
	if _, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	} else if exists {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "encrypted key already exists"})
		return
	}
	var req struct {
		Cipher *encryptedKeyEnvelope `json:"cipher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if req.Cipher == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cipher is required"})
		return
	}
	if err := saveEncryptedKeyEnvelope(d.startup.KeyPath, *req.Cipher); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (d *managedDaemon) handleKeyExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	env, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !exists {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "encrypted key not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cipher": env})
}

func (d *managedDaemon) handleKeyUnlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if d.isUnlocked() {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
		return
	}
	env, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !exists || env == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "encrypted key not found"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	privHex, err := decryptPrivateKeyEnvelope(*env, req.Password)
	if err != nil {
		obs.Error("bitcast-client", "api_unlock_decrypt_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（密码或密钥材料错误）: %s\n", err.Error())
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
		return
	}
	if err := d.startRuntime(privHex); err != nil {
		obs.Error("bitcast-client", "api_unlock_start_runtime_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（运行时启动失败）: %s\n", err.Error())
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
}

func (d *managedDaemon) handleKeyLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := d.stopRuntime(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": false})
}

func (d *managedDaemon) startRuntime(privHex string) error {
	d.mu.Lock()
	if d.rt != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	guardURL, stopGuard, err := woc.EnsureGuardRunning(d.rootCtx, woc.GuardRuntimeOptions{Network: d.cfg.BSV.Network})
	if err != nil {
		return err
	}

	runCfg, _, err := loadRuntimeConfigOrInit(d.startup.ConfigPath, d.initNetwork)
	if err != nil {
		stopGuard()
		return err
	}
	d.applyDesktopRuntimeBootstrap(&runCfg)
	d.overrides.apply(&runCfg)
	runIn := clientapp.NewRunInputFromConfig(runCfg, privHex)
	runIn.ConfigPath = d.startup.ConfigPath
	runIn.PostWorkspaceBootstrap = d.systemHomepageBootstrapHook()
	// 设计说明：
	// managed 模式统一由单一入口承载 API，不再启动 runtime 内部 HTTP 监听。
	runIn.DisableHTTPServer = true
	runIn.WebAssets = webAssets
	actionChain, err := chainbridge.NewDefaultFeePoolChain(chainbridge.RouteConfig{
		Provider: chainapi.WhatsOnChainProvider,
		Network:  d.cfg.BSV.Network,
	}, 1*time.Second)
	if err != nil {
		stopGuard()
		return err
	}
	runIn.ActionChain = actionChain
	runIn.WalletChain = woc.NewGuardClient(guardURL)

	runCtx, cancel := context.WithCancel(d.rootCtx)
	rt, err := clientapp.Run(runCtx, runIn)
	if err != nil {
		cancel()
		stopGuard()
		return err
	}
	runtimeAPI, err := clientapp.NewRuntimeAPIHandler(rt, webAssets)
	if err != nil {
		_ = rt.Close()
		cancel()
		stopGuard()
		return err
	}

	d.mu.Lock()
	d.rt = rt
	d.rtCancel = cancel
	d.rtAPI = runtimeAPI
	d.guardStop = stopGuard
	d.mu.Unlock()

	// 控制台打印“解锁后运行摘要”。
	// 设计约束：统一放在 startRuntime 成功路径，保证 CLI/API 两种解锁方式输出一致。
	d.printUnlockedRuntimeSummary(runCfg, rt)
	obs.Important("bitcast-client", "managed_runtime_started", map[string]any{
		"transport_peer_id": rt.Host.ID().String(),
	})
	return nil
}

func (d *managedDaemon) prepareSystemHomepage() error {
	state, err := loadSystemHomepageState(d.desktop.systemHomepageBundle, d.cfg.Storage.WorkspaceDir)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if err := state.InstallIntoWorkspace(); err != nil {
		return err
	}
	d.systemHomepage = state
	logSystemHomepageInstalled(state)
	return nil
}

func (d *managedDaemon) systemHomepageBootstrapHook() func(db *sql.DB) error {
	if d == nil || d.systemHomepage == nil {
		return nil
	}
	resaleDiscountBPS := d.cfg.Seller.Pricing.ResaleDiscountBPS
	return func(db *sql.DB) error {
		if db == nil {
			return fmt.Errorf("runtime db not ready for system homepage bootstrap")
		}
		if err := d.systemHomepage.ApplySeedMetadata(db); err != nil {
			return err
		}
		return d.systemHomepage.EnsureSeedPrices(db, resaleDiscountBPS)
	}
}

func (d *managedDaemon) applyDesktopRuntimeBootstrap(cfg *clientapp.Config) {
	if d == nil || cfg == nil {
		return
	}
	if d.systemHomepage == nil {
		return
	}
	// 设计说明：
	// - 桌面托管模式要求“系统首页”在解锁后立刻可本地命中；
	// - 因此这次 runtime 启动必须先完成 workspace 首次全量扫描，再进入首页补价与首页打开阶段；
	// - 这里作为“本次运行覆盖”强制打开 startup_full_scan，不修改用户长期配置文件。
	cfg.Scan.StartupFullScan = true
}

func (d *managedDaemon) defaultHomeSeedHash() string {
	if d.systemHomepage == nil {
		return ""
	}
	return strings.TrimSpace(d.systemHomepage.DefaultSeedHash)
}

func (d *managedDaemon) printLockedStartupSummary() {
	fmt.Fprintf(os.Stderr, "=== BitFS 客户端启动信息（待解锁）===\n")
	fmt.Fprintf(os.Stderr, "vault_path: %s\n", d.startup.VaultPath)
	fmt.Fprintf(os.Stderr, "config_path: %s\n", d.startup.ConfigPath)
	fmt.Fprintf(os.Stderr, "key_path: %s\n", d.startup.KeyPath)
	fmt.Fprintf(os.Stderr, "network: %s\n", d.currentNetworkName())
	fmt.Fprintf(os.Stderr, "managed_api.listen_addr: %s\n", strings.TrimSpace(d.cfg.HTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "index_db_path: %s\n", strings.TrimSpace(d.startup.IndexDBPath))
	fmt.Fprintf(os.Stderr, "runtime_config.status: %s\n", strings.TrimSpace(d.startup.RuntimeConfigStatus))
	fmt.Fprintf(os.Stderr, "状态: 已启动（锁定），等待解锁密码或管理 API 解锁。\n")
}

func (d *managedDaemon) printUnlockedRuntimeSummary(runCfg clientapp.Config, rt *clientapp.Runtime) {
	if rt == nil {
		return
	}
	pubHex, pubErr := runtimePubKeyHex(rt)
	pubLine := pubHex
	if pubErr != nil {
		pubLine = "unavailable (" + pubErr.Error() + ")"
	}
	fmt.Fprintf(os.Stderr, "=== BitFS 客户端运行信息（已解锁）===\n")
	fmt.Fprintf(os.Stderr, "vault_path: %s\n", d.startup.VaultPath)
	fmt.Fprintf(os.Stderr, "config_path: %s\n", d.startup.ConfigPath)
	fmt.Fprintf(os.Stderr, "key_path: %s\n", d.startup.KeyPath)
	fmt.Fprintf(os.Stderr, "network: %s\n", d.currentNetworkName())
	fmt.Fprintf(os.Stderr, "pubkey_hex: %s\n", strings.TrimSpace(pubLine))
	fmt.Fprintf(os.Stderr, "transport_peer_id: %s\n", strings.TrimSpace(rt.Host.ID().String()))
	fmt.Fprintf(os.Stderr, "managed_api.listen_addr: %s\n", strings.TrimSpace(d.cfg.HTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "fs_http.listen_addr: %s\n", strings.TrimSpace(runCfg.FSHTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "index_db_path: %s\n", strings.TrimSpace(d.startup.IndexDBPath))
}

func runtimePubKeyHex(rt *clientapp.Runtime) (string, error) {
	if rt == nil || rt.Host == nil {
		return "", fmt.Errorf("runtime host not ready")
	}
	pub := rt.Host.Peerstore().PubKey(rt.Host.ID())
	if pub == nil {
		return "", fmt.Errorf("missing host public key")
	}
	raw, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("marshal host public key failed: %w", err)
	}
	return strings.ToLower(strings.TrimSpace(hex.EncodeToString(raw))), nil
}

func (d *managedDaemon) currentNetworkName() string {
	if n := strings.TrimSpace(d.cfg.BSV.Network); n != "" {
		return n
	}
	if n := strings.TrimSpace(d.initNetwork); n != "" {
		return n
	}
	return "unknown"
}

func (d *managedDaemon) stopRuntime() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	guardStop := d.guardStop
	d.rtCancel = nil
	d.rt = nil
	d.rtAPI = nil
	d.guardStop = nil
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if guardStop != nil {
		guardStop()
	}
	obs.Important("bitcast-client", "managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
	return nil
}

func (d *managedDaemon) cliUnlockLoop() {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		obs.Info("bitcast-client", "cli_unlock_loop_skipped_non_interactive_stdin", map[string]any{"vault_path": d.startup.VaultPath})
		return
	}
	for {
		select {
		case <-d.rootCtx.Done():
			return
		default:
		}
		if d.isUnlocked() {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		env, exists, err := loadEncryptedKeyEnvelope(d.startup.KeyPath)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_load_key_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if !exists || env == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		password, cancelled, err := d.readPasswordCancelable(msg("prompt_password_unlock"))
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_read_password_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cancelled {
			// 管理 API 已解锁，等待下一轮状态变化（例如再次 lock）。
			time.Sleep(150 * time.Millisecond)
			continue
		}
		password = strings.TrimSpace(password)
		if password == "" {
			obs.Info("bitcast-client", "cli_unlock_empty_password", map[string]any{"vault_path": d.startup.VaultPath})
			continue
		}
		if d.isUnlocked() {
			fmt.Fprintln(os.Stderr, "已解锁（管理 API 已生效）")
			continue
		}
		privHex, err := decryptPrivateKeyEnvelope(*env, password)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_decrypt_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "解锁失败（密码或密钥材料错误）: %s\n", err.Error())
			continue
		}
		if err := d.startRuntime(privHex); err != nil {
			obs.Error("bitcast-client", "cli_unlock_start_runtime_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "解锁失败（运行时启动失败）: %s\n", err.Error())
			continue
		}
		obs.Important("bitcast-client", "cli_unlock_succeeded", map[string]any{"vault_path": d.startup.VaultPath})
	}
}

// readPasswordCancelable 在终端输入密码时支持“被 API 解锁后取消等待”。
// 返回 cancelled=true 表示输入过程中检测到已解锁，调用方应停止本次输入流程。
func (d *managedDaemon) readPasswordCancelable(prompt string) (password string, cancelled bool, err error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		p, e := readPassword(prompt)
		return p, false, e
	}
	fmt.Fprint(os.Stderr, prompt)
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = term.Restore(fd, oldState)
		fmt.Fprintln(os.Stderr)
	}()

	buf := make([]byte, 0, 128)
	for {
		if d.isUnlocked() {
			fmt.Fprint(os.Stderr, "\r已解锁（管理 API 已生效），取消命令行密码输入。")
			return "", true, nil
		}
		select {
		case <-d.rootCtx.Done():
			return "", false, d.rootCtx.Err()
		default:
		}

		pollFds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, pollErr := unix.Poll(pollFds, 200)
		if pollErr != nil {
			if errors.Is(pollErr, unix.EINTR) {
				continue
			}
			return "", false, pollErr
		}
		if n == 0 {
			continue
		}
		var one [1]byte
		readN, readErr := unix.Read(fd, one[:])
		if readErr != nil {
			if errors.Is(readErr, unix.EINTR) {
				continue
			}
			return "", false, readErr
		}
		if readN != 1 {
			continue
		}
		ch := one[0]
		switch ch {
		case '\r', '\n':
			return strings.TrimSpace(string(buf)), false, nil
		case 127, 8:
			if len(buf) > 0 {
				buf = buf[:len(buf)-1]
			}
		case 3:
			return "", false, context.Canceled
		default:
			// 忽略控制字符，仅收集可见输入。
			if ch >= 32 && ch <= 126 {
				buf = append(buf, ch)
			}
		}
	}
}

func (d *managedDaemon) currentRuntimeAPI() http.Handler {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rtAPI
}

func (d *managedDaemon) isUnlocked() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rt != nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func pathClean(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return filepath.ToSlash(filepath.Clean(p))
}
