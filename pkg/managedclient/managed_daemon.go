package managedclient

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/bsv8/BFTP/pkg/infra/caps"
	"github.com/bsv8/BFTP/pkg/infra/lhttp"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type managedBackendPhase string

type managedRuntimePhase string

type managedKeyState string

// 设计说明：
// - backendPhase 只描述受管后端自己有没有活着，和钱包是否解锁无关；
// - runtimePhase 只描述钱包运行时本轮会话的状态，首次启动和锁后再解锁都走这里；
// - keyState 只描述 daemon 内存里的密钥是否可用，避免后面再把“有 key 文件”和“runtime ready”混成一个概念。

const (
	managedBackendPhaseStarting     managedBackendPhase = "starting"
	managedBackendPhaseAvailable    managedBackendPhase = "available"
	managedBackendPhaseStartupError managedBackendPhase = "startup_error"
	managedBackendPhaseStopped      managedBackendPhase = "stopped"

	managedRuntimePhaseStopped  managedRuntimePhase = "stopped"
	managedRuntimePhaseStarting managedRuntimePhase = "starting"
	managedRuntimePhaseReady    managedRuntimePhase = "ready"
	managedRuntimePhaseError    managedRuntimePhase = "error"

	managedKeyStateMissing  managedKeyState = "missing"
	managedKeyStateLocked   managedKeyState = "locked"
	managedKeyStateUnlocked managedKeyState = "unlocked"

	managedWOCProxyListenAddr = "127.0.0.1:19183"
	managedWOCUpstreamRootURL = wocproxy.DefaultUpstreamRootURL

	bitfsManagedHTTPKeyAbility      = "bitfs.managed_http.key@1"
	bitfsManagedHTTPProxyAbility    = "bitfs.managed_http.proxy@1"
	bitfsManagedHTTPFallbackAbility = "bitfs.managed_http.fallback@1"
)

type startupErrorState struct {
	Service    string
	ListenAddr string
	Message    string
}

type chainAccessState struct {
	Mode            string
	BaseURL         string
	RouteAuth       chainbridge.AuthConfig
	WalletAuth      whatsonchain.AuthConfig
	WOCProxyEnabled bool
	WOCProxyAddr    string
	UpstreamRootURL string
	MinInterval     time.Duration
}

type managedDaemon struct {
	initNetwork          string
	cfg                  clientapp.Config
	startup              StartupSummary
	overrides            RuntimeListenOverrides
	desktop              DesktopBootstrapOptions
	unlockPasswordPrompt string
	controlStream        ManagedControlStream

	rootCtx    context.Context
	rootCancel context.CancelFunc

	srv *http.Server

	fsHTTPReserved net.Listener
	wocProxySrv    *http.Server

	mu       sync.RWMutex
	rt       *clientapp.Runtime
	rtCancel context.CancelFunc
	rtAPI    http.Handler

	backendPhase        managedBackendPhase
	runtimePhase        managedRuntimePhase
	runtimeErrorMessage string
	unlockedPrivHex     string
	runtimeStartSeq     uint64
	startupError        startupErrorState
	chainAccess         chainAccessState

	systemHomepage *systemHomepageState
}

func RunManagedDaemon(opts DaemonOptions) error {
	rootCtx, rootCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer rootCancel()
	logFile, logConsoleMinLevel := clientapp.ResolveLogConfig(&opts.Config)
	if err := obs.Init(logFile, logConsoleMinLevel); err != nil {
		return err
	}
	defer func() { _ = obs.Close() }()

	d := &managedDaemon{
		initNetwork:          opts.InitNetwork,
		cfg:                  opts.Config,
		startup:              opts.Startup,
		overrides:            opts.Overrides,
		desktop:              opts.Desktop,
		unlockPasswordPrompt: strings.TrimSpace(opts.UnlockPasswordPrompt),
		controlStream:        opts.ControlStream,
		rootCtx:              rootCtx,
		rootCancel:           rootCancel,
		backendPhase:         managedBackendPhaseStarting,
		runtimePhase:         managedRuntimePhaseStopped,
	}
	if d.controlStream == nil {
		d.controlStream = noopManagedControlStream{}
	}
	if d.unlockPasswordPrompt == "" {
		d.unlockPasswordPrompt = "Unlock password: "
	}
	d.chainAccess = d.resolveChainAccessState()
	d.emitBackendSnapshot("booting")
	if err := d.prepareSystemHomepage(); err != nil {
		d.setStartupError("system_homepage", "", err)
		<-rootCtx.Done()
		return d.close()
	}
	d.emitBackendSnapshot("system_homepage_ready")
	if err := d.startHTTPServer(); err != nil {
		d.setStartupError("managed_api", d.cfg.HTTP.ListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	d.emitBackendSnapshot("managed_api_ready")
	if err := d.reserveFSHTTPListener(); err != nil {
		d.setStartupError("fs_http", d.cfg.FSHTTP.ListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	d.emitBackendSnapshot("fs_http_reserved")
	if err := d.startManagedWOCProxy(); err != nil {
		d.setStartupError("woc_proxy", managedWOCProxyListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	d.emitBackendSnapshot("chain_access_ready")
	d.setBackendPhase(managedBackendPhaseAvailable)
	d.printLockedStartupSummary()
	go d.cliUnlockLoop()
	<-rootCtx.Done()
	return d.close()
}

func (d *managedDaemon) close() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	d.rtCancel = nil
	d.rt = nil
	d.rtAPI = nil
	d.unlockedPrivHex = ""
	d.runtimeErrorMessage = ""
	d.runtimePhase = managedRuntimePhaseStopped
	fsHTTPReserved := d.fsHTTPReserved
	d.fsHTTPReserved = nil
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if d.srv != nil {
		ctx, stop := context.WithTimeout(context.WithoutCancel(d.rootCtx), 5*time.Second)
		_ = d.srv.Shutdown(ctx)
		stop()
	}
	if d.wocProxySrv != nil {
		ctx, stop := context.WithTimeout(context.WithoutCancel(d.rootCtx), 5*time.Second)
		_ = d.wocProxySrv.Shutdown(ctx)
		stop()
	}
	if fsHTTPReserved != nil {
		_ = fsHTTPReserved.Close()
	}
	d.setBackendPhase(managedBackendPhaseStopped)
	return nil
}

func (d *managedDaemon) startHTTPServer() error {
	decls := d.httpRouteDecls()
	caps.MustAssemble(lhttp.ModuleSpecs(decls...)...)
	mux := lhttp.NewServeMux(lhttp.FlattenDecls(decls...)...)
	started, err := lhttp.StartServer(lhttp.ServerOptions{
		ListenAddr: d.cfg.HTTP.ListenAddr,
		Handler:    mux,
	})
	if err != nil {
		return err
	}
	d.srv = started.Server
	d.cfg.HTTP.ListenAddr = started.Listener.Addr().String()
	obs.Important("bitcast-client", "managed_api_started", map[string]any{
		"listen_addr":   started.Listener.Addr().String(),
		"config_path":   d.startup.ConfigPath,
		"backend_phase": string(d.currentBackendPhase()),
	})
	lhttp.ServeInBackground(started, func(err error) {
		obs.Error("bitcast-client", "managed_api_stopped", map[string]any{"error": err.Error()})
		d.rootCancel()
	})
	return nil
}

func (d *managedDaemon) httpRouteDecls() []lhttp.RouteDecl {
	return []lhttp.RouteDecl{
		{
			InternalAbility: bitfsManagedHTTPKeyAbility,
			Routes: []lhttp.Route{
				{Path: "/api/v1/key/status", Handler: d.handleKeyStatus},
				{Path: "/api/v1/key/new", Handler: d.handleKeyNew},
				{Path: "/api/v1/key/import", Handler: d.handleKeyImport},
				{Path: "/api/v1/key/export", Handler: d.handleKeyExport},
				{Path: "/api/v1/key/unlock", Handler: d.handleKeyUnlock},
				{Path: "/api/v1/key/lock", Handler: d.handleKeyLock},
			},
		},
		{
			InternalAbility: bitfsManagedHTTPProxyAbility,
			Routes: []lhttp.Route{
				{Path: "/api", Handler: d.handleAPIProxyOrLocked},
				{Path: "/api/", Handler: d.handleAPIProxyOrLocked},
			},
		},
		{
			InternalAbility: bitfsManagedHTTPFallbackAbility,
			Routes: []lhttp.Route{
				{Path: "/", Handler: d.handleNonAPIRequest},
			},
		},
	}
}

func (d *managedDaemon) handleNonAPIRequest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
}

func (d *managedDaemon) handleAPIProxyOrLocked(w http.ResponseWriter, r *http.Request) {
	if phase := d.currentBackendPhase(); phase == managedBackendPhaseStartupError {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":         d.currentStartupError().Message,
			"backend_phase": string(phase),
			"service":       d.currentStartupError().Service,
			"listen_addr":   d.currentStartupError().ListenAddr,
		})
		return
	}
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
	_, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	d.mu.RLock()
	backendPhase := d.backendPhase
	runtimePhase := d.runtimePhase
	runtimeErrorMessage := strings.TrimSpace(d.runtimeErrorMessage)
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	startupErr := d.startupError
	chainAccess := d.chainAccess
	d.mu.RUnlock()
	keyState := managedKeyStateMissing
	switch {
	case unlocked:
		keyState = managedKeyStateUnlocked
	case exists:
		keyState = managedKeyStateLocked
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vault_path":             d.startup.VaultPath,
		"config_path":            d.startup.ConfigPath,
		"key_path":               d.startup.KeyPath,
		"index_db_path":          d.startup.IndexDBPath,
		"backend_phase":          string(backendPhase),
		"runtime_phase":          string(runtimePhase),
		"key_state":              string(keyState),
		"startup_error_service":  startupErr.Service,
		"startup_error_listen":   startupErr.ListenAddr,
		"startup_error_message":  startupErr.Message,
		"runtime_error_message":  runtimeErrorMessage,
		"chain_access_mode":      chainAccess.Mode,
		"wallet_chain_base_url":  chainAccess.BaseURL,
		"woc_proxy_enabled":      chainAccess.WOCProxyEnabled,
		"woc_proxy_listen_addr":  chainAccess.WOCProxyAddr,
		"woc_upstream_root_url":  chainAccess.UpstreamRootURL,
		"woc_min_interval":       chainAccess.MinInterval.String(),
		"has_system_home_bundle": d.systemHomepage != nil && d.systemHomepage.HasBundle(),
		"default_home_seed_hash": d.defaultHomeSeedHash(),
	})
}

func (d *managedDaemon) handleKeyNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client key is already unlocked, lock first"})
		return
	}
	if _, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath); err != nil {
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
	privHex, err := GeneratePrivateKeyHex()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	env, err := EncryptPrivateKeyEnvelope(privHex, req.Password)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := SaveEncryptedKeyEnvelope(d.startup.KeyPath, env); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	d.emitBackendSnapshot("key_material_ready")
	pubHex, _ := PubHexFromPrivHex(privHex)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pubkey_hex": pubHex})
}

func (d *managedDaemon) handleKeyImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client key is already unlocked, lock first"})
		return
	}
	if _, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	} else if exists {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "encrypted key already exists"})
		return
	}
	var req struct {
		Cipher *EncryptedKeyEnvelope `json:"cipher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if req.Cipher == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cipher is required"})
		return
	}
	if err := SaveEncryptedKeyEnvelope(d.startup.KeyPath, *req.Cipher); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	d.emitBackendSnapshot("key_material_ready")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (d *managedDaemon) handleKeyExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	env, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath)
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
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		if err := d.startRuntimeAsync(); err != nil {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
		return
	}
	env, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath)
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
	privHex, err := DecryptPrivateKeyEnvelope(*env, req.Password)
	if err != nil {
		obs.Error("bitcast-client", "api_unlock_decrypt_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（密码或密钥材料错误）: %s\n", err.Error())
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
		return
	}
	d.rememberUnlockedKey(privHex)
	if err := d.startRuntimeAsync(); err != nil {
		d.clearUnlockedKey()
		obs.Error("bitcast-client", "api_unlock_start_runtime_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（运行时启动失败）: %s\n", err.Error())
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": true})
}

func (d *managedDaemon) handleKeyLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if err := d.lockRuntime(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": false})
}

func (d *managedDaemon) startRuntime(privHex string, seq uint64) error {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return err
	}
	d.mu.Lock()
	if d.rt != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	runCfg, _, err := LoadRuntimeConfigOrInit(d.startup.ConfigPath, d.initNetwork)
	if err != nil {
		return err
	}
	d.applyDesktopRuntimeBootstrap(&runCfg)
	d.overrides.Apply(&runCfg)
	runIn := clientapp.NewRunInputFromConfig(runCfg, privHex)
	runIn.ConfigPath = d.startup.ConfigPath
	runIn.StartupMode = clientapp.StartupModeProduct
	runIn.PostWorkspaceBootstrap = d.systemHomepageBootstrapHook()
	runIn.DisableHTTPServer = true
	runIn.FSHTTPListener = d.takeReservedFSHTTPListener()
	runIn.ObsSink = d.controlStream.ObsSink()
	indexDBPath := strings.TrimSpace(d.startup.IndexDBPath)
	if !filepath.IsAbs(indexDBPath) {
		indexDBPath = filepath.Join(strings.TrimSpace(runCfg.Storage.DataDir), indexDBPath)
	}
	if err := os.MkdirAll(filepath.Dir(indexDBPath), 0o755); err != nil {
		return err
	}
	openedDB, err := sqliteactor.Open(indexDBPath, runCfg.Debug)
	if err != nil {
		return err
	}
	deps := clientapp.RunDeps{
		Store:   clientapp.NewClientStore(openedDB.DB, openedDB.Actor),
		RawDB:   openedDB.DB,
		DBActor: openedDB.Actor,
		OwnsDB:  true,
	}
	actionChain, err := chainbridge.NewEmbeddedFeePoolChain(chainbridge.RouteConfig{
		Provider: chainbridge.WhatsOnChainProvider,
		Network:  d.cfg.BSV.Network,
		BaseURL:  d.chainAccess.BaseURL,
		Auth:     d.chainAccess.RouteAuth,
	})
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
			_ = d.reserveFSHTTPListener()
		}
		_ = openedDB.Actor.Close()
		return err
	}
	walletChain, err := clientapp.NewWalletChainClientWithBaseURL(chainbridge.Route{
		Provider: chainbridge.WhatsOnChainProvider,
		Network:  d.cfg.BSV.Network,
	}, d.chainAccess.BaseURL, d.chainAccess.WalletAuth)
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
			_ = d.reserveFSHTTPListener()
		}
		_ = openedDB.Actor.Close()
		return err
	}
	runIn.ActionChain = actionChain
	runIn.WalletChain = walletChain

	runCtx, cancel := context.WithCancel(d.rootCtx)
	rt, err := clientapp.Run(runCtx, runIn, deps)
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
			_ = d.reserveFSHTTPListener()
		}
		cancel()
		_ = openedDB.Actor.Close()
		return err
	}
	runtimeAPI, err := clientapp.NewRuntimeAPIHandler(rt)
	if err != nil {
		_ = rt.Close()
		cancel()
		_ = d.reserveFSHTTPListener()
		return err
	}

	if !d.commitRuntimeStartup(seq, rt, cancel, runtimeAPI) {
		_ = rt.Close()
		cancel()
		_ = d.reserveFSHTTPListener()
		return nil
	}
	d.printUnlockedRuntimeSummary(runCfg, rt)
	obs.Important("bitcast-client", "managed_runtime_started", map[string]any{
		"transport_peer_id": rt.Host.ID().String(),
	})
	return nil
}

func (d *managedDaemon) prepareSystemHomepage() error {
	state, err := loadSystemHomepageState(d.desktop.SystemHomepageBundle, d.cfg.Storage.WorkspaceDir)
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

func (d *managedDaemon) systemHomepageBootstrapHook() func(ctx context.Context, store clientapp.ClientStore) error {
	if d == nil || d.systemHomepage == nil {
		return nil
	}
	resaleDiscountBPS := d.cfg.Seller.Pricing.ResaleDiscountBPS
	return func(ctx context.Context, store clientapp.ClientStore) error {
		if ctx == nil {
			return fmt.Errorf("ctx is required")
		}
		if store == nil {
			return fmt.Errorf("runtime db not ready for system homepage bootstrap")
		}
		return store.Do(ctx, func(db clientapp.SQLConn) error {
			rawDB, ok := db.(*sql.DB)
			if !ok {
				return fmt.Errorf("runtime db must be *sql.DB")
			}
			if err := d.systemHomepage.ApplySeedMetadata(rawDB); err != nil {
				return err
			}
			return d.systemHomepage.EnsureSeedPrices(rawDB, resaleDiscountBPS)
		})
	}
}

func (d *managedDaemon) applyDesktopRuntimeBootstrap(cfg *clientapp.Config) {
	if d == nil || cfg == nil {
		return
	}
	if d.systemHomepage == nil {
		return
	}
	cfg.Scan.StartupFullScan = true
}

func (d *managedDaemon) defaultHomeSeedHash() string {
	if d.systemHomepage == nil {
		return ""
	}
	return strings.TrimSpace(d.systemHomepage.DefaultSeedHash)
}

func (d *managedDaemon) printLockedStartupSummary() {
	chainAccess := d.currentChainAccess()
	fmt.Fprintf(os.Stderr, "=== BitFS 客户端启动信息（待解锁）===\n")
	fmt.Fprintf(os.Stderr, "vault_path: %s\n", d.startup.VaultPath)
	fmt.Fprintf(os.Stderr, "config_path: %s\n", d.startup.ConfigPath)
	fmt.Fprintf(os.Stderr, "key_path: %s\n", d.startup.KeyPath)
	fmt.Fprintf(os.Stderr, "network: %s\n", d.currentNetworkName())
	fmt.Fprintf(os.Stderr, "managed_api.listen_addr: %s\n", strings.TrimSpace(d.cfg.HTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "fs_http.listen_addr: %s\n", strings.TrimSpace(d.cfg.FSHTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "chain_access.mode: %s\n", strings.TrimSpace(chainAccess.Mode))
	fmt.Fprintf(os.Stderr, "wallet_chain.base_url: %s\n", strings.TrimSpace(chainAccess.BaseURL))
	if chainAccess.WOCProxyEnabled {
		fmt.Fprintf(os.Stderr, "woc_proxy.listen_addr: %s\n", strings.TrimSpace(chainAccess.WOCProxyAddr))
	}
	fmt.Fprintf(os.Stderr, "index_db_path: %s\n", strings.TrimSpace(d.startup.IndexDBPath))
	fmt.Fprintf(os.Stderr, "runtime_config.status: %s\n", strings.TrimSpace(d.startup.RuntimeConfigStatus))
	fmt.Fprintf(os.Stderr, "状态: 已启动（锁定），等待解锁密码或管理 API 解锁。\n")
}

func (d *managedDaemon) printUnlockedRuntimeSummary(runCfg clientapp.Config, rt *clientapp.Runtime) {
	if rt == nil {
		return
	}
	chainAccess := d.currentChainAccess()
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
	fmt.Fprintf(os.Stderr, "chain_access.mode: %s\n", strings.TrimSpace(chainAccess.Mode))
	fmt.Fprintf(os.Stderr, "wallet_chain.base_url: %s\n", strings.TrimSpace(chainAccess.BaseURL))
	if chainAccess.WOCProxyEnabled {
		fmt.Fprintf(os.Stderr, "woc_proxy.listen_addr: %s\n", strings.TrimSpace(chainAccess.WOCProxyAddr))
	}
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

func (d *managedDaemon) resolveChainAccessState() chainAccessState {
	auth := chainbridge.AuthConfig{
		Mode:  "bearer",
		Value: strings.TrimSpace(d.cfg.ExternalAPI.WOC.APIKey),
	}
	minInterval := time.Duration(d.cfg.ExternalAPI.WOC.MinIntervalMS) * time.Millisecond
	if minInterval <= 0 {
		minInterval = 1 * time.Second
	}
	state := chainAccessState{
		Mode:            "proxy",
		UpstreamRootURL: managedWOCUpstreamRootURL,
		MinInterval:     minInterval,
		RouteAuth:       auth,
		WalletAuth: whatsonchain.AuthConfig{
			Mode:  auth.Mode,
			Name:  auth.Name,
			Value: auth.Value,
		},
	}
	// 设计说明：
	// - e2e 协调层会统一注入 `BSV_CHAIN_API_URL`，要求所有业务进程共享同一条受保护链路；
	// - 桌面独立运行时再退回内嵌 WOC proxy，避免把 e2e 专属端口 guard 暴露给真实产品环境；
	// - 这里必须优先吃环境注入，否则 Electron e2e 会偷偷绕开 shared guard，重新各自起 proxy。
	if injected := strings.TrimRight(strings.TrimSpace(os.Getenv(chainbridge.FeePoolChainBaseURLEnv)), "/"); injected != "" {
		state.Mode = "injected_env"
		state.BaseURL = injected
		state.RouteAuth = chainbridge.AuthConfig{}
		state.WalletAuth = whatsonchain.AuthConfig{}
		state.WOCProxyEnabled = false
		state.WOCProxyAddr = ""
		return state
	}
	if strings.TrimSpace(auth.Value) != "" {
		state.Mode = "direct_woc"
		state.BaseURL = wocproxy.BaseURLForNetwork(state.UpstreamRootURL, d.currentNetworkName())
		return state
	}
	state.WOCProxyEnabled = true
	state.WOCProxyAddr = managedWOCProxyListenAddr
	state.BaseURL = wocproxy.BaseURLForNetwork("http://"+state.WOCProxyAddr, d.currentNetworkName())
	state.RouteAuth = chainbridge.AuthConfig{}
	state.WalletAuth = whatsonchain.AuthConfig{}
	return state
}

func (d *managedDaemon) currentBackendPhase() managedBackendPhase {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.backendPhase
}

func (d *managedDaemon) currentRuntimePhase() managedRuntimePhase {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.runtimePhase
}

func (d *managedDaemon) currentStartupError() startupErrorState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.startupError
}

func (d *managedDaemon) currentChainAccess() chainAccessState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.chainAccess
}

func (d *managedDaemon) currentKeyState() managedKeyState {
	d.mu.RLock()
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	d.mu.RUnlock()
	if unlocked {
		return managedKeyStateUnlocked
	}
	if d.currentKeyExistsLocked() {
		return managedKeyStateLocked
	}
	return managedKeyStateMissing
}

func (d *managedDaemon) setBackendPhase(phase managedBackendPhase) {
	d.mu.Lock()
	d.backendPhase = phase
	if phase != managedBackendPhaseStartupError {
		d.startupError = startupErrorState{}
	}
	d.mu.Unlock()
	d.emitBackendSnapshot(string(phase))
	d.emitPhaseEvent()
}

func (d *managedDaemon) setStartupError(service, listenAddr string, err error) {
	message := ""
	if err != nil {
		message = strings.TrimSpace(err.Error())
	}
	d.mu.Lock()
	d.backendPhase = managedBackendPhaseStartupError
	d.startupError = startupErrorState{
		Service:    strings.TrimSpace(service),
		ListenAddr: strings.TrimSpace(listenAddr),
		Message:    message,
	}
	d.mu.Unlock()
	obs.Error("bitcast-client", "managed_startup_failed", map[string]any{
		"service":     strings.TrimSpace(service),
		"listen_addr": strings.TrimSpace(listenAddr),
		"error":       message,
	})
	d.emitBackendSnapshot("startup_error")
	d.emitPhaseEvent()
}

func (d *managedDaemon) emitBackendSnapshot(step string) {
	if d == nil || d.controlStream == nil {
		return
	}
	d.controlStream.Emit("backend.snapshot", "private", "managed_daemon", "", d.buildBackendSnapshotPayload(step))
}

func (d *managedDaemon) emitPhaseEvent() {
	if d == nil || d.controlStream == nil {
		return
	}
	d.mu.RLock()
	backendPhase := string(d.backendPhase)
	runtimePhase := string(d.runtimePhase)
	runtimeErrorMessage := strings.TrimSpace(d.runtimeErrorMessage)
	startupErr := d.startupError
	chainAccess := d.chainAccess
	hasSystemHomeBundle := d.systemHomepage != nil && d.systemHomepage.HasBundle()
	defaultHomeSeedHash := d.defaultHomeSeedHash()
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	d.mu.RUnlock()
	keyState := managedKeyStateMissing
	switch {
	case unlocked:
		keyState = managedKeyStateUnlocked
	case d.currentKeyExistsLocked():
		keyState = managedKeyStateLocked
	}
	lastError := runtimeErrorMessage
	if strings.TrimSpace(startupErr.Message) != "" {
		lastError = strings.TrimSpace(startupErr.Message)
	}

	privatePayload := map[string]any{
		"backend_phase":          backendPhase,
		"runtime_phase":          runtimePhase,
		"key_state":              string(keyState),
		"last_error":             lastError,
		"startup_error_service":  strings.TrimSpace(startupErr.Service),
		"startup_error_listen":   strings.TrimSpace(startupErr.ListenAddr),
		"startup_error_message":  strings.TrimSpace(startupErr.Message),
		"runtime_error_message":  runtimeErrorMessage,
		"chain_access_mode":      strings.TrimSpace(chainAccess.Mode),
		"wallet_chain_base_url":  strings.TrimSpace(chainAccess.BaseURL),
		"woc_proxy_enabled":      chainAccess.WOCProxyEnabled,
		"woc_proxy_listen_addr":  strings.TrimSpace(chainAccess.WOCProxyAddr),
		"woc_upstream_root_url":  strings.TrimSpace(chainAccess.UpstreamRootURL),
		"woc_min_interval":       chainAccess.MinInterval.String(),
		"has_system_home_bundle": hasSystemHomeBundle,
		"default_home_seed_hash": strings.TrimSpace(defaultHomeSeedHash),
	}
	d.controlStream.Emit("backend.phase.changed", "private", "managed_daemon", "", privatePayload)
	d.controlStream.Emit("client.status.changed", "public", "managed_daemon", "", map[string]any{
		"backend_phase": backendPhase,
		"runtime_phase": runtimePhase,
		"key_state":     string(keyState),
	})
}

func (d *managedDaemon) buildBackendSnapshotPayload(step string) map[string]any {
	d.mu.RLock()
	backendPhase := string(d.backendPhase)
	runtimePhase := string(d.runtimePhase)
	runtimeErrorMessage := strings.TrimSpace(d.runtimeErrorMessage)
	startupErr := d.startupError
	chainAccess := d.chainAccess
	hasSystemHomeBundle := d.systemHomepage != nil && d.systemHomepage.HasBundle()
	defaultHomeSeedHash := d.defaultHomeSeedHash()
	apiListenAddr := strings.TrimSpace(d.cfg.HTTP.ListenAddr)
	fsHTTPListenAddr := strings.TrimSpace(d.cfg.FSHTTP.ListenAddr)
	network := d.currentNetworkName()
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	d.mu.RUnlock()
	keyState := managedKeyStateMissing
	switch {
	case unlocked:
		keyState = managedKeyStateUnlocked
	case d.currentKeyExistsLocked():
		keyState = managedKeyStateLocked
	}
	lastError := runtimeErrorMessage
	if strings.TrimSpace(startupErr.Message) != "" {
		lastError = strings.TrimSpace(startupErr.Message)
	}

	return map[string]any{
		"step":                   strings.TrimSpace(step),
		"backend_phase":          backendPhase,
		"runtime_phase":          runtimePhase,
		"key_state":              string(keyState),
		"last_error":             lastError,
		"network":                strings.TrimSpace(network),
		"vault_path":             strings.TrimSpace(d.startup.VaultPath),
		"config_path":            strings.TrimSpace(d.startup.ConfigPath),
		"key_path":               strings.TrimSpace(d.startup.KeyPath),
		"index_db_path":          strings.TrimSpace(d.startup.IndexDBPath),
		"api_listen_addr":        apiListenAddr,
		"fs_http_listen_addr":    fsHTTPListenAddr,
		"startup_error_service":  strings.TrimSpace(startupErr.Service),
		"startup_error_listen":   strings.TrimSpace(startupErr.ListenAddr),
		"startup_error_message":  strings.TrimSpace(startupErr.Message),
		"runtime_error_message":  runtimeErrorMessage,
		"chain_access_mode":      strings.TrimSpace(chainAccess.Mode),
		"wallet_chain_base_url":  strings.TrimSpace(chainAccess.BaseURL),
		"woc_proxy_enabled":      chainAccess.WOCProxyEnabled,
		"woc_proxy_listen_addr":  strings.TrimSpace(chainAccess.WOCProxyAddr),
		"woc_upstream_root_url":  strings.TrimSpace(chainAccess.UpstreamRootURL),
		"woc_min_interval":       chainAccess.MinInterval.String(),
		"has_system_home_bundle": hasSystemHomeBundle,
		"default_home_seed_hash": strings.TrimSpace(defaultHomeSeedHash),
	}
}

func (d *managedDaemon) currentKeyExistsLocked() bool {
	_, err := os.Stat(strings.TrimSpace(d.startup.KeyPath))
	return err == nil
}

func (d *managedDaemon) ensureKeyWorkflowReady() error {
	switch d.currentBackendPhase() {
	case managedBackendPhaseAvailable:
		return nil
	case managedBackendPhaseStartupError:
		se := d.currentStartupError()
		if se.Message != "" {
			return fmt.Errorf("client startup failed: %s", se.Message)
		}
		return fmt.Errorf("client startup failed")
	default:
		return fmt.Errorf("client is still starting")
	}
}

func (d *managedDaemon) rememberUnlockedKey(privHex string) {
	d.mu.Lock()
	d.unlockedPrivHex = strings.TrimSpace(privHex)
	d.runtimeErrorMessage = ""
	d.mu.Unlock()
}

func (d *managedDaemon) clearUnlockedKey() {
	d.mu.Lock()
	d.unlockedPrivHex = ""
	d.runtimeErrorMessage = ""
	d.runtimePhase = managedRuntimePhaseStopped
	d.runtimeStartSeq++
	d.mu.Unlock()
	d.emitBackendSnapshot("key_locked")
	d.emitPhaseEvent()
}

func (d *managedDaemon) startRuntimeAsync() error {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return err
	}
	d.mu.Lock()
	// 设计说明：
	// - `unlock` 只负责把密钥放进 daemon 内存，并异步触发运行时启动；
	// - 真正的长耗时启动在 goroutine 里做，避免再把“验密”和“启动完整 runtime”绑成一个同步 HTTP。
	if strings.TrimSpace(d.unlockedPrivHex) == "" {
		d.mu.Unlock()
		return fmt.Errorf("client key is locked")
	}
	if d.rt != nil || d.runtimePhase == managedRuntimePhaseStarting {
		d.mu.Unlock()
		return nil
	}
	d.runtimeStartSeq++
	seq := d.runtimeStartSeq
	privHex := d.unlockedPrivHex
	d.runtimePhase = managedRuntimePhaseStarting
	d.runtimeErrorMessage = ""
	d.mu.Unlock()

	d.emitBackendSnapshot("runtime_starting")
	d.emitPhaseEvent()

	go func() {
		if err := d.startRuntime(privHex, seq); err != nil {
			obs.Error("bitcast-client", "managed_runtime_start_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "运行时启动失败: %s\n", err.Error())
			d.failRuntimeStartup(seq, err)
		}
	}()
	return nil
}

func (d *managedDaemon) failRuntimeStartup(seq uint64, err error) {
	message := ""
	if err != nil {
		message = strings.TrimSpace(err.Error())
	}
	d.mu.Lock()
	if seq != d.runtimeStartSeq || strings.TrimSpace(d.unlockedPrivHex) == "" {
		d.mu.Unlock()
		return
	}
	d.runtimePhase = managedRuntimePhaseError
	d.runtimeErrorMessage = message
	d.mu.Unlock()
	d.emitBackendSnapshot("runtime_error")
	d.emitPhaseEvent()
}

func (d *managedDaemon) commitRuntimeStartup(seq uint64, rt *clientapp.Runtime, cancel context.CancelFunc, runtimeAPI http.Handler) bool {
	d.mu.Lock()
	if seq != d.runtimeStartSeq || strings.TrimSpace(d.unlockedPrivHex) == "" {
		d.mu.Unlock()
		return false
	}
	if d.rt != nil {
		d.mu.Unlock()
		return false
	}
	d.rt = rt
	d.rtCancel = cancel
	d.rtAPI = runtimeAPI
	d.runtimePhase = managedRuntimePhaseReady
	d.runtimeErrorMessage = ""
	d.mu.Unlock()
	d.emitBackendSnapshot("runtime_ready")
	d.emitPhaseEvent()
	return true
}

func (d *managedDaemon) reserveFSHTTPListener() error {
	d.mu.Lock()
	if d.fsHTTPReserved != nil {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	ln, err := net.Listen("tcp", strings.TrimSpace(d.cfg.FSHTTP.ListenAddr))
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.fsHTTPReserved = ln
	d.cfg.FSHTTP.ListenAddr = ln.Addr().String()
	d.mu.Unlock()
	return nil
}

func (d *managedDaemon) takeReservedFSHTTPListener() net.Listener {
	d.mu.Lock()
	defer d.mu.Unlock()
	ln := d.fsHTTPReserved
	d.fsHTTPReserved = nil
	return ln
}

func (d *managedDaemon) startManagedWOCProxy() error {
	d.mu.Lock()
	if d.wocProxySrv != nil || !d.chainAccess.WOCProxyEnabled {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	proxy, err := wocproxy.New(wocproxy.Config{
		UpstreamRootURL: managedWOCUpstreamRootURL,
		MinInterval:     d.currentChainAccess().MinInterval,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", managedWOCProxyListenAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           proxy.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	d.mu.Lock()
	d.wocProxySrv = srv
	d.chainAccess.WOCProxyAddr = ln.Addr().String()
	d.chainAccess.BaseURL = wocproxy.BaseURLForNetwork("http://"+ln.Addr().String(), d.currentNetworkName())
	d.mu.Unlock()
	obs.Important("bitcast-client", "managed_woc_proxy_started", map[string]any{
		"listen_addr":       ln.Addr().String(),
		"upstream_root_url": proxy.UpstreamRootURL(),
		"min_interval":      d.currentChainAccess().MinInterval.String(),
		"chain_access_mode": "proxy",
		"wallet_chain_base": wocproxy.BaseURLForNetwork("http://"+ln.Addr().String(), d.currentNetworkName()),
	})
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			obs.Error("bitcast-client", "managed_woc_proxy_stopped", map[string]any{"error": err.Error()})
			d.setStartupError("woc_proxy", ln.Addr().String(), err)
		}
	}()
	return nil
}

func (d *managedDaemon) stopRuntime() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	runtimeWasStarting := d.runtimePhase == managedRuntimePhaseStarting
	d.rtCancel = nil
	d.rt = nil
	d.rtAPI = nil
	d.runtimePhase = managedRuntimePhaseStopped
	d.runtimeErrorMessage = ""
	d.runtimeStartSeq++
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	// 设计说明：
	// - 运行时异步启动阶段已经提前拿走了 FS listener；
	// - 这时如果立刻 lock，不要抢着二次 reserve，否则会和那条启动 goroutine 打架；
	// - 让后台 goroutine 在收尾时自己把 listener 还回来，端口状态才不会乱。
	if rt == nil && runtimeWasStarting {
		d.emitBackendSnapshot("runtime_stopped")
		d.emitPhaseEvent()
		obs.Important("bitcast-client", "managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
		return nil
	}
	if err := d.reserveFSHTTPListener(); err != nil {
		d.setStartupError("fs_http", d.cfg.FSHTTP.ListenAddr, err)
		return err
	}
	d.emitBackendSnapshot("runtime_stopped")
	d.emitPhaseEvent()
	obs.Important("bitcast-client", "managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
	return nil
}

func (d *managedDaemon) lockRuntime() error {
	if err := d.stopRuntime(); err != nil {
		return err
	}
	d.clearUnlockedKey()
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
		if phase := d.currentBackendPhase(); phase != managedBackendPhaseAvailable {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if d.currentKeyState() == managedKeyStateUnlocked {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		env, exists, err := LoadEncryptedKeyEnvelope(d.startup.KeyPath)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_load_key_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if !exists || env == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		password, cancelled, err := d.readPasswordCancelable(d.unlockPasswordPrompt)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_read_password_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cancelled {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		password = strings.TrimSpace(password)
		if password == "" {
			obs.Info("bitcast-client", "cli_unlock_empty_password", map[string]any{"vault_path": d.startup.VaultPath})
			continue
		}
		if d.currentKeyState() == managedKeyStateUnlocked {
			fmt.Fprintln(os.Stderr, "已解锁（管理 API 已生效）")
			continue
		}
		privHex, err := DecryptPrivateKeyEnvelope(*env, password)
		if err != nil {
			obs.Error("bitcast-client", "cli_unlock_decrypt_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "解锁失败（密码或密钥材料错误）: %s\n", err.Error())
			continue
		}
		d.rememberUnlockedKey(privHex)
		if err := d.startRuntimeAsync(); err != nil {
			d.clearUnlockedKey()
			obs.Error("bitcast-client", "cli_unlock_start_runtime_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "解锁失败（运行时启动失败）: %s\n", err.Error())
			continue
		}
		obs.Important("bitcast-client", "cli_unlock_succeeded", map[string]any{"vault_path": d.startup.VaultPath})
	}
}

func (d *managedDaemon) readPasswordCancelable(prompt string) (password string, cancelled bool, err error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		p, e := ReadPassword(prompt)
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
		if d.currentKeyState() == managedKeyStateUnlocked {
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
	return d.currentKeyState() == managedKeyStateUnlocked
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
