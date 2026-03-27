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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/infra/lhttp"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

const managedBootstrapPrefix = "BITFS_MANAGED_STATE "

type managedPhase string

const (
	managedPhaseStarting     managedPhase = "starting"
	managedPhaseStartupError managedPhase = "startup_error"
	managedPhaseLocked       managedPhase = "locked"
	managedPhaseReady        managedPhase = "ready"
	managedPhaseStopped      managedPhase = "stopped"

	managedWOCProxyListenAddr = "127.0.0.1:19183"
	managedWOCUpstreamRootURL = wocproxy.DefaultUpstreamRootURL
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

	phase        managedPhase
	startupError startupErrorState
	chainAccess  chainAccessState

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
		phase:                managedPhaseStarting,
	}
	if d.controlStream == nil {
		d.controlStream = NewManagedControlStreamFromEnv()
	}
	if d.unlockPasswordPrompt == "" {
		d.unlockPasswordPrompt = "Unlock password: "
	}
	d.chainAccess = d.resolveChainAccessState()
	d.emitBootstrapState()
	if err := d.prepareSystemHomepage(); err != nil {
		d.setStartupError("system_homepage", "", err)
		<-rootCtx.Done()
		return d.close()
	}
	if err := d.startHTTPServer(); err != nil {
		d.setStartupError("managed_api", d.cfg.HTTP.ListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	if err := d.reserveFSHTTPListener(); err != nil {
		d.setStartupError("fs_http", d.cfg.FSHTTP.ListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	if err := d.startManagedWOCProxy(); err != nil {
		d.setStartupError("woc_proxy", managedWOCProxyListenAddr, err)
		<-rootCtx.Done()
		return d.close()
	}
	d.setPhase(managedPhaseLocked)
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
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = d.srv.Shutdown(ctx)
		stop()
	}
	if d.wocProxySrv != nil {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = d.wocProxySrv.Shutdown(ctx)
		stop()
	}
	if fsHTTPReserved != nil {
		_ = fsHTTPReserved.Close()
	}
	d.setPhase(managedPhaseStopped)
	return nil
}

func (d *managedDaemon) startHTTPServer() error {
	mux := lhttp.NewServeMux(
		lhttp.Route{Path: "/api/v1/key/status", Handler: d.handleKeyStatus},
		lhttp.Route{Path: "/api/v1/key/new", Handler: d.handleKeyNew},
		lhttp.Route{Path: "/api/v1/key/import", Handler: d.handleKeyImport},
		lhttp.Route{Path: "/api/v1/key/export", Handler: d.handleKeyExport},
		lhttp.Route{Path: "/api/v1/key/unlock", Handler: d.handleKeyUnlock},
		lhttp.Route{Path: "/api/v1/key/lock", Handler: d.handleKeyLock},
		lhttp.Route{Path: "/api", Handler: d.handleAPIProxyOrLocked},
		lhttp.Route{Path: "/api/", Handler: d.handleAPIProxyOrLocked},
		lhttp.Route{Path: "/", Handler: d.handleNonAPIRequest},
	)
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
		"listen_addr": started.Listener.Addr().String(),
		"config_path": d.startup.ConfigPath,
		"phase":       string(d.currentPhase()),
	})
	lhttp.ServeInBackground(started, func(err error) {
		obs.Error("bitcast-client", "managed_api_stopped", map[string]any{"error": err.Error()})
		d.rootCancel()
	})
	return nil
}

func (d *managedDaemon) handleNonAPIRequest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
}

func (d *managedDaemon) handleAPIProxyOrLocked(w http.ResponseWriter, r *http.Request) {
	if phase := d.currentPhase(); phase == managedPhaseStartupError {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       d.currentStartupError().Message,
			"phase":       string(phase),
			"service":     d.currentStartupError().Service,
			"listen_addr": d.currentStartupError().ListenAddr,
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
	unlocked := d.rt != nil
	phase := d.phase
	startupErr := d.startupError
	chainAccess := d.chainAccess
	d.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"vault_path":             d.startup.VaultPath,
		"config_path":            d.startup.ConfigPath,
		"key_path":               d.startup.KeyPath,
		"index_db_path":          d.startup.IndexDBPath,
		"has_key":                exists,
		"unlocked":               unlocked,
		"phase":                  string(phase),
		"startup_error_service":  startupErr.Service,
		"startup_error_listen":   startupErr.ListenAddr,
		"startup_error_message":  startupErr.Message,
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
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
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
	if d.isUnlocked() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "client is unlocked, lock first"})
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
	if d.isUnlocked() {
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
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if err := d.stopRuntime(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "unlocked": false})
}

func (d *managedDaemon) startRuntime(privHex string) error {
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
	runIn.PostWorkspaceBootstrap = d.systemHomepageBootstrapHook()
	runIn.DisableHTTPServer = true
	runIn.FSHTTPListener = d.takeReservedFSHTTPListener()
	runIn.ObsSink = d.controlStream.ObsSink()
	actionChain, err := chainbridge.NewEmbeddedFeePoolChain(chainbridge.RouteConfig{
		Provider: chainbridge.WhatsOnChainProvider,
		Network:  d.cfg.BSV.Network,
		BaseURL:  d.chainAccess.BaseURL,
		Auth:     d.chainAccess.RouteAuth,
	})
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
		}
		return err
	}
	walletChain, err := clientapp.NewWalletChainClientWithBaseURL(chainbridge.Route{
		Provider: chainbridge.WhatsOnChainProvider,
		Network:  d.cfg.BSV.Network,
	}, d.chainAccess.BaseURL, d.chainAccess.WalletAuth)
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
		}
		return err
	}
	runIn.ActionChain = actionChain
	runIn.WalletChain = walletChain

	runCtx, cancel := context.WithCancel(d.rootCtx)
	rt, err := clientapp.Run(runCtx, runIn)
	if err != nil {
		if runIn.FSHTTPListener != nil {
			_ = runIn.FSHTTPListener.Close()
		}
		cancel()
		return err
	}
	runtimeAPI, err := clientapp.NewRuntimeAPIHandler(rt)
	if err != nil {
		_ = rt.Close()
		cancel()
		return err
	}

	d.mu.Lock()
	d.rt = rt
	d.rtCancel = cancel
	d.rtAPI = runtimeAPI
	d.mu.Unlock()

	d.setPhase(managedPhaseReady)
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
		Value: strings.TrimSpace(d.cfg.WOCAPIKey),
	}
	state := chainAccessState{
		Mode:            "proxy",
		UpstreamRootURL: managedWOCUpstreamRootURL,
		MinInterval:     1 * time.Second,
		RouteAuth:       auth,
		WalletAuth: whatsonchain.AuthConfig{
			Mode:  auth.Mode,
			Name:  auth.Name,
			Value: auth.Value,
		},
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

func (d *managedDaemon) currentPhase() managedPhase {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.phase
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

func (d *managedDaemon) setPhase(phase managedPhase) {
	d.mu.Lock()
	d.phase = phase
	if phase != managedPhaseStartupError {
		d.startupError = startupErrorState{}
	}
	d.mu.Unlock()
	d.emitBootstrapState()
	d.emitPhaseEvent()
}

func (d *managedDaemon) setStartupError(service, listenAddr string, err error) {
	message := ""
	if err != nil {
		message = strings.TrimSpace(err.Error())
	}
	d.mu.Lock()
	d.phase = managedPhaseStartupError
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
	d.emitBootstrapState()
	d.emitPhaseEvent()
}

func (d *managedDaemon) emitBootstrapState() {
	d.mu.RLock()
	payload := map[string]any{
		"type":                  "bootstrap_state",
		"phase":                 string(d.phase),
		"startup_error_service": d.startupError.Service,
		"startup_error_listen":  d.startupError.ListenAddr,
		"startup_error_message": d.startupError.Message,
		"chain_access_mode":     d.chainAccess.Mode,
		"wallet_chain_base_url": d.chainAccess.BaseURL,
		"woc_proxy_enabled":     d.chainAccess.WOCProxyEnabled,
		"woc_proxy_listen_addr": d.chainAccess.WOCProxyAddr,
		"woc_upstream_root_url": d.chainAccess.UpstreamRootURL,
		"woc_min_interval":      d.chainAccess.MinInterval.String(),
	}
	d.mu.RUnlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stdout, "%s%s\n", managedBootstrapPrefix, string(raw))
}

func (d *managedDaemon) emitPhaseEvent() {
	if d == nil || d.controlStream == nil {
		return
	}
	d.mu.RLock()
	phase := string(d.phase)
	startupErr := d.startupError
	chainAccess := d.chainAccess
	unlocked := d.rt != nil
	hasSystemHomeBundle := d.systemHomepage != nil && d.systemHomepage.HasBundle()
	defaultHomeSeedHash := d.defaultHomeSeedHash()
	d.mu.RUnlock()

	privatePayload := map[string]any{
		"phase":                  phase,
		"unlocked":               unlocked,
		"startup_error_service":  strings.TrimSpace(startupErr.Service),
		"startup_error_listen":   strings.TrimSpace(startupErr.ListenAddr),
		"startup_error_message":  strings.TrimSpace(startupErr.Message),
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
		"phase":    phase,
		"unlocked": unlocked,
	})
}

func (d *managedDaemon) ensureKeyWorkflowReady() error {
	switch d.currentPhase() {
	case managedPhaseLocked, managedPhaseReady:
		return nil
	case managedPhaseStartupError:
		se := d.currentStartupError()
		if se.Message != "" {
			return fmt.Errorf("client startup failed: %s", se.Message)
		}
		return fmt.Errorf("client startup failed")
	default:
		return fmt.Errorf("client is still starting")
	}
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
	d.emitBootstrapState()
	return nil
}

func (d *managedDaemon) stopRuntime() error {
	d.mu.Lock()
	cancel := d.rtCancel
	rt := d.rt
	d.rtCancel = nil
	d.rt = nil
	d.rtAPI = nil
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rt != nil {
		_ = rt.Close()
	}
	if err := d.reserveFSHTTPListener(); err != nil {
		d.setStartupError("fs_http", d.cfg.FSHTTP.ListenAddr, err)
		return err
	}
	d.setPhase(managedPhaseLocked)
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
		if phase := d.currentPhase(); phase != managedPhaseLocked && phase != managedPhaseReady {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if d.isUnlocked() {
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
		if d.isUnlocked() {
			fmt.Fprintln(os.Stderr, "已解锁（管理 API 已生效）")
			continue
		}
		privHex, err := DecryptPrivateKeyEnvelope(*env, password)
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
