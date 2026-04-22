package managedclient

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
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
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type managedBackendPhase string

type managedRuntimePhase string

type managedKeyState string
type managedUnlockResult string

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

	apiUnlockWaitRuntimeTimeout = 2 * time.Second

	unlockSourceAPI = "api"
	unlockSourceCLI = "cli"
	unlockSourceObs = "obs"

	controlActionKeyCreateRandom = "key.create_random"
	controlActionKeyAssertExists = "key.assert_exists"
	controlActionKeyUnlock       = "key.unlock"
	controlActionKeyLock         = "key.lock"

	controlActionPricingSetBase          = "pricing.set_base"
	controlActionPricingResetSeed        = "pricing.reset_seed"
	controlActionPricingFeedSeed         = "pricing.feed_seed"
	controlActionPricingSetForce         = "pricing.set_force"
	controlActionPricingReleaseForce     = "pricing.release_force"
	controlActionPricingRunTick          = "pricing.run_tick"
	controlActionPricingTriggerReconcile = "pricing.trigger_reconcile"
	controlActionPricingGetConfig        = "pricing.get_config"
	controlActionPricingGetState         = "pricing.get_state"
	controlActionPricingGetAudits        = "pricing.get_audits"
	controlActionPricingListSeeds        = "pricing.list_seeds"

	controlActionWorkspaceList     = "workspace.list"
	controlActionWorkspaceAdd      = "workspace.add"
	controlActionWorkspaceUpdate   = "workspace.update"
	controlActionWorkspaceDelete   = "workspace.delete"
	controlActionWorkspaceSyncOnce = "workspace.sync_once"

	controlActionWalletPayBSV           = "wallet.pay_bsv"
	controlActionWalletTokenSendPreview = "wallet.token_send_preview"
	controlActionWalletTokenSendSign    = "wallet.token_send_sign"
	controlActionWalletTokenSendSubmit  = "wallet.token_send_submit"

	controlActionGatewayPublishDemand                = "gateway.publish_demand"
	controlActionGatewayPublishDemandBatch           = "gateway.publish_demand_batch"
	controlActionGatewayPublishLiveDemand            = "gateway.publish_live_demand"
	controlActionGatewayPublishDemandChainTxQuotePay = "gateway.publish_demand_chain_tx_quote_pay"
	controlActionGatewayReachabilityAnnounce         = "gateway.reachability_announce"
	controlActionGatewayReachabilityQuery            = "gateway.reachability_query"
	controlActionFeePoolEnsureActive                 = "feepool.ensure_active"
	controlActionFeePoolClose                        = "feepool.close"

	controlActionDomainResolve   = "domain.resolve"
	controlActionDomainRegister  = "domain.register"
	controlActionDomainSetTarget = "domain.set_target"

	controlActionPeerCall    = "peer.call"
	controlActionPeerSelf    = "peer.self"
	controlActionPeerConnect = "peer.connect"

	managedUnlockResultSucceeded managedUnlockResult = "succeeded"
	managedUnlockResultAlready   managedUnlockResult = "already_unlocked"
)

var runClientRuntime = clientapp.Run
var buildRuntimeAPIHandler = clientapp.NewRuntimeAPIHandler

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

type unlockTicket struct {
	Result managedUnlockResult
	Token  string
	Winner string
}

type controlCommandResult struct {
	CommandID    string
	Action       string
	OK           bool
	Result       string
	Error        string
	BackendPhase managedBackendPhase
	RuntimePhase managedRuntimePhase
	KeyState     managedKeyState
	UnlockOwner  string
	UnlockToken  string
	Payload      map[string]any
}

type controlCommandRequest struct {
	CommandID string
	Action    string
	Payload   map[string]any
}

type keyMaterialTicket struct {
	Result string
	PubHex string
}

type unlockHTTPError struct {
	Status int
	Cause  error
}

func (e *unlockHTTPError) Error() string {
	if e == nil || e.Cause == nil {
		return ""
	}
	return e.Cause.Error()
}

func (e *unlockHTTPError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func wrapUnlockHTTPError(status int, err error) error {
	if err == nil {
		return nil
	}
	return &unlockHTTPError{
		Status: status,
		Cause:  err,
	}
}

func unlockHTTPStatus(err error) int {
	var typed *unlockHTTPError
	if errors.As(err, &typed) && typed != nil && typed.Status > 0 {
		return typed.Status
	}
	return http.StatusInternalServerError
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

	mu                 sync.RWMutex
	rt                 *clientapp.Runtime
	rtCancel           context.CancelFunc
	rtAPI              http.Handler
	controlReady       bool
	controlErr         error
	commandWaiters     map[string]chan controlCommandResult
	commandResultsSeen map[string]struct{}

	backendPhase        managedBackendPhase
	runtimePhase        managedRuntimePhase
	runtimeErrorMessage string
	unlockedPrivHex     string
	unlockSuccessToken  string
	unlockSuccessBy     string
	runtimeStartSeq     uint64
	runtimeFSHTTPReady  bool
	runtimeTipReady     bool
	runtimeUTXOReady    bool
	activeConfigPath    string
	activeIndexDBPath   string
	startupError        startupErrorState
	chainAccess         chainAccessState

	systemHomepage *systemHomepageState
}

func RunManagedDaemon(opts DaemonOptions) error {
	rootCtx, rootCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer rootCancel()
	if err := clientapp.ApplyConfigDefaults(&opts.Config); err != nil {
		return err
	}
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
	if err := d.controlStream.StartCommandLoop(d.handleManagedControlCommand); err != nil {
		return err
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
	d.controlReady = false
	d.controlErr = nil
	d.commandWaiters = nil
	d.commandResultsSeen = nil
	d.unlockedPrivHex = ""
	d.unlockSuccessToken = ""
	d.unlockSuccessBy = ""
	d.runtimeErrorMessage = ""
	d.runtimePhase = managedRuntimePhaseStopped
	d.runtimeFSHTTPReady = false
	d.runtimeTipReady = false
	d.runtimeUTXOReady = false
	d.activeConfigPath = ""
	d.activeIndexDBPath = ""
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
	obs.Important(obs.ServiceBitFSClient, "managed_api_started", map[string]any{
		"listen_addr":   started.Listener.Addr().String(),
		"config_path":   d.startup.ConfigPath,
		"backend_phase": string(d.currentBackendPhase()),
	})
	lhttp.ServeInBackground(started, func(err error) {
		obs.Error(obs.ServiceBitFSClient, "managed_api_stopped", map[string]any{"error": err.Error()})
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
				{Path: "/api/v1/settings/keys", Handler: d.handleSettingsKeys},
				{Path: "/api/v1/settings/default-key", Handler: d.handleSettingsDefaultKey},
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

func writeKeyAPISuccess(w http.ResponseWriter, data any) {
	if data == nil {
		data = map[string]any{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data":   data,
	})
}

func writeKeyAPIError(w http.ResponseWriter, status int, code string, message string) {
	code = strings.TrimSpace(code)
	if code == "" {
		code = "INTERNAL_ERROR"
	}
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    code,
			"message": strings.TrimSpace(message),
		},
	})
}

func keyAPIErrorCodeByStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHORIZED"
	case http.StatusNotFound:
		return "KEY_NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusMethodNotAllowed:
		return "METHOD_NOT_ALLOWED"
	default:
		return "INTERNAL_ERROR"
	}
}

type keyBusinessError struct {
	Kind    string
	Code    string
	Message string
}

func (e *keyBusinessError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

func newKeyBusinessError(kind, code, message string) error {
	return &keyBusinessError{
		Kind:    strings.TrimSpace(kind),
		Code:    strings.TrimSpace(code),
		Message: strings.TrimSpace(message),
	}
}

func keyBusinessErrorToHTTP(err error) (int, string) {
	var kb *keyBusinessError
	if errors.As(err, &kb) && kb != nil {
		status := http.StatusInternalServerError
		switch strings.TrimSpace(kb.Kind) {
		case "invalid_input":
			status = http.StatusBadRequest
		case "not_found":
			status = http.StatusNotFound
		case "conflict":
			status = http.StatusConflict
		}
		code := strings.TrimSpace(kb.Code)
		if code == "" {
			code = keyAPIErrorCodeByStatus(status)
		}
		return status, code
	}
	return http.StatusInternalServerError, "INTERNAL_ERROR"
}

func (d *managedDaemon) currentDefaultKeyPubHex() (string, error) {
	cfg, _, err := LoadRootProfileConfig(d.startup.VaultPath)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(cfg.DefaultKey) == "" {
		return "", nil
	}
	return NormalizePubkeyHex(cfg.DefaultKey)
}

func (d *managedDaemon) setDefaultKeyPubHex(pubKeyHex string) error {
	pubHex, err := NormalizePubkeyHex(pubKeyHex)
	if err != nil {
		return newKeyBusinessError("invalid_input", "INVALID_PUBKEY_HEX", err.Error())
	}
	keyPath, err := ResolveProfileKeyPath(d.startup.VaultPath, pubHex)
	if err != nil {
		return err
	}
	env, exists, err := LoadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		return err
	}
	if !exists || env == nil {
		return newKeyBusinessError("not_found", "KEY_NOT_FOUND", "encrypted key not found")
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		return newKeyBusinessError("conflict", "CONFLICT", "client key is already unlocked, lock first")
	}
	if err := SaveRootProfileConfig(d.startup.VaultPath, RootProfileConfig{DefaultKey: pubHex}); err != nil {
		return err
	}
	d.mu.Lock()
	d.startup.KeyPath = keyPath
	d.mu.Unlock()
	d.emitBackendSnapshot("default_key_changed")
	d.emitPhaseEvent()
	return nil
}

func (d *managedDaemon) currentDefaultKeyPath() (string, string, error) {
	pubHex, err := d.currentDefaultKeyPubHex()
	if err != nil {
		return "", "", err
	}
	if pubHex == "" {
		legacyKeyPath := strings.TrimSpace(d.startup.KeyPath)
		if legacyKeyPath != "" {
			return "", legacyKeyPath, nil
		}
		return "", "", nil
	}
	keyPath, err := ResolveProfileKeyPath(d.startup.VaultPath, pubHex)
	if err != nil {
		return "", "", err
	}
	return pubHex, keyPath, nil
}

func (d *managedDaemon) requireDefaultKeyPath() (string, string, error) {
	pubHex, keyPath, err := d.currentDefaultKeyPath()
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(keyPath) == "" {
		return "", "", newKeyBusinessError("not_found", "DEFAULT_KEY_NOT_SET", "default key is not set")
	}
	return pubHex, keyPath, nil
}

func (d *managedDaemon) listKeyPubHexes() ([]string, error) {
	root := strings.TrimSpace(d.startup.VaultPath)
	if root == "" {
		return nil, fmt.Errorf("config root is empty")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}
	items := make([]string, 0, len(entries))
	for _, one := range entries {
		if !one.IsDir() {
			continue
		}
		pubHex, err := NormalizePubkeyHex(one.Name())
		if err != nil {
			continue
		}
		keyPath, err := ResolveProfileKeyPath(root, pubHex)
		if err != nil {
			return nil, err
		}
		if _, statErr := os.Stat(keyPath); statErr == nil {
			items = append(items, pubHex)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
	}
	sort.Strings(items)
	return items, nil
}

func (d *managedDaemon) handleKeyStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	_, keyPath, err := d.currentDefaultKeyPath()
	exists := false
	if err == nil && strings.TrimSpace(keyPath) != "" {
		_, exists, err = LoadEncryptedKeyEnvelope(keyPath)
	}
	if err != nil {
		writeKeyAPIError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	d.mu.RLock()
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	d.mu.RUnlock()
	keyState := managedKeyStateMissing
	switch {
	case unlocked:
		keyState = managedKeyStateUnlocked
	case exists:
		keyState = managedKeyStateLocked
	}
	writeKeyAPISuccess(w, map[string]any{
		"key_state": string(keyState),
	})
}

func (d *managedDaemon) createRandomKeyMaterial(password string) (keyMaterialTicket, error) {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return keyMaterialTicket{}, err
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		return keyMaterialTicket{}, fmt.Errorf("client key is already unlocked, lock first")
	}
	privHex, err := GeneratePrivateKeyHex()
	if err != nil {
		return keyMaterialTicket{}, err
	}
	pubHex, err := PubHexFromPrivHex(privHex)
	if err != nil {
		return keyMaterialTicket{}, err
	}
	keyPath, err := ResolveProfileKeyPath(d.startup.VaultPath, pubHex)
	if err != nil {
		return keyMaterialTicket{}, err
	}
	if _, exists, err := LoadEncryptedKeyEnvelope(keyPath); err != nil {
		return keyMaterialTicket{}, err
	} else if exists {
		return keyMaterialTicket{}, fmt.Errorf("encrypted key already exists")
	}
	env, err := EncryptPrivateKeyEnvelope(privHex, password)
	if err != nil {
		return keyMaterialTicket{}, err
	}
	env.PubkeyHex = pubHex
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		return keyMaterialTicket{}, err
	}
	defaultKey, err := d.currentDefaultKeyPubHex()
	if err != nil {
		return keyMaterialTicket{}, err
	}
	if strings.TrimSpace(defaultKey) == "" {
		if err := SaveRootProfileConfig(d.startup.VaultPath, RootProfileConfig{DefaultKey: pubHex}); err != nil {
			return keyMaterialTicket{}, err
		}
		d.mu.Lock()
		d.startup.KeyPath = keyPath
		d.mu.Unlock()
	}
	d.emitBackendSnapshot("key_material_ready")
	return keyMaterialTicket{
		Result: "created",
		PubHex: pubHex,
	}, nil
}

func (d *managedDaemon) importEncryptedKeyMaterial(cipher *EncryptedKeyEnvelope) error {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return newKeyBusinessError("conflict", "CONFLICT", err.Error())
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		return newKeyBusinessError("conflict", "CONFLICT", "client key is already unlocked, lock first")
	}
	if cipher == nil {
		return newKeyBusinessError("invalid_input", "CIPHER_REQUIRED", "cipher is required")
	}
	targetPubHex := strings.TrimSpace(cipher.PubkeyHex)
	if targetPubHex == "" {
		currentDefault, err := d.currentDefaultKeyPubHex()
		if err != nil {
			return err
		}
		if currentDefault == "" {
			return newKeyBusinessError("invalid_input", "CIPHER_PUBKEY_REQUIRED", "cipher pubkey_hex is required when default key is not set")
		}
		targetPubHex = currentDefault
	}
	targetPubHex, err := NormalizePubkeyHex(targetPubHex)
	if err != nil {
		return newKeyBusinessError("invalid_input", "INVALID_PUBKEY_HEX", err.Error())
	}
	keyPath, err := ResolveProfileKeyPath(d.startup.VaultPath, targetPubHex)
	if err != nil {
		return err
	}
	if _, exists, err := LoadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if exists {
		return newKeyBusinessError("conflict", "CONFLICT", "encrypted key already exists")
	}
	cipher.PubkeyHex = targetPubHex
	if err := SaveEncryptedKeyEnvelope(keyPath, *cipher); err != nil {
		return err
	}
	currentDefault, err := d.currentDefaultKeyPubHex()
	if err != nil {
		return err
	}
	if strings.TrimSpace(currentDefault) == "" {
		if err := SaveRootProfileConfig(d.startup.VaultPath, RootProfileConfig{DefaultKey: targetPubHex}); err != nil {
			return err
		}
		currentDefault = targetPubHex
	}
	if strings.EqualFold(currentDefault, targetPubHex) {
		d.mu.Lock()
		d.startup.KeyPath = keyPath
		d.mu.Unlock()
	}
	d.emitBackendSnapshot("key_material_ready")
	return nil
}

func (d *managedDaemon) assertKeyMaterialExists() error {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return err
	}
	_, keyPath, err := d.requireDefaultKeyPath()
	if err != nil {
		return err
	}
	if _, exists, err := LoadEncryptedKeyEnvelope(keyPath); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("encrypted key not found")
	}
	return nil
}

func (d *managedDaemon) exportEncryptedKeyMaterial() (*EncryptedKeyEnvelope, error) {
	_, keyPath, err := d.requireDefaultKeyPath()
	if err != nil {
		return nil, err
	}
	env, exists, err := LoadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, newKeyBusinessError("not_found", "KEY_NOT_FOUND", "encrypted key not found")
	}
	return env, nil
}

func (d *managedDaemon) handleSettingsKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	defaultKey, err := d.currentDefaultKeyPubHex()
	if err != nil {
		writeKeyAPIError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	keys, err := d.listKeyPubHexes()
	if err != nil {
		writeKeyAPIError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeKeyAPISuccess(w, map[string]any{
		"default_key": defaultKey,
		"keys":        keys,
	})
}

func (d *managedDaemon) handleSettingsDefaultKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req struct {
		DefaultKey string `json:"default_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeKeyAPIError(w, http.StatusBadRequest, "INVALID_JSON", "invalid json")
		return
	}
	if strings.TrimSpace(req.DefaultKey) == "" {
		writeKeyAPIError(w, http.StatusBadRequest, "DEFAULT_KEY_REQUIRED", "default_key is required")
		return
	}
	if err := d.setDefaultKeyPubHex(req.DefaultKey); err != nil {
		status, code := keyBusinessErrorToHTTP(err)
		writeKeyAPIError(w, status, code, err.Error())
		return
	}
	pubHex, _ := NormalizePubkeyHex(req.DefaultKey)
	writeKeyAPISuccess(w, map[string]any{
		"default_key": pubHex,
	})
}

func (d *managedDaemon) unlockWithPassword(ctx context.Context, source, password string) (unlockTicket, error) {
	if err := d.ensureKeyWorkflowReady(); err != nil {
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
	}
	if d.currentKeyState() == managedKeyStateUnlocked {
		ticket := d.currentUnlockTicket(source)
		if err := d.startRuntimeAsync(); err != nil {
			return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
		}
		if err := d.waitRuntimeReadyForAPI(ctx); err != nil {
			obs.Error(obs.ServiceBitFSClient, "unlock_wait_runtime_failed", map[string]any{"error": err.Error()})
			return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
		}
		return ticket, nil
	}
	defaultPubHex, keyPath, err := d.requireDefaultKeyPath()
	if err != nil {
		status, code := keyBusinessErrorToHTTP(err)
		if code == "DEFAULT_KEY_NOT_SET" {
			status = http.StatusNotFound
		}
		return unlockTicket{}, wrapUnlockHTTPError(status, err)
	}
	env, exists, err := LoadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusInternalServerError, err)
	}
	if !exists || env == nil {
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusNotFound, fmt.Errorf("encrypted key not found"))
	}
	privHex, err := DecryptPrivateKeyEnvelope(*env, password)
	if err != nil {
		obs.Error(obs.ServiceBitFSClient, "unlock_decrypt_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（密码或密钥材料错误）: %s\n", err.Error())
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusUnauthorized, err)
	}
	unlockPubHex, err := PubHexFromPrivHex(privHex)
	if err != nil {
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusUnauthorized, err)
	}
	if strings.TrimSpace(defaultPubHex) != "" && !strings.EqualFold(strings.TrimSpace(defaultPubHex), strings.TrimSpace(unlockPubHex)) {
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, fmt.Errorf("default key does not match key material"))
	}
	ticket := d.claimUnlockTicket(source, privHex)
	if ticket.Result == managedUnlockResultAlready {
		if err := d.startRuntimeAsync(); err != nil {
			return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
		}
		if err := d.waitRuntimeReadyForAPI(ctx); err != nil {
			obs.Error(obs.ServiceBitFSClient, "unlock_wait_runtime_failed", map[string]any{"error": err.Error()})
			fmt.Fprintf(os.Stderr, "解锁失败（运行时未就绪）: %s\n", err.Error())
			return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
		}
		return ticket, nil
	}
	if err := d.startRuntimeAsync(); err != nil {
		d.clearUnlockedKey()
		obs.Error(obs.ServiceBitFSClient, "unlock_start_runtime_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（运行时启动失败）: %s\n", err.Error())
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
	}
	if err := d.waitRuntimeReadyForAPI(ctx); err != nil {
		obs.Error(obs.ServiceBitFSClient, "unlock_wait_runtime_failed", map[string]any{"error": err.Error()})
		fmt.Fprintf(os.Stderr, "解锁失败（运行时未就绪）: %s\n", err.Error())
		return unlockTicket{}, wrapUnlockHTTPError(http.StatusConflict, err)
	}
	return ticket, nil
}

func (d *managedDaemon) handleManagedControlCommand(cmd ManagedControlCommandFrame) {
	result := d.executeManagedControlCommand(controlCommandRequest{
		CommandID: strings.TrimSpace(cmd.CommandID),
		Action:    strings.TrimSpace(cmd.Action),
		Payload:   cmd.Payload,
	})
	d.emitManagedCommandResult(result)
}

func (d *managedDaemon) executeManagedControlCommand(req controlCommandRequest) controlCommandResult {
	result := controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
	}
	req.Action = strings.TrimSpace(req.Action)
	result.Action = req.Action
	var (
		out controlCommandResult
		err error
	)
	switch req.Action {
	case controlActionKeyCreateRandom:
		out, err = managedObsControlHandleKeyCreateRandom(d, req)
	case controlActionKeyAssertExists:
		out, err = managedObsControlHandleKeyAssertExists(d, req)
	case controlActionKeyUnlock:
		out, err = managedObsControlHandleKeyUnlock(d, req)
	case controlActionKeyLock:
		out, err = managedObsControlHandleKeyLock(d, req)
	case controlActionWalletPayBSV,
		controlActionWalletTokenSendPreview,
		controlActionWalletTokenSendSign,
		controlActionWalletTokenSendSubmit,
		controlActionGatewayPublishDemand,
		controlActionGatewayPublishDemandBatch,
		controlActionGatewayPublishLiveDemand,
		controlActionGatewayPublishDemandChainTxQuotePay,
		controlActionGatewayReachabilityAnnounce,
		controlActionGatewayReachabilityQuery,
		controlActionFeePoolEnsureActive,
		controlActionFeePoolClose,
		controlActionDomainResolve,
		controlActionDomainRegister,
		controlActionDomainSetTarget,
		controlActionPeerCall,
		controlActionPeerSelf,
		controlActionPeerConnect:
		out, err = d.executeManagedBusinessControlCommand(req)
	case controlActionPricingSetBase,
		controlActionPricingResetSeed,
		controlActionPricingFeedSeed,
		controlActionPricingSetForce,
		controlActionPricingReleaseForce,
		controlActionPricingRunTick,
		controlActionPricingTriggerReconcile,
		controlActionPricingGetConfig,
		controlActionPricingGetState,
		controlActionPricingGetAudits,
		controlActionPricingListSeeds:
		out, err = d.executeManagedPricingControlCommand(req)
	case controlActionWorkspaceList,
		controlActionWorkspaceAdd,
		controlActionWorkspaceUpdate,
		controlActionWorkspaceDelete,
		controlActionWorkspaceSyncOnce:
		out, err = d.executeManagedWorkspaceControlCommand(req)
	default:
		// 剩余动作统一交给注册表分发，不在这里按前缀拆渠道。
		out, err = d.executeManagedOBSControlCommand(req)
		return out
	}
	if err != nil {
		result.Result = "failed"
		result.Error = err.Error()
		return result
	}
	return out
}

func (d *managedDaemon) executeManagedOBSControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	result := controlCommandResult{
		CommandID:    req.CommandID,
		Action:       strings.TrimSpace(req.Action),
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
	}
	rt := d.currentRuntime()
	if rt == nil {
		result.Result = "failed"
		result.Error = "MODULE_DISABLED"
		return result, fmt.Errorf("MODULE_DISABLED")
	}
	hooks := rt.Modules()
	if hooks == nil {
		result.Result = "failed"
		result.Error = "MODULE_DISABLED"
		return result, fmt.Errorf("MODULE_DISABLED")
	}
	resp, err := hooks.DispatchOBSControl(d.rootCtx, req.Action, req.Payload)
	if err != nil {
		result.Result = "failed"
		switch code := clientapp.ModuleHookCodeOf(err); code {
		case "MODULE_DISABLED":
			result.Error = "MODULE_DISABLED"
		case "HANDLER_NOT_REGISTERED":
			result.Error = "handler not registered"
		case "UNSUPPORTED_CONTROL_ACTION":
			result.Error = fmt.Sprintf("unsupported control action: %s", result.Action)
		case "":
			result.Error = err.Error()
		default:
			result.Error = fmt.Sprintf("%s: %s", code, clientapp.ModuleHookMessageOf(err))
		}
		return result, err
	}
	result.OK = resp.OK
	result.Result = resp.Result
	result.Error = resp.Error
	result.Payload = resp.Payload
	return result, nil
}

func managedObsControlHandleKeyCreateRandom(d *managedDaemon, req controlCommandRequest) (controlCommandResult, error) {
	result := controlCommandResult{
		CommandID: req.CommandID,
		Action:    req.Action,
	}
	password := strings.TrimSpace(controlCommandPayloadString(req.Payload, "password"))
	if password == "" {
		result.Result = "failed"
		result.Error = "password is required"
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	ticket, err := d.createRandomKeyMaterial(password)
	if err != nil {
		result.Result = "failed"
		result.Error = err.Error()
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	result.OK = true
	result.Result = ticket.Result
	result.BackendPhase = d.currentBackendPhase()
	result.RuntimePhase = d.currentRuntimePhase()
	result.KeyState = d.currentKeyState()
	return result, nil
}

func managedObsControlHandleKeyAssertExists(d *managedDaemon, req controlCommandRequest) (controlCommandResult, error) {
	result := controlCommandResult{
		CommandID: req.CommandID,
		Action:    req.Action,
	}
	if err := d.assertKeyMaterialExists(); err != nil {
		result.Result = "failed"
		result.Error = err.Error()
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	result.OK = true
	result.Result = "exists"
	result.BackendPhase = d.currentBackendPhase()
	result.RuntimePhase = d.currentRuntimePhase()
	result.KeyState = d.currentKeyState()
	return result, nil
}

func managedObsControlHandleKeyUnlock(d *managedDaemon, req controlCommandRequest) (controlCommandResult, error) {
	result := controlCommandResult{
		CommandID: req.CommandID,
		Action:    req.Action,
	}
	password := strings.TrimSpace(controlCommandPayloadString(req.Payload, "password"))
	if password == "" {
		result.Result = "failed"
		result.Error = "password is required"
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	ticket, err := d.unlockWithPassword(d.rootCtx, unlockSourceObs, password)
	if err != nil {
		result.Result = "failed"
		result.Error = err.Error()
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	result.OK = true
	result.Result = string(ticket.Result)
	result.UnlockOwner = strings.TrimSpace(ticket.Winner)
	result.UnlockToken = strings.TrimSpace(ticket.Token)
	result.BackendPhase = d.currentBackendPhase()
	result.RuntimePhase = d.currentRuntimePhase()
	result.KeyState = d.currentKeyState()
	return result, nil
}

func managedObsControlHandleKeyLock(d *managedDaemon, req controlCommandRequest) (controlCommandResult, error) {
	result := controlCommandResult{
		CommandID: req.CommandID,
		Action:    req.Action,
	}
	if err := d.lockRuntime(); err != nil {
		result.Result = "failed"
		result.Error = err.Error()
		result.BackendPhase = d.currentBackendPhase()
		result.RuntimePhase = d.currentRuntimePhase()
		result.KeyState = d.currentKeyState()
		return result, nil
	}
	result.OK = true
	result.Result = "locked"
	result.BackendPhase = d.currentBackendPhase()
	result.RuntimePhase = d.currentRuntimePhase()
	result.KeyState = d.currentKeyState()
	return result, nil
}

func (d *managedDaemon) emitManagedCommandResult(result controlCommandResult) {
	if d == nil || d.controlStream == nil {
		return
	}
	payload := map[string]any{
		"command_id":    strings.TrimSpace(result.CommandID),
		"action":        strings.TrimSpace(result.Action),
		"ok":            result.OK,
		"result":        strings.TrimSpace(result.Result),
		"error":         strings.TrimSpace(result.Error),
		"backend_phase": strings.TrimSpace(string(result.BackendPhase)),
		"runtime_phase": strings.TrimSpace(string(result.RuntimePhase)),
		"key_state":     strings.TrimSpace(string(result.KeyState)),
		"unlock_owner":  strings.TrimSpace(result.UnlockOwner),
		"unlock_token":  strings.TrimSpace(result.UnlockToken),
	}
	for key, value := range result.Payload {
		payload[key] = value
	}
	d.controlStream.Emit("backend.command.result", "private", "managed_daemon", "", payload)
}

func (d *managedDaemon) executeManagedWorkspaceControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	if d == nil {
		return controlCommandResult{
			CommandID: req.CommandID,
			Action:    req.Action,
			Result:    "failed",
			Error:     "runtime not initialized",
		}, nil
	}
	rt := d.currentRuntime()
	if rt == nil {
		return workspaceActionFailure(req, d, "runtime not initialized", nil), nil
	}
	workspace := rt.Workspace
	walletMode := strings.TrimSpace(d.cfg.Storage.WorkspaceDir) == ""
	if workspace == nil {
		switch strings.TrimSpace(req.Action) {
		case controlActionWorkspaceList:
			if walletMode {
				return workspaceActionSuccess(req, "listed", map[string]any{"items": []any{}, "total": 0}, d), nil
			}
			return workspaceActionFailure(req, d, "workspace manager not initialized", nil), nil
		case controlActionWorkspaceSyncOnce:
			if walletMode {
				return workspaceActionSuccess(req, "synced", map[string]any{"seed_count": 0, "biz_seeds": []any{}}, d), nil
			}
			return workspaceActionFailure(req, d, "workspace manager not initialized", nil), nil
		default:
			return workspaceActionFailure(req, d, "workspace manager not initialized", nil), nil
		}
	}
	ctx := d.rootCtx
	if ctx == nil {
		return workspaceActionFailure(req, d, "runtime context is not ready", nil), nil
	}

	switch strings.TrimSpace(req.Action) {
	case controlActionWorkspaceList:
		items, err := workspace.ListWithContext(ctx)
		if err != nil {
			return workspaceActionFailure(req, d, err.Error(), nil), nil
		}
		return workspaceActionSuccess(req, "listed", map[string]any{
			"items": items,
			"total": len(items),
		}, d), nil

	case controlActionWorkspaceAdd:
		path := strings.TrimSpace(controlCommandPayloadString(req.Payload, "path"))
		if path == "" {
			return workspaceActionFailure(req, d, "path is required", nil), nil
		}
		maxBytes := uint64(0)
		if value, ok := req.Payload["max_bytes"]; ok {
			parsed, valid := workspacePayloadUint64(value)
			if !valid {
				return workspaceActionFailure(req, d, "max_bytes must be a number", nil), nil
			}
			maxBytes = parsed
		}
		item, err := workspace.AddWithContext(ctx, path, maxBytes)
		payload := map[string]any{
			"workspace":        item,
			"mutation_applied": err == nil,
		}
		if err == nil {
			syncSeeds, syncErr := workspace.SyncOnce(ctx)
			payload["sync"] = map[string]any{
				"seed_count": len(syncSeeds),
				"biz_seeds":  []any{},
			}
			payload["need_sync"] = syncErr != nil
			if syncErr != nil {
				err = syncErr
			}
		}
		if err != nil {
			return workspaceActionFailure(req, d, err.Error(), payload), nil
		}
		return workspaceActionSuccess(req, "added", payload, d), nil

	case controlActionWorkspaceUpdate:
		workspacePath := strings.TrimSpace(controlCommandPayloadString(req.Payload, "workspace_path"))
		if workspacePath == "" {
			return workspaceActionFailure(req, d, "workspace_path is required", nil), nil
		}
		var maxBytes *uint64
		if value, ok := req.Payload["max_bytes"]; ok {
			parsed, valid := workspacePayloadUint64(value)
			if !valid {
				return workspaceActionFailure(req, d, "max_bytes must be a number", nil), nil
			}
			maxBytes = &parsed
		}
		var enabled *bool
		if value, ok := req.Payload["enabled"]; ok {
			parsed, valid := workspacePayloadBool(value)
			if !valid {
				return workspaceActionFailure(req, d, "enabled must be a boolean", nil), nil
			}
			enabled = &parsed
		}
		if maxBytes == nil && enabled == nil {
			return workspaceActionFailure(req, d, "no fields to update", nil), nil
		}
		item, err := workspace.UpdateByPathWithContext(ctx, workspacePath, maxBytes, enabled)
		payload := map[string]any{
			"workspace":        item,
			"mutation_applied": err == nil,
		}
		if err == nil {
			syncSeeds, syncErr := workspace.SyncOnce(ctx)
			payload["sync"] = map[string]any{
				"seed_count": len(syncSeeds),
				"biz_seeds":  []any{},
			}
			payload["need_sync"] = syncErr != nil
			if syncErr != nil {
				err = syncErr
			}
		}
		if err != nil {
			return workspaceActionFailure(req, d, err.Error(), payload), nil
		}
		return workspaceActionSuccess(req, "updated", payload, d), nil

	case controlActionWorkspaceDelete:
		workspacePath := strings.TrimSpace(controlCommandPayloadString(req.Payload, "workspace_path"))
		if workspacePath == "" {
			return workspaceActionFailure(req, d, "workspace_path is required", nil), nil
		}
		err := workspace.DeleteByPathWithContext(ctx, workspacePath)
		payload := map[string]any{
			"workspace_path":   workspacePath,
			"mutation_applied": err == nil,
		}
		if err == nil {
			syncSeeds, syncErr := workspace.SyncOnce(ctx)
			payload["sync"] = map[string]any{
				"seed_count": len(syncSeeds),
				"biz_seeds":  []any{},
			}
			payload["need_sync"] = syncErr != nil
			if syncErr != nil {
				err = syncErr
			}
		}
		if err != nil {
			return workspaceActionFailure(req, d, err.Error(), payload), nil
		}
		return workspaceActionSuccess(req, "deleted", payload, d), nil

	case controlActionWorkspaceSyncOnce:
		seeds, err := workspace.SyncOnce(ctx)
		if err != nil {
			return workspaceActionFailure(req, d, err.Error(), nil), nil
		}
		return workspaceActionSuccess(req, "synced", map[string]any{
			"seed_count": len(seeds),
			"biz_seeds":  []any{},
		}, d), nil

	default:
		return controlCommandResult{}, fmt.Errorf("unsupported control action: %s", req.Action)
	}
}

func workspaceActionSuccess(req controlCommandRequest, result string, payload map[string]any, d *managedDaemon) controlCommandResult {
	return controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		OK:           true,
		Result:       result,
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
		Payload:      payload,
	}
}

func workspaceActionFailure(req controlCommandRequest, d *managedDaemon, errText string, payload map[string]any) controlCommandResult {
	return controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		Result:       "failed",
		Error:        strings.TrimSpace(errText),
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
		Payload:      payload,
	}
}

func workspacePayloadUint64(raw any) (uint64, bool) {
	switch v := raw.(type) {
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case uint:
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	default:
		return 0, false
	}
}

func workspacePayloadBool(raw any) (bool, bool) {
	switch v := raw.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true":
			return true, true
		case "false":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

func controlCommandPayloadString(payload map[string]any, key string) string {
	if len(payload) == 0 {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func (d *managedDaemon) handleKeyNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeKeyAPIError(w, http.StatusBadRequest, "INVALID_JSON", "invalid json")
		return
	}
	if strings.TrimSpace(req.Password) == "" {
		writeKeyAPIError(w, http.StatusBadRequest, "PASSWORD_REQUIRED", "password is required")
		return
	}
	ticket, err := d.createRandomKeyMaterial(req.Password)
	if err != nil {
		writeKeyAPIError(w, http.StatusConflict, "CONFLICT", err.Error())
		return
	}
	writeKeyAPISuccess(w, map[string]any{
		"pubkey_hex": ticket.PubHex,
		"key_state":  string(managedKeyStateLocked),
	})
}

func (d *managedDaemon) handleKeyImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req struct {
		Cipher *EncryptedKeyEnvelope `json:"cipher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeKeyAPIError(w, http.StatusBadRequest, "INVALID_JSON", "invalid json")
		return
	}
	if err := d.importEncryptedKeyMaterial(req.Cipher); err != nil {
		status, code := keyBusinessErrorToHTTP(err)
		writeKeyAPIError(w, status, code, err.Error())
		return
	}
	writeKeyAPISuccess(w, map[string]any{
		"key_state": string(managedKeyStateLocked),
	})
}

func (d *managedDaemon) handleKeyExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	env, err := d.exportEncryptedKeyMaterial()
	if err != nil {
		status, code := keyBusinessErrorToHTTP(err)
		writeKeyAPIError(w, status, code, err.Error())
		return
	}
	defaultKey, _ := d.currentDefaultKeyPubHex()
	writeKeyAPISuccess(w, map[string]any{
		"cipher":     env,
		"pubkey_hex": defaultKey,
	})
}

func (d *managedDaemon) handleKeyUnlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeKeyAPIError(w, http.StatusBadRequest, "INVALID_JSON", "invalid json")
			return
		}
	}
	ticket, err := d.unlockWithPassword(r.Context(), unlockSourceAPI, req.Password)
	if err != nil {
		status := unlockHTTPStatus(err)
		writeKeyAPIError(w, status, keyAPIErrorCodeByStatus(status), err.Error())
		return
	}
	d.emitUnlockResult(unlockSourceAPI, ticket)
	writeUnlockResponse(w, ticket)
}

func (d *managedDaemon) handleKeyLock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeKeyAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if err := d.ensureKeyWorkflowReady(); err != nil {
		writeKeyAPIError(w, http.StatusConflict, "CONFLICT", err.Error())
		return
	}
	if err := d.lockRuntime(); err != nil {
		writeKeyAPIError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeKeyAPISuccess(w, map[string]any{
		"key_state": string(managedKeyStateLocked),
	})
}

func (d *managedDaemon) waitRuntimeReadyForAPI(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	waitCtx, cancel := context.WithTimeout(ctx, apiUnlockWaitRuntimeTimeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		d.mu.RLock()
		runtimePhase := d.runtimePhase
		runtimeErrMsg := strings.TrimSpace(d.runtimeErrorMessage)
		fsHTTPReady := d.runtimeFSHTTPReady
		tipReady := d.runtimeTipReady
		utxoReady := d.runtimeUTXOReady
		d.mu.RUnlock()
		switch runtimePhase {
		case managedRuntimePhaseReady:
			if fsHTTPReady && tipReady && utxoReady {
				return nil
			}
		case managedRuntimePhaseError:
			if runtimeErrMsg == "" {
				return fmt.Errorf("runtime startup failed")
			}
			return fmt.Errorf("runtime startup failed: %s", runtimeErrMsg)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("runtime startup wait timeout: %w", waitCtx.Err())
		case <-d.rootCtx.Done():
			return fmt.Errorf("runtime root context ended: %w", d.rootCtx.Err())
		case <-ticker.C:
		}
	}
}

func (d *managedDaemon) runtimeObsSink(next obs.Sink) obs.Sink {
	return obs.SinkFunc(func(ev obs.Event) {
		d.captureRuntimeReadySignals(ev)
		if next != nil {
			next.Handle(ev)
		}
	})
}

func (d *managedDaemon) captureRuntimeReadySignals(ev obs.Event) {
	if strings.TrimSpace(ev.Service) != obs.ServiceBitFSClient {
		return
	}
	eventName := strings.TrimSpace(ev.Name)
	d.mu.Lock()
	defer d.mu.Unlock()
	switch eventName {
	case "fs_http_started":
		d.runtimeFSHTTPReady = true
	case "chain_tip_task_registered":
		d.runtimeTipReady = true
	case "chain_utxo_task_registered":
		d.runtimeUTXOReady = true
	}
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

	pubHex, err := PubHexFromPrivHex(privHex)
	if err != nil {
		return err
	}
	pubHex = strings.ToLower(strings.TrimSpace(pubHex))
	configRoot := strings.TrimSpace(d.startup.VaultPath)
	if configRoot == "" {
		return fmt.Errorf("config root is empty")
	}
	configPath := filepath.Join(configRoot, pubHex, "config.yaml")
	runCfg, _, err := LoadRuntimeConfigOrInit(configPath, d.initNetwork)
	if err != nil {
		return err
	}
	systemHomepageActive := d.systemHomepage != nil && strings.TrimSpace(d.cfg.Storage.WorkspaceDir) != ""
	if systemHomepageActive {
		d.systemHomepage.BindWorkspace(d.cfg.Storage.WorkspaceDir)
		if err := d.systemHomepage.InstallIntoWorkspace(); err != nil {
			return err
		}
		logSystemHomepageInstalled(d.systemHomepage)
	}
	if logFile := strings.TrimSpace(runCfg.Log.File); logFile != "" {
		if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
			return err
		}
	}
	d.applyDesktopRuntimeBootstrap(&runCfg)
	var obsSink obs.Sink
	if d.controlStream != nil {
		obsSink = d.controlStream.ObsSink()
	}
	obsSink = d.runtimeObsSink(obsSink)
	runOpt := clientapp.RunOptions{
		ConfigPath:          configPath,
		StartupMode:         clientapp.StartupModeProduct,
		DisableHTTPServer:   true,
		FSHTTPListener:      d.takeReservedFSHTTPListener(),
		ObsSink:             obsSink,
		EffectivePrivKeyHex: privHex,
	}
	indexDBPath := strings.TrimSpace(runCfg.Index.SQLitePath)
	if indexDBPath == "" {
		return fmt.Errorf("runtime index db path is empty")
	}
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
		if runOpt.FSHTTPListener != nil {
			_ = runOpt.FSHTTPListener.Close()
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
		if runOpt.FSHTTPListener != nil {
			_ = runOpt.FSHTTPListener.Close()
			_ = d.reserveFSHTTPListener()
		}
		_ = openedDB.Actor.Close()
		return err
	}
	runOpt.ActionChain = actionChain
	runOpt.WalletChain = walletChain

	runCtx, cancel := context.WithCancel(d.rootCtx)
	rt, err := runClientRuntime(runCtx, runCfg, deps, runOpt)
	if err != nil {
		if runOpt.FSHTTPListener != nil {
			_ = runOpt.FSHTTPListener.Close()
			_ = d.reserveFSHTTPListener()
		}
		cancel()
		_ = openedDB.Actor.Close()
		return err
	}
	if systemHomepageActive {
		if hook := d.systemHomepageBootstrapHook(); hook != nil {
			if err := hook(runCtx, deps.Store); err != nil {
				_ = rt.Close()
				cancel()
				_ = d.reserveFSHTTPListener()
				return err
			}
		}
	}
	runtimeAPI, err := buildRuntimeAPIHandler(rt)
	if err != nil {
		_ = rt.Close()
		cancel()
		_ = d.reserveFSHTTPListener()
		return err
	}

	if !d.commitRuntimeStartup(seq, rt, cancel, runtimeAPI, configPath, indexDBPath) {
		_ = rt.Close()
		cancel()
		_ = d.reserveFSHTTPListener()
		return nil
	}
	d.printUnlockedRuntimeSummary(runCfg, rt, configPath, indexDBPath)
	obs.Important(obs.ServiceBitFSClient, "managed_runtime_started", map[string]any{
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
	d.systemHomepage = state
	return nil
}

func (d *managedDaemon) systemHomepageBootstrapHook() func(ctx context.Context, store moduleapi.SeedStore) error {
	if d == nil || d.systemHomepage == nil {
		return nil
	}
	resaleDiscountBPS := d.cfg.Seller.Pricing.ResaleDiscountBPS
	return func(ctx context.Context, store moduleapi.SeedStore) error {
		if ctx == nil {
			return fmt.Errorf("ctx is required")
		}
		if store == nil {
			return fmt.Errorf("runtime db not ready for system homepage bootstrap")
		}
		if err := d.systemHomepage.ApplySeedMetadata(ctx, store); err != nil {
			return err
		}
		return d.systemHomepage.EnsureSeedPrices(ctx, store, resaleDiscountBPS)
	}
}

func (d *managedDaemon) applyDesktopRuntimeBootstrap(cfg *clientapp.Config) {
	if d == nil || cfg == nil {
		return
	}
	if d.systemHomepage == nil {
		return
	}
	if strings.TrimSpace(d.cfg.Storage.WorkspaceDir) == "" {
		// 设计说明：
		// - workspace_dir 为空代表钱包模式；
		// - 该模式不做系统首页落盘与启动全量扫描。
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
	configPath := strings.TrimSpace(d.startup.ConfigPath)
	if configPath == "" {
		configPath = "(pending unlock)"
	}
	indexDBPath := strings.TrimSpace(d.startup.IndexDBPath)
	if indexDBPath == "" {
		indexDBPath = "(pending unlock)"
	}
	keyPath := strings.TrimSpace(d.startup.KeyPath)
	if _, currentKeyPath, err := d.currentDefaultKeyPath(); err == nil && strings.TrimSpace(currentKeyPath) != "" {
		keyPath = strings.TrimSpace(currentKeyPath)
	}
	if keyPath == "" {
		keyPath = "(pending default_key)"
	}
	fmt.Fprintf(os.Stderr, "=== BitFS 客户端启动信息（待解锁）===\n")
	fmt.Fprintf(os.Stderr, "vault_path: %s\n", d.startup.VaultPath)
	fmt.Fprintf(os.Stderr, "config_path: %s\n", configPath)
	fmt.Fprintf(os.Stderr, "key_path: %s\n", keyPath)
	fmt.Fprintf(os.Stderr, "network: %s\n", d.currentNetworkName())
	fmt.Fprintf(os.Stderr, "managed_api.listen_addr: %s\n", strings.TrimSpace(d.cfg.HTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "fs_http.listen_addr: %s\n", strings.TrimSpace(d.cfg.FSHTTP.ListenAddr))
	fmt.Fprintf(os.Stderr, "chain_access.mode: %s\n", strings.TrimSpace(chainAccess.Mode))
	fmt.Fprintf(os.Stderr, "wallet_chain.base_url: %s\n", strings.TrimSpace(chainAccess.BaseURL))
	if chainAccess.WOCProxyEnabled {
		fmt.Fprintf(os.Stderr, "woc_proxy.listen_addr: %s\n", strings.TrimSpace(chainAccess.WOCProxyAddr))
	}
	fmt.Fprintf(os.Stderr, "index_db_path: %s\n", indexDBPath)
	fmt.Fprintf(os.Stderr, "runtime_config.status: %s\n", strings.TrimSpace(d.startup.RuntimeConfigStatus))
	fmt.Fprintf(os.Stderr, "状态: 已启动（锁定），等待解锁密码或管理 API 解锁。\n")
}

func (d *managedDaemon) printUnlockedRuntimeSummary(runCfg clientapp.Config, rt *clientapp.Runtime, configPath string, indexDBPath string) {
	if rt == nil {
		return
	}
	chainAccess := d.currentChainAccess()
	pubHex, pubErr := runtimePubKeyHex(rt)
	pubLine := pubHex
	if pubErr != nil {
		pubLine = "unavailable (" + pubErr.Error() + ")"
	}
	keyPath := strings.TrimSpace(d.startup.KeyPath)
	if _, currentKeyPath, err := d.currentDefaultKeyPath(); err == nil && strings.TrimSpace(currentKeyPath) != "" {
		keyPath = strings.TrimSpace(currentKeyPath)
	}
	if keyPath == "" {
		keyPath = "(pending default_key)"
	}
	fmt.Fprintf(os.Stderr, "=== BitFS 客户端运行信息（已解锁）===\n")
	fmt.Fprintf(os.Stderr, "vault_path: %s\n", d.startup.VaultPath)
	fmt.Fprintf(os.Stderr, "config_path: %s\n", strings.TrimSpace(configPath))
	fmt.Fprintf(os.Stderr, "key_path: %s\n", keyPath)
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
	fmt.Fprintf(os.Stderr, "index_db_path: %s\n", strings.TrimSpace(indexDBPath))
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
	obs.Error(obs.ServiceBitFSClient, "managed_startup_failed", map[string]any{
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
	unlockToken := strings.TrimSpace(d.unlockSuccessToken)
	unlockOwner := strings.TrimSpace(d.unlockSuccessBy)
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
		"unlock_token":           unlockToken,
		"unlock_owner":           unlockOwner,
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
	unlockToken := strings.TrimSpace(d.unlockSuccessToken)
	unlockOwner := strings.TrimSpace(d.unlockSuccessBy)
	startupErr := d.startupError
	chainAccess := d.chainAccess
	hasSystemHomeBundle := d.systemHomepage != nil && d.systemHomepage.HasBundle()
	defaultHomeSeedHash := d.defaultHomeSeedHash()
	apiListenAddr := strings.TrimSpace(d.cfg.HTTP.ListenAddr)
	fsHTTPListenAddr := strings.TrimSpace(d.cfg.FSHTTP.ListenAddr)
	network := d.currentNetworkName()
	unlocked := strings.TrimSpace(d.unlockedPrivHex) != ""
	activeConfigPath := strings.TrimSpace(d.activeConfigPath)
	activeIndexDBPath := strings.TrimSpace(d.activeIndexDBPath)
	d.mu.RUnlock()
	keyPath := strings.TrimSpace(d.startup.KeyPath)
	if _, currentKeyPath, err := d.currentDefaultKeyPath(); err == nil && strings.TrimSpace(currentKeyPath) != "" {
		keyPath = strings.TrimSpace(currentKeyPath)
	}
	configPath := strings.TrimSpace(d.startup.ConfigPath)
	if activeConfigPath != "" {
		configPath = activeConfigPath
	}
	indexDBPath := strings.TrimSpace(d.startup.IndexDBPath)
	if activeIndexDBPath != "" {
		indexDBPath = activeIndexDBPath
	}
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
		"config_path":            configPath,
		"key_path":               keyPath,
		"index_db_path":          indexDBPath,
		"api_listen_addr":        apiListenAddr,
		"fs_http_listen_addr":    fsHTTPListenAddr,
		"startup_error_service":  strings.TrimSpace(startupErr.Service),
		"startup_error_listen":   strings.TrimSpace(startupErr.ListenAddr),
		"startup_error_message":  strings.TrimSpace(startupErr.Message),
		"runtime_error_message":  runtimeErrorMessage,
		"unlock_token":           unlockToken,
		"unlock_owner":           unlockOwner,
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
	_, keyPath, err := d.currentDefaultKeyPath()
	if err != nil || strings.TrimSpace(keyPath) == "" {
		return false
	}
	_, err = os.Stat(keyPath)
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

func normalizeUnlockSource(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case unlockSourceCLI:
		return unlockSourceCLI
	case unlockSourceObs:
		return unlockSourceObs
	default:
		return unlockSourceAPI
	}
}

func (d *managedDaemon) claimUnlockTicket(source string, privHex string) unlockTicket {
	source = normalizeUnlockSource(source)
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.TrimSpace(d.unlockedPrivHex) == "" {
		d.unlockedPrivHex = strings.TrimSpace(privHex)
		d.runtimeErrorMessage = ""
		d.unlockSuccessToken = d.nextUnlockSuccessTokenLocked()
		d.unlockSuccessBy = source
		return unlockTicket{
			Result: managedUnlockResultSucceeded,
			Token:  d.unlockSuccessToken,
			Winner: d.unlockSuccessBy,
		}
	}
	if strings.TrimSpace(d.unlockSuccessToken) == "" {
		d.unlockSuccessToken = d.nextUnlockSuccessTokenLocked()
	}
	if strings.TrimSpace(d.unlockSuccessBy) == "" {
		d.unlockSuccessBy = source
	}
	return unlockTicket{
		Result: managedUnlockResultAlready,
		Token:  strings.TrimSpace(d.unlockSuccessToken),
		Winner: strings.TrimSpace(d.unlockSuccessBy),
	}
}

func (d *managedDaemon) currentUnlockTicket(source string) unlockTicket {
	source = normalizeUnlockSource(source)
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.TrimSpace(d.unlockSuccessToken) == "" {
		d.unlockSuccessToken = d.nextUnlockSuccessTokenLocked()
	}
	if strings.TrimSpace(d.unlockSuccessBy) == "" {
		d.unlockSuccessBy = source
	}
	return unlockTicket{
		Result: managedUnlockResultAlready,
		Token:  strings.TrimSpace(d.unlockSuccessToken),
		Winner: strings.TrimSpace(d.unlockSuccessBy),
	}
}

func (d *managedDaemon) nextUnlockSuccessTokenLocked() string {
	seed := time.Now().UnixNano()
	return fmt.Sprintf("unlock-%d-%d", seed, d.runtimeStartSeq+1)
}

func unlockResultEventName(source string, result managedUnlockResult) string {
	source = normalizeUnlockSource(source)
	switch source {
	case unlockSourceCLI:
		if result == managedUnlockResultSucceeded {
			return "cli_unlock_succeeded"
		}
		return "cli_unlock_already_unlocked"
	case unlockSourceObs:
		if result == managedUnlockResultSucceeded {
			return "obs_unlock_succeeded"
		}
		return "obs_unlock_already_unlocked"
	default:
		if result == managedUnlockResultSucceeded {
			return "api_unlock_succeeded"
		}
		return "api_unlock_already_unlocked"
	}
}

func unlockResultPayload(ticket unlockTicket) map[string]any {
	return map[string]any{
		"unlock_result": string(ticket.Result),
		"unlock_token":  strings.TrimSpace(ticket.Token),
		"unlock_owner":  strings.TrimSpace(ticket.Winner),
	}
}

func (d *managedDaemon) emitUnlockResult(source string, ticket unlockTicket) {
	eventName := unlockResultEventName(source, ticket.Result)
	fields := unlockResultPayload(ticket)
	obs.Important(obs.ServiceBitFSClient, eventName, fields)
	d.emitManagedBackendObs(eventName, fields)
}

func writeUnlockResponse(w http.ResponseWriter, ticket unlockTicket) {
	writeKeyAPISuccess(w, map[string]any{
		"key_state":     string(managedKeyStateUnlocked),
		"unlock_result": string(ticket.Result),
	})
}

func (d *managedDaemon) clearUnlockedKey() {
	d.mu.Lock()
	d.unlockedPrivHex = ""
	d.unlockSuccessToken = ""
	d.unlockSuccessBy = ""
	d.runtimeErrorMessage = ""
	d.runtimePhase = managedRuntimePhaseStopped
	d.runtimeFSHTTPReady = false
	d.runtimeTipReady = false
	d.runtimeUTXOReady = false
	d.activeConfigPath = ""
	d.activeIndexDBPath = ""
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
	d.runtimeFSHTTPReady = false
	d.runtimeTipReady = false
	d.runtimeUTXOReady = false
	d.mu.Unlock()

	d.emitBackendSnapshot("runtime_starting")
	d.emitPhaseEvent()

	go func() {
		if err := d.startRuntime(privHex, seq); err != nil {
			obs.Error(obs.ServiceBitFSClient, "managed_runtime_start_failed", map[string]any{"error": err.Error()})
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

func (d *managedDaemon) commitRuntimeStartup(seq uint64, rt *clientapp.Runtime, cancel context.CancelFunc, runtimeAPI http.Handler, configPath string, indexDBPath string) bool {
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
	d.activeConfigPath = strings.TrimSpace(configPath)
	d.activeIndexDBPath = strings.TrimSpace(indexDBPath)
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
	obs.Important(obs.ServiceBitFSClient, "managed_woc_proxy_started", map[string]any{
		"listen_addr":       ln.Addr().String(),
		"upstream_root_url": proxy.UpstreamRootURL(),
		"min_interval":      d.currentChainAccess().MinInterval.String(),
		"chain_access_mode": "proxy",
		"wallet_chain_base": wocproxy.BaseURLForNetwork("http://"+ln.Addr().String(), d.currentNetworkName()),
	})
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			obs.Error(obs.ServiceBitFSClient, "managed_woc_proxy_stopped", map[string]any{"error": err.Error()})
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
	d.runtimeFSHTTPReady = false
	d.runtimeTipReady = false
	d.runtimeUTXOReady = false
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
		obs.Important(obs.ServiceBitFSClient, "managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
		d.emitManagedBackendObs("managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
		return nil
	}
	if err := d.reserveFSHTTPListener(); err != nil {
		d.setStartupError("fs_http", d.cfg.FSHTTP.ListenAddr, err)
		return err
	}
	d.emitBackendSnapshot("runtime_stopped")
	d.emitPhaseEvent()
	obs.Important(obs.ServiceBitFSClient, "managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
	d.emitManagedBackendObs("managed_runtime_stopped", map[string]any{"vault_path": d.startup.VaultPath})
	return nil
}

func (d *managedDaemon) emitManagedBackendObs(eventName string, fields map[string]any) {
	if d == nil || d.controlStream == nil {
		return
	}
	eventName = strings.TrimSpace(eventName)
	if eventName == "" {
		return
	}
	payload := map[string]any{
		"event": eventName,
	}
	if len(fields) > 0 {
		payload["fields"] = cloneManagedEventPayload(fields)
	}
	d.controlStream.Emit("backend.obs", "private", "managed_daemon", "", payload)
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
		obs.Info(obs.ServiceBitFSClient, "cli_unlock_loop_skipped_non_interactive_stdin", map[string]any{"vault_path": d.startup.VaultPath})
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
		password, cancelled, err := d.readPasswordCancelable(d.unlockPasswordPrompt)
		if err != nil {
			obs.Error(obs.ServiceBitFSClient, "cli_unlock_read_password_failed", map[string]any{"error": err.Error()})
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cancelled {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		password = strings.TrimSpace(password)
		if password == "" {
			obs.Info(obs.ServiceBitFSClient, "cli_unlock_empty_password", map[string]any{"vault_path": d.startup.VaultPath})
			continue
		}
		ticket, err := d.unlockWithPassword(d.rootCtx, unlockSourceCLI, password)
		if err != nil {
			obs.Error(obs.ServiceBitFSClient, "cli_unlock_failed", map[string]any{"error": err.Error()})
			continue
		}
		d.emitUnlockResult(unlockSourceCLI, ticket)
		fmt.Fprintln(os.Stderr, "已解锁（管理控制命令已生效）")
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
			fmt.Fprint(os.Stderr, "\r已解锁（管理控制命令已生效），取消命令行密码输入。")
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
