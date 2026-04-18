package managedclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
	contractfnlock "github.com/bsv8/bitfs-contract/pkg/v1/fnlock"
	libp2p "github.com/libp2p/go-libp2p"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type capturedManagedControlStream struct {
	events []ManagedRuntimeEvent
}

func (s *capturedManagedControlStream) Emit(topic, scope, producer, traceID string, payload map[string]any) {
	s.events = append(s.events, ManagedRuntimeEvent{
		Topic:    topic,
		Scope:    scope,
		Producer: producer,
		TraceID:  traceID,
		Payload:  cloneManagedEventPayload(payload),
	})
}

func (*capturedManagedControlStream) ObsSink() obs.Sink {
	return nil
}

func (*capturedManagedControlStream) StartCommandLoop(ManagedControlCommandHandler) error {
	return nil
}

func TestRuntimeListenOverridesApply(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	overrides := RuntimeListenOverrides{
		HTTPListenAddr:   "127.0.0.1:19181",
		FSHTTPListenAddr: "127.0.0.1:19182",
	}
	overrides.Apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:19181"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:19182"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestRuntimeListenOverridesApply_EmptyKeepsConfig(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	var overrides RuntimeListenOverrides
	overrides.Apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:18080"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:18090"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestApplyDesktopRuntimeBootstrap_EnablesStartupFullScanWhenSystemHomepageExists(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd",
		},
	}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false
	cfg.Storage.WorkspaceDir = filepath.Join(t.TempDir(), "workspace")

	d.applyDesktopRuntimeBootstrap(&cfg)

	if !cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should be enabled for desktop system homepage bootstrap")
	}
}

func TestApplyDesktopRuntimeBootstrap_NoHomepageKeepsScanConfig(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false

	d.applyDesktopRuntimeBootstrap(&cfg)

	if cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should stay unchanged when no system homepage bundle is active")
	}
}

func TestApplyDesktopRuntimeBootstrap_EmptyWorkspaceKeepsScanConfig(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd",
		},
	}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false
	cfg.Storage.WorkspaceDir = ""

	d.applyDesktopRuntimeBootstrap(&cfg)

	if cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should stay unchanged when workspace_dir is empty")
	}
}

func TestHandleNonAPIRequest_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/index.html", nil)
	rec := httptest.NewRecorder()

	var d managedDaemon
	d.handleNonAPIRequest(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusNotFound)
	}

	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response json: %v", err)
	}
	if payload.Error != "not found" {
		t.Fatalf("error=%q, want %q", payload.Error, "not found")
	}
}

func TestHandleManagedControlCommand_UnlockSuccess_StartsRuntimeAndEmitsResult(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)
	stream := &capturedManagedControlStream{}

	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}
	d.chainAccess = d.resolveChainAccessState()

	env, err := EncryptPrivateKeyEnvelope("1111111111111111111111111111111111111111111111111111111111111111", "pass")
	if err != nil {
		t.Fatalf("encrypt private key envelope: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save encrypted key envelope: %v", err)
	}

	oldRun := runClientRuntime
	oldBuild := buildRuntimeAPIHandler
	defer func() {
		runClientRuntime = oldRun
		buildRuntimeAPIHandler = oldBuild
		if d.rt != nil && d.rt.Host != nil {
			_ = d.rt.Host.Close()
		}
	}()

	built := make(chan struct{}, 1)
	runClientRuntime = func(ctx context.Context, runCfg clientapp.Config, deps clientapp.RunDeps, opt clientapp.RunOptions) (*clientapp.Runtime, error) {
		if ctx == nil {
			return nil, fmt.Errorf("ctx is required")
		}
		if !opt.DisableHTTPServer {
			return nil, fmt.Errorf("disable http server should stay enabled in managed mode")
		}
		if strings.TrimSpace(opt.EffectivePrivKeyHex) == "" {
			return nil, fmt.Errorf("effective priv key is required")
		}
		if deps.DBActor != nil {
			_ = deps.DBActor.Close()
		}
		if opt.FSHTTPListener != nil {
			_ = opt.FSHTTPListener.Close()
		}
		if opt.ObsSink != nil {
			for _, name := range []string{"fs_http_started", "chain_tip_task_registered", "chain_utxo_task_registered"} {
				opt.ObsSink.Handle(obs.Event{
					Service: "bitcast-client",
					Name:    name,
				})
			}
		}
		h, err := libp2p.New(libp2p.NoListenAddrs)
		if err != nil {
			return nil, err
		}
		return &clientapp.Runtime{Host: h}, nil
	}
	buildRuntimeAPIHandler = func(rt *clientapp.Runtime) (http.Handler, error) {
		if rt == nil || rt.Host == nil {
			return nil, fmt.Errorf("runtime host is nil")
		}
		select {
		case built <- struct{}{}:
		default:
		}
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-unlock-1",
		Action:    "key.unlock",
		Payload: map[string]any{
			"password": "pass",
		},
	})

	deadline := time.Now().Add(5 * time.Second)
	for d.currentRuntimePhase() != managedRuntimePhaseReady {
		if time.Now().After(deadline) {
			t.Fatalf("runtime phase did not become ready, got=%s", d.currentRuntimePhase())
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-built:
	default:
		t.Fatal("runtime api handler was not built")
	}
	if d.rt == nil || d.rtAPI == nil {
		t.Fatal("runtime or runtime api handler not committed")
	}
	result, ok := findControlCommandResult(stream.events, "cmd-unlock-1")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := result["action"], "key.unlock"; got != want {
		t.Fatalf("command action=%q, want %q", got, want)
	}
	if got, want := strings.ToLower(strings.TrimSpace(result["ok"])), "true"; got != want {
		t.Fatalf("command ok=%q, want %q", got, want)
	}
	if got, want := result["result"], "succeeded"; got != want {
		t.Fatalf("command result=%q, want %q", got, want)
	}
	if got, want := result["key_state"], "unlocked"; got != want {
		t.Fatalf("command key_state=%q, want %q", got, want)
	}
	if got, want := result["runtime_phase"], string(managedRuntimePhaseReady); got != want {
		t.Fatalf("command runtime_phase=%q, want %q", got, want)
	}
}

func TestSystemHomepageBootstrapHook_AppliesMetadataAndPrice(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "bootstrap.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE biz_seeds(
		seed_hash TEXT PRIMARY KEY,
		seed_file_path TEXT NOT NULL,
		chunk_count INTEGER,
		file_size INTEGER,
		recommended_file_name TEXT NOT NULL DEFAULT '',
		mime_hint TEXT NOT NULL DEFAULT '',
		created_at_unix INTEGER
	)`); err != nil {
		t.Fatalf("create biz_seeds: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE biz_seed_pricing_policy(
		seed_hash TEXT PRIMARY KEY,
		floor_unit_price_sat_per_64k INTEGER,
		resale_discount_bps INTEGER,
		pricing_source TEXT,
		updated_at_unix INTEGER
	)`); err != nil {
		t.Fatalf("create biz_seed_pricing_policy: %v", err)
	}

	seedHash := "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd"
	seedPath := filepath.Join(root, seedHash)
	if err := os.WriteFile(seedPath, []byte("homepage"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO biz_seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`,
		seedHash, seedPath, 1, 128, "", "", 1,
	); err != nil {
		t.Fatalf("insert biz_seed: %v", err)
	}

	var cfg clientapp.Config
	cfg.Seller.Pricing.ResaleDiscountBPS = 4321

	d := &managedDaemon{
		cfg: cfg,
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: seedHash,
			SeedHashes:      []string{seedHash},
			FileMetaBySeed: map[string]systemHomepageFileMeta{
				seedHash: {
					OriginalName: "index.html",
					MIME:         "text/html",
				},
			},
		},
	}

	hook := d.systemHomepageBootstrapHook()
	if hook == nil {
		t.Fatalf("expected non-nil bootstrap hook")
	}
	if err := hook(context.Background(), clientapp.NewClientStore(db, nil)); err != nil {
		t.Fatalf("run bootstrap hook: %v", err)
	}

	var name, mime string
	if err := db.QueryRow(`SELECT recommended_file_name,mime_hint FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&name, &mime); err != nil {
		t.Fatalf("query biz_seed metadata: %v", err)
	}
	if name != "index.html" || mime != "text/html" {
		t.Fatalf("seed metadata mismatch: name=%q mime=%q", name, mime)
	}

	var floorPrice uint64
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k FROM biz_seed_pricing_policy WHERE seed_hash=?`, seedHash).Scan(&floorPrice); err != nil {
		t.Fatalf("query biz_seed pricing_policy: %v", err)
	}
	if floorPrice != systemHomepageFloorPriceSatPer64K {
		t.Fatalf("seed floor price mismatch: got=%d want=%d", floorPrice, systemHomepageFloorPriceSatPer64K)
	}
}

func TestResolveChainAccessState_DefaultsToEmbeddedProxy(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "proxy"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if !state.WOCProxyEnabled {
		t.Fatal("expected embedded proxy to be enabled")
	}
	if got, want := state.WOCProxyAddr, managedWOCProxyListenAddr; got != want {
		t.Fatalf("woc proxy addr=%q, want %q", got, want)
	}
	if got, want := state.BaseURL, wocproxy.BaseURLForNetwork("http://"+managedWOCProxyListenAddr, "test"); got != want {
		t.Fatalf("base_url=%q, want %q", got, want)
	}
	if got, want := state.MinInterval, time.Second; got != want {
		t.Fatalf("min_interval=%s, want %s", got, want)
	}
}

func TestResolveChainAccessState_UsesInjectedEnvBaseURL(t *testing.T) {
	t.Setenv(chainbridge.FeePoolChainBaseURLEnv, " http://127.0.0.1:33333/v1/bsv/test/ ")

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "injected_env"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if state.WOCProxyEnabled {
		t.Fatal("expected embedded proxy to be disabled when env is injected")
	}
	if got, want := state.BaseURL, "http://127.0.0.1:33333/v1/bsv/test"; got != want {
		t.Fatalf("base_url=%q, want %q", got, want)
	}
	if got, want := state.WOCProxyAddr, ""; got != want {
		t.Fatalf("woc_proxy_addr=%q, want empty", got)
	}
	if state.RouteAuth != (chainbridge.AuthConfig{}) {
		t.Fatalf("route_auth=%+v, want empty", state.RouteAuth)
	}
	if state.WalletAuth != (whatsonchain.AuthConfig{}) {
		t.Fatalf("wallet_auth=%+v, want empty", state.WalletAuth)
	}
}

func TestResolveChainAccessState_UsesPerProviderWOCSettings(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"
	d.cfg.ExternalAPI.WOC.APIKey = "provider-key"
	d.cfg.ExternalAPI.WOC.MinIntervalMS = 2500

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "direct_woc"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if got, want := state.RouteAuth.Value, "provider-key"; got != want {
		t.Fatalf("route_auth.value=%q, want %q", got, want)
	}
	if got, want := state.MinInterval, 2500*time.Millisecond; got != want {
		t.Fatalf("min_interval=%s, want %s", got, want)
	}
}

func TestEmitBackendSnapshot_ReportsKeyPresence(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	stream := &capturedManagedControlStream{}
	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  filepath.Join(root, "config.yaml"),
			KeyPath:     keyPath,
			IndexDBPath: filepath.Join(root, "data", "client-index.sqlite"),
		},
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
		chainAccess: chainAccessState{
			Mode:            "proxy",
			BaseURL:         "http://127.0.0.1:19183/v1/bsv/test",
			WOCProxyEnabled: true,
			WOCProxyAddr:    "127.0.0.1:19183",
			UpstreamRootURL: managedWOCUpstreamRootURL,
			MinInterval:     time.Second,
		},
	}

	d.emitBackendSnapshot("locked")
	if len(stream.events) != 1 {
		t.Fatalf("event_count=%d, want 1", len(stream.events))
	}
	if got, want := stream.events[0].Topic, "backend.snapshot"; got != want {
		t.Fatalf("topic=%q, want %q", got, want)
	}
	if got, want := stream.events[0].Payload["key_state"], "missing"; got != want {
		t.Fatalf("first key_state=%v, want %v", got, want)
	}

	env, err := EncryptPrivateKeyEnvelope("1111111111111111111111111111111111111111111111111111111111111111", "pass")
	if err != nil {
		t.Fatalf("encrypt private key envelope: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save encrypted key envelope: %v", err)
	}

	d.emitBackendSnapshot("key_material_ready")
	if len(stream.events) != 2 {
		t.Fatalf("event_count=%d, want 2", len(stream.events))
	}
	last := stream.events[1]
	if got, want := last.Payload["key_state"], "locked"; got != want {
		t.Fatalf("second key_state=%v, want %v", got, want)
	}
	if got, want := last.Payload["step"], "key_material_ready"; got != want {
		t.Fatalf("step=%v, want %v", got, want)
	}
}

func TestHandleKeyUnlockStartsRuntimeAndBuildsHandler(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")
	workspaceDir := filepath.Join(root, "workspace")
	dataDir := filepath.Join(root, "data")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.Enabled = false
	cfg.FSHTTP.Enabled = false
	cfg.Storage.WorkspaceDir = workspaceDir
	cfg.Storage.DataDir = dataDir
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaultsForMode(&cfg, clientapp.StartupModeProduct); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.HTTP.Enabled = false
	cfg.FSHTTP.Enabled = false
	if err := clientapp.SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)

	d := &managedDaemon{
		initNetwork:          "test",
		cfg:                  cfg,
		startup:              StartupSummary{ConfigPath: configPath, IndexDBPath: "client-index.sqlite"},
		rootCtx:              rootCtx,
		rootCancel:           rootCancel,
		controlStream:        noopManagedControlStream{},
		backendPhase:         managedBackendPhaseAvailable,
		runtimePhase:         managedRuntimePhaseStopped,
		unlockedPrivHex:      strings.Repeat("a", 64),
		unlockPasswordPrompt: "Unlock password: ",
	}
	d.startup.VaultPath = root
	d.startup.KeyPath = filepath.Join(root, "key.json")
	d.chainAccess = d.resolveChainAccessState()

	runCalled := make(chan struct{}, 1)
	buildCalled := make(chan struct{}, 1)
	origRunClientRuntime := runClientRuntime
	origBuildRuntimeAPIHandler := buildRuntimeAPIHandler
	runClientRuntime = func(ctx context.Context, runCfg clientapp.Config, deps clientapp.RunDeps, opt clientapp.RunOptions) (*clientapp.Runtime, error) {
		select {
		case runCalled <- struct{}{}:
		default:
		}
		if opt.ObsSink != nil {
			for _, name := range []string{"fs_http_started", "chain_tip_task_registered", "chain_utxo_task_registered"} {
				opt.ObsSink.Handle(obs.Event{
					Service: "bitcast-client",
					Name:    name,
				})
			}
		}
		return &clientapp.Runtime{
			Host: h,
		}, nil
	}
	buildRuntimeAPIHandler = func(rt *clientapp.Runtime) (http.Handler, error) {
		select {
		case buildCalled <- struct{}{}:
		default:
		}
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil
	}
	defer func() {
		runClientRuntime = origRunClientRuntime
		buildRuntimeAPIHandler = origBuildRuntimeAPIHandler
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/key/unlock", nil)
	rec := httptest.NewRecorder()
	d.handleKeyUnlock(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unlock status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	select {
	case <-runCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not start")
	}
	select {
	case <-buildCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime api handler was not built")
	}
	if phase := d.currentRuntimePhase(); phase != managedRuntimePhaseReady {
		t.Fatalf("runtime phase mismatch: got=%s want=%s", phase, managedRuntimePhaseReady)
	}
	if d.rtAPI == nil {
		t.Fatal("runtime api handler not committed")
	}
}

func TestHandleKeyUnlock_KeyNotFoundReturnsNotFound(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	keyPath := filepath.Join(root, "key.json")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)

	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: noopManagedControlStream{},
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/key/unlock", strings.NewReader(`{"password":"pass"}`))
	rec := httptest.NewRecorder()
	d.handleKeyUnlock(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unlock status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "encrypted key not found") {
		t.Fatalf("unlock body mismatch: body=%s", rec.Body.String())
	}
}

func TestHandleKeyUnlock_BackendNotReadyReturnsConflict(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	keyPath := filepath.Join(root, "key.json")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)

	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: noopManagedControlStream{},
		backendPhase:  managedBackendPhaseStarting,
		runtimePhase:  managedRuntimePhaseStopped,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/key/unlock", strings.NewReader(`{"password":"pass"}`))
	rec := httptest.NewRecorder()
	d.handleKeyUnlock(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("unlock status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "client is still starting") {
		t.Fatalf("unlock body mismatch: body=%s", rec.Body.String())
	}
}

func TestHandleManagedControlCommand_CreateRandomCreatesLockedKey(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)
	stream := &capturedManagedControlStream{}
	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-create-random-1",
		Action:    "key.create_random",
		Payload: map[string]any{
			"password": "pass",
		},
	})

	if _, exists, err := LoadEncryptedKeyEnvelope(keyPath); err != nil {
		t.Fatalf("load encrypted key envelope: %v", err)
	} else if !exists {
		t.Fatal("encrypted key should exist after key.create_random")
	}
	if got, want := d.currentKeyState(), managedKeyStateLocked; got != want {
		t.Fatalf("key state mismatch: got=%s want=%s", got, want)
	}
	result, ok := findControlCommandResult(stream.events, "cmd-create-random-1")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := result["action"], "key.create_random"; got != want {
		t.Fatalf("command action=%q, want %q", got, want)
	}
	if got, want := strings.ToLower(strings.TrimSpace(result["ok"])), "true"; got != want {
		t.Fatalf("command ok=%q, want %q", got, want)
	}
	if got, want := result["result"], "created"; got != want {
		t.Fatalf("command result=%q, want %q", got, want)
	}
}

func TestHandleManagedControlCommand_AssertExists(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)
	stream := &capturedManagedControlStream{}
	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}
	env, err := EncryptPrivateKeyEnvelope(strings.Repeat("1", 64), "pass")
	if err != nil {
		t.Fatalf("encrypt private key envelope: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save encrypted key envelope: %v", err)
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-assert-exists-1",
		Action:    "key.assert_exists",
	})

	result, ok := findControlCommandResult(stream.events, "cmd-assert-exists-1")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := result["action"], "key.assert_exists"; got != want {
		t.Fatalf("command action=%q, want %q", got, want)
	}
	if got, want := strings.ToLower(strings.TrimSpace(result["ok"])), "true"; got != want {
		t.Fatalf("command ok=%q, want %q", got, want)
	}
	if got, want := result["result"], "exists"; got != want {
		t.Fatalf("command result=%q, want %q", got, want)
	}
}

func TestHandleManagedControlCommand_AssertExistsFailsWhenMissing(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)
	stream := &capturedManagedControlStream{}
	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-assert-exists-missing",
		Action:    "key.assert_exists",
	})

	result, ok := findControlCommandResult(stream.events, "cmd-assert-exists-missing")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := result["action"], "key.assert_exists"; got != want {
		t.Fatalf("command action=%q, want %q", got, want)
	}
	if got := strings.ToLower(strings.TrimSpace(result["ok"])); got == "true" {
		t.Fatalf("command ok=%q, want false", got)
	}
	if got := strings.TrimSpace(result["error"]); !strings.Contains(got, "encrypted key not found") {
		t.Fatalf("command error=%q, want contains %q", got, "encrypted key not found")
	}
}

func TestHandleManagedControlCommand_UnlockFailureKeepsLocked(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)
	stream := &capturedManagedControlStream{}
	d := &managedDaemon{
		cfg: cfg,
		startup: StartupSummary{
			VaultPath:   root,
			ConfigPath:  configPath,
			KeyPath:     keyPath,
			IndexDBPath: indexDBPath,
		},
		rootCtx:       rootCtx,
		rootCancel:    rootCancel,
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
	}

	env, err := EncryptPrivateKeyEnvelope(strings.Repeat("1", 64), "pass")
	if err != nil {
		t.Fatalf("encrypt private key envelope: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save encrypted key envelope: %v", err)
	}

	runtimeCalled := false
	oldRun := runClientRuntime
	oldBuild := buildRuntimeAPIHandler
	defer func() {
		runClientRuntime = oldRun
		buildRuntimeAPIHandler = oldBuild
	}()
	runClientRuntime = func(context.Context, clientapp.Config, clientapp.RunDeps, clientapp.RunOptions) (*clientapp.Runtime, error) {
		runtimeCalled = true
		return nil, fmt.Errorf("runtime should not start on bad password")
	}
	buildRuntimeAPIHandler = func(*clientapp.Runtime) (http.Handler, error) {
		runtimeCalled = true
		return nil, fmt.Errorf("runtime api should not be built")
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-unlock-bad",
		Action:    "key.unlock",
		Payload: map[string]any{
			"password": "wrong-pass",
		},
	})

	if runtimeCalled {
		t.Fatal("runtime should not start on bad password")
	}
	if got, want := d.currentKeyState(), managedKeyStateLocked; got != want {
		t.Fatalf("key state mismatch: got=%s want=%s", got, want)
	}
	result, ok := findControlCommandResult(stream.events, "cmd-unlock-bad")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := strings.ToLower(strings.TrimSpace(result["ok"])), "false"; got != want {
		t.Fatalf("command ok=%q, want %q", got, want)
	}
	if got, want := result["result"], "failed"; got != want {
		t.Fatalf("command result=%q, want %q", got, want)
	}
	if strings.TrimSpace(result["error"]) == "" {
		t.Fatal("command error should not be empty")
	}
}

func TestHandleManagedControlCommand_ConcurrentUnlockOnlyOneSucceeds(t *testing.T) {
	root := t.TempDir()
	keyPath := filepath.Join(root, "key.json")
	configPath := filepath.Join(root, "config.yaml")
	indexDBPath := filepath.Join(root, "data", "client-index.sqlite")
	workspaceDir := filepath.Join(root, "workspace")
	dataDir := filepath.Join(root, "data")

	cfg := clientapp.Config{}
	cfg.BSV.Network = "test"
	cfg.HTTP.Enabled = false
	cfg.FSHTTP.Enabled = false
	cfg.Storage.WorkspaceDir = workspaceDir
	cfg.Storage.DataDir = dataDir
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = indexDBPath
	if err := clientapp.ApplyConfigDefaultsForMode(&cfg, clientapp.StartupModeProduct); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	cfg.HTTP.Enabled = false
	cfg.FSHTTP.Enabled = false
	if err := clientapp.SaveConfigFile(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	rootCtx, rootCancel := context.WithCancel(context.Background())
	t.Cleanup(rootCancel)

	stream := &capturedManagedControlStream{}
	d := &managedDaemon{
		initNetwork:          "test",
		cfg:                  cfg,
		startup:              StartupSummary{ConfigPath: configPath, IndexDBPath: "client-index.sqlite"},
		rootCtx:              rootCtx,
		rootCancel:           rootCancel,
		controlStream:        stream,
		backendPhase:         managedBackendPhaseAvailable,
		runtimePhase:         managedRuntimePhaseStopped,
		unlockPasswordPrompt: "Unlock password: ",
	}
	d.startup.VaultPath = root
	d.startup.KeyPath = keyPath
	d.chainAccess = d.resolveChainAccessState()

	env, err := EncryptPrivateKeyEnvelope(strings.Repeat("1", 64), "pass")
	if err != nil {
		t.Fatalf("encrypt private key envelope: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save encrypted key envelope: %v", err)
	}

	oldRun := runClientRuntime
	oldBuild := buildRuntimeAPIHandler
	defer func() {
		runClientRuntime = oldRun
		buildRuntimeAPIHandler = oldBuild
	}()
	runClientRuntime = func(ctx context.Context, runCfg clientapp.Config, deps clientapp.RunDeps, opt clientapp.RunOptions) (*clientapp.Runtime, error) {
		if ctx == nil {
			return nil, fmt.Errorf("ctx is required")
		}
		if !opt.DisableHTTPServer {
			return nil, fmt.Errorf("disable http server should stay enabled in managed mode")
		}
		if strings.TrimSpace(opt.EffectivePrivKeyHex) == "" {
			return nil, fmt.Errorf("effective priv key is required")
		}
		if deps.DBActor != nil {
			_ = deps.DBActor.Close()
		}
		if opt.FSHTTPListener != nil {
			_ = opt.FSHTTPListener.Close()
		}
		if opt.ObsSink != nil {
			for _, name := range []string{"fs_http_started", "chain_tip_task_registered", "chain_utxo_task_registered"} {
				opt.ObsSink.Handle(obs.Event{
					Service: "bitcast-client",
					Name:    name,
				})
			}
		}
		return &clientapp.Runtime{Host: h}, nil
	}
	buildRuntimeAPIHandler = func(rt *clientapp.Runtime) (http.Handler, error) {
		if rt == nil || rt.Host == nil {
			return nil, fmt.Errorf("runtime host is nil")
		}
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		d.handleManagedControlCommand(ManagedControlCommandFrame{
			Type:      "command",
			CommandID: "cmd-concurrent-1",
			Action:    "key.unlock",
			Payload: map[string]any{
				"password": "pass",
			},
		})
	}()
	go func() {
		defer wg.Done()
		d.handleManagedControlCommand(ManagedControlCommandFrame{
			Type:      "command",
			CommandID: "cmd-concurrent-2",
			Action:    "key.unlock",
			Payload: map[string]any{
				"password": "pass",
			},
		})
	}()
	wg.Wait()

	if got, want := d.currentRuntimePhase(), managedRuntimePhaseReady; got != want {
		t.Fatalf("runtime phase mismatch: got=%s want=%s", got, want)
	}
	if got, want := d.currentKeyState(), managedKeyStateUnlocked; got != want {
		t.Fatalf("key state mismatch: got=%s want=%s", got, want)
	}
	first, ok := findControlCommandResult(stream.events, "cmd-concurrent-1")
	if !ok {
		t.Fatal("first command result event not found")
	}
	second, ok := findControlCommandResult(stream.events, "cmd-concurrent-2")
	if !ok {
		t.Fatal("second command result event not found")
	}
	successCount := 0
	alreadyCount := 0
	for _, result := range []map[string]string{first, second} {
		switch strings.TrimSpace(result["result"]) {
		case "succeeded":
			successCount++
		case "already_unlocked":
			alreadyCount++
		default:
			t.Fatalf("unexpected command result: %+v", result)
		}
	}
	if successCount != 1 || alreadyCount != 1 {
		t.Fatalf("concurrent command mismatch: success=%d already=%d", successCount, alreadyCount)
	}
}

func TestHandleManagedControlCommand_PricingCommands(t *testing.T) {
	stream := &capturedManagedControlStream{}
	rt, seedHash := newManagedPricingControlRuntime(t)
	d := &managedDaemon{
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
		rootCtx:       t.Context(),
		rt:            rt,
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-base",
		Action:    "pricing.set_base",
		Payload: map[string]any{
			"base_price_sat_per_64k": uint64(1600),
		},
	})
	baseEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-base")
	if !ok {
		t.Fatal("pricing set_base result event not found")
	}
	if got, want := strings.TrimSpace(fmt.Sprint(baseEvent.Payload["result"])), "updated"; got != want {
		t.Fatalf("set_base result=%q, want %q", got, want)
	}
	if cfgPayload, ok := baseEvent.Payload["pricing_config"].(map[string]any); !ok {
		t.Fatal("pricing_config payload missing after set_base")
	} else if got, want := fmt.Sprint(cfgPayload["base_price_sat_per_64k"]), "1600"; got != want {
		t.Fatalf("pricing base=%q, want %q", got, want)
	}

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-reset",
		Action:    "pricing.reset_seed",
		Payload: map[string]any{
			"seed_hash": seedHash,
		},
	})
	resetEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-reset")
	if !ok {
		t.Fatal("pricing reset_seed result event not found")
	}
	resetState := mustPricingStatePayload(t, resetEvent.Payload)
	if got, want := resetState["seed_hash"], seedHash; got != want {
		t.Fatalf("reset seed hash=%q, want %q", got, want)
	}
	if got, want := resetState["base_price_sat_per_64k"], "1600"; got != want {
		t.Fatalf("reset base price=%q, want %q", got, want)
	}
	if got, want := resetState["effective_price_sat_per_64k"], "1600"; got != want {
		t.Fatalf("reset effective price=%q, want %q", got, want)
	}
	if got, want := resetState["pricing_mode"], "auto"; got != want {
		t.Fatalf("reset pricing mode=%q, want %q", got, want)
	}
	assertManagedCommandResultFields(t, resetEvent, "pricing.reset_seed")

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-feed",
		Action:    "pricing.feed_seed",
		Payload: map[string]any{
			"seed_hash":     seedHash,
			"query_count":   uint32(40),
			"deal_count":    uint32(35),
			"silence_hours": uint32(0),
		},
	})
	feedEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-feed")
	if !ok {
		t.Fatal("pricing feed_seed result event not found")
	}
	feedState := mustPricingStatePayload(t, feedEvent.Payload)
	if got, want := feedState["pending_query_count"], "40"; got != want {
		t.Fatalf("feed pending query=%q, want %q", got, want)
	}
	if got, want := feedState["pending_deal_count"], "35"; got != want {
		t.Fatalf("feed pending deal=%q, want %q", got, want)
	}
	assertManagedCommandResultFields(t, feedEvent, "pricing.feed_seed")

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-tick",
		Action:    "pricing.run_tick",
		Payload: map[string]any{
			"hours": uint32(1),
		},
	})
	tickEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-tick")
	if !ok {
		t.Fatal("pricing run_tick result event not found")
	}
	tickStates := mustPricingStatesPayload(t, tickEvent.Payload)
	if len(tickStates) == 0 {
		t.Fatal("pricing_states payload missing")
	}
	tickState := tickStates[0]
	if got, want := tickState["pending_query_count"], "0"; got != want {
		t.Fatalf("tick pending query=%q, want %q", got, want)
	}
	if got, want := tickState["pending_deal_count"], "0"; got != want {
		t.Fatalf("tick pending deal=%q, want %q", got, want)
	}
	if got, want := tickState["pricing_mode"], "auto"; got != want {
		t.Fatalf("tick pricing mode=%q, want %q", got, want)
	}
	if audits := mustPricingAuditsPayload(t, tickEvent.Payload); len(audits) == 0 {
		t.Fatal("pricing audits should be present after tick")
	}
	assertManagedCommandResultFields(t, tickEvent, "pricing.run_tick")

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-force",
		Action:    "pricing.set_force",
		Payload: map[string]any{
			"seed_hash":               seedHash,
			"force_price_sat_per_64k": uint64(7777),
			"force_hours":             uint32(6),
		},
	})
	forceEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-force")
	if !ok {
		t.Fatal("pricing set_force result event not found")
	}
	forceState := mustPricingStatePayload(t, forceEvent.Payload)
	if got, want := forceState["pricing_mode"], "force"; got != want {
		t.Fatalf("force pricing mode=%q, want %q", got, want)
	}
	if got, want := forceState["effective_price_sat_per_64k"], "7777"; got != want {
		t.Fatalf("force effective price=%q, want %q", got, want)
	}
	assertManagedCommandResultFields(t, forceEvent, "pricing.set_force")

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-release",
		Action:    "pricing.release_force",
		Payload: map[string]any{
			"seed_hash": seedHash,
		},
	})
	releaseEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-release")
	if !ok {
		t.Fatal("pricing release_force result event not found")
	}
	releaseState := mustPricingStatePayload(t, releaseEvent.Payload)
	if got, want := releaseState["pricing_mode"], "auto"; got != want {
		t.Fatalf("release pricing mode=%q, want %q", got, want)
	}
	if got, want := releaseState["forced_price_sat_per_64k"], "0"; got != want {
		t.Fatalf("release forced price=%q, want %q", got, want)
	}
	assertManagedCommandResultFields(t, releaseEvent, "pricing.release_force")

	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-reconcile",
		Action:    "pricing.trigger_reconcile",
		Payload: map[string]any{
			"seed_hash": seedHash,
			"now_unix":  mustInt64(t, tickState["last_rate_calc_at_unix"]) + 3601,
		},
	})
	reconcileEvent, ok := findManagedCommandEvent(stream.events, "cmd-pricing-reconcile")
	if !ok {
		t.Fatal("pricing trigger_reconcile result event not found")
	}
	reconcileState := mustPricingStatePayload(t, reconcileEvent.Payload)
	if got, want := reconcileState["pending_query_count"], "0"; got != want {
		t.Fatalf("reconcile pending query=%q, want %q", got, want)
	}
	if got, want := reconcileState["pending_deal_count"], "0"; got != want {
		t.Fatalf("reconcile pending deal=%q, want %q", got, want)
	}
	if audits := mustPricingAuditsPayload(t, reconcileEvent.Payload); len(audits) == 0 {
		t.Fatal("pricing audits should be present after reconcile")
	}
	assertManagedCommandResultFields(t, reconcileEvent, "pricing.trigger_reconcile")
}

func TestHandleManagedControlCommand_PricingCommandsMissingParams(t *testing.T) {
	cases := []ManagedControlCommandFrame{
		{Type: "command", CommandID: "cmd-missing-base", Action: "pricing.set_base"},
		{Type: "command", CommandID: "cmd-missing-reset", Action: "pricing.reset_seed"},
		{Type: "command", CommandID: "cmd-missing-feed", Action: "pricing.feed_seed"},
		{Type: "command", CommandID: "cmd-missing-force", Action: "pricing.set_force", Payload: map[string]any{"seed_hash": strings.Repeat("ab", 32)}},
		{Type: "command", CommandID: "cmd-missing-release", Action: "pricing.release_force"},
		{Type: "command", CommandID: "cmd-missing-tick", Action: "pricing.run_tick"},
		{Type: "command", CommandID: "cmd-missing-reconcile", Action: "pricing.trigger_reconcile"},
	}
	for _, tc := range cases {
		t.Run(tc.CommandID, func(t *testing.T) {
			stream := &capturedManagedControlStream{}
			rt, _ := newManagedPricingControlRuntime(t)
			d := &managedDaemon{
				controlStream: stream,
				backendPhase:  managedBackendPhaseAvailable,
				runtimePhase:  managedRuntimePhaseStopped,
				rootCtx:       t.Context(),
				rt:            rt,
			}
			d.handleManagedControlCommand(tc)
			result, ok := findManagedCommandResult(stream.events, tc.CommandID)
			if !ok {
				t.Fatal("command result event not found")
			}
			if got, want := strings.TrimSpace(result["ok"]), "false"; got != want {
				t.Fatalf("command ok=%q, want %q", got, want)
			}
			if strings.TrimSpace(result["error"]) == "" {
				t.Fatal("command error should not be empty")
			}
		})
	}
}

func TestHandleManagedControlCommand_PricingCommandsUnsupportedAction(t *testing.T) {
	stream := &capturedManagedControlStream{}
	rt, _ := newManagedPricingControlRuntime(t)
	d := &managedDaemon{
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
		rootCtx:       t.Context(),
		rt:            rt,
	}
	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-unsupported",
		Action:    "pricing.unknown",
	})
	result, ok := findManagedCommandResult(stream.events, "cmd-pricing-unsupported")
	if !ok {
		t.Fatal("command result event not found")
	}
	if got, want := strings.TrimSpace(result["error"]), "unsupported control action: pricing.unknown"; got != want {
		t.Fatalf("unsupported error=%q, want %q", got, want)
	}
}

func TestHandleManagedControlCommand_WorkspaceCommands(t *testing.T) {
	t.Run("wallet_mode_list_and_sync", func(t *testing.T) {
		stream := &capturedManagedControlStream{}
		rt := newManagedWorkspaceWalletRuntime(t)
		d := &managedDaemon{
			controlStream: stream,
			backendPhase:  managedBackendPhaseAvailable,
			runtimePhase:  managedRuntimePhaseStopped,
			rootCtx:       t.Context(),
			rt:            rt,
		}

		d.handleManagedControlCommand(ManagedControlCommandFrame{
			Type:      "command",
			CommandID: "cmd-workspace-list",
			Action:    "workspace.list",
		})
		listEvent, ok := findManagedCommandEvent(stream.events, "cmd-workspace-list")
		if !ok {
			t.Fatal("workspace list result event not found")
		}
		assertManagedCommandResultFields(t, listEvent, "workspace.list")
		if got, want := strings.TrimSpace(fmt.Sprint(listEvent.Payload["ok"])), "true"; got != want {
			t.Fatalf("workspace list ok=%q, want %q", got, want)
		}
		if got, want := strings.TrimSpace(fmt.Sprint(listEvent.Payload["result"])), "listed"; got != want {
			t.Fatalf("workspace list result=%q, want %q", got, want)
		}
		if got, want := strings.TrimSpace(fmt.Sprint(listEvent.Payload["total"])), "0"; got != want {
			t.Fatalf("workspace list total=%q, want %q", got, want)
		}

		d.handleManagedControlCommand(ManagedControlCommandFrame{
			Type:      "command",
			CommandID: "cmd-workspace-sync",
			Action:    "workspace.sync_once",
		})
		syncEvent, ok := findManagedCommandEvent(stream.events, "cmd-workspace-sync")
		if !ok {
			t.Fatal("workspace sync_once result event not found")
		}
		assertManagedCommandResultFields(t, syncEvent, "workspace.sync_once")
		if got, want := strings.TrimSpace(fmt.Sprint(syncEvent.Payload["ok"])), "true"; got != want {
			t.Fatalf("workspace sync ok=%q, want %q", got, want)
		}
		if got, want := strings.TrimSpace(fmt.Sprint(syncEvent.Payload["result"])), "synced"; got != want {
			t.Fatalf("workspace sync result=%q, want %q", got, want)
		}
		if got, want := strings.TrimSpace(fmt.Sprint(syncEvent.Payload["seed_count"])), "0"; got != want {
			t.Fatalf("workspace sync seed_count=%q, want %q", got, want)
		}
	})

	t.Run("missing_params_and_unknown_action", func(t *testing.T) {
		cases := []ManagedControlCommandFrame{
			{Type: "command", CommandID: "cmd-workspace-add-missing", Action: "workspace.add"},
			{Type: "command", CommandID: "cmd-workspace-update-missing", Action: "workspace.update", Payload: map[string]any{"workspace_path": "/tmp/demo"}},
			{Type: "command", CommandID: "cmd-workspace-delete-missing", Action: "workspace.delete"},
			{Type: "command", CommandID: "cmd-workspace-unknown", Action: "workspace.unknown"},
		}
		for _, tc := range cases {
			stream := &capturedManagedControlStream{}
			rt := newManagedWorkspaceWalletRuntime(t)
			d := &managedDaemon{
				controlStream: stream,
				backendPhase:  managedBackendPhaseAvailable,
				runtimePhase:  managedRuntimePhaseStopped,
				rootCtx:       t.Context(),
				rt:            rt,
			}
			d.handleManagedControlCommand(tc)
			result, ok := findManagedCommandResult(stream.events, tc.CommandID)
			if !ok {
				t.Fatalf("command result event not found: %s", tc.CommandID)
			}
			if got, want := strings.TrimSpace(result["ok"]), "false"; got != want {
				t.Fatalf("command ok=%q, want %q", got, want)
			}
			if strings.TrimSpace(result["error"]) == "" {
				t.Fatalf("command error should not be empty: %s", tc.CommandID)
			}
		}
	})
}

func TestManagedObsControlWhitelist_AllActionsRouted(t *testing.T) {
	t.Parallel()

	if err := ensureManagedObsControlRouteCoverage(); err != nil {
		t.Fatalf("obs control route coverage mismatch: %v", err)
	}

	for _, action := range contractfnlock.ObsControlActions() {
		action = strings.TrimSpace(action)
		if action == "" {
			t.Fatal("obs control whitelist action should not be empty")
		}
		if !isManagedObsControlActionRouted(action) {
			t.Fatalf("obs control whitelist action is not routed: %s", action)
		}
		lockID, ok := contractfnlock.ObsControlActionLockID(action)
		if !ok {
			t.Fatalf("obs control action lock id mapping missing: %s", action)
		}
		lockID = strings.TrimSpace(lockID)
		if lockID == "" {
			t.Fatalf("obs control whitelist lock id should not be empty: action=%s", action)
		}
		if !isManagedObsControlLockRouted(lockID) {
			t.Fatalf("obs control action lock id has no daemon handler: action=%s lock_id=%s", action, lockID)
		}
	}
	if !isManagedObsControlLockRouted("bitfs.managed.control.execute_workspace") {
		t.Fatal("workspace execute lock should be routed")
	}
	if isManagedObsControlActionRouted("pricing.unknown") {
		t.Fatal("pricing.unknown should not be treated as routed")
	}
}

func TestHandleManagedControlCommand_PricingCommandResultFieldsComplete(t *testing.T) {
	stream := &capturedManagedControlStream{}
	rt, _ := newManagedPricingControlRuntime(t)
	d := &managedDaemon{
		controlStream: stream,
		backendPhase:  managedBackendPhaseAvailable,
		runtimePhase:  managedRuntimePhaseStopped,
		rootCtx:       t.Context(),
		rt:            rt,
	}
	d.handleManagedControlCommand(ManagedControlCommandFrame{
		Type:      "command",
		CommandID: "cmd-pricing-fields",
		Action:    "pricing.reset_seed",
		Payload: map[string]any{
			"seed_hash": strings.Repeat("ab", 32),
		},
	})
	result, ok := findManagedCommandResult(stream.events, "cmd-pricing-fields")
	if !ok {
		t.Fatal("command result event not found")
	}
	for _, key := range []string{"command_id", "action", "ok", "result", "error", "backend_phase", "runtime_phase", "key_state", "unlock_owner", "unlock_token"} {
		if _, exists := result[key]; !exists {
			t.Fatalf("result field %q missing", key)
		}
	}
}

func findControlCommandResult(events []ManagedRuntimeEvent, commandID string) (map[string]string, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Topic != "backend.command.result" {
			continue
		}
		fields := map[string]string{}
		for key, value := range ev.Payload {
			fields[key] = strings.TrimSpace(fmt.Sprint(value))
		}
		if strings.TrimSpace(fields["command_id"]) != strings.TrimSpace(commandID) {
			continue
		}
		return fields, true
	}
	return nil, false
}

func findManagedCommandEvent(events []ManagedRuntimeEvent, commandID string) (ManagedRuntimeEvent, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Topic != "backend.command.result" {
			continue
		}
		if strings.TrimSpace(fmt.Sprint(ev.Payload["command_id"])) != strings.TrimSpace(commandID) {
			continue
		}
		return ev, true
	}
	return ManagedRuntimeEvent{}, false
}

func findManagedCommandResult(events []ManagedRuntimeEvent, commandID string) (map[string]string, bool) {
	return findControlCommandResult(events, commandID)
}

func assertManagedCommandResultFields(t *testing.T, ev ManagedRuntimeEvent, wantAction string) {
	t.Helper()
	if got := strings.TrimSpace(fmt.Sprint(ev.Payload["action"])); got != wantAction {
		t.Fatalf("result action=%q, want %q", got, wantAction)
	}
	for _, key := range []string{"command_id", "action", "ok", "result", "error", "backend_phase", "runtime_phase", "key_state", "unlock_owner", "unlock_token"} {
		if _, exists := ev.Payload[key]; !exists {
			t.Fatalf("result field %q missing", key)
		}
	}
}

func mustPricingStatePayload(t *testing.T, payload map[string]any) map[string]string {
	t.Helper()
	statePayload, ok := payload["pricing_state"].(map[string]any)
	if !ok {
		t.Fatal("pricing_state payload missing")
	}
	out := map[string]string{}
	for key, value := range statePayload {
		out[key] = strings.TrimSpace(fmt.Sprint(value))
	}
	return out
}

func mustPricingAuditsPayload(t *testing.T, payload map[string]any) []map[string]string {
	t.Helper()
	raw, ok := payload["pricing_audits"]
	if !ok || raw == nil {
		return nil
	}
	items, ok := raw.([]map[string]any)
	if !ok {
		t.Fatal("pricing_audits payload has unexpected type")
	}
	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		next := map[string]string{}
		for key, value := range item {
			next[key] = strings.TrimSpace(fmt.Sprint(value))
		}
		out = append(out, next)
	}
	return out
}

func mustPricingStatesPayload(t *testing.T, payload map[string]any) []map[string]string {
	t.Helper()
	raw, ok := payload["pricing_states"]
	if !ok || raw == nil {
		return nil
	}
	items, ok := raw.([]map[string]any)
	if !ok {
		t.Fatal("pricing_states payload has unexpected type")
	}
	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		next := map[string]string{}
		for key, value := range item {
			next[key] = strings.TrimSpace(fmt.Sprint(value))
		}
		out = append(out, next)
	}
	return out
}

func mustInt64(t *testing.T, raw string) int64 {
	t.Helper()
	var out int64
	if _, err := fmt.Sscan(strings.TrimSpace(raw), &out); err != nil {
		t.Fatalf("parse int64: %v", err)
	}
	return out
}

func newManagedPricingControlRuntime(t *testing.T) (*clientapp.Runtime, string) {
	t.Helper()
	root := t.TempDir()
	dbPath := filepath.Join(root, "pricing.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := clientapp.EnsureClientStoreSchema(t.Context(), clientapp.NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	seedHash := strings.Repeat("ab", 32)
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, int64(4), int64(1024), filepath.Join(root, "seed", "sample.bse"), "sample.bin", "application/octet-stream"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	var cfg clientapp.Config
	if err := clientapp.ApplyConfigDefaultsForMode(&cfg, clientapp.StartupModeTest); err != nil {
		t.Fatalf("apply config defaults: %v", err)
	}
	cfg.Storage.WorkspaceDir = filepath.Join(root, "workspace")
	cfg.Storage.DataDir = filepath.Join(root, "data")
	rt, err := clientapp.NewPricingTestRuntime(t.Context(), db, cfg)
	if err != nil {
		t.Fatalf("new pricing runtime: %v", err)
	}
	return rt, seedHash
}

func newManagedWorkspaceWalletRuntime(t *testing.T) *clientapp.Runtime {
	t.Helper()
	var cfg clientapp.Config
	if err := clientapp.ApplyConfigDefaultsForMode(&cfg, clientapp.StartupModeTest); err != nil {
		t.Fatalf("apply config defaults: %v", err)
	}
	cfg.Storage.WorkspaceDir = ""
	cfg.Storage.DataDir = filepath.Join(t.TempDir(), "data")
	rt, err := clientapp.NewPricingTestRuntime(t.Context(), nil, cfg)
	if err != nil {
		t.Fatalf("new workspace runtime: %v", err)
	}
	cfgSvc := rt.RuntimeConfigService()
	if cfgSvc == nil {
		t.Fatal("runtime config service is nil")
	}
	next := cfgSvc.Snapshot()
	next.Storage.WorkspaceDir = ""
	if err := cfgSvc.UpdateMemoryOnly(next); err != nil {
		t.Fatalf("set wallet workspace_dir: %v", err)
	}
	return rt
}
