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
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
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

func TestHandleKeyUnlock_StartsRuntimeAndBuildsManagedHandler(t *testing.T) {
	t.Parallel()

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
		controlStream: &capturedManagedControlStream{},
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

	req := httptest.NewRequest(http.MethodPost, "/api/v1/key/unlock", strings.NewReader(`{"password":"pass"}`))
	rec := httptest.NewRecorder()
	d.handleKeyUnlock(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unlock status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

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
