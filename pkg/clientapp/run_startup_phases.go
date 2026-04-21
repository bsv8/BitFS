package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type runStartupState struct {
	removeObs func()

	dbPath             string
	logFile            string
	logConsoleMinLevel string
	sqlTraceDir        string
	sqlTraceSummary    string
	sqlTraceInited     bool

	db           *sql.DB
	dbActor      *sqliteactor.Actor
	store        *clientDB
	closeOwnedDB func()

	catalog      *sellerCatalog
	workspaceMgr *workspaceManager

	host            host.Host
	trace           pproto.TraceSink
	closeTrace      func() error
	clientPubkeyHex string
	identity        *clientIdentityCaps

	healthyGateways []peer.AddrInfo
	healthyArbiters []peer.AddrInfo

	cfgSvc       *runtimeConfigService
	closeModules []func()

	rtCtx    context.Context
	rtCancel context.CancelFunc
	wg       sync.WaitGroup

	rt *Runtime
}

func (p *runStartupPhases) Cleanup() {
	if p == nil {
		return
	}
	st := &p.state
	if st.rt != nil {
		_ = st.rt.Close()
		st.rt = nil
		return
	}
	if st.rtCancel != nil {
		st.rtCancel()
	}
	if st.host != nil {
		_ = st.host.Close()
		st.host = nil
	}
	if st.closeTrace != nil {
		_ = st.closeTrace()
		st.closeTrace = nil
	}
	if st.closeOwnedDB != nil {
		st.closeOwnedDB()
		st.closeOwnedDB = nil
	}
	for i := len(st.closeModules) - 1; i >= 0; i-- {
		if st.closeModules[i] != nil {
			st.closeModules[i]()
		}
	}
	st.closeModules = nil
	if st.removeObs != nil {
		st.removeObs()
		st.removeObs = nil
	}
	if st.sqlTraceInited {
		_ = closeSQLTrace()
		st.sqlTraceInited = false
	}
}

func (p *runStartupPhases) BuildCore() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	runtimeCfg := cloneConfig(p.runtimeCfg)
	st := &p.state

	if p.opt.ObsSink != nil {
		st.removeObs = obs.AddListener(func(ev obs.Event) {
			if ev.Service != ServiceName {
				return
			}
			p.opt.ObsSink.Handle(ev)
		})
	}
	if err := initDataDirs(&runtimeCfg); err != nil {
		return err
	}
	st.dbPath = runtimeCfg.Index.SQLitePath
	if !filepath.IsAbs(st.dbPath) {
		st.dbPath = filepath.Join(runtimeCfg.Storage.DataDir, st.dbPath)
	}
	if err := os.MkdirAll(filepath.Dir(st.dbPath), 0o755); err != nil {
		return err
	}
	logFile, logConsoleMinLevel := ResolveLogConfig(&runtimeCfg)
	runtimeCfg.Log.File = logFile
	st.logFile = logFile
	st.logConsoleMinLevel = logConsoleMinLevel

	sqlTraceMgr, err := initSQLTrace(logFile, runtimeCfg.Debug)
	if err != nil {
		return err
	}
	st.sqlTraceInited = true
	if sqlTraceMgr != nil {
		st.sqlTraceDir = sqlTraceMgr.TracePath()
		st.sqlTraceSummary = sqlTraceMgr.SummaryPath()
	}

	st.db = p.deps.RawDB
	st.dbActor = p.deps.DBActor
	st.store = p.deps.Store
	st.closeOwnedDB = func() {
		if !p.deps.OwnsDB {
			return
		}
		if st.dbActor != nil {
			_ = st.dbActor.Close()
			return
		}
		if st.db != nil {
			_ = st.db.Close()
		}
	}
	if err := dbSyncSystemSeedPricingPolicies(p.ctx, st.store, runtimeCfg.Seller.Pricing.FloorPriceSatPer64K, runtimeCfg.Seller.Pricing.ResaleDiscountBPS); err != nil {
		return err
	}
	if err := dbUpsertPricingAutopilotConfig(p.ctx, st.store, PricingConfig{BasePriceSatPer64K: runtimeCfg.Seller.Pricing.LiveBasePriceSatPer64K}, time.Now().Unix()); err != nil {
		return err
	}

	st.catalog = newSellerCatalog()
	st.workspaceMgr = &workspaceManager{
		ctx:     p.ctx,
		cfg:     &runtimeCfg,
		db:      st.db,
		store:   st.store,
		catalog: st.catalog,
	}
	if err := st.workspaceMgr.EnsureDefaultWorkspace(); err != nil {
		return err
	}
	if err := st.workspaceMgr.ValidateLiveCacheCapacity(runtimeCfg.Live.CacheMaxBytes); err != nil {
		return err
	}
	if _, err := st.workspaceMgr.LoadSeedCatalogSnapshot(p.ctx); err != nil {
		return err
	}
	if runtimeCfg.Scan.StartupFullScan {
		if _, err := st.workspaceMgr.SyncOnce(p.ctx); err != nil {
			return err
		}
	}
	if err := st.workspaceMgr.EnforceLiveCacheLimit(runtimeCfg.Live.CacheMaxBytes); err != nil {
		return err
	}
	if p.opt.PostWorkspaceBootstrap != nil {
		if err := p.opt.PostWorkspaceBootstrap(p.ctx, st.store); err != nil {
			return err
		}
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	}
	effectivePrivHex, err := normalizeRawSecp256k1PrivKeyHex(p.opt.EffectivePrivKeyHex)
	if err != nil {
		return err
	}
	if strings.TrimSpace(effectivePrivHex) == "" {
		return fmt.Errorf("effective private key is required")
	}
	runtimeCfg.Keys.PrivkeyHex = effectivePrivHex
	priv, err := parsePrivHex(effectivePrivHex)
	if err != nil {
		return err
	}
	opts = append(opts, libp2p.Identity(priv))
	st.host, err = libp2p.New(opts...)
	if err != nil {
		return err
	}
	st.clientPubkeyHex, err = localPubKeyHex(st.host)
	if err != nil {
		return err
	}
	if runtimeCfg.ClientID != "" && !strings.EqualFold(strings.TrimSpace(runtimeCfg.ClientID), st.clientPubkeyHex) {
		obs.Info(ServiceName, "client_pubkey_hex_overridden_by_pubkey", map[string]any{"configured_client_pubkey_hex": runtimeCfg.ClientID, "effective_client_pubkey_hex": st.clientPubkeyHex})
	}
	runtimeCfg.ClientID = st.clientPubkeyHex
	st.identity, err = buildClientIdentityCaps(runtimeCfg, effectivePrivHex)
	if err != nil {
		return err
	}
	p.runtimeCfg = runtimeCfg
	return nil
}

func (p *runStartupPhases) ConnectExternal() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	st := &p.state
	runtimeCfg := cloneConfig(p.runtimeCfg)

	activeGWs, err := connectGateways(p.ctx, st.host, runtimeCfg.Network.Gateways)
	if err != nil {
		return err
	}
	if len(activeGWs) == 0 {
		obs.Business(ServiceName, "waiting_gateway_config", map[string]any{
			"message": noEnabledGatewayMessage(runtimeCfg.HTTP.Enabled, runtimeCfg.FSHTTP.Enabled),
		})
	}
	arbInfo, err := connectArbiters(p.ctx, st.host, runtimeCfg.Network.Arbiters)
	if err != nil {
		return err
	}
	st.trace = p.opt.RPCTrace
	if st.trace == nil && runtimeCfg.Debug {
		localTrace, err := pproto.NewLocalRawTraceSink(st.logFile)
		if err != nil {
			return err
		}
		st.trace = localTrace
		st.closeTrace = localTrace.Close
	}
	st.healthyGateways = checkPeerHealth(p.ctx, st.host, activeGWs, ProtoHealth, gwSec(st.trace), "gateway")
	if len(activeGWs) > 0 && len(st.healthyGateways) == 0 {
		obs.Error(ServiceName, "gateway_health_all_failed", map[string]any{
			"configured_gateway_count": len(activeGWs),
			"fallback":                 "use_connected_gateways_and_retry_in_listen_loop",
		})
		st.healthyGateways = activeGWs
	}
	st.healthyArbiters = checkPeerHealth(p.ctx, st.host, arbInfo, ProtoArbHealth, arbSec(st.trace), "arbiter")
	if len(st.healthyArbiters) == 0 && len(arbInfo) > 0 {
		obs.Error(ServiceName, "arbiter_health_all_failed", map[string]any{
			"configured_arbiter_count": len(arbInfo),
		})
	}
	obs.Important(ServiceName, "started", map[string]any{
		"transport_peer_id": st.host.ID().String(),
		"pubkey_hex":        st.clientPubkeyHex,
		"client_pubkey_hex": runtimeCfg.ClientID,
		"seller_enabled":    runtimeCfg.Seller.Enabled,
		"listen_enabled":    cfgBool(runtimeCfg.Listen.Enabled, true),
		"gateway_count":     len(st.healthyGateways),
		"arbiter_count":     len(st.healthyArbiters),
		"db":                st.dbPath,
		"log_file":          st.logFile,
		"log_console":       st.logConsoleMinLevel,
		"protocol_suite":    BBroadcastSuiteVersion,
		"protocol_doc_name": BBroadcastProtocolName,
	})
	st.cfgSvc, err = newRuntimeConfigService(runtimeCfg, p.opt.ConfigPath, p.startupMode)
	if err != nil {
		return err
	}
	return nil
}

func (p *runStartupPhases) StartServices() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	st := &p.state
	runtimeCfg := cloneConfig(p.runtimeCfg)
	rt := &Runtime{
		Host:                     st.host,
		ctx:                      p.ctx,
		store:                    st.store,
		identity:                 st.identity,
		config:                   st.cfgSvc,
		StartedAtUnix:            time.Now().Unix(),
		HealthyGWs:               st.healthyGateways,
		HealthyArbiters:          st.healthyArbiters,
		Workspace:                st.workspaceMgr,
		Catalog:                  st.catalog,
		ActionChain:              p.opt.ActionChain,
		WalletChain:              p.opt.WalletChain,
		live:                     newLiveRuntime(),
		sqlTraceDir:              st.sqlTraceDir,
		sqlTraceSummaryPath:      st.sqlTraceSummary,
		feePools:                 map[string]*feePoolSession{},
		feePoolPayLocks:          map[string]*sync.Mutex{},
		triplePool:               map[string]*triplePoolSession{},
		transferPoolSessionLocks: map[string]*sync.Mutex{},
		rpcTrace:                 st.trace,
	}
	st.rtCtx, st.rtCancel = context.WithCancel(p.ctx)
	rt.bgCancel = st.rtCancel
	rt.taskSched = newTaskScheduler(st.store, ServiceName)
	rt.taskSched.ctx = st.rtCtx
	rt.kernel = newClientKernel(rt, st.store, st.workspaceMgr)
	rt.orch = newOrchestrator(rt, st.store)
	if closeModule, err := installBuiltinModules(st.rtCtx, rt, st.store); err != nil {
		return err
	} else if closeModule != nil {
		st.closeModules = append(st.closeModules, closeModule)
	}
	if rt.modules != nil {
		if err := rt.modules.RunOpenHooks(st.rtCtx); err != nil {
			for i := len(st.closeModules) - 1; i >= 0; i-- {
				if st.closeModules[i] != nil {
					st.closeModules[i]()
				}
			}
			st.closeModules = nil
			return err
		}
	}
	registerLiveHandlers(st.store, rt)
	registerDirectQuoteSubmitHandler(st.host, st.store, st.trace)
	if runtimeCfg.Seller.Enabled {
		registerSellerHandlers(rt, st.host, st.store, rt.live, st.trace, runtimeCfg)
	}
	if rt.ActionChain == nil {
		actionChain, err := chainbridge.NewDefaultFeePoolChain(chainbridge.RouteConfig{
			Provider: chainbridge.WhatsOnChainProvider,
			Network:  runtimeCfg.BSV.Network,
		})
		if err != nil {
			return err
		}
		rt.ActionChain = actionChain
	}
	if rt.WalletChain == nil {
		walletChain, err := NewWalletChainClient(chainbridge.Route{
			Provider: chainbridge.WhatsOnChainProvider,
			Network:  runtimeCfg.BSV.Network,
		})
		if err != nil {
			return err
		}
		rt.WalletChain = walletChain
	}
	rt.gwManager = newGatewayManager(rt, st.host)
	_ = rt.gwManager.InitFromConfig(st.rtCtx, runtimeCfg.Network.Gateways)
	rt.HealthyGWs = rt.gwManager.GetConnectedGateways()

	if rt.orch != nil {
		rt.orch.Start(st.rtCtx)
	}
	startChainMaintainer(st.rtCtx, rt, st.store)
	startListenLoops(st.rtCtx, rt, st.store)
	startAutoNodeReachabilityAnnounceLoop(st.rtCtx, rt, st.store)
	if runtimeCfg.HTTP.Enabled && !p.opt.DisableHTTPServer {
		rt.HTTP = newHTTPAPIServer(rt, st.cfgSvc, st.db, st.store, st.host, st.healthyGateways, st.workspaceMgr, st.trace)
		st.wg.Add(1)
		go func() {
			defer st.wg.Done()
			if err := rt.HTTP.Start(); err != nil {
				obs.Error(ServiceName, "http_api_stopped", map[string]any{"error": err.Error()})
			}
		}()
	}
	if runtimeCfg.FSHTTP.Enabled {
		rt.FSHTTP = newFileHTTPServer(rt, st.cfgSvc, st.store, st.workspaceMgr)
		fsHTTPListener := p.opt.FSHTTPListener
		st.wg.Add(1)
		go func() {
			defer st.wg.Done()
			var err error
			if fsHTTPListener != nil {
				err = rt.FSHTTP.StartOnListener(fsHTTPListener)
			} else {
				err = rt.FSHTTP.Start()
			}
			if err != nil {
				obs.Error(ServiceName, "fs_http_stopped", map[string]any{"error": err.Error()})
			}
		}()
	}
	go restorePersistedLiveFollows(st.rtCtx, st.store, rt)
	st.rt = rt
	return nil
}

func (p *runStartupPhases) BindShutdown() error {
	if p == nil {
		return fmt.Errorf("startup phases are required")
	}
	st := &p.state
	if st.rt == nil || st.host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	rt := st.rt
	rtCtx := st.rtCtx
	rt.closeFn = func() error {
		if rt.bgCancel != nil {
			rt.bgCancel()
		}
		if rt.taskSched != nil {
			if err := rt.taskSched.Shutdown(); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					obs.Error(ServiceName, "task_scheduler_shutdown_failed", map[string]any{"error": err.Error()})
				}
			}
		}
		if rt.chainMaint != nil {
			rt.chainMaint.Wait()
		}
		if rt.HTTP != nil {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(rtCtx), 5*time.Second)
			_ = rt.HTTP.Shutdown(shutdownCtx)
			cancel()
		}
		if rt.FSHTTP != nil {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(rtCtx), 5*time.Second)
			_ = rt.FSHTTP.Shutdown(shutdownCtx)
			cancel()
		}
		st.wg.Wait()
		if st.removeObs != nil {
			st.removeObs()
			st.removeObs = nil
		}
		var firstErr error
		if rt.modules != nil {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(rtCtx), 5*time.Second)
			if err := rt.modules.RunCloseHooks(shutdownCtx); err != nil && firstErr == nil {
				firstErr = err
			}
			cancel()
		}
		for i := len(st.closeModules) - 1; i >= 0; i-- {
			if st.closeModules[i] != nil {
				st.closeModules[i]()
			}
		}
		st.closeModules = nil
		if rt.modules != nil {
			rt.modules = nil
		}
		if err := st.host.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if st.closeOwnedDB != nil {
			st.closeOwnedDB()
			st.closeOwnedDB = nil
		}
		if st.closeTrace != nil {
			if err := st.closeTrace(); err != nil && firstErr == nil {
				firstErr = err
			}
			st.closeTrace = nil
		}
		if st.sqlTraceInited {
			if err := closeSQLTrace(); err != nil && firstErr == nil {
				firstErr = err
			}
			st.sqlTraceInited = false
		}
		return firstErr
	}
	go func() {
		<-p.ctx.Done()
		_ = rt.Close()
	}()
	return nil
}
