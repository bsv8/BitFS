package clientapp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/libp2p/go-libp2p/core/host"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type txHistoryEntry struct {
	GatewayPeerID string
	EventType     string
	Direction     string
	AmountSatoshi int64
	Purpose       string
	Note          string
	PoolID        string
	MsgID         string
	SequenceNum   uint32
	CycleIndex    uint32
}

type gatewayEventEntry struct {
	GatewayPeerID string
	CommandID     string
	Action        string
	MsgID         string
	SequenceNum   uint32
	PoolID        string
	AmountSatoshi int64
	Payload       any
}

type walletFundFlowEntry struct {
	VisitID         string
	VisitLocator    string
	FlowID          string
	FlowType        string
	RefID           string
	Stage           string
	Direction       string
	Purpose         string
	AmountSatoshi   int64
	UsedSatoshi     int64
	ReturnedSatoshi int64
	RelatedTxID     string
	Note            string
	Payload         any
}

type requestVisitMeta struct {
	VisitID      string
	VisitLocator string
}

type requestVisitContextKey string

const (
	headerVisitID              = "X-BitFS-Visit-ID"
	headerVisitLocator         = "X-BitFS-Visit-Locator"
	requestVisitMetaContextKey = requestVisitContextKey("bitfs_request_visit_meta")
)

type walletLedgerEntry struct {
	TxID              string
	Direction         string
	Category          string
	AmountSatoshi     int64
	CounterpartyLabel string
	Status            string
	BlockHeight       int64
	OccurredAtUnix    int64
	RawRefID          string
	Note              string
	Payload           any
}

type schedulerTaskSnapshot struct {
	Name              string
	Status            string
	LastTrigger       string
	LastStartedAtUnix int64
	LastEndedAtUnix   int64
	LastDurationMS    int64
	LastError         string
	InFlight          bool
}

func requestVisitMetaFromRequest(r *http.Request) requestVisitMeta {
	if r == nil {
		return requestVisitMeta{}
	}
	return requestVisitMeta{
		VisitID:      normalizeVisitIDHeader(r.Header.Get(headerVisitID)),
		VisitLocator: normalizeVisitLocatorHeader(r.Header.Get(headerVisitLocator)),
	}
}

func requestVisitMetaFromContext(ctx context.Context) requestVisitMeta {
	if ctx == nil {
		return requestVisitMeta{}
	}
	meta, _ := ctx.Value(requestVisitMetaContextKey).(requestVisitMeta)
	meta.VisitID = normalizeVisitIDHeader(meta.VisitID)
	meta.VisitLocator = normalizeVisitLocatorHeader(meta.VisitLocator)
	return meta
}

func withRequestVisitMeta(r *http.Request) *http.Request {
	if r == nil {
		return r
	}
	meta := requestVisitMetaFromRequest(r)
	return r.WithContext(context.WithValue(r.Context(), requestVisitMetaContextKey, meta))
}

func normalizeVisitIDHeader(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if len(value) > 128 {
		value = value[:128]
	}
	for _, ch := range value {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' {
			continue
		}
		return ""
	}
	return value
}

func normalizeVisitLocatorHeader(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if len(value) > 512 {
		value = value[:512]
	}
	return value
}

type httpAPIServer struct {
	rt        *Runtime
	cfg       *Config
	db        *sql.DB
	dbActor   *sqliteactor.Actor
	store     *clientDB
	h         host.Host
	gateways  []peer.AddrInfo
	workspace *workspaceManager
	srv       *http.Server
	startedAt time.Time
	jobsMu    sync.RWMutex
	getJobs   map[string]*fileGetJob
	rpcTrace  pproto.TraceSink
}

// 说明：
// - HTTP API 仅作为本地管理平面（触发、查询、配置）；
// - 所有业务消息与文件交易必须通过 libp2p 的 Trigger* 入口执行。

type fileGetStep struct {
	Index         int               `json:"index"`
	Name          string            `json:"name"`
	Status        string            `json:"status"`
	StartedAtUnix int64             `json:"started_at_unix"`
	EndedAtUnix   int64             `json:"ended_at_unix,omitempty"`
	Detail        map[string]string `json:"detail,omitempty"`
}

type fileGetJob struct {
	ID              string             `json:"id"`
	SeedHash        string             `json:"seed_hash"`
	ChunkCount      uint32             `json:"chunk_count"`
	GatewayPeerID   string             `json:"gateway_pubkey_hex"`
	Status          string             `json:"status"`
	CancelRequested bool               `json:"cancel_requested,omitempty"`
	StartedAtUnix   int64              `json:"started_at_unix"`
	EndedAtUnix     int64              `json:"ended_at_unix,omitempty"`
	OutputFilePath  string             `json:"output_file_path,omitempty"`
	Error           string             `json:"error,omitempty"`
	Steps           []fileGetStep      `json:"steps"`
	cancel          context.CancelFunc `json:"-"`
}

func newHTTPAPIServer(rt *Runtime, cfg *Config, db *sql.DB, dbActor *sqliteactor.Actor, h host.Host, gateways []peer.AddrInfo, workspace *workspaceManager, trace pproto.TraceSink) *httpAPIServer {
	return &httpAPIServer{
		rt:        rt,
		cfg:       cfg,
		db:        db,
		dbActor:   dbActor,
		store:     newClientDB(db, dbActor),
		h:         h,
		gateways:  gateways,
		workspace: workspace,
		startedAt: time.Now(),
		getJobs:   map[string]*fileGetJob{},
		rpcTrace:  trace,
	}
}

func (s *httpAPIServer) buildMux() (*http.ServeMux, error) {
	mux := http.NewServeMux()
	registerAPI := func(prefix string) {
		mux.HandleFunc(prefix+"/v1/info", s.withAuth(s.handleInfo))
		mux.HandleFunc(prefix+"/v1/balance", s.withAuth(s.handleBalance))
		mux.HandleFunc(prefix+"/v1/wallet/summary", s.withAuth(s.handleWalletSummary))
		mux.HandleFunc(prefix+"/v1/wallet/sync-once", s.withAuth(s.handleWalletSyncOnce))
		mux.HandleFunc(prefix+"/v1/wallet/business/preview", s.withAuth(s.handleWalletBusinessPreview))
		mux.HandleFunc(prefix+"/v1/wallet/business/sign", s.withAuth(s.handleWalletBusinessSign))
		mux.HandleFunc(prefix+"/v1/wallet/ledger", s.withAuth(s.handleWalletLedger))
		mux.HandleFunc(prefix+"/v1/wallet/ledger/detail", s.withAuth(s.handleWalletLedgerDetail))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/create/preview", s.withAuth(s.handleWalletTokenCreatePreview))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/create/sign", s.withAuth(s.handleWalletTokenCreateSign))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/create/submit", s.withAuth(s.handleWalletTokenCreateSubmit))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/create/status", s.withAuth(s.handleWalletTokenCreateStatus))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/create/status/refresh", s.withAuth(s.handleWalletTokenCreateStatusRefresh))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/send/preview", s.withAuth(s.handleWalletTokenSendPreview))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/send/sign", s.withAuth(s.handleWalletTokenSendSign))
		mux.HandleFunc(prefix+"/v1/wallet/tokens/send/submit", s.withAuth(s.handleWalletTokenSendSubmit))
		mux.HandleFunc(prefix+"/v1/wallet/fund-flows", s.withAuth(s.handleWalletFundFlows))
		mux.HandleFunc(prefix+"/v1/wallet/fund-flows/detail", s.withAuth(s.handleWalletFundFlowDetail))
		mux.HandleFunc(prefix+"/v1/direct/quotes", s.withAuth(s.handleDirectQuotes))
		mux.HandleFunc(prefix+"/v1/direct/quotes/detail", s.withAuth(s.handleDirectQuoteDetail))
		// 【第五步：调试/兼容接口】direct_transfer_pools 已降级为协议运行态表
		// 业务状态查询请使用 /v1/downloads/settlement-status
		mux.HandleFunc(prefix+"/v1/direct/transfer-pools", s.withAuth(s.handleDirectTransferPoolsCompat))
		mux.HandleFunc(prefix+"/v1/direct/transfer-pools/detail", s.withAuth(s.handleDirectTransferPoolDetailCompat))
		mux.HandleFunc(prefix+"/v1/transactions", s.withAuth(s.handleTransactions))
		mux.HandleFunc(prefix+"/v1/transactions/detail", s.withAuth(s.handleTransactionDetail))
		mux.HandleFunc(prefix+"/v1/purchases", s.withAuth(s.handlePurchases))
		mux.HandleFunc(prefix+"/v1/purchases/detail", s.withAuth(s.handlePurchaseDetail))
		mux.HandleFunc(prefix+"/v1/purchases/summary", s.withAuth(s.handlePurchaseSummary))
		mux.HandleFunc(prefix+"/v1/downloads/settlement-status", s.withAuth(s.handleDownloadSettlementStatus))
		mux.HandleFunc(prefix+"/v1/gateways/events", s.withAuth(s.handleGatewayEvents))
		mux.HandleFunc(prefix+"/v1/gateways/events/detail", s.withAuth(s.handleGatewayEventDetail))
		mux.HandleFunc(prefix+"/v1/files/get-file", s.withAuth(s.handleGetFileStart))
		mux.HandleFunc(prefix+"/v1/files/get-file/plan", s.withAuth(s.handleGetFilePlan))
		mux.HandleFunc(prefix+"/v1/files/get-file/status", s.withAuth(s.handleGetFileStatus))
		mux.HandleFunc(prefix+"/v1/files/get-file/ensure", s.withAuth(s.handleGetFileEnsure))
		mux.HandleFunc(prefix+"/v1/files/get-file/content", s.withAuth(s.handleGetFileContent))
		mux.HandleFunc(prefix+"/v1/files/get-file/job", s.withAuth(s.handleGetFileJob))
		mux.HandleFunc(prefix+"/v1/files/get-file/jobs", s.withAuth(s.handleGetFileJobs))
		mux.HandleFunc(prefix+"/v1/files/get-file/cancel", s.withAuth(s.handleGetFileCancel))
		mux.HandleFunc(prefix+"/v1/call", s.withAuth(s.handleCall))
		mux.HandleFunc(prefix+"/v1/resolve", s.withAuth(s.handleResolve))
		mux.HandleFunc(prefix+"/v1/resolvers/resolve", s.withAuth(s.handleResolverResolve))
		mux.HandleFunc(prefix+"/v1/domains/register", s.withAuth(s.handleDomainRegister))
		mux.HandleFunc(prefix+"/v1/domains/set-target", s.withAuth(s.handleDomainSetTarget))
		mux.HandleFunc(prefix+"/v1/domains/settlement-status", s.withAuth(s.handleDomainSettlementStatus))
		mux.HandleFunc(prefix+"/v1/inbox/messages", s.withAuth(s.handleInboxMessages))
		mux.HandleFunc(prefix+"/v1/inbox/messages/detail", s.withAuth(s.handleInboxMessageDetail))
		mux.HandleFunc(prefix+"/v1/filehash", s.withAuth(s.handleFileHash))
		mux.HandleFunc(prefix+"/v1/workspace/sync-once", s.withAuth(s.handleWorkspaceSyncOnce))
		mux.HandleFunc(prefix+"/v1/workspace/files", s.withAuth(s.handleWorkspaceFiles))
		mux.HandleFunc(prefix+"/v1/workspace/seeds", s.withAuth(s.handleWorkspaceSeeds))
		mux.HandleFunc(prefix+"/v1/workspace/seeds/price", s.withAuth(s.handleSeedPriceUpdate))
		mux.HandleFunc(prefix+"/v1/live/subscribe-uri", s.withAuth(s.handleLiveSubscribeURI))
		mux.HandleFunc(prefix+"/v1/live/subscribe", s.withAuth(s.handleLiveSubscribe))
		mux.HandleFunc(prefix+"/v1/live/demand/publish", s.withAuth(s.handleLiveDemandPublish))
		mux.HandleFunc(prefix+"/v1/live/quotes", s.withAuth(s.handleLiveQuotes))
		mux.HandleFunc(prefix+"/v1/live/publish/segment", s.withAuth(s.handleLivePublishSegment))
		mux.HandleFunc(prefix+"/v1/live/publish/latest", s.withAuth(s.handleLivePublishLatest))
		mux.HandleFunc(prefix+"/v1/live/latest", s.withAuth(s.handleLiveLatest))
		mux.HandleFunc(prefix+"/v1/live/plan", s.withAuth(s.handleLivePlan))
		mux.HandleFunc(prefix+"/v1/live/follow/start", s.withAuth(s.handleLiveFollowStart))
		mux.HandleFunc(prefix+"/v1/live/follow/stop", s.withAuth(s.handleLiveFollowStop))
		mux.HandleFunc(prefix+"/v1/live/follow/status", s.withAuth(s.handleLiveFollowStatus))
		// 网关管理 API
		mux.HandleFunc(prefix+"/v1/gateways", s.withAuth(s.handleGateways))
		mux.HandleFunc(prefix+"/v1/gateways/master", s.withAuth(s.handleGatewayMaster))
		mux.HandleFunc(prefix+"/v1/gateways/health", s.withAuth(s.handleGatewayHealth))
		mux.HandleFunc(prefix+"/v1/gateways/reachability/announce", s.withAuth(s.handleGatewayReachabilityAnnounce))
		mux.HandleFunc(prefix+"/v1/gateways/reachability/query", s.withAuth(s.handleGatewayReachabilityQuery))
		// 仲裁管理 API
		mux.HandleFunc(prefix+"/v1/arbiters", s.withAuth(s.handleArbiters))
		mux.HandleFunc(prefix+"/v1/arbiters/health", s.withAuth(s.handleArbiterHealth))
		// 文件系统管理 API
		mux.HandleFunc(prefix+"/v1/admin/workspaces", s.withAuth(s.handleAdminWorkspaces))
		mux.HandleFunc(prefix+"/v1/admin/downloads/resume", s.withAuth(s.handleAdminResumeDownload))
		mux.HandleFunc(prefix+"/v1/admin/fs-http/strategy-debug-log", s.withAuth(s.handleAdminStrategyDebugLog))
		mux.HandleFunc(prefix+"/v1/admin/live/streams", s.withAuth(s.handleAdminLiveStreams))
		mux.HandleFunc(prefix+"/v1/admin/live/streams/detail", s.withAuth(s.handleAdminLiveStreamDetail))
		mux.HandleFunc(prefix+"/v1/admin/live/storage/summary", s.withAuth(s.handleAdminLiveStorageSummary))
		mux.HandleFunc(prefix+"/v1/admin/static/tree", s.withAuth(s.handleAdminStaticTree))
		mux.HandleFunc(prefix+"/v1/admin/static/mkdir", s.withAuth(s.handleAdminStaticMkdir))
		mux.HandleFunc(prefix+"/v1/admin/static/upload", s.withAuth(s.handleAdminStaticUpload))
		mux.HandleFunc(prefix+"/v1/admin/static/move", s.withAuth(s.handleAdminStaticMove))
		mux.HandleFunc(prefix+"/v1/admin/static/entry", s.withAuth(s.handleAdminStaticEntry))
		mux.HandleFunc(prefix+"/v1/admin/static/price/set", s.withAuth(s.handleAdminStaticPriceSet))
		mux.HandleFunc(prefix+"/v1/admin/static/price", s.withAuth(s.handleAdminStaticPriceGet))
		mux.HandleFunc(prefix+"/v1/admin/routes/indexes", s.withAuth(s.handleAdminRouteIndexes))
		// 费用池审计旧接口（兼容口，非主入口，仅保留原始事实查询能力）
		// 主排障入口请使用 /v1/admin/feepool/audit/gateway-timeline 和 command-timeline
		mux.HandleFunc(prefix+"/v1/admin/feepool/commands", s.withAuth(s.handleAdminFeePoolCommands))
		mux.HandleFunc(prefix+"/v1/admin/feepool/commands/detail", s.withAuth(s.handleAdminFeePoolCommandDetail))
		mux.HandleFunc(prefix+"/v1/admin/feepool/events", s.withAuth(s.handleAdminFeePoolEvents))
		mux.HandleFunc(prefix+"/v1/admin/feepool/events/detail", s.withAuth(s.handleAdminFeePoolEventDetail))
		mux.HandleFunc(prefix+"/v1/admin/feepool/states", s.withAuth(s.handleAdminFeePoolStates))
		mux.HandleFunc(prefix+"/v1/admin/feepool/states/detail", s.withAuth(s.handleAdminFeePoolStateDetail))
		mux.HandleFunc(prefix+"/v1/admin/feepool/observed-states", s.withAuth(s.handleAdminFeePoolObservedStates))
		mux.HandleFunc(prefix+"/v1/admin/feepool/observed-states/detail", s.withAuth(s.handleAdminFeePoolObservedStateDetail))
		mux.HandleFunc(prefix+"/v1/admin/feepool/effects", s.withAuth(s.handleAdminFeePoolEffects))
		mux.HandleFunc(prefix+"/v1/admin/feepool/effects/detail", s.withAuth(s.handleAdminFeePoolEffectDetail))
		// 费用池审计主入口（时间线统一视角）
		mux.HandleFunc(prefix+"/v1/admin/feepool/audit/gateway-timeline", s.withAuth(s.handleAdminFeePoolGatewayAuditTimeline))
		mux.HandleFunc(prefix+"/v1/admin/feepool/audit/command-timeline", s.withAuth(s.handleAdminFeePoolCommandAuditTimeline))
		mux.HandleFunc(prefix+"/v1/admin/client-kernel/commands", s.withAuth(s.handleAdminClientKernelCommands))
		mux.HandleFunc(prefix+"/v1/admin/client-kernel/commands/detail", s.withAuth(s.handleAdminClientKernelCommandDetail))
		mux.HandleFunc(prefix+"/v1/admin/orchestrator/logs", s.withAuth(s.handleAdminOrchestratorLogs))
		mux.HandleFunc(prefix+"/v1/admin/orchestrator/logs/detail", s.withAuth(s.handleAdminOrchestratorLogDetail))
		mux.HandleFunc(prefix+"/v1/admin/scheduler/tasks", s.withAuth(s.handleAdminSchedulerTasks))
		mux.HandleFunc(prefix+"/v1/admin/scheduler/runs", s.withAuth(s.handleAdminSchedulerRuns))
		mux.HandleFunc(prefix+"/v1/admin/chain/tip/status", s.withAuth(s.handleAdminChainTipStatus))
		mux.HandleFunc(prefix+"/v1/admin/chain/tip/logs", s.withAuth(s.handleAdminChainTipLogs))
		mux.HandleFunc(prefix+"/v1/admin/chain/utxo/status", s.withAuth(s.handleAdminChainUTXOStatus))
		mux.HandleFunc(prefix+"/v1/admin/chain/utxo/logs", s.withAuth(s.handleAdminChainUTXOLogs))
		mux.HandleFunc(prefix+"/v1/admin/wallet/utxos", s.withAuth(s.handleAdminWalletUTXOs))
		mux.HandleFunc(prefix+"/v1/admin/wallet/utxos/detail", s.withAuth(s.handleAdminWalletUTXODetail))
		mux.HandleFunc(prefix+"/v1/admin/wallet/utxo-events", s.withAuth(s.handleAdminWalletUTXOEvents))
		mux.HandleFunc(prefix+"/v1/admin/wallet/utxo-events/detail", s.withAuth(s.handleAdminWalletUTXOEventDetail))
		mux.HandleFunc(prefix+"/v1/admin/finance/businesses", s.withAuth(s.handleAdminFinanceBusinesses))
		mux.HandleFunc(prefix+"/v1/admin/finance/businesses/detail", s.withAuth(s.handleAdminFinanceBusinessDetail))
		mux.HandleFunc(prefix+"/v1/admin/finance/process-events", s.withAuth(s.handleAdminFinanceProcessEvents))
		mux.HandleFunc(prefix+"/v1/admin/finance/process-events/detail", s.withAuth(s.handleAdminFinanceProcessEventDetail))
		mux.HandleFunc(prefix+"/v1/admin/finance/breakdowns", s.withAuth(s.handleAdminFinanceBreakdowns))
		mux.HandleFunc(prefix+"/v1/admin/finance/breakdowns/detail", s.withAuth(s.handleAdminFinanceBreakdownDetail))
		mux.HandleFunc(prefix+"/v1/admin/finance/utxo-links", s.withAuth(s.handleAdminFinanceUTXOLinks))
		mux.HandleFunc(prefix+"/v1/admin/finance/utxo-links/detail", s.withAuth(s.handleAdminFinanceUTXOLinkDetail))
		mux.HandleFunc(prefix+"/v1/admin/config", s.withAuth(s.handleAdminConfig))
		mux.HandleFunc(prefix+"/v1/admin/config/schema", s.withAuth(s.handleAdminConfigSchema))
	}
	registerAPI("/api")
	registerAPI("")

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" || r.URL.Path == "/api/" || strings.HasPrefix(r.URL.Path, "/api/") {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "api not found"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	})
	return mux, nil
}

// Handler 返回 runtime API 的 HTTP handler。
// 设计说明：用于 managed 进程内直调，避免内部反向代理与额外监听端口。
func (s *httpAPIServer) Handler() (http.Handler, error) {
	if s == nil {
		return nil, fmt.Errorf("http api server is nil")
	}
	return s.buildMux()
}

func (s *httpAPIServer) Start() error {
	mux, err := s.buildMux()
	if err != nil {
		return err
	}

	s.srv = &http.Server{
		Addr:              s.cfg.HTTP.ListenAddr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	obs.Important("bitcast-client", "http_api_started", map[string]any{"listen_addr": s.cfg.HTTP.ListenAddr})
	err = s.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *httpAPIServer) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(shutdownCtx)
}

func (s *httpAPIServer) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		next(w, withRequestVisitMeta(r))
	}
}

func (s *httpAPIServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	startedAtUnix := int64(0)
	if s != nil && s.rt != nil && s.rt.StartedAtUnix > 0 {
		startedAtUnix = s.rt.StartedAtUnix
	} else if s != nil && !s.startedAt.IsZero() {
		startedAtUnix = s.startedAt.Unix()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"client_pubkey_hex":   s.cfg.ClientID,
		"transport_peer_id":   s.h.ID().String(),
		"pubkey_hex":          s.cfg.ClientID,
		"seller_enabled":      s.cfg.Seller.Enabled,
		"workspace_dir":       s.cfg.Storage.WorkspaceDir,
		"data_dir":            s.cfg.Storage.DataDir,
		"gateway_count":       len(s.gateways),
		"arbiter_count":       len(s.cfg.Network.Arbiters),
		"rescan_interval_sec": s.cfg.Scan.RescanIntervalSeconds,
		"started_at_unix":     startedAtUnix,
	})
}

func (s *httpAPIServer) handleBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusNotImplemented, map[string]any{
		"error": "balance endpoint removed (legacy off-chain balance pool)",
	})
}

func (s *httpAPIServer) handleWalletSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || (s.dbActor == nil && s.db == nil) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	counters, err := dbLoadWalletSummaryCounters(r.Context(), httpStore(s))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	walletAddr := ""
	onchainBal := int64(0)
	onchainBalErr := ""
	walletUTXOTotalCount := 0
	walletUTXOTotalBalanceSatoshi := int64(0)
	walletPlainBSVUTXOCount := 0
	walletPlainBSVBalanceSatoshi := int64(0)
	walletProtectedUTXOCount := 0
	walletProtectedBalanceSatoshi := int64(0)
	walletUnknownUTXOCount := 0
	walletUnknownBalanceSatoshi := int64(0)
	walletUTXOSyncUpdatedAtUnix := int64(0)
	walletUTXOSyncLastError := ""
	walletUTXOSyncLastTrigger := ""
	walletUTXOSyncLastDurationMS := int64(0)
	walletUTXOSyncLastRoundID := ""
	walletUTXOSyncLastFailedStep := ""
	walletUTXOSyncLastUpstreamPath := ""
	walletUTXOSyncLastHTTPStatus := 0
	runtimeStartedAtUnix := int64(0)
	walletUTXOSyncStateIsStale := false
	walletUTXOSyncStateStaleReason := ""
	walletUTXOSyncSchedulerStatus := ""
	walletUTXOSyncSchedulerLastTrigger := ""
	walletUTXOSyncSchedulerLastStartedAtUnix := int64(0)
	walletUTXOSyncSchedulerLastEndedAtUnix := int64(0)
	walletUTXOSyncSchedulerLastDurationMS := int64(0)
	walletUTXOSyncSchedulerLastError := ""
	walletUTXOSyncSchedulerInFlight := false
	chainMaintQueueLength := 0
	chainMaintInFlight := false
	chainMaintInFlightTaskType := ""
	chainMaintLastTaskStartedAtUnix := int64(0)
	chainMaintLastTaskEndedAtUnix := int64(0)
	chainMaintLastError := ""
	walletChainBaseURL := ""
	walletChainType := ""
	if s != nil && s.rt != nil && s.rt.StartedAtUnix > 0 {
		runtimeStartedAtUnix = s.rt.StartedAtUnix
	} else if s != nil && !s.startedAt.IsZero() {
		runtimeStartedAtUnix = s.startedAt.Unix()
	}
	if s != nil && s.rt != nil {
		walletChainBaseURL = walletChainBaseURLOfRuntime(s.rt)
		walletChainType = walletChainTypeOfRuntime(s.rt)
		addr, bal, err := walletAddressAndOnchainBalance(r.Context(), s.store, s.rt)
		walletAddr = addr
		onchainBal = int64(bal)
		if err != nil {
			onchainBalErr = err.Error()
		}
		if strings.TrimSpace(walletAddr) != "" {
			if syncState, syncErr := dbLoadWalletUTXOSyncState(r.Context(), s.store, walletAddr); syncErr == nil {
				walletUTXOSyncUpdatedAtUnix = syncState.UpdatedAtUnix
				walletUTXOSyncLastError = strings.TrimSpace(syncState.LastError)
				walletUTXOSyncLastTrigger = strings.TrimSpace(syncState.LastTrigger)
				walletUTXOSyncLastDurationMS = syncState.LastDurationMS
				walletUTXOSyncLastRoundID = strings.TrimSpace(syncState.LastSyncRoundID)
				walletUTXOSyncLastFailedStep = strings.TrimSpace(syncState.LastFailedStep)
				walletUTXOSyncLastUpstreamPath = strings.TrimSpace(syncState.LastUpstreamPath)
				walletUTXOSyncLastHTTPStatus = syncState.LastHTTPStatus
				walletUTXOSyncStateIsStale, walletUTXOSyncStateStaleReason = walletUTXOSyncStateStaleness(syncState, runtimeStartedAtUnix)
			}
			if aggregate, aggErr := dbLoadWalletUTXOAggregate(r.Context(), s.store, walletAddr); aggErr == nil {
				walletUTXOTotalCount = aggregate.UTXOCount
				walletUTXOTotalBalanceSatoshi = int64(aggregate.BalanceSatoshi)
				walletPlainBSVUTXOCount = aggregate.PlainBSVUTXOCount
				walletPlainBSVBalanceSatoshi = int64(aggregate.PlainBSVBalanceSatoshi)
				walletProtectedUTXOCount = aggregate.ProtectedUTXOCount
				walletProtectedBalanceSatoshi = int64(aggregate.ProtectedBalanceSatoshi)
				walletUnknownUTXOCount = aggregate.UnknownUTXOCount
				walletUnknownBalanceSatoshi = int64(aggregate.UnknownBalanceSatoshi)
			}
		}
	}
	if s != nil && s.store != nil {
		if schedulerState, err := httpDBValue(r.Context(), s, func(db *sql.DB) (schedulerTaskSnapshot, error) {
			return loadSchedulerTaskSnapshot(db, "chain_utxo_sync")
		}); err == nil {
			walletUTXOSyncSchedulerStatus = strings.TrimSpace(schedulerState.Status)
			walletUTXOSyncSchedulerLastTrigger = strings.TrimSpace(schedulerState.LastTrigger)
			walletUTXOSyncSchedulerLastStartedAtUnix = schedulerState.LastStartedAtUnix
			walletUTXOSyncSchedulerLastEndedAtUnix = schedulerState.LastEndedAtUnix
			walletUTXOSyncSchedulerLastDurationMS = schedulerState.LastDurationMS
			walletUTXOSyncSchedulerLastError = strings.TrimSpace(schedulerState.LastError)
			walletUTXOSyncSchedulerInFlight = schedulerState.InFlight
		}
	}
	if s != nil && s.rt != nil {
		maintStatus := getChainMaintainer(s.rt).snapshotStatus()
		chainMaintQueueLength = maintStatus.QueueLength
		chainMaintInFlight = maintStatus.InFlight
		chainMaintInFlightTaskType = strings.TrimSpace(maintStatus.InFlightTaskType)
		chainMaintLastTaskStartedAtUnix = maintStatus.LastTaskStartedAt
		chainMaintLastTaskEndedAtUnix = maintStatus.LastTaskEndedAt
		chainMaintLastError = strings.TrimSpace(maintStatus.LastError)
	}
	resp := map[string]any{
		"flow_count":                                      counters.FlowCount,
		"tx_event_count":                                  counters.TxCount,
		"purchase_count":                                  counters.PurchaseCount,
		"gateway_event_count":                             counters.GatewayEventCount,
		"total_in_satoshi":                                counters.TotalIn,
		"total_out_satoshi":                               counters.TotalOut,
		"total_used_satoshi":                              counters.TotalUsed,
		"total_returned_satoshi":                          counters.TotalReturned,
		"net_spent_satoshi":                               counters.TotalUsed - counters.TotalReturned,
		"net_amount_delta_satoshi":                        counters.TotalIn - counters.TotalOut,
		"ledger_count":                                    counters.LedgerCount,
		"ledger_total_in_satoshi":                         counters.LedgerIn,
		"ledger_total_out_satoshi":                        counters.LedgerOut,
		"ledger_net_satoshi":                              counters.LedgerIn - counters.LedgerOut,
		"wallet_address":                                  walletAddr,
		"onchain_balance_satoshi":                         onchainBal,
		"wallet_total_unspent_utxo_count":                 walletUTXOTotalCount,
		"wallet_total_unspent_satoshi":                    walletUTXOTotalBalanceSatoshi,
		"wallet_plain_bsv_utxo_count":                     walletPlainBSVUTXOCount,
		"wallet_plain_bsv_balance_satoshi":                walletPlainBSVBalanceSatoshi,
		"wallet_protected_utxo_count":                     walletProtectedUTXOCount,
		"wallet_protected_balance_satoshi":                walletProtectedBalanceSatoshi,
		"wallet_unknown_utxo_count":                       walletUnknownUTXOCount,
		"wallet_unknown_balance_satoshi":                  walletUnknownBalanceSatoshi,
		"balance_source":                                  "wallet_utxo_db",
		"wallet_utxo_sync_updated_at_unix":                walletUTXOSyncUpdatedAtUnix,
		"wallet_utxo_sync_last_error":                     walletUTXOSyncLastError,
		"wallet_utxo_sync_last_trigger":                   walletUTXOSyncLastTrigger,
		"wallet_utxo_sync_last_duration_ms":               walletUTXOSyncLastDurationMS,
		"wallet_utxo_sync_last_round_id":                  walletUTXOSyncLastRoundID,
		"wallet_utxo_sync_last_failed_step":               walletUTXOSyncLastFailedStep,
		"wallet_utxo_sync_last_upstream_path":             walletUTXOSyncLastUpstreamPath,
		"wallet_utxo_sync_last_http_status":               walletUTXOSyncLastHTTPStatus,
		"runtime_started_at_unix":                         runtimeStartedAtUnix,
		"wallet_utxo_sync_state_is_stale":                 walletUTXOSyncStateIsStale,
		"wallet_utxo_sync_state_stale_reason":             walletUTXOSyncStateStaleReason,
		"wallet_utxo_sync_scheduler_status":               walletUTXOSyncSchedulerStatus,
		"wallet_utxo_sync_scheduler_last_trigger":         walletUTXOSyncSchedulerLastTrigger,
		"wallet_utxo_sync_scheduler_last_started_at_unix": walletUTXOSyncSchedulerLastStartedAtUnix,
		"wallet_utxo_sync_scheduler_last_ended_at_unix":   walletUTXOSyncSchedulerLastEndedAtUnix,
		"wallet_utxo_sync_scheduler_last_duration_ms":     walletUTXOSyncSchedulerLastDurationMS,
		"wallet_utxo_sync_scheduler_last_error":           walletUTXOSyncSchedulerLastError,
		"wallet_utxo_sync_scheduler_in_flight":            walletUTXOSyncSchedulerInFlight,
		"chain_maintainer_queue_length":                   chainMaintQueueLength,
		"chain_maintainer_in_flight":                      chainMaintInFlight,
		"chain_maintainer_in_flight_task_type":            chainMaintInFlightTaskType,
		"chain_maintainer_last_task_started_at_unix":      chainMaintLastTaskStartedAtUnix,
		"chain_maintainer_last_task_ended_at_unix":        chainMaintLastTaskEndedAtUnix,
		"chain_maintainer_last_error":                     chainMaintLastError,
		"wallet_chain_base_url":                           walletChainBaseURL,
		"wallet_chain_type":                               walletChainType,
	}
	if onchainBalErr != "" {
		resp["onchain_balance_error"] = onchainBalErr
		obs.Error("bitcast-client", "wallet_summary_degraded", map[string]any{
			"wallet_address":                                  walletAddr,
			"wallet_chain_type":                               walletChainType,
			"wallet_chain_base_url":                           walletChainBaseURL,
			"wallet_utxo_sync_updated_at_unix":                walletUTXOSyncUpdatedAtUnix,
			"wallet_utxo_sync_last_trigger":                   walletUTXOSyncLastTrigger,
			"wallet_utxo_sync_last_duration_ms":               walletUTXOSyncLastDurationMS,
			"wallet_utxo_sync_last_error":                     walletUTXOSyncLastError,
			"wallet_utxo_sync_last_round_id":                  walletUTXOSyncLastRoundID,
			"wallet_utxo_sync_last_failed_step":               walletUTXOSyncLastFailedStep,
			"wallet_utxo_sync_last_upstream_path":             walletUTXOSyncLastUpstreamPath,
			"wallet_utxo_sync_last_http_status":               walletUTXOSyncLastHTTPStatus,
			"runtime_started_at_unix":                         runtimeStartedAtUnix,
			"wallet_utxo_sync_state_is_stale":                 walletUTXOSyncStateIsStale,
			"wallet_utxo_sync_state_stale_reason":             walletUTXOSyncStateStaleReason,
			"wallet_utxo_sync_scheduler_status":               walletUTXOSyncSchedulerStatus,
			"wallet_utxo_sync_scheduler_last_trigger":         walletUTXOSyncSchedulerLastTrigger,
			"wallet_utxo_sync_scheduler_last_started_at_unix": walletUTXOSyncSchedulerLastStartedAtUnix,
			"wallet_utxo_sync_scheduler_last_ended_at_unix":   walletUTXOSyncSchedulerLastEndedAtUnix,
			"wallet_utxo_sync_scheduler_last_duration_ms":     walletUTXOSyncSchedulerLastDurationMS,
			"wallet_utxo_sync_scheduler_last_error":           walletUTXOSyncSchedulerLastError,
			"wallet_utxo_sync_scheduler_in_flight":            walletUTXOSyncSchedulerInFlight,
			"chain_maintainer_queue_length":                   chainMaintQueueLength,
			"chain_maintainer_in_flight":                      chainMaintInFlight,
			"chain_maintainer_in_flight_task_type":            chainMaintInFlightTaskType,
			"chain_maintainer_last_task_started_at_unix":      chainMaintLastTaskStartedAtUnix,
			"chain_maintainer_last_task_ended_at_unix":        chainMaintLastTaskEndedAtUnix,
			"chain_maintainer_last_error":                     chainMaintLastError,
			"onchain_balance_error":                           onchainBalErr,
		})
	} else {
		obs.Info("bitcast-client", "wallet_summary_served", map[string]any{
			"wallet_address":                    walletAddr,
			"wallet_chain_type":                 walletChainType,
			"wallet_chain_base_url":             walletChainBaseURL,
			"wallet_utxo_sync_updated_at_unix":  walletUTXOSyncUpdatedAtUnix,
			"wallet_utxo_sync_last_trigger":     walletUTXOSyncLastTrigger,
			"wallet_utxo_sync_last_duration_ms": walletUTXOSyncLastDurationMS,
			"wallet_utxo_sync_last_round_id":    walletUTXOSyncLastRoundID,
			"wallet_utxo_sync_state_is_stale":   walletUTXOSyncStateIsStale,
			"wallet_utxo_sync_scheduler_status": walletUTXOSyncSchedulerStatus,
			"chain_maintainer_queue_length":     chainMaintQueueLength,
			"chain_maintainer_in_flight":        chainMaintInFlight,
			"onchain_balance_satoshi":           onchainBal,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func walletUTXOSyncStateStaleness(syncState walletUTXOSyncState, runtimeStartedAtUnix int64) (bool, string) {
	if syncState.UpdatedAtUnix <= 0 {
		return true, "sync_state_missing"
	}
	if runtimeStartedAtUnix > 0 && syncState.UpdatedAtUnix < runtimeStartedAtUnix {
		return true, "sync_state_older_than_runtime"
	}
	if strings.TrimSpace(syncState.LastError) != "" && strings.TrimSpace(syncState.LastSyncRoundID) == "" {
		return true, "sync_error_missing_round_id"
	}
	return false, ""
}

func loadSchedulerTaskSnapshot(db *sql.DB, taskName string) (schedulerTaskSnapshot, error) {
	if db == nil {
		return schedulerTaskSnapshot{}, fmt.Errorf("db is nil")
	}
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return schedulerTaskSnapshot{}, fmt.Errorf("task name is empty")
	}
	var out schedulerTaskSnapshot
	var inFlightInt int
	err := db.QueryRow(
		`SELECT task_name,status,last_trigger,last_started_at_unix,last_ended_at_unix,last_duration_ms,last_error,in_flight
		 FROM scheduler_tasks WHERE task_name=?`,
		taskName,
	).Scan(&out.Name, &out.Status, &out.LastTrigger, &out.LastStartedAtUnix, &out.LastEndedAtUnix, &out.LastDurationMS, &out.LastError, &inFlightInt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return schedulerTaskSnapshot{}, nil
		}
		return schedulerTaskSnapshot{}, err
	}
	out.InFlight = inFlightInt != 0
	return out, nil
}

func walletAddressAndOnchainBalance(ctx context.Context, store *clientDB, rt *Runtime) (string, uint64, error) {
	return getWalletBalanceFromDB(ctx, store, rt)
}

func walletChainBaseURLOfRuntime(rt *Runtime) string {
	if rt == nil || rt.WalletChain == nil {
		return ""
	}
	return strings.TrimSpace(rt.WalletChain.BaseURL())
}

func walletChainTypeOfRuntime(rt *Runtime) string {
	if rt == nil || rt.WalletChain == nil {
		return ""
	}
	return fmt.Sprintf("%T", rt.WalletChain)
}

func (s *httpAPIServer) handleWalletLedger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	direction := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("direction")))
	category := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("category")))
	status := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))
	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	data, err := dbListWalletLedger(r.Context(), httpStore(s), walletLedgerFilter{
		Limit:     limit,
		Offset:    offset,
		Direction: direction,
		Category:  category,
		Status:    status,
		TxID:      txid,
		Query:     q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": data.Total, "limit": limit, "offset": offset, "items": data.Items})
}

func (s *httpAPIServer) handleWalletLedgerDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetWalletLedgerItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleWalletFundFlows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	flowID := strings.TrimSpace(r.URL.Query().Get("flow_id"))
	flowType := strings.TrimSpace(r.URL.Query().Get("flow_type"))
	refID := strings.TrimSpace(r.URL.Query().Get("ref_id"))
	stage := strings.TrimSpace(r.URL.Query().Get("stage"))
	direction := strings.TrimSpace(r.URL.Query().Get("direction"))
	purpose := strings.TrimSpace(r.URL.Query().Get("purpose"))
	relatedTxID := strings.TrimSpace(r.URL.Query().Get("related_txid"))
	visitID := normalizeVisitIDHeader(r.URL.Query().Get("visit_id"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListWalletFundFlows(r.Context(), httpStore(s), walletFundFlowFilter{
		Limit:       limit,
		Offset:      offset,
		FlowID:      flowID,
		FlowType:    flowType,
		RefID:       refID,
		Stage:       stage,
		Direction:   direction,
		Purpose:     purpose,
		RelatedTxID: relatedTxID,
		VisitID:     visitID,
		Query:       q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleWalletFundFlowDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetWalletFundFlowItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleDirectQuotes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	demandID := strings.TrimSpace(r.URL.Query().Get("demand_id"))
	sellerPeerID := strings.TrimSpace(r.URL.Query().Get("seller_pubkey_hex"))
	page, err := dbListDemandQuotes(r.Context(), httpStore(s), demandQuoteFilter{
		Limit:        limit,
		Offset:       offset,
		DemandID:     demandID,
		SellerPubHex: sellerPeerID,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleDirectQuoteDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetDemandQuoteItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// handleDirectTransferPoolsCompat 【第五步：调试/兼容接口】
// - 此接口返回的是协议运行态数据，不是业务结算状态
// - 业务状态查询请使用 /v1/downloads/settlement-status (GetFrontOrderSettlementSummary)
// - direct_transfer_pools.status 是协议运行时状态，不代表业务是否完成
func (s *httpAPIServer) handleDirectTransferPoolsCompat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	dealID := strings.TrimSpace(r.URL.Query().Get("deal_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	sellerPeerID := strings.TrimSpace(r.URL.Query().Get("seller_pubkey_hex"))
	buyerPeerID := strings.TrimSpace(r.URL.Query().Get("buyer_pubkey_hex"))
	arbiterPeerID := strings.TrimSpace(r.URL.Query().Get("arbiter_pubkey_hex"))
	page, err := dbListDirectTransferPoolsCompat(r.Context(), httpStore(s), directTransferPoolFilter{
		Limit:         limit,
		Offset:        offset,
		SessionID:     sessionID,
		DealID:        dealID,
		Status:        status,
		SellerPubHex:  sellerPeerID,
		BuyerPubHex:   buyerPeerID,
		ArbiterPubHex: arbiterPeerID,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data_role":    "runtime_debug_only",
		"status_note":  "direct_transfer_pools.status is protocol runtime status, not settlement status",
		"total":        page.Total,
		"limit":        limit,
		"offset":       offset,
		"items":        page.Items,
	})
}

// handleDirectTransferPoolDetailCompat 【第五步：调试/兼容接口】
// - 此接口返回的是协议运行态数据，不是业务结算状态
// - 业务状态查询请使用 /v1/downloads/settlement-status (GetFrontOrderSettlementSummary)
// - direct_transfer_pools.status 是协议运行时状态，不代表业务是否完成
func (s *httpAPIServer) handleDirectTransferPoolDetailCompat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "session_id is required"})
		return
	}
	it, err := dbGetDirectTransferPoolItemCompat(r.Context(), httpStore(s), sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data_role":    "runtime_debug_only",
		"status_note":  "direct_transfer_pools.status is protocol runtime status, not settlement status",
		"item":         it,
	})
}

func (s *httpAPIServer) handleTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
	direction := strings.TrimSpace(r.URL.Query().Get("direction"))
	purpose := strings.TrimSpace(r.URL.Query().Get("purpose"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListTxHistory(r.Context(), httpStore(s), txHistoryFilter{
		Limit:     limit,
		Offset:    offset,
		EventType: eventType,
		Direction: direction,
		Purpose:   purpose,
		Query:     q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleTransactionDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetTxHistoryItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handlePurchases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	demandID := strings.TrimSpace(r.URL.Query().Get("demand_id"))
	sellerPubHex := strings.TrimSpace(r.URL.Query().Get("seller_pubkey_hex"))
	arbiterPubHex := strings.TrimSpace(r.URL.Query().Get("arbiter_pubkey_hex"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	page, err := dbListPurchases(r.Context(), httpStore(s), purchaseFilter{
		Limit:         limit,
		Offset:        offset,
		DemandID:      demandID,
		SellerPubHex:  sellerPubHex,
		ArbiterPubHex: arbiterPubHex,
		Status:        status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handlePurchaseDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetPurchaseItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handlePurchaseSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	demandID := strings.TrimSpace(r.URL.Query().Get("demand_id"))
	if demandID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "demand_id is required"})
		return
	}
	summary, err := dbSummarizeDemandPurchases(r.Context(), httpStore(s), demandID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

// handleDownloadSettlementStatus 下载结算状态查询（第四步新主线）
// 设计：统一走 GetFrontOrderSettlementSummary 聚合读模型，支持一 front_order 多 business 多 seller 汇总
// 不再直接查 direct_transfer_pools / purchases 来判断支付状态
func (s *httpAPIServer) handleDownloadSettlementStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	frontOrderID := strings.TrimSpace(r.URL.Query().Get("front_order_id"))
	if frontOrderID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "front_order_id is required"})
		return
	}

	summary, err := GetFrontOrderSettlementSummary(r.Context(), httpStore(s), frontOrderID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (s *httpAPIServer) handleGatewayEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	action := strings.TrimSpace(r.URL.Query().Get("action"))
	page, err := dbListGatewayEvents(r.Context(), httpStore(s), gatewayEventFilter{
		Limit:         limit,
		Offset:        offset,
		GatewayPeerID: gatewayPeerID,
		CommandID:     commandID,
		Action:        action,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleGatewayEventDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetGatewayEventItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 兼容口：原始命令查询，非主入口。排障请使用 handleAdminFeePoolGatewayAuditTimeline。
func (s *httpAPIServer) handleAdminFeePoolCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandType := strings.TrimSpace(r.URL.Query().Get("command_type"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	triggerKey := strings.TrimSpace(r.URL.Query().Get("trigger_key"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListCommandTimeline(r.Context(), httpStore(s), commandTimelineFilter{
		Limit:         limit,
		Offset:        offset,
		CommandType:   commandType,
		GatewayPeerID: gatewayPeerID,
		Status:        status,
		CommandID:     commandID,
		TriggerKey:    triggerKey,
		Query:         q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 兼容口：单条命令详情，非主入口。
func (s *httpAPIServer) handleAdminFeePoolCommandDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetCommandTimelineItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 兼容口：原始领域事件查询，非主入口。排障请使用 handleAdminFeePoolGatewayAuditTimeline。
func (s *httpAPIServer) handleAdminFeePoolEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	eventName := strings.TrimSpace(r.URL.Query().Get("event_name"))
	page, err := dbListDomainEvents(r.Context(), httpStore(s), domainEventFilter{
		Limit:         limit,
		Offset:        offset,
		CommandID:     commandID,
		GatewayPeerID: gatewayPeerID,
		EventName:     eventName,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 兼容口：单条事件详情，非主入口。
func (s *httpAPIServer) handleAdminFeePoolEventDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetDomainEventItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 兼容口：原始观察事实查询，非主入口。排障请使用 handleAdminFeePoolGatewayAuditTimeline。
func (s *httpAPIServer) handleAdminFeePoolObservedStates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	sourceRef := strings.TrimSpace(r.URL.Query().Get("source_ref"))
	eventName := strings.TrimSpace(r.URL.Query().Get("event_name"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	page, err := dbListObservedGatewayTimeline(r.Context(), httpStore(s), observedGatewayTimelineFilter{
		Limit:         limit,
		Offset:        offset,
		GatewayPeerID: gatewayPeerID,
		SourceRef:     sourceRef,
		EventName:     eventName,
		State:         state,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 兼容口：单条观察事实详情，非主入口。
func (s *httpAPIServer) handleAdminFeePoolObservedStateDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetObservedGatewayTimelineItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 兼容口：原始状态快照查询，非主入口。排障请使用 handleAdminFeePoolGatewayAuditTimeline。
func (s *httpAPIServer) handleAdminFeePoolStates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	page, err := dbListStateSnapshots(r.Context(), httpStore(s), stateSnapshotFilter{
		Limit:         limit,
		Offset:        offset,
		CommandID:     commandID,
		GatewayPeerID: gatewayPeerID,
		State:         state,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 兼容口：单条状态快照详情，非主入口。
func (s *httpAPIServer) handleAdminFeePoolStateDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetStateSnapshotItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 兼容口：原始效果日志查询，非主入口。排障请使用 handleAdminFeePoolGatewayAuditTimeline。
func (s *httpAPIServer) handleAdminFeePoolEffects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	effectType := strings.TrimSpace(r.URL.Query().Get("effect_type"))
	stage := strings.TrimSpace(r.URL.Query().Get("stage"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	page, err := dbListEffectLogs(r.Context(), httpStore(s), effectLogFilter{
		Limit:         limit,
		Offset:        offset,
		CommandID:     commandID,
		GatewayPeerID: gatewayPeerID,
		EffectType:    effectType,
		Stage:         stage,
		Status:        status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 兼容口：单条效果日志详情，非主入口。
func (s *httpAPIServer) handleAdminFeePoolEffectDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetEffectLogItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// 主入口：网关审计时间线。排障/复盘/状态演进观察请优先使用此接口。
func (s *httpAPIServer) handleAdminFeePoolGatewayAuditTimeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 100, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	gatewayPubkeyHex := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	page, err := dbListGatewayAuditTimeline(r.Context(), httpStore(s), AuditTimelineFilter{
		Limit:            limit,
		Offset:           offset,
		GatewayPubkeyHex: gatewayPubkeyHex,
	})
	if err != nil {
		if strings.Contains(err.Error(), "required") {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

// 主入口：命令审计时间线。命令执行链追踪请优先使用此接口。
func (s *httpAPIServer) handleAdminFeePoolCommandAuditTimeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 100, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	page, err := dbListCommandAuditTimeline(r.Context(), httpStore(s), AuditTimelineFilter{
		Limit:     limit,
		Offset:    offset,
		CommandID: commandID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		if strings.Contains(err.Error(), "required") {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

var adminClientKernelCommandTypes = []string{
	clientKernelCommandFeePoolEnsureActive,
	clientKernelCommandFeePoolCycleTick,
	clientKernelCommandFeePoolMaintain,
	clientKernelCommandLivePlanPurchase,
	clientKernelCommandWorkspaceSync,
	clientKernelCommandDirectDownloadCore,
	clientKernelCommandTransferByStrategy,
}

func isAdminClientKernelCommandType(v string) bool {
	needle := strings.TrimSpace(v)
	if needle == "" {
		return false
	}
	for _, t := range adminClientKernelCommandTypes {
		if strings.EqualFold(strings.TrimSpace(t), needle) {
			return true
		}
	}
	return false
}

func (s *httpAPIServer) handleAdminClientKernelCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	commandType := strings.TrimSpace(r.URL.Query().Get("command_type"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	commandID := strings.TrimSpace(r.URL.Query().Get("command_id"))
	triggerKey := strings.TrimSpace(r.URL.Query().Get("trigger_key"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if commandType != "" && !isAdminClientKernelCommandType(commandType) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unsupported client kernel command_type"})
		return
	}
	page, err := dbListCommandJournal(r.Context(), httpStore(s), commandJournalFilter{
		Limit:         limit,
		Offset:        offset,
		CommandTypes:  adminClientKernelCommandTypes,
		CommandType:   commandType,
		GatewayPeerID: gatewayPeerID,
		Status:        status,
		CommandID:     commandID,
		TriggerKey:    triggerKey,
		Query:         q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminClientKernelCommandDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetCommandJournalItem(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !isAdminClientKernelCommandType(it.CommandType) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleAdminOrchestratorLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
	signalType := strings.TrimSpace(r.URL.Query().Get("signal_type"))
	source := strings.TrimSpace(r.URL.Query().Get("source"))
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_pubkey_hex"))
	idempotencyKey := strings.TrimSpace(r.URL.Query().Get("idempotency_key"))
	taskStatus := strings.TrimSpace(r.URL.Query().Get("task_status"))
	page, err := dbListOrchestratorLogs(r.Context(), httpStore(s), orchestratorLogFilter{
		Limit:          limit,
		Offset:         offset,
		EventType:      eventType,
		SignalType:     signalType,
		Source:         source,
		GatewayPeerID:  gatewayPeerID,
		IdempotencyKey: idempotencyKey,
		TaskStatus:     taskStatus,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminOrchestratorLogDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	eventID := strings.TrimSpace(r.URL.Query().Get("event_id"))
	if eventID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "event_id is required"})
		return
	}
	detail, err := dbGetOrchestratorLogDetail(r.Context(), httpStore(s), eventID)
	if err != nil {
		if strings.Contains(err.Error(), "invalid event_id") {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid event_id"})
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "event not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *httpAPIServer) handleAdminSchedulerTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	query := r.URL.Query()
	modeFilter := strings.ToLower(strings.TrimSpace(query.Get("mode")))
	ownerFilter := strings.TrimSpace(query.Get("owner"))
	namePrefix := strings.TrimSpace(query.Get("name_prefix"))
	statusFilter := strings.ToLower(strings.TrimSpace(query.Get("status")))
	inFlightFilterRaw := strings.TrimSpace(query.Get("in_flight"))
	hasErrorFilterRaw := strings.TrimSpace(query.Get("has_error"))
	orderBy := strings.ToLower(strings.TrimSpace(query.Get("order_by")))
	parseOptionalBool := func(raw string) (bool, bool, error) {
		raw = strings.ToLower(strings.TrimSpace(raw))
		if raw == "" {
			return false, false, nil
		}
		switch raw {
		case "1", "true", "yes":
			return true, true, nil
		case "0", "false", "no":
			return false, true, nil
		default:
			return false, false, fmt.Errorf("invalid bool value: %s", raw)
		}
	}
	inFlightFilter, inFlightSet, err := parseOptionalBool(inFlightFilterRaw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid in_flight"})
		return
	}
	hasErrorFilter, hasErrorSet, err := parseOptionalBool(hasErrorFilterRaw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid has_error"})
		return
	}
	if modeFilter != "" && modeFilter != "static" && modeFilter != "dynamic" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid mode"})
		return
	}
	if statusFilter != "" && statusFilter != "active" && statusFilter != "stopped" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid status"})
		return
	}
	switch orderBy {
	case "", "default":
	case "name_asc":
	case "updated_desc":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid order_by"})
		return
	}
	page, err := dbListSchedulerTasks(r.Context(), httpStore(s), schedulerTaskFilter{
		Mode:        modeFilter,
		Owner:       ownerFilter,
		NamePrefix:  namePrefix,
		Status:      statusFilter,
		InFlightSet: inFlightSet,
		InFlight:    inFlightFilter,
		HasErrorSet: hasErrorSet,
		HasError:    hasErrorFilter,
		OrderBy:     orderBy,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"now_unix": time.Now().Unix(),
		"summary": map[string]any{
			"task_count":           len(page.Items),
			"in_flight_count":      page.InFlightCount,
			"failure_count_total":  page.FailureTotal,
			"enabled_task_count":   page.EnabledTaskCount,
			"filtered_task_count":  len(page.Items),
			"active_task_count":    page.ActiveCount,
			"stopped_task_count":   page.StoppedCount,
			"scheduler_available":  true,
			"filter_applied_count": page.FilterCount,
		},
		"items": page.Items,
	})
}

func (s *httpAPIServer) handleAdminSchedulerRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 20, 1, 200)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	taskName := strings.TrimSpace(r.URL.Query().Get("task_name"))
	owner := strings.TrimSpace(r.URL.Query().Get("owner"))
	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	if mode != "" && mode != "static" && mode != "dynamic" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid mode"})
		return
	}
	if status != "" && status != "success" && status != "failed" && status != "canceled" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid status"})
		return
	}
	page, err := dbListSchedulerRuns(r.Context(), httpStore(s), schedulerRunFilter{
		Limit:    limit,
		Offset:   offset,
		TaskName: taskName,
		Owner:    owner,
		Mode:     mode,
		Status:   status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": page.Total,
		"items": page.Items,
	})
}

func (s *httpAPIServer) handleAdminChainTipStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	state, err := dbLoadChainTipState(r.Context(), s.store)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *httpAPIServer) handleAdminChainUTXOStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	addr, err := clientWalletAddress(s.rt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	state, err := dbLoadWalletUTXOSyncState(r.Context(), s.store, addr)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	stats, err := dbLoadWalletUTXOAggregate(r.Context(), s.store, addr)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if strings.TrimSpace(state.Address) == "" {
		state.Address = addr
		state.WalletID = walletIDByAddress(addr)
	}
	state.UTXOCount = stats.UTXOCount
	state.BalanceSatoshi = stats.BalanceSatoshi
	state.PlainBSVUTXOCount = stats.PlainBSVUTXOCount
	state.PlainBSVBalanceSatoshi = stats.PlainBSVBalanceSatoshi
	state.ProtectedUTXOCount = stats.ProtectedUTXOCount
	state.ProtectedBalanceSatoshi = stats.ProtectedBalanceSatoshi
	state.UnknownUTXOCount = stats.UnknownUTXOCount
	state.UnknownBalanceSatoshi = stats.UnknownBalanceSatoshi
	writeJSON(w, http.StatusOK, state)
}

func (s *httpAPIServer) handleAdminChainTipLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	page, err := dbListChainTipWorkerLogs(r.Context(), httpStore(s), chainWorkerLogFilter{
		Limit:  limit,
		Offset: offset,
		Status: status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminChainUTXOLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	page, err := dbListChainUTXOWorkerLogs(r.Context(), httpStore(s), chainWorkerLogFilter{
		Limit:  limit,
		Offset: offset,
		Status: status,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminWalletUTXOs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	walletID := strings.TrimSpace(r.URL.Query().Get("wallet_id"))
	address := strings.TrimSpace(r.URL.Query().Get("address"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListWalletUTXOs(r.Context(), httpStore(s), walletUTXOFilter{
		Limit:    limit,
		Offset:   offset,
		WalletID: walletID,
		Address:  address,
		State:    state,
		TxID:     txid,
		Query:    q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminWalletUTXODetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	if utxoID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "utxo_id is required"})
		return
	}
	it, err := dbGetWalletUTXO(r.Context(), httpStore(s), utxoID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleAdminWalletUTXOEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
	refTxID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("ref_txid")))
	refBusinessID := strings.TrimSpace(r.URL.Query().Get("ref_business_id"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListWalletUTXOEvents(r.Context(), httpStore(s), walletUTXOEventFilter{
		Limit:         limit,
		Offset:        offset,
		UTXOID:        utxoID,
		EventType:     eventType,
		RefTxID:       refTxID,
		RefBusinessID: refBusinessID,
		Query:         q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminWalletUTXOEventDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetWalletUTXOEvent(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// handleAdminFinanceBusinesses 查询财务业务记录
// 第六次迭代起不再支持旧口径参数（scene_type/scene_subtype/ref_id）
// 只支持主口径参数：source_type/source_id/accounting_scene/accounting_subtype/pool_allocation_id
func (s *httpAPIServer) handleAdminFinanceBusinesses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	businessID := strings.TrimSpace(r.URL.Query().Get("business_id"))
	poolAllocationID := strings.TrimSpace(r.URL.Query().Get("pool_allocation_id"))

	// 主口径参数
	sourceType := strings.TrimSpace(r.URL.Query().Get("source_type"))
	sourceID := strings.TrimSpace(r.URL.Query().Get("source_id"))
	accountingScene := strings.TrimSpace(r.URL.Query().Get("accounting_scene"))
	accountingSubType := strings.TrimSpace(r.URL.Query().Get("accounting_subtype"))

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	fromPartyID := strings.TrimSpace(r.URL.Query().Get("from_party_id"))
	toPartyID := strings.TrimSpace(r.URL.Query().Get("to_party_id"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	// pool_allocation_id 只做主键换算，不允许直接把 allocation_id 塞进 source_id
	if poolAllocationID != "" && sourceType == "" && sourceID == "" {
		poolAllocID, err := dbGetPoolAllocationIDByAllocationID(r.Context(), httpStore(s), poolAllocationID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusOK, map[string]any{"total": 0, "limit": limit, "offset": offset, "items": []financeBusinessItem{}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		sourceType = "pool_allocation"
		sourceID = fmt.Sprintf("%d", poolAllocID)
	}

	page, err := dbListFinanceBusinesses(r.Context(), httpStore(s), financeBusinessFilter{
		Limit:             limit,
		Offset:            offset,
		BusinessID:        businessID,
		SourceType:        sourceType,
		SourceID:          sourceID,
		AccountingScene:   accountingScene,
		AccountingSubtype: accountingSubType,
		Status:            status,
		FromPartyID:       fromPartyID,
		ToPartyID:         toPartyID,
		Query:             q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminFinanceBusinessDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	businessID := strings.TrimSpace(r.URL.Query().Get("business_id"))
	if businessID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "business_id is required"})
		return
	}
	it, err := dbGetFinanceBusiness(r.Context(), httpStore(s), businessID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// handleAdminFinanceProcessEvents 查询财务流程事件记录
// 第六次迭代起不再支持旧口径参数（scene_type/scene_subtype/ref_id）
// 只支持主口径参数：source_type/source_id/accounting_scene/accounting_subtype/pool_allocation_id
func (s *httpAPIServer) handleAdminFinanceProcessEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	processID := strings.TrimSpace(r.URL.Query().Get("process_id"))
	poolAllocationID := strings.TrimSpace(r.URL.Query().Get("pool_allocation_id"))

	// 主口径参数
	sourceType := strings.TrimSpace(r.URL.Query().Get("source_type"))
	sourceID := strings.TrimSpace(r.URL.Query().Get("source_id"))
	accountingScene := strings.TrimSpace(r.URL.Query().Get("accounting_scene"))
	accountingSubType := strings.TrimSpace(r.URL.Query().Get("accounting_subtype"))
	eventType := strings.TrimSpace(r.URL.Query().Get("event_type"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	// pool_allocation_id 只做主键换算，不允许直接把 allocation_id 塞进 source_id
	if poolAllocationID != "" && sourceType == "" && sourceID == "" {
		poolAllocID, err := dbGetPoolAllocationIDByAllocationID(r.Context(), httpStore(s), poolAllocationID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusOK, map[string]any{"total": 0, "limit": limit, "offset": offset, "items": []financeProcessEventItem{}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		sourceType = "pool_allocation"
		sourceID = fmt.Sprintf("%d", poolAllocID)
	}

	page, err := dbListFinanceProcessEvents(r.Context(), httpStore(s), financeProcessEventFilter{
		Limit:             limit,
		Offset:            offset,
		ProcessID:         processID,
		SourceType:        sourceType,
		SourceID:          sourceID,
		AccountingScene:   accountingScene,
		AccountingSubtype: accountingSubType,
		EventType:         eventType,
		Status:            status,
		Query:             q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminFinanceProcessEventDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetFinanceProcessEvent(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleAdminFinanceBreakdowns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	businessID := strings.TrimSpace(r.URL.Query().Get("business_id"))
	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	page, err := dbListFinanceBreakdowns(r.Context(), httpStore(s), financeBreakdownFilter{
		Limit:      limit,
		Offset:     offset,
		BusinessID: businessID,
		TxID:       txid,
		Query:      q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminFinanceBreakdownDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetFinanceBreakdown(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleAdminFinanceUTXOLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	businessID := strings.TrimSpace(r.URL.Query().Get("business_id"))
	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	utxoID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("utxo_id")))
	txRole := strings.TrimSpace(r.URL.Query().Get("tx_role"))
	ioSide := strings.TrimSpace(r.URL.Query().Get("io_side"))
	utxoRole := strings.TrimSpace(r.URL.Query().Get("utxo_role"))
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	page, err := dbListFinanceUTXOLinks(r.Context(), httpStore(s), financeUTXOLinkFilter{
		Limit:      limit,
		Offset:     offset,
		BusinessID: businessID,
		TxID:       txid,
		UTXOID:     utxoID,
		TxRole:     txRole,
		IOSide:     ioSide,
		UTXORole:   utxoRole,
		Query:      q,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": page.Total, "limit": limit, "offset": offset, "items": page.Items})
}

func (s *httpAPIServer) handleAdminFinanceUTXOLinkDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	it, err := dbGetFinanceUTXOLink(r.Context(), httpStore(s), int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleGetFileStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	type reqBody struct {
		SeedHash      string `json:"seed_hash"`
		ChunkCount    uint32 `json:"chunk_count"`
		GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
	}
	var req reqBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	req.SeedHash = strings.ToLower(strings.TrimSpace(req.SeedHash))
	req.GatewayPeerID = strings.TrimSpace(req.GatewayPeerID)
	if req.SeedHash == "" || req.ChunkCount == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "seed_hash and chunk_count are required"})
		return
	}
	if req.ChunkCount > 10000 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "chunk_count too large"})
		return
	}
	job := &fileGetJob{
		ID:            newJobID(),
		SeedHash:      req.SeedHash,
		ChunkCount:    req.ChunkCount,
		Status:        "running",
		StartedAtUnix: time.Now().Unix(),
		Steps:         make([]fileGetStep, 0, 16),
	}
	jobCtx, cancel := context.WithCancel(context.Background())
	job.cancel = cancel
	s.jobsMu.Lock()
	s.getJobs[job.ID] = job
	s.jobsMu.Unlock()

	go s.runGetFileJob(jobCtx, job.ID, req.SeedHash, req.ChunkCount, req.GatewayPeerID)
	writeJSON(w, http.StatusOK, map[string]any{"job_id": job.ID, "status": job.Status})
}

func (s *httpAPIServer) handleGetFileJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	s.jobsMu.RLock()
	items := make([]*fileGetJob, 0, len(s.getJobs))
	for _, j := range s.getJobs {
		c := *j
		c.Steps = append([]fileGetStep(nil), j.Steps...)
		items = append(items, &c)
	}
	s.jobsMu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (s *httpAPIServer) handleGetFileJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	s.jobsMu.RLock()
	j, ok := s.getJobs[id]
	if !ok {
		s.jobsMu.RUnlock()
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "job not found"})
		return
	}
	c := *j
	c.Steps = append([]fileGetStep(nil), j.Steps...)
	s.jobsMu.RUnlock()
	writeJSON(w, http.StatusOK, c)
}

func (s *httpAPIServer) handleGetFileCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	s.jobsMu.Lock()
	j, ok := s.getJobs[id]
	if !ok {
		s.jobsMu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "job not found"})
		return
	}
	if j.Status != "running" {
		status := j.Status
		s.jobsMu.Unlock()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "job is not running", "status": status})
		return
	}
	j.CancelRequested = true
	cancel := j.cancel
	s.jobsMu.Unlock()
	if cancel != nil {
		cancel()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id, "status": "canceling"})
}

func (s *httpAPIServer) runGetFileJob(parentCtx context.Context, jobID, seedHash string, chunkCount uint32, gwOverride string) {
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Minute)
	defer cancel()

	markCanceled := func() {
		s.jobsMu.Lock()
		if j, ok := s.getJobs[jobID]; ok {
			j.Status = "canceled"
			j.Error = "job canceled"
			j.EndedAtUnix = time.Now().Unix()
			j.cancel = nil
		}
		s.jobsMu.Unlock()
	}
	fail := func(msg string) {
		s.jobsMu.Lock()
		if j, ok := s.getJobs[jobID]; ok {
			j.Status = "failed"
			j.Error = msg
			j.EndedAtUnix = time.Now().Unix()
			j.cancel = nil
		}
		s.jobsMu.Unlock()
	}
	addStep := func(name string, detail map[string]string) int {
		s.jobsMu.Lock()
		defer s.jobsMu.Unlock()
		j := s.getJobs[jobID]
		idx := len(j.Steps)
		j.Steps = append(j.Steps, fileGetStep{
			Index:         idx,
			Name:          name,
			Status:        "running",
			StartedAtUnix: time.Now().Unix(),
			Detail:        detail,
		})
		return idx
	}
	endStep := func(idx int, status string, detail map[string]string) {
		s.jobsMu.Lock()
		defer s.jobsMu.Unlock()
		j := s.getJobs[jobID]
		if idx < 0 || idx >= len(j.Steps) {
			return
		}
		j.Steps[idx].Status = status
		j.Steps[idx].EndedAtUnix = time.Now().Unix()
		if len(detail) > 0 {
			if j.Steps[idx].Detail == nil {
				j.Steps[idx].Detail = map[string]string{}
			}
			for k, v := range detail {
				j.Steps[idx].Detail[k] = v
			}
		}
	}
	gw, err := pickGatewayForBusiness(s.rt, gwOverride)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			markCanceled()
			return
		}
		fail(err.Error())
		return
	}
	s.jobsMu.Lock()
	s.getJobs[jobID].GatewayPeerID = gw.ID.String()
	s.jobsMu.Unlock()
	stepIndex := map[string]int{}
	download, err := runDirectDownloadCore(ctx, s.rt, directDownloadCoreParams{
		SeedHash:           seedHash,
		DemandChunkCount:   chunkCount,
		TransferChunkCount: chunkCount,
		GatewayPeerID:      gw.ID.String(),
		QuoteMaxRetry:      10,
		QuoteInterval:      2 * time.Second,
		MaxChunkPrice:      s.cfg.FSHTTP.MaxChunkPriceSatPer64K,
		Strategy:           TransferStrategySmart,
	}, directDownloadCoreHooks{
		OnStepStart: func(name string, detail map[string]string) {
			stepIndex[name] = addStep(name, detail)
		},
		OnStepDone: func(name string, detail map[string]string) {
			if idx, ok := stepIndex[name]; ok {
				endStep(idx, "done", detail)
			}
		},
		OnStepFail: func(name string, _ error, detail map[string]string) {
			if idx, ok := stepIndex[name]; ok {
				endStep(idx, "failed", detail)
			}
		},
	})
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			markCanceled()
			return
		}
		fail(err.Error())
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		markCanceled()
		return
	}

	step := addStep("write_file", map[string]string{
		"selected_file_name": download.FileName,
		"bytes":              fmt.Sprintf("%d", len(download.Transfer.Data)),
	})
	if s.workspace == nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			markCanceled()
			return
		}
		endStep(step, "failed", map[string]string{"error": "workspace manager not initialized"})
		fail("workspace manager not initialized")
		return
	}
	outPath, err := s.workspace.SelectOutputPath(download.FileName, uint64(len(download.Transfer.Data)))
	if err != nil {
		endStep(step, "failed", map[string]string{"error": err.Error()})
		fail("select workspace output failed: " + err.Error())
		return
	}
	if err := os.WriteFile(outPath, download.Transfer.Data, 0o644); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			markCanceled()
			return
		}
		endStep(step, "failed", map[string]string{"error": err.Error()})
		fail("write output failed: " + err.Error())
		return
	}
	if _, err := s.workspace.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              outPath,
		Seed:                  download.Transfer.Seed,
		AvailableChunkIndexes: contiguousChunkIndexes(download.Transfer.ChunkCount),
		RecommendedFileName:   download.FileName,
		MIMEHint:              pickRecommendedMIMEHint(download.Quotes),
	}); err != nil {
		endStep(step, "failed", map[string]string{"error": err.Error()})
		fail("workspace register failed: " + err.Error())
		return
	}
	endStep(step, "done", map[string]string{"output_file_path": outPath, "bytes": fmt.Sprintf("%d", len(download.Transfer.Data))})

	s.jobsMu.Lock()
	if j, ok := s.getJobs[jobID]; ok {
		j.Status = "done"
		j.OutputFilePath = outPath
		j.EndedAtUnix = time.Now().Unix()
		j.cancel = nil
	}
	s.jobsMu.Unlock()
}

func newJobID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("gf_%d_%s", time.Now().Unix(), hex.EncodeToString(b))
}

func (s *httpAPIServer) handleFileHash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	path := strings.TrimSpace(r.URL.Query().Get("path"))
	if path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "path is required"})
		return
	}
	resolved, err := resolveWorkspacePath(s.cfg.Storage.WorkspaceDir, path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	st, err := os.Stat(resolved)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	if st.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "path must be a regular file"})
		return
	}
	_, seedHash, chunkCount, err := buildSeedV1(resolved)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"path":            resolved,
		"seed_hash":       seedHash,
		"chunk_count":     chunkCount,
		"file_size":       st.Size(),
		"seed_block_size": seedBlockSize,
	})
}

func (s *httpAPIServer) handleWorkspaceSyncOnce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	seeds, err := s.workspace.SyncOnce(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"seed_count":      len(seeds),
		"synced_at_unix":  time.Now().Unix(),
		"workspace_dir":   s.cfg.Storage.WorkspaceDir,
		"workspace_files": len(seeds),
	})
}

func (s *httpAPIServer) handleWorkspaceFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 100, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	pathLike := strings.TrimSpace(r.URL.Query().Get("path_like"))
	page, err := dbListWorkspaceFiles(r.Context(), httpStore(s), limit, offset, pathLike)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleWorkspaceSeeds(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 100, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	seedHash := strings.TrimSpace(r.URL.Query().Get("seed_hash"))
	seedHashLike := strings.TrimSpace(r.URL.Query().Get("seed_hash_like"))
	page, err := dbListWorkspaceSeeds(r.Context(), httpStore(s), limit, offset, seedHash, seedHashLike)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  page.Total,
		"limit":  limit,
		"offset": offset,
		"items":  page.Items,
	})
}

func (s *httpAPIServer) handleSeedPriceUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	type reqBody struct {
		SeedHash            string `json:"seed_hash"`
		FloorPriceSatPer64K uint64 `json:"floor_price_sat_per_64k"`
		ResaleDiscountBPS   uint64 `json:"resale_discount_bps"`
	}
	var req reqBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.SeedHash) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "seed_hash is required"})
		return
	}
	if req.FloorPriceSatPer64K == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "floor_price_sat_per_64k must be > 0"})
		return
	}
	if req.ResaleDiscountBPS == 0 {
		req.ResaleDiscountBPS = s.cfg.Seller.Pricing.ResaleDiscountBPS
	}
	if req.ResaleDiscountBPS > 10000 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "resale_discount_bps must be <= 10000"})
		return
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	store := httpStore(s)
	if _, err := dbGetSeedFilePathByHash(r.Context(), store, seedHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "seed not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	unit, total, err := func() (uint64, uint64, error) {
		if err := dbUpsertSeedPricingPolicy(store.db, seedHash, req.FloorPriceSatPer64K, req.ResaleDiscountBPS, "user", time.Now().Unix()); err != nil {
			return 0, 0, err
		}
		var chunkCount uint32
		if err := store.db.QueryRow(`SELECT chunk_count FROM seeds WHERE seed_hash=?`, seedHash).Scan(&chunkCount); err != nil {
			return 0, 0, err
		}
		return req.FloorPriceSatPer64K, req.FloorPriceSatPer64K * uint64(chunkCount), nil
	}()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"seed_hash":          seedHash,
		"chunk_price_sat":    unit,
		"seed_price_satoshi": total,
		"pricing_source":     "user",
	})
}

func (s *httpAPIServer) handleLiveSubscribeURI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil || s.rt.Host == nil || s.cfg == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream_id")))
	if !isSeedHashHex(streamID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
		return
	}
	pubHex, err := localPubKeyHex(s.rt.Host)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	uri, err := BuildLiveSubscribeURI(pubHex, streamID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_id":                  streamID,
		"publisher_pubkey":           pubHex,
		"subscribe_uri":              uri,
		"broadcast_window":           s.cfg.Live.Publish.BroadcastWindow,
		"broadcast_interval_seconds": s.cfg.Live.Publish.BroadcastIntervalSec,
	})
}

func (s *httpAPIServer) handleLiveSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamURI string `json:"stream_uri"`
		Window    uint32 `json:"window"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.StreamURI) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "stream_uri is required"})
		return
	}
	res, err := TriggerLiveSubscribe(r.Context(), s.rt, req.StreamURI, req.Window)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_id":        res.StreamID,
		"publisher_pubkey": res.PublisherPubKey,
		"recent_segments":  res.RecentSegments,
		"recent_count":     len(res.RecentSegments),
	})
}

func (s *httpAPIServer) handleLiveDemandPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamID         string `json:"stream_id"`
		HaveSegmentIndex int64  `json:"have_segment_index"`
		Window           uint32 `json:"window"`
		GatewayPeerID    string `json:"gateway_pubkey_hex,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if req.Window == 0 {
		req.Window = s.cfg.Live.Publish.BroadcastWindow
		if req.Window == 0 {
			req.Window = 10
		}
	}
	resp, err := TriggerGatewayPublishLiveDemand(r.Context(), s.store, s.rt, PublishLiveDemandParams{
		StreamID:         req.StreamID,
		HaveSegmentIndex: req.HaveSegmentIndex,
		Window:           req.Window,
		GatewayPeerID:    req.GatewayPeerID,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleLiveQuotes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	demandID := strings.TrimSpace(r.URL.Query().Get("demand_id"))
	if demandID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "demand_id is required"})
		return
	}
	quotes, err := TriggerClientListLiveQuotes(r.Context(), s.store, demandID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"demand_id": demandID,
		"quotes":    quotes,
		"count":     len(quotes),
	})
}

func (s *httpAPIServer) handleLivePublishSegment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil || s.rt.Workspace == nil || s.rt.Host == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamID          string  `json:"stream_id"`
		SegmentIndex      *uint64 `json:"segment_index,omitempty"`
		PrevSeedHash      string  `json:"prev_seed_hash"`
		DurationMs        uint64  `json:"duration_ms"`
		PublishedAtUnixMs int64   `json:"published_at_unix_ms"`
		IsDiscontinuity   bool    `json:"is_discontinuity"`
		MIMEType          string  `json:"mime_type"`
		InitSeedHash      string  `json:"init_seed_hash"`
		PlaylistURIHint   string  `json:"playlist_uri_hint"`
		MediaSequence     *uint64 `json:"media_sequence,omitempty"`
		IsEnd             bool    `json:"is_end"`
		MediaBytes        []byte  `json:"media_bytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if len(req.MediaBytes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "media_bytes is required"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(req.StreamID))
	pubHex, err := localPubKeyHex(s.rt.Host)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	recent, lastRef, err := s.currentLivePublishWindow(streamID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var segmentIndex uint64
	if req.SegmentIndex != nil {
		segmentIndex = *req.SegmentIndex
	} else if lastRef != nil {
		segmentIndex = lastRef.SegmentIndex + 1
	}
	if streamID == "" && segmentIndex != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "first live segment must use segment_index=0"})
		return
	}
	if streamID != "" && !isSeedHashHex(streamID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
		return
	}
	prevSeedHash := strings.ToLower(strings.TrimSpace(req.PrevSeedHash))
	if prevSeedHash == "" && lastRef != nil {
		prevSeedHash = lastRef.SeedHash
	}
	if streamID != "" && segmentIndex > 0 && prevSeedHash == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "prev_seed_hash is required"})
		return
	}
	publishedAtUnixMs := req.PublishedAtUnixMs
	if publishedAtUnixMs == 0 {
		publishedAtUnixMs = time.Now().UnixMilli()
	}
	mediaSequence := uint64(0)
	if req.MediaSequence != nil {
		mediaSequence = *req.MediaSequence
	} else if segmentIndex > 0 {
		mediaSequence = segmentIndex
	}
	segData := liveSegmentDataPB{
		Version:           1,
		StreamID:          streamID,
		SegmentIndex:      segmentIndex,
		PrevSeedHash:      prevSeedHash,
		PublisherPubKey:   pubHex,
		DurationMs:        req.DurationMs,
		PublishedAtUnixMs: publishedAtUnixMs,
		IsDiscontinuity:   req.IsDiscontinuity,
		MIMEType:          req.MIMEType,
		InitSeedHash:      req.InitSeedHash,
		PlaylistURIHint:   req.PlaylistURIHint,
		MediaSequence:     mediaSequence,
		IsEnd:             req.IsEnd,
	}
	segmentBytes, _, err := BuildLiveSegment(r.Context(), s.rt, segData, req.MediaBytes)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	seedBytes, seedHash, chunkCount, err := s.buildSeedForLiveSegment(segmentBytes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if streamID == "" {
		streamID = seedHash
		segData.StreamID = ""
		recent = nil
	}
	outPath, err := s.rt.Workspace.SelectLiveSegmentOutputPath(streamID, segmentIndex, uint64(len(segmentBytes)))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := os.WriteFile(outPath, segmentBytes, 0o644); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if _, err := s.rt.Workspace.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              outPath,
		Seed:                  seedBytes,
		AvailableChunkIndexes: contiguousChunkIndexes(chunkCount),
		RecommendedFileName:   filepath.Base(outPath),
		MIMEHint:              "",
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.rt.Workspace.EnforceLiveCacheLimit(s.cfg.Live.CacheMaxBytes); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	recent = append(recent, LiveSegmentRef{
		SegmentIndex:    segmentIndex,
		SeedHash:        seedHash,
		PublishedAtUnix: publishedAtUnixMs / 1000,
	})
	recent = trimLiveSegmentRefs(normalizeLiveSegmentRefs(recent), clampLiveWindow(s.cfg.Live.Publish.BroadcastWindow))
	if err := TriggerLivePublishLatest(r.Context(), s.rt, streamID, recent); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	subscribeURI, err := BuildLiveSubscribeURI(pubHex, streamID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"stream_id":       streamID,
		"seed_hash":       seedHash,
		"segment_index":   segmentIndex,
		"chunk_count":     chunkCount,
		"output_file":     outPath,
		"subscribe_uri":   subscribeURI,
		"recent_segments": recent,
	})
}

func (s *httpAPIServer) handleLivePublishLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamID       string           `json:"stream_id"`
		RecentSegments []LiveSegmentRef `json:"recent_segments"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if err := TriggerLivePublishLatest(r.Context(), s.rt, req.StreamID, req.RecentSegments); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"stream_id":    strings.ToLower(strings.TrimSpace(req.StreamID)),
		"recent_count": len(normalizeLiveSegmentRefs(req.RecentSegments)),
	})
}

func (s *httpAPIServer) buildSeedForLiveSegment(segmentBytes []byte) ([]byte, string, uint32, error) {
	tmpDir := filepath.Join(s.cfg.Storage.DataDir, "live-publish")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, "", 0, err
	}
	tmpPath := filepath.Join(tmpDir, randHex(8)+".seg")
	if err := os.WriteFile(tmpPath, segmentBytes, 0o644); err != nil {
		return nil, "", 0, err
	}
	defer os.Remove(tmpPath)
	return buildSeedV1(tmpPath)
}

func (s *httpAPIServer) currentLivePublishWindow(streamID string) ([]LiveSegmentRef, *LiveSegmentRef, error) {
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if streamID == "" {
		return nil, nil, nil
	}
	if _, recent, ok := s.rt.live.publishedSnapshot(streamID); ok {
		if len(recent) == 0 {
			return nil, nil, nil
		}
		last := recent[len(recent)-1]
		return recent, &last, nil
	}
	rows, err := dbListLiveWorkspaceEntries(context.Background(), httpStore(s), "%"+string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)+"%", false)
	if err != nil {
		return nil, nil, err
	}
	recent := make([]LiveSegmentRef, 0, maxLiveWindowSize)
	for _, row := range rows {
		segmentBytes, err := os.ReadFile(row.Path)
		if err != nil {
			continue
		}
		data, _, _, err := VerifyLiveSegment(segmentBytes)
		if err != nil {
			continue
		}
		recent = append(recent, LiveSegmentRef{
			SegmentIndex:    data.SegmentIndex,
			SeedHash:        strings.ToLower(strings.TrimSpace(row.SeedHash)),
			PublishedAtUnix: row.UpdatedAtUnix,
		})
	}
	recent = trimLiveSegmentRefs(normalizeLiveSegmentRefs(recent), clampLiveWindow(s.cfg.Live.Publish.BroadcastWindow))
	if len(recent) == 0 {
		return nil, nil, nil
	}
	last := recent[len(recent)-1]
	return recent, &last, nil
}

func (s *httpAPIServer) handleLiveLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream_id")))
	if !isSeedHashHex(streamID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
		return
	}
	snap, err := TriggerLiveGetLatest(s.rt, streamID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *httpAPIServer) handleLivePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamID         string `json:"stream_id"`
		HaveSegmentIndex int64  `json:"have_segment_index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	snap, err := TriggerLiveGetLatest(s.rt, req.StreamID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	plan, err := TriggerLivePlan(r.Context(), s.rt, LivePlanParams{
		StreamID:         req.StreamID,
		HaveSegmentIndex: req.HaveSegmentIndex,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_id":          strings.ToLower(strings.TrimSpace(req.StreamID)),
		"have_segment_index": req.HaveSegmentIndex,
		"decision":           plan.Decision,
		"snapshot":           snap,
	})
}

func (s *httpAPIServer) handleLiveFollowStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamURI string `json:"stream_uri"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	st, err := TriggerLiveFollowStart(r.Context(), s.store, s.rt, req.StreamURI)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *httpAPIServer) handleLiveFollowStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		StreamID string `json:"stream_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if err := TriggerLiveFollowStop(s.store, s.rt, req.StreamID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": strings.ToLower(strings.TrimSpace(req.StreamID))})
}

func (s *httpAPIServer) handleLiveFollowStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream_id")))
	st, err := TriggerLiveFollowStatus(s.store, s.rt, streamID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func parseBoundInt(raw string, def, minV, maxV int) int {
	v := def
	if strings.TrimSpace(raw) != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			v = n
		}
	}
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func pathClean(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return filepath.ToSlash(filepath.Clean(p))
}

func resolveWorkspacePath(root, input string) (string, error) {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	p := input
	if !filepath.IsAbs(p) {
		p = filepath.Join(rootAbs, p)
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, abs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path is outside workspace_dir")
	}
	return abs, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// handleGateways 处理网关 CRUD: GET /api/v1/gateways, POST /api/v1/gateways, PUT /api/v1/gateways?id={id}, DELETE /api/v1/gateways?id={id}
func (s *httpAPIServer) handleGateways(w http.ResponseWriter, r *http.Request) {
	gm := s.rt.gwManager
	if gm == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "gateway manager not initialized"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		// 列出所有网关
		gateways := gm.ListGateways()
		healthy := map[string]bool{}
		for _, gw := range s.rt.HealthyGWs {
			healthy[gw.ID.String()] = true
		}
		master := gm.GetMasterGateway().String()
		type gwResp struct {
			ID                        int    `json:"id"`
			PeerID                    string `json:"transport_peer_id,omitempty"`
			Addr                      string `json:"addr"`
			Pubkey                    string `json:"pubkey"`
			Enabled                   bool   `json:"enabled"`
			ListenOfferPaymentSatoshi uint64 `json:"listen_offer_payment_satoshi,omitempty"`
			Connected                 bool   `json:"connected"`
			Connectedness             string `json:"connectedness"`
			InHealthyGWs              bool   `json:"in_healthy_gws"`
			FeePoolReady              bool   `json:"fee_pool_ready"`
			IsMaster                  bool   `json:"is_master"`
			LastError                 string `json:"last_error,omitempty"`
			LastConnectedAtUnix       int64  `json:"last_connected_at_unix"`
			LastRuntimeError          string `json:"last_runtime_error,omitempty"`
			LastRuntimeErrorStage     string `json:"last_runtime_error_stage,omitempty"`
			LastRuntimeErrorAtUnix    int64  `json:"last_runtime_error_at_unix"`
		}
		items := make([]gwResp, len(gateways))
		for i, g := range gateways {
			it := gwResp{ID: i, Addr: g.Addr, Pubkey: g.Pubkey, Enabled: g.Enabled, ListenOfferPaymentSatoshi: g.ListenOfferPaymentSatoshi}
			ai, err := parseAddr(g.Addr)
			if err != nil {
				it.Connectedness = "invalid_addr"
				it.LastError = err.Error()
				items[i] = it
				continue
			}
			it.PeerID = ai.ID.String()
			if s.h == nil || s.h.Network() == nil {
				it.Connectedness = "unknown"
			} else {
				conn := s.h.Network().Connectedness(ai.ID)
				it.Connectedness = strings.ToLower(conn.String())
				it.Connected = conn == libnetwork.Connected
			}
			it.InHealthyGWs = healthy[it.PeerID]
			it.IsMaster = master != "" && it.PeerID == master
			st := gm.GetGatewayState(ai.ID)
			it.LastConnectedAtUnix = st.LastConnectedAtUnix
			if !it.Connected && strings.TrimSpace(st.LastError) != "" {
				it.LastError = st.LastError
			}
			it.LastRuntimeError = st.LastRuntimeError
			it.LastRuntimeErrorStage = st.LastRuntimeErrorStage
			it.LastRuntimeErrorAtUnix = st.LastRuntimeErrorAtUnix
			it.FeePoolReady = it.Enabled && it.Connected && it.InHealthyGWs && strings.TrimSpace(it.LastRuntimeError) == ""
			items[i] = it
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})

	case http.MethodPost:
		// 添加网关
		var req struct {
			Addr                      string `json:"addr"`
			Pubkey                    string `json:"pubkey"`
			Enabled                   bool   `json:"enabled"`
			ListenOfferPaymentSatoshi uint64 `json:"listen_offer_payment_satoshi,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		req.Addr = strings.TrimSpace(req.Addr)
		req.Pubkey = strings.ToLower(strings.TrimSpace(req.Pubkey))
		if req.Addr == "" || req.Pubkey == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "addr and pubkey are required"})
			return
		}
		// 验证地址格式
		ai, err := parseAddr(req.Addr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid addr: " + err.Error()})
			return
		}
		// 验证 pubkey 与 transport_peer_id 匹配
		pidFromPub, err := peerIDFromSecp256k1PubHex(req.Pubkey)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid pubkey: " + err.Error()})
			return
		}
		if ai.ID != pidFromPub {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "pubkey does not match addr transport_peer_id"})
			return
		}
		// 检查重复
		for i, g := range s.rt.runIn.Network.Gateways {
			if strings.EqualFold(g.Pubkey, req.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
		}

		node := PeerNode{Enabled: req.Enabled, Addr: req.Addr, Pubkey: req.Pubkey, ListenOfferPaymentSatoshi: req.ListenOfferPaymentSatoshi}
		idx, err := gm.AddGateway(r.Context(), node)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		// 保存配置
		if err := gm.SaveConfig(); err != nil {
			obs.Error("bitcast-client", "gateway_add_save_failed", map[string]any{"error": err.Error()})
		}
		// 更新 HealthyGWs
		s.rt.HealthyGWs = gm.GetConnectedGateways()
		s.gateways = s.rt.HealthyGWs

		writeJSON(w, http.StatusOK, map[string]any{"id": idx, "success": true})

	case http.MethodPut:
		// 更新网关
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		if idStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
			return
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
			return
		}

		var req struct {
			Addr                      string `json:"addr"`
			Pubkey                    string `json:"pubkey"`
			Enabled                   bool   `json:"enabled"`
			ListenOfferPaymentSatoshi uint64 `json:"listen_offer_payment_satoshi,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		req.Addr = strings.TrimSpace(req.Addr)
		req.Pubkey = strings.ToLower(strings.TrimSpace(req.Pubkey))
		if req.Addr == "" || req.Pubkey == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "addr and pubkey are required"})
			return
		}

		node := PeerNode{Enabled: req.Enabled, Addr: req.Addr, Pubkey: req.Pubkey, ListenOfferPaymentSatoshi: req.ListenOfferPaymentSatoshi}
		if err := gm.UpdateGateway(r.Context(), id, node); err != nil {
			if strings.Contains(err.Error(), "out of range") {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "gateway not found"})
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return
		}
		// 保存配置
		if err := gm.SaveConfig(); err != nil {
			obs.Error("bitcast-client", "gateway_update_save_failed", map[string]any{"error": err.Error()})
		}
		// 更新 HealthyGWs
		s.rt.HealthyGWs = gm.GetConnectedGateways()
		s.gateways = s.rt.HealthyGWs

		writeJSON(w, http.StatusOK, map[string]any{"id": id, "success": true})

	case http.MethodDelete:
		// 删除网关
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		if idStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
			return
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
			return
		}

		if err := gm.DeleteGateway(id); err != nil {
			if strings.Contains(err.Error(), "out of range") {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "gateway not found"})
			} else if strings.Contains(err.Error(), "disable") {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			} else {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			}
			return
		}
		// 保存配置
		if err := gm.SaveConfig(); err != nil {
			obs.Error("bitcast-client", "gateway_delete_save_failed", map[string]any{"error": err.Error()})
		}
		// 更新 HealthyGWs
		s.rt.HealthyGWs = gm.GetConnectedGateways()
		s.gateways = s.rt.HealthyGWs

		writeJSON(w, http.StatusOK, map[string]any{"id": id, "success": true, "deleted": true})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

// handleGatewayMaster 获取/设置主网关: GET /api/v1/gateways/master, POST /api/v1/gateways/master
func (s *httpAPIServer) handleGatewayMaster(w http.ResponseWriter, r *http.Request) {
	gm := s.rt.gwManager
	if gm == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "gateway manager not initialized"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		master := gm.GetMasterGateway()
		if master == "" {
			writeJSON(w, http.StatusOK, map[string]any{"master_gateway_pubkey_hex": "", "has_master": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"master_gateway_pubkey_hex": master.String(), "has_master": true})
	case http.MethodPost:
		var req struct {
			MasterPeerID string `json:"master_gateway_pubkey_hex"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		target := strings.TrimSpace(req.MasterPeerID)
		if target == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "master_gateway_pubkey_hex is required"})
			return
		}
		targetID, err := peer.Decode(target)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid master_gateway_pubkey_hex"})
			return
		}

		seenEnabled := false
		for _, g := range gm.ListGateways() {
			ai, parseErr := parseAddr(g.Addr)
			if parseErr != nil {
				continue
			}
			if ai.ID != targetID {
				continue
			}
			if !g.Enabled {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "gateway is disabled"})
				return
			}
			seenEnabled = true
			break
		}
		if !seenEnabled {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "gateway transport_peer_id not configured"})
			return
		}
		if s.h == nil || s.h.Network() == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "host network not initialized"})
			return
		}
		if s.h.Network().Connectedness(targetID) != libnetwork.Connected {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "gateway is not connected"})
			return
		}

		old := gm.GetMasterGateway()
		s.rt.masterGWMu.Lock()
		s.rt.masterGW = targetID
		s.rt.masterGWMu.Unlock()
		changed := old != targetID
		if changed {
			obs.Business("bitcast-client", "master_gateway_set_by_admin", map[string]any{
				"old_master_gateway_pubkey_hex": old.String(),
				"new_master_gateway_pubkey_hex": targetID.String(),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                        true,
			"master_gateway_pubkey_hex": targetID.String(),
			"changed":                   changed,
		})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (s *httpAPIServer) handleGatewayHealth(w http.ResponseWriter, r *http.Request) {
	gm := s.rt.gwManager
	if gm == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "gateway manager not initialized"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	healthy := map[string]bool{}
	for _, gw := range s.rt.HealthyGWs {
		healthy[gw.ID.String()] = true
	}
	master := gm.GetMasterGateway().String()
	type healthItem struct {
		ID                     int    `json:"id"`
		PeerID                 string `json:"transport_peer_id,omitempty"`
		Addr                   string `json:"addr"`
		Pubkey                 string `json:"pubkey"`
		Enabled                bool   `json:"enabled"`
		Connected              bool   `json:"connected"`
		Connectedness          string `json:"connectedness"`
		IsMaster               bool   `json:"is_master"`
		InHealthyGWs           bool   `json:"in_healthy_gws"`
		FeePoolReady           bool   `json:"fee_pool_ready"`
		Error                  string `json:"error,omitempty"`
		LastRuntimeError       string `json:"last_runtime_error,omitempty"`
		LastRuntimeErrorStage  string `json:"last_runtime_error_stage,omitempty"`
		LastRuntimeErrorAtUnix int64  `json:"last_runtime_error_at_unix"`
	}
	nodes := gm.ListGateways()
	items := make([]healthItem, 0, len(nodes))
	connectedCount := 0
	for i, g := range nodes {
		it := healthItem{
			ID:      i,
			Addr:    g.Addr,
			Pubkey:  g.Pubkey,
			Enabled: g.Enabled,
		}
		ai, err := parseAddr(g.Addr)
		if err != nil {
			it.Connectedness = "invalid_addr"
			it.Error = err.Error()
			items = append(items, it)
			continue
		}
		it.PeerID = ai.ID.String()
		if s.h == nil || s.h.Network() == nil {
			it.Connectedness = "unknown"
		} else {
			conn := s.h.Network().Connectedness(ai.ID)
			it.Connectedness = strings.ToLower(conn.String())
			it.Connected = conn == libnetwork.Connected
		}
		if it.Connected {
			connectedCount++
		}
		it.IsMaster = it.PeerID == master && master != ""
		it.InHealthyGWs = healthy[it.PeerID]
		st := gm.GetGatewayState(ai.ID)
		it.LastRuntimeError = st.LastRuntimeError
		it.LastRuntimeErrorStage = st.LastRuntimeErrorStage
		it.LastRuntimeErrorAtUnix = st.LastRuntimeErrorAtUnix
		it.FeePoolReady = it.Enabled && it.Connected && it.InHealthyGWs && strings.TrimSpace(it.LastRuntimeError) == ""
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": len(items),
		"enabled_total": func() int {
			c := 0
			for _, it := range items {
				if it.Enabled {
					c++
				}
			}
			return c
		}(),
		"connected_total":           connectedCount,
		"master_gateway_pubkey_hex": master,
		"items":                     items,
	})
}

// handleArbiters 处理仲裁节点 CRUD: GET /api/v1/arbiters, POST /api/v1/arbiters, PUT /api/v1/arbiters?id={id}, DELETE /api/v1/arbiters?id={id}
func (s *httpAPIServer) handleArbiters(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		type arbResp struct {
			ID      int    `json:"id"`
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
		}
		items := make([]arbResp, len(s.rt.runIn.Network.Arbiters))
		for i, a := range s.rt.runIn.Network.Arbiters {
			items[i] = arbResp{ID: i, Addr: a.Addr, Pubkey: a.Pubkey, Enabled: a.Enabled}
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
	case http.MethodPost:
		var req struct {
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		node, err := normalizeArbiterNode(req.Addr, req.Pubkey, req.Enabled)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		newAI, _ := parseAddr(node.Addr)
		for i, a := range s.rt.runIn.Network.Arbiters {
			if strings.EqualFold(strings.TrimSpace(a.Pubkey), node.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
			ai, parseErr := parseAddr(strings.TrimSpace(a.Addr))
			if parseErr == nil && ai.ID == newAI.ID {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("addr transport_peer_id already exists at index %d", i)})
				return
			}
		}
		next := s.rt.runIn
		next.Network.Arbiters = append(next.Network.Arbiters, node)
		s.rt.runIn = next
		if s.cfg != nil {
			*s.cfg = next.toConfig()
		}
		if err := SaveConfigFile(s.rt.runIn.ConfigPath, next.toConfig()); err != nil {
			obs.Error("bitcast-client", "arbiter_add_save_failed", map[string]any{"error": err.Error()})
		}
		s.refreshHealthyArbiters(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"id": len(next.Network.Arbiters) - 1, "success": true})
	case http.MethodPut:
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		if idStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
			return
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
			return
		}
		if id >= len(s.rt.runIn.Network.Arbiters) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "arbiter not found"})
			return
		}
		var req struct {
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		node, err := normalizeArbiterNode(req.Addr, req.Pubkey, req.Enabled)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		newAI, _ := parseAddr(node.Addr)
		for i, a := range s.rt.runIn.Network.Arbiters {
			if i == id {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(a.Pubkey), node.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
			ai, parseErr := parseAddr(strings.TrimSpace(a.Addr))
			if parseErr == nil && ai.ID == newAI.ID {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("addr transport_peer_id already exists at index %d", i)})
				return
			}
		}
		next := s.rt.runIn
		next.Network.Arbiters[id] = node
		s.rt.runIn = next
		if s.cfg != nil {
			*s.cfg = next.toConfig()
		}
		if err := SaveConfigFile(s.rt.runIn.ConfigPath, next.toConfig()); err != nil {
			obs.Error("bitcast-client", "arbiter_update_save_failed", map[string]any{"error": err.Error()})
		}
		s.refreshHealthyArbiters(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "success": true})
	case http.MethodDelete:
		idStr := strings.TrimSpace(r.URL.Query().Get("id"))
		if idStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
			return
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
			return
		}
		if id >= len(s.rt.runIn.Network.Arbiters) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "arbiter not found"})
			return
		}
		if s.rt.runIn.Network.Arbiters[id].Enabled {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cannot delete enabled arbiter, please disable it first"})
			return
		}
		next := s.rt.runIn
		next.Network.Arbiters = append(next.Network.Arbiters[:id], next.Network.Arbiters[id+1:]...)
		s.rt.runIn = next
		if s.cfg != nil {
			*s.cfg = next.toConfig()
		}
		if err := SaveConfigFile(s.rt.runIn.ConfigPath, next.toConfig()); err != nil {
			obs.Error("bitcast-client", "arbiter_delete_save_failed", map[string]any{"error": err.Error()})
		}
		s.refreshHealthyArbiters(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "success": true, "deleted": true})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func normalizeArbiterNode(addr, pubkey string, enabled bool) (PeerNode, error) {
	addr = strings.TrimSpace(addr)
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if addr == "" || pubkey == "" {
		return PeerNode{}, fmt.Errorf("addr and pubkey are required")
	}
	ai, err := parseAddr(addr)
	if err != nil {
		return PeerNode{}, fmt.Errorf("invalid addr: %w", err)
	}
	pidFromPub, err := peerIDFromSecp256k1PubHex(pubkey)
	if err != nil {
		return PeerNode{}, fmt.Errorf("invalid pubkey: %w", err)
	}
	if ai.ID != pidFromPub {
		return PeerNode{}, fmt.Errorf("pubkey does not match addr transport_peer_id")
	}
	return PeerNode{Enabled: enabled, Addr: addr, Pubkey: pubkey}, nil
}

func (s *httpAPIServer) refreshHealthyArbiters(ctx context.Context) {
	if s == nil || s.rt == nil || s.h == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cfgArbs := s.rt.runIn.Network.Arbiters
	infos := make([]peer.AddrInfo, 0, len(cfgArbs))
	for i, a := range cfgArbs {
		if !a.Enabled {
			continue
		}
		ai, err := parseAddr(strings.TrimSpace(a.Addr))
		if err != nil {
			obs.Error("bitcast-client", "arbiter_addr_invalid", map[string]any{"index": i, "addr": a.Addr, "error": err.Error()})
			continue
		}
		if err := s.h.Connect(ctx, *ai); err != nil {
			obs.Error("bitcast-client", "arbiter_connect_failed", map[string]any{"index": i, "transport_peer_id": ai.ID.String(), "error": err.Error()})
			continue
		}
		infos = append(infos, *ai)
	}
	s.rt.HealthyArbiters = checkPeerHealth(ctx, s.h, infos, ProtoArbHealth, arbSec(s.rpcTrace), "arbiter")
}

func (s *httpAPIServer) handleArbiterHealth(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	s.refreshHealthyArbiters(r.Context())
	healthy := map[string]bool{}
	for _, ai := range s.rt.HealthyArbiters {
		healthy[ai.ID.String()] = true
	}
	type healthItem struct {
		ID            int    `json:"id"`
		PeerID        string `json:"transport_peer_id,omitempty"`
		Addr          string `json:"addr"`
		Pubkey        string `json:"pubkey"`
		Enabled       bool   `json:"enabled"`
		Connected     bool   `json:"connected"`
		Connectedness string `json:"connectedness"`
		InHealthyArbs bool   `json:"in_healthy_arbiters"`
		Error         string `json:"error,omitempty"`
	}
	nodes := s.rt.runIn.Network.Arbiters
	items := make([]healthItem, 0, len(nodes))
	connectedCount := 0
	for i, a := range nodes {
		it := healthItem{ID: i, Addr: a.Addr, Pubkey: a.Pubkey, Enabled: a.Enabled}
		ai, err := parseAddr(strings.TrimSpace(a.Addr))
		if err != nil {
			it.Connectedness = "invalid_addr"
			it.Error = err.Error()
			items = append(items, it)
			continue
		}
		it.PeerID = ai.ID.String()
		if s.h == nil || s.h.Network() == nil {
			it.Connectedness = "unknown"
		} else {
			conn := s.h.Network().Connectedness(ai.ID)
			it.Connectedness = strings.ToLower(conn.String())
			it.Connected = conn == libnetwork.Connected
		}
		if it.Connected {
			connectedCount++
		}
		it.InHealthyArbs = healthy[it.PeerID]
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": len(items),
		"enabled_total": func() int {
			c := 0
			for _, it := range items {
				if it.Enabled {
					c++
				}
			}
			return c
		}(),
		"connected_total": connectedCount,
		"healthy_total":   len(s.rt.HealthyArbiters),
		"items":           items,
	})
}

func (s *httpAPIServer) handleAdminWorkspaces(w http.ResponseWriter, r *http.Request) {
	if s.workspace == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace manager not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := s.workspace.List()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
	case http.MethodPost:
		var req struct {
			Path     string `json:"path"`
			MaxBytes uint64 `json:"max_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		it, err := s.workspace.Add(req.Path, req.MaxBytes)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "workspace": it})
	case http.MethodPut:
		workspacePath := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if workspacePath == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "workspace_path is required"})
			return
		}
		var req struct {
			MaxBytes *uint64 `json:"max_bytes,omitempty"`
			Enabled  *bool   `json:"enabled,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		it, err := s.workspace.UpdateByPath(workspacePath, req.MaxBytes, req.Enabled)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "not found") {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "workspace": it})
	case http.MethodDelete:
		workspacePath := strings.TrimSpace(r.URL.Query().Get("workspace_path"))
		if workspacePath == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "workspace_path is required"})
			return
		}
		if err := s.workspace.DeleteByPath(workspacePath); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "not found") {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "workspace_path": workspacePath})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (s *httpAPIServer) handleAdminResumeDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s.rt == nil || s.rt.FSHTTP == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "file http server not enabled"})
		return
	}
	var req struct {
		SeedHash string `json:"seed_hash"`
		Full     bool   `json:"full"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if err := s.rt.FSHTTP.Resume(req.SeedHash, req.Full); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "seed_hash": strings.ToLower(strings.TrimSpace(req.SeedHash)), "full": req.Full})
}

// handleAdminStrategyDebugLog 动态查询/更新 fs_http.strategy_debug_log_enabled。
// 设计约束：
// - POST 成功后立即影响运行态，无需重启；
// - 同步写回 config.yaml，保证重启后状态一致；
// - 文件持久化失败时回滚内存配置，避免运行态与持久态分叉。
func (s *httpAPIServer) handleAdminStrategyDebugLog(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.rt == nil || s.cfg == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"strategy_debug_log_enabled": s.cfg.FSHTTP.StrategyDebugLogEnabled,
		})
	case http.MethodPost:
		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		oldEnabled := s.cfg.FSHTTP.StrategyDebugLogEnabled
		s.cfg.FSHTTP.StrategyDebugLogEnabled = req.Enabled
		cfg := s.rt.runIn
		cfg.FSHTTP.StrategyDebugLogEnabled = req.Enabled
		if err := SaveConfigFile(s.rt.runIn.ConfigPath, cfg.toConfig()); err != nil {
			s.cfg.FSHTTP.StrategyDebugLogEnabled = oldEnabled
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		s.rt.runIn = cfg
		obs.Business("bitcast-client", "admin_strategy_debug_log_updated", map[string]any{
			"enabled": req.Enabled,
		})
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                         true,
			"strategy_debug_log_enabled": req.Enabled,
			"changed":                    oldEnabled != req.Enabled,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (s *httpAPIServer) handleAdminLiveStreams(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items, err := s.queryLiveStreamStats()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
	case http.MethodDelete:
		streamID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream_id")))
		if !isSeedHashHex(streamID) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
			return
		}
		root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
		if root == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
			return
		}
		livePath, err := resolveWorkspacePath(root, filepath.Join("live", streamID))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		prefix := filepath.Clean(livePath) + string(filepath.Separator) + "%"
		_ = os.RemoveAll(livePath)
		before, err := dbDeleteLiveStreamWorkspaceRows(r.Context(), httpStore(s), prefix)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if s.workspace != nil {
			if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"deleted":        true,
			"stream_id":      streamID,
			"deleted_files":  before,
			"deleted_prefix": livePath,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (s *httpAPIServer) handleAdminLiveStreamDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream_id")))
	if !isSeedHashHex(streamID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
		return
	}
	sep := string(filepath.Separator)
	prefix := "%" + sep + "live" + sep + streamID + sep + "%"
	rows, err := dbListLiveWorkspaceEntries(r.Context(), httpStore(s), prefix, true)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	type item struct {
		Path          string `json:"path"`
		FileSize      int64  `json:"file_size"`
		MtimeUnix     int64  `json:"mtime_unix"`
		UpdatedAtUnix int64  `json:"updated_at_unix"`
	}
	out := make([]item, 0, 32)
	var totalBytes uint64
	var newest int64
	for _, row := range rows {
		it := item{Path: row.Path, FileSize: row.FileSize, MtimeUnix: row.MtimeUnix, UpdatedAtUnix: row.UpdatedAtUnix}
		if it.FileSize > 0 {
			totalBytes += uint64(it.FileSize)
		}
		if it.UpdatedAtUnix > newest {
			newest = it.UpdatedAtUnix
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_id":       streamID,
		"file_count":      len(out),
		"total_bytes":     totalBytes,
		"last_updated":    newest,
		"live_root_path":  filepath.Join(strings.TrimSpace(s.cfg.Storage.WorkspaceDir), "live", streamID),
		"segment_entries": out,
	})
}

func (s *httpAPIServer) handleAdminLiveStorageSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	items, err := s.queryLiveStreamStats()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var totalBytes uint64
	var fileCount int64
	for _, it := range items {
		totalBytes += it.TotalBytes
		fileCount += it.FileCount
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_count": len(items),
		"file_count":   fileCount,
		"total_bytes":  totalBytes,
	})
}

type adminLiveStreamStat struct {
	StreamID         string   `json:"stream_id"`
	FileCount        int64    `json:"file_count"`
	TotalBytes       uint64   `json:"total_bytes"`
	LastUpdatedUnix  int64    `json:"last_updated_unix"`
	WorkspaceRoots   []string `json:"workspace_roots,omitempty"`
	LiveStreamFolder string   `json:"live_stream_folder"`
}

func (s *httpAPIServer) queryLiveStreamStats() ([]adminLiveStreamStat, error) {
	if s == nil || httpStore(s) == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	rows, err := dbListLiveStreamStats(context.Background(), httpStore(s))
	if err != nil {
		return nil, err
	}
	out := make([]adminLiveStreamStat, 0, len(rows))
	for _, row := range rows {
		liveFolder := filepath.Join(strings.TrimSpace(s.cfg.Storage.WorkspaceDir), "live", row.StreamID)
		out = append(out, adminLiveStreamStat{
			StreamID:         row.StreamID,
			FileCount:        row.FileCount,
			TotalBytes:       row.TotalBytes,
			LastUpdatedUnix:  row.LastUpdatedUnix,
			WorkspaceRoots:   row.WorkspaceRoots,
			LiveStreamFolder: liveFolder,
		})
	}
	return out, nil
}

func extractLiveStreamIDFromPath(fullPath string) (string, string, bool) {
	clean := filepath.Clean(strings.TrimSpace(fullPath))
	if clean == "" {
		return "", "", false
	}
	parts := strings.Split(clean, string(filepath.Separator))
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "live" {
			continue
		}
		streamID := strings.ToLower(strings.TrimSpace(parts[i+1]))
		if !isSeedHashHex(streamID) {
			return "", "", false
		}
		root := strings.Join(parts[:i], string(filepath.Separator))
		if strings.TrimSpace(root) == "" {
			root = string(filepath.Separator)
		}
		return streamID, root, true
	}
	return "", "", false
}

func (s *httpAPIServer) handleAdminStaticTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	abs, rel, err := resolveStaticPath(root, r.URL.Query().Get("path"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	recursive := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "1") ||
		strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "true")
	maxDepth := parseBoundInt(r.URL.Query().Get("max_depth"), 1, 1, 16)
	if !recursive {
		maxDepth = 1
	}
	st, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "path not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !st.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "path is not directory"})
		return
	}
	out, err := s.buildAdminStaticTree(abs, rel, 1, maxDepth)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	parent := "/"
	if rel != "/" {
		parent = filepath.ToSlash(filepath.Dir(rel))
		if !strings.HasPrefix(parent, "/") {
			parent = "/" + parent
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"root_path":    root,
		"current_path": rel,
		"parent_path":  parent,
		"recursive":    recursive,
		"max_depth":    maxDepth,
		"total":        len(out),
		"items":        out,
	})
}

type adminStaticTreeNode struct {
	Name                string                `json:"name"`
	Path                string                `json:"path"`
	Type                string                `json:"type"`
	Size                int64                 `json:"size"`
	MtimeUnix           int64                 `json:"mtime_unix"`
	HasChildren         bool                  `json:"has_children,omitempty"`
	Children            []adminStaticTreeNode `json:"children,omitempty"`
	SeedHash            string                `json:"seed_hash,omitempty"`
	FloorPriceSatPer64K uint64                `json:"floor_unit_price_sat_per_64k,omitempty"`
	ResaleDiscountBPS   uint64                `json:"resale_discount_bps,omitempty"`
	PriceUpdatedAtUnix  int64                 `json:"price_updated_at_unix,omitempty"`
}

func (s *httpAPIServer) buildAdminStaticTree(abs string, rel string, depth int, maxDepth int) ([]adminStaticTreeNode, error) {
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	out := make([]adminStaticTreeNode, 0, len(entries))
	for _, de := range entries {
		name := strings.TrimSpace(de.Name())
		if name == "" {
			continue
		}
		full := filepath.Join(abs, name)
		info, err := de.Info()
		if err != nil {
			continue
		}
		relPath := filepath.ToSlash(filepath.Join(rel, name))
		if !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		node := adminStaticTreeNode{
			Name:      name,
			Path:      relPath,
			Size:      info.Size(),
			MtimeUnix: info.ModTime().Unix(),
		}
		if de.IsDir() {
			node.Type = "dir"
			if kids, err := os.ReadDir(full); err == nil && len(kids) > 0 {
				node.HasChildren = true
			}
			if depth < maxDepth {
				children, err := s.buildAdminStaticTree(full, relPath, depth+1, maxDepth)
				if err == nil {
					node.Children = children
				}
			}
		} else {
			node.Type = "file"
			node.SeedHash, _ = dbGetWorkspaceFileSeedHash(context.Background(), httpStore(s), full)
			if price, err := dbGetStaticFilePrice(context.Background(), httpStore(s), full); err == nil {
				node.FloorPriceSatPer64K = price.FloorPriceSatPer64K
				node.ResaleDiscountBPS = price.ResaleDiscountBPS
				node.PriceUpdatedAtUnix = price.UpdatedAtUnix
			}
		}
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type == "dir"
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (s *httpAPIServer) handleAdminStaticMkdir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	abs, rel, err := resolveStaticPath(root, req.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "path": rel})
}

func (s *httpAPIServer) handleAdminStaticUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	if err := r.ParseMultipartForm(128 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid multipart form"})
		return
	}
	targetDirRaw := strings.TrimSpace(r.FormValue("target_dir"))
	if targetDirRaw == "" {
		targetDirRaw = strings.TrimSpace(r.FormValue("path"))
	}
	if targetDirRaw == "" {
		targetDirRaw = "/"
	}
	targetAbs, targetRel, err := resolveStaticPath(root, targetDirRaw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	overwrite := strings.EqualFold(strings.TrimSpace(r.FormValue("overwrite")), "1") ||
		strings.EqualFold(strings.TrimSpace(r.FormValue("overwrite")), "true")
	fileName := sanitizeRecommendedFileName(strings.TrimSpace(r.FormValue("file_name")))
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "file is required"})
		return
	}
	defer file.Close()
	if fileName == "" {
		fileName = sanitizeRecommendedFileName(strings.TrimSpace(fileHeader.Filename))
	}
	if fileName == "" {
		fileName = fmt.Sprintf("upload_%d.bin", time.Now().UnixNano())
	}
	if err := os.MkdirAll(targetAbs, 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	dst := filepath.Join(targetAbs, fileName)
	if _, err := os.Stat(dst); err == nil && !overwrite {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "target file already exists"})
		return
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer out.Close()
	written, err := io.Copy(out, file)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if s.workspace != nil {
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	relPath := filepath.ToSlash(filepath.Join(targetRel, fileName))
	if !strings.HasPrefix(relPath, "/") {
		relPath = "/" + relPath
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"path":       relPath,
		"target_dir": targetRel,
		"bytes":      written,
		"overwrite":  overwrite,
	})
}

func (s *httpAPIServer) handleAdminStaticMove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	var req struct {
		FromPath   string `json:"from_path"`
		ToPath     string `json:"to_path"`
		SourcePath string `json:"source_path"`
		TargetDir  string `json:"target_dir"`
		NewName    string `json:"new_name"`
		Overwrite  bool   `json:"overwrite"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	sourcePath := strings.TrimSpace(req.SourcePath)
	if sourcePath == "" {
		sourcePath = strings.TrimSpace(req.FromPath)
	}
	fromAbs, fromRel, err := resolveStaticPath(root, sourcePath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	targetPath := strings.TrimSpace(req.ToPath)
	if strings.TrimSpace(req.TargetDir) != "" || strings.TrimSpace(req.NewName) != "" {
		targetDir := strings.TrimSpace(req.TargetDir)
		if targetDir == "" {
			targetDir = filepath.ToSlash(filepath.Dir(fromRel))
		}
		newName := sanitizeRecommendedFileName(strings.TrimSpace(req.NewName))
		if newName == "" {
			newName = filepath.Base(fromRel)
		}
		targetPath = filepath.ToSlash(filepath.Join(targetDir, newName))
		if !strings.HasPrefix(targetPath, "/") {
			targetPath = "/" + targetPath
		}
	}
	toAbs, toRel, err := resolveStaticPath(root, targetPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Stat(fromAbs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "source path not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Stat(toAbs); err == nil {
		if !req.Overwrite {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "target path already exists"})
			return
		}
		_ = os.RemoveAll(toAbs)
	}
	if err := os.MkdirAll(filepath.Dir(toAbs), 0o755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := os.Rename(fromAbs, toAbs); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.rewriteStaticPricePaths(fromAbs, toAbs); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if s.workspace != nil {
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"from_path": fromRel,
		"to_path":   toRel,
	})
}

func (s *httpAPIServer) handleAdminStaticEntry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	abs, rel, err := resolveStaticPath(root, r.URL.Query().Get("path"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	recursive := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "1") ||
		strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("recursive")), "true")
	st, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "path not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if st.IsDir() {
		if !recursive {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "directory delete requires recursive=true"})
			return
		}
		if err := os.RemoveAll(abs); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	} else {
		if err := os.Remove(abs); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	if s.workspace != nil {
		if _, err := s.workspace.SyncOnce(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "path": rel})
}

func (s *httpAPIServer) handleAdminStaticPriceSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	var req struct {
		Path                string `json:"path"`
		FloorPriceSatPer64K uint64 `json:"floor_price_sat_per_64k"`
		ResaleDiscountBPS   uint64 `json:"resale_discount_bps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	if req.FloorPriceSatPer64K == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "floor_price_sat_per_64k must be > 0"})
		return
	}
	if req.ResaleDiscountBPS == 0 {
		req.ResaleDiscountBPS = s.cfg.Seller.Pricing.ResaleDiscountBPS
	}
	if req.ResaleDiscountBPS > 10000 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "resale_discount_bps must be <= 10000"})
		return
	}
	abs, rel, err := resolveStaticPath(root, req.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	st, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "path not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if st.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "path is directory"})
		return
	}
	seedHash, unit, total, pricingBound, err := dbBindStaticPriceToSeed2(r.Context(), httpStore(s), abs, req.FloorPriceSatPer64K, req.ResaleDiscountBPS)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                           true,
		"path":                         rel,
		"seed_hash":                    seedHash,
		"floor_unit_price_sat_per_64k": req.FloorPriceSatPer64K,
		"resale_discount_bps":          req.ResaleDiscountBPS,
		"seed_price_satoshi":           total,
		"chunk_price_sat":              unit,
		"pricing_bound":                pricingBound,
	})
}

func (s *httpAPIServer) handleAdminStaticPriceGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	root := strings.TrimSpace(s.cfg.Storage.WorkspaceDir)
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace_dir not configured"})
		return
	}
	abs, rel, err := resolveStaticPath(root, r.URL.Query().Get("path"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	price, err := dbGetStaticFilePrice(r.Context(), httpStore(s), abs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "price not configured"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	seedHash, _ := dbGetWorkspaceFileSeedHash(r.Context(), httpStore(s), abs)
	writeJSON(w, http.StatusOK, map[string]any{
		"path":                         rel,
		"seed_hash":                    seedHash,
		"floor_unit_price_sat_per_64k": price.FloorPriceSatPer64K,
		"resale_discount_bps":          price.ResaleDiscountBPS,
		"price_updated_at_unix":        price.UpdatedAtUnix,
	})
}

func resolveStaticPath(root, input string) (string, string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", "", fmt.Errorf("workspace_dir not configured")
	}
	clean := pathClean(input)
	if clean == "/" {
		abs, err := resolveWorkspacePath(root, ".")
		if err != nil {
			return "", "", err
		}
		return abs, "/", nil
	}
	rel := strings.TrimPrefix(clean, "/")
	abs, err := resolveWorkspacePath(root, rel)
	if err != nil {
		return "", "", err
	}
	return abs, "/" + filepath.ToSlash(rel), nil
}

func (s *httpAPIServer) rewriteStaticPricePaths(fromAbs, toAbs string) error {
	return dbRewriteStaticPricePaths(context.Background(), httpStore(s), fromAbs, toAbs)
}

type adminConfigValueType string

const (
	adminConfigString adminConfigValueType = "string"
	adminConfigInt    adminConfigValueType = "int"
	adminConfigFloat  adminConfigValueType = "float"
	adminConfigBool   adminConfigValueType = "bool"
)

type adminConfigRule struct {
	Key         string
	Type        adminConfigValueType
	MinInt      int64
	MaxInt      int64
	MinFloat    float64
	MaxFloat    float64
	MinLen      int
	MaxLen      int
	Description string
}

func adminConfigRules() []adminConfigRule {
	return []adminConfigRule{
		{Key: "http.listen_addr", Type: adminConfigString, MinLen: 3, MaxLen: 128, Description: "管理 API 监听地址"},
		{Key: "fs_http.listen_addr", Type: adminConfigString, MinLen: 3, MaxLen: 128, Description: "文件 HTTP 监听地址"},
		{Key: "external_api.woc.api_key", Type: adminConfigString, MinLen: 0, MaxLen: 512, Description: "WOC API key（统一纳入外部 API 保护器）"},
		{Key: "external_api.woc.min_interval_ms", Type: adminConfigInt, MinInt: 1, MaxInt: 60000, Description: "WOC 最小请求间隔毫秒"},
		{Key: "listen.enabled", Type: adminConfigBool, Description: "是否启用监听费用池自动循环"},
		{Key: "listen.renew_threshold_seconds", Type: adminConfigInt, MinInt: 1, MaxInt: 86400, Description: "监听续费阈值秒"},
		{Key: "listen.auto_renew_rounds", Type: adminConfigInt, MinInt: 1, MaxInt: 1 << 20, Description: "监听自动续费轮数（统一配置，不区分测试网/主网）"},
		{Key: "listen.offer_payment_satoshi", Type: adminConfigInt, MinInt: 0, MaxInt: 1 << 40, Description: "监听续费每次向 gateway 主动提出的预算 sat"},
		{Key: "listen.tick_seconds", Type: adminConfigInt, MinInt: 1, MaxInt: 3600, Description: "监听循环调度周期秒"},
		{Key: "payment.preferred_scheme", Type: adminConfigString, MinLen: 1, MaxLen: 32, Description: "系统默认优先支付通道：pool_2of2_v1 或 chain_tx_v1"},
		{Key: "reachability.auto_announce_enabled", Type: adminConfigBool, Description: "是否自动发布本节点地址声明到 gateway 目录"},
		{Key: "reachability.announce_ttl_seconds", Type: adminConfigInt, MinInt: 60, MaxInt: 604800, Description: "地址声明有效期秒"},
		{Key: "scan.rescan_interval_seconds", Type: adminConfigInt, MinInt: 5, MaxInt: 86400, Description: "全量扫描间隔秒"},
		{Key: "storage.min_free_bytes", Type: adminConfigInt, MinInt: 0, MaxInt: 1 << 50, Description: "最小空闲空间"},
		{Key: "live.cache_max_bytes", Type: adminConfigInt, MinInt: 0, MaxInt: 1 << 50, Description: "直播缓存上限"},
		{Key: "live.publish.broadcast_window", Type: adminConfigInt, MinInt: 1, MaxInt: maxLiveWindowSize, Description: "直播广播窗口"},
		{Key: "live.publish.broadcast_interval_seconds", Type: adminConfigInt, MinInt: 1, MaxInt: 3600, Description: "直播广播间隔秒"},
		{Key: "seller.pricing.floor_price_sat_per_64k", Type: adminConfigInt, MinInt: 1, MaxInt: 1 << 40, Description: "静态底价"},
		{Key: "seller.pricing.live_base_price_sat_per_64k", Type: adminConfigInt, MinInt: 1, MaxInt: 1 << 40, Description: "直播基准价"},
		{Key: "seller.pricing.live_floor_price_sat_per_64k", Type: adminConfigInt, MinInt: 1, MaxInt: 1 << 40, Description: "直播底价"},
		{Key: "seller.pricing.live_decay_per_minute_bps", Type: adminConfigInt, MinInt: 0, MaxInt: 10000, Description: "直播每分钟衰减 bps"},
		{Key: "seller.pricing.resale_discount_bps", Type: adminConfigInt, MinInt: 0, MaxInt: 10000, Description: "转售折扣 bps"},
		// 浮点输入示例：用比例表达 bps，写入时转换为整数 bps。
		{Key: "seller.pricing.resale_discount_ratio", Type: adminConfigFloat, MinFloat: 0, MaxFloat: 1, Description: "转售折扣比例(0~1)"},
		{Key: "seller.enabled", Type: adminConfigBool, Description: "是否启用卖方模式"},
		{Key: "fs_http.strategy_debug_log_enabled", Type: adminConfigBool, Description: "策略调试日志开关"},
	}
}

func adminConfigRuleMap() map[string]adminConfigRule {
	out := make(map[string]adminConfigRule, len(adminConfigRules()))
	for _, r := range adminConfigRules() {
		out[r.Key] = r
	}
	return out
}

func (s *httpAPIServer) handleAdminConfigSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	rules := adminConfigRules()
	type item struct {
		Key         string  `json:"key"`
		Type        string  `json:"type"`
		MinInt      int64   `json:"min_int,omitempty"`
		MaxInt      int64   `json:"max_int,omitempty"`
		MinFloat    float64 `json:"min_float,omitempty"`
		MaxFloat    float64 `json:"max_float,omitempty"`
		MinLen      int     `json:"min_len,omitempty"`
		MaxLen      int     `json:"max_len,omitempty"`
		Description string  `json:"description,omitempty"`
	}
	out := make([]item, 0, len(rules))
	for _, it := range rules {
		out = append(out, item{
			Key:         it.Key,
			Type:        string(it.Type),
			MinInt:      it.MinInt,
			MaxInt:      it.MaxInt,
			MinFloat:    it.MinFloat,
			MaxFloat:    it.MaxFloat,
			MinLen:      it.MinLen,
			MaxLen:      it.MaxLen,
			Description: it.Description,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "total": len(out)})
}

func (s *httpAPIServer) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.rt == nil || s.cfg == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"config": adminConfigSnapshot(s.rt.runIn.toConfig())})
	case http.MethodPost:
		var req struct {
			ValidateOnly bool `json:"validate_only"`
			Items        []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		if len(req.Items) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "items is required"})
			return
		}
		next := s.rt.runIn
		nextCfg := next.toConfig()
		ruleMap := adminConfigRuleMap()
		applied := make([]string, 0, len(req.Items))
		for _, it := range req.Items {
			key := strings.TrimSpace(it.Key)
			rule, ok := ruleMap[key]
			if !ok {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unsupported key: " + key})
				return
			}
			if err := adminConfigApplyOne(&nextCfg, rule, it.Value); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "key": key})
				return
			}
			applied = append(applied, key)
		}
		// 全局校验兜底，避免单字段校验通过但组合非法。
		if err := validateConfig(&nextCfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		next.applyConfig(nextCfg)
		if req.ValidateOnly {
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":            true,
				"validate_only": true,
				"applied_keys":  applied,
				"config":        adminConfigSnapshot(nextCfg),
			})
			return
		}
		if err := SaveConfigFile(s.rt.runIn.ConfigPath, nextCfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		s.rt.runIn = next
		*s.cfg = next.toConfig()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":            true,
			"validate_only": false,
			"applied_keys":  applied,
			"config":        adminConfigSnapshot(nextCfg),
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func adminConfigSnapshot(cfg Config) map[string]any {
	return map[string]any{
		"http.listen_addr":                            cfg.HTTP.ListenAddr,
		"fs_http.listen_addr":                         cfg.FSHTTP.ListenAddr,
		"external_api.woc.api_key":                    maskSecretForAdminConfig(cfg.ExternalAPI.WOC.APIKey),
		"external_api.woc.min_interval_ms":            cfg.ExternalAPI.WOC.MinIntervalMS,
		"listen.enabled":                              cfgBool(cfg.Listen.Enabled, true),
		"listen.renew_threshold_seconds":              cfg.Listen.RenewThresholdSeconds,
		"listen.auto_renew_rounds":                    cfg.Listen.AutoRenewRounds,
		"listen.offer_payment_satoshi":                cfg.Listen.OfferPaymentSatoshi,
		"listen.tick_seconds":                         cfg.Listen.TickSeconds,
		"payment.preferred_scheme":                    cfg.Payment.PreferredScheme,
		"reachability.auto_announce_enabled":          cfgBool(cfg.Reachability.AutoAnnounceEnabled, true),
		"reachability.announce_ttl_seconds":           cfg.Reachability.AnnounceTTLSeconds,
		"scan.rescan_interval_seconds":                cfg.Scan.RescanIntervalSeconds,
		"storage.min_free_bytes":                      cfg.Storage.MinFreeBytes,
		"live.cache_max_bytes":                        cfg.Live.CacheMaxBytes,
		"live.publish.broadcast_window":               cfg.Live.Publish.BroadcastWindow,
		"live.publish.broadcast_interval_seconds":     cfg.Live.Publish.BroadcastIntervalSec,
		"seller.pricing.floor_price_sat_per_64k":      cfg.Seller.Pricing.FloorPriceSatPer64K,
		"seller.pricing.live_base_price_sat_per_64k":  cfg.Seller.Pricing.LiveBasePriceSatPer64K,
		"seller.pricing.live_floor_price_sat_per_64k": cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
		"seller.pricing.live_decay_per_minute_bps":    cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
		"seller.pricing.resale_discount_bps":          cfg.Seller.Pricing.ResaleDiscountBPS,
		"seller.pricing.resale_discount_ratio":        float64(cfg.Seller.Pricing.ResaleDiscountBPS) / 10000.0,
		"seller.enabled":                              cfg.Seller.Enabled,
		"fs_http.strategy_debug_log_enabled":          cfg.FSHTTP.StrategyDebugLogEnabled,
	}
}

func adminConfigApplyOne(cfg *Config, rule adminConfigRule, raw any) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	switch rule.Type {
	case adminConfigString:
		v, err := adminConfigAsString(raw)
		if err != nil {
			return err
		}
		l := len([]rune(v))
		if rule.MinLen > 0 && l < rule.MinLen {
			return fmt.Errorf("%s length must be >= %d", rule.Key, rule.MinLen)
		}
		if rule.MaxLen > 0 && l > rule.MaxLen {
			return fmt.Errorf("%s length must be <= %d", rule.Key, rule.MaxLen)
		}
		return adminConfigSetString(cfg, rule.Key, v)
	case adminConfigInt:
		v, err := adminConfigAsInt(raw)
		if err != nil {
			return err
		}
		if v < rule.MinInt || v > rule.MaxInt {
			return fmt.Errorf("%s must be between %d and %d", rule.Key, rule.MinInt, rule.MaxInt)
		}
		return adminConfigSetInt(cfg, rule.Key, v)
	case adminConfigFloat:
		v, err := adminConfigAsFloat(raw)
		if err != nil {
			return err
		}
		if v < rule.MinFloat || v > rule.MaxFloat {
			return fmt.Errorf("%s must be between %g and %g", rule.Key, rule.MinFloat, rule.MaxFloat)
		}
		return adminConfigSetFloat(cfg, rule.Key, v)
	case adminConfigBool:
		v, err := adminConfigAsBool(raw)
		if err != nil {
			return err
		}
		return adminConfigSetBool(cfg, rule.Key, v)
	default:
		return fmt.Errorf("unsupported value type: %s", rule.Type)
	}
}

func adminConfigAsString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("value must be string")
	}
	return strings.TrimSpace(s), nil
}

func adminConfigAsInt(v any) (int64, error) {
	switch x := v.(type) {
	case float64:
		if x != math.Trunc(x) {
			return 0, fmt.Errorf("value must be integer")
		}
		return int64(x), nil
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case json.Number:
		i, err := x.Int64()
		if err == nil {
			return i, nil
		}
		f, ferr := x.Float64()
		if ferr != nil || f != math.Trunc(f) {
			return 0, fmt.Errorf("value must be integer")
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("value must be integer")
	}
}

func adminConfigAsFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, fmt.Errorf("value must be float")
		}
		return f, nil
	default:
		return 0, fmt.Errorf("value must be float")
	}
}

func adminConfigAsBool(v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("value must be bool")
	}
	return b, nil
}

func adminConfigSetString(cfg *Config, key, v string) error {
	switch key {
	case "http.listen_addr":
		cfg.HTTP.ListenAddr = v
	case "fs_http.listen_addr":
		cfg.FSHTTP.ListenAddr = v
	case "external_api.woc.api_key":
		cfg.ExternalAPI.WOC.APIKey = v
	case "payment.preferred_scheme":
		scheme, err := normalizePreferredPaymentScheme(v)
		if err != nil {
			return err
		}
		cfg.Payment.PreferredScheme = scheme
	default:
		return fmt.Errorf("unsupported string key: %s", key)
	}
	return nil
}

func adminConfigSetInt(cfg *Config, key string, v int64) error {
	u := uint64(v)
	switch key {
	case "external_api.woc.min_interval_ms":
		cfg.ExternalAPI.WOC.MinIntervalMS = uint32(v)
	case "listen.renew_threshold_seconds":
		cfg.Listen.RenewThresholdSeconds = uint32(v)
	case "listen.auto_renew_rounds":
		cfg.Listen.AutoRenewRounds = u
	case "listen.offer_payment_satoshi":
		cfg.Listen.OfferPaymentSatoshi = u
	case "listen.tick_seconds":
		cfg.Listen.TickSeconds = uint32(v)
	case "reachability.announce_ttl_seconds":
		cfg.Reachability.AnnounceTTLSeconds = uint32(v)
	case "scan.rescan_interval_seconds":
		cfg.Scan.RescanIntervalSeconds = uint32(v)
	case "storage.min_free_bytes":
		cfg.Storage.MinFreeBytes = u
	case "live.cache_max_bytes":
		cfg.Live.CacheMaxBytes = u
	case "live.publish.broadcast_window":
		cfg.Live.Publish.BroadcastWindow = uint32(v)
	case "live.publish.broadcast_interval_seconds":
		cfg.Live.Publish.BroadcastIntervalSec = uint32(v)
	case "seller.pricing.floor_price_sat_per_64k":
		cfg.Seller.Pricing.FloorPriceSatPer64K = u
	case "seller.pricing.live_base_price_sat_per_64k":
		cfg.Seller.Pricing.LiveBasePriceSatPer64K = u
	case "seller.pricing.live_floor_price_sat_per_64k":
		cfg.Seller.Pricing.LiveFloorPriceSatPer64K = u
	case "seller.pricing.live_decay_per_minute_bps":
		cfg.Seller.Pricing.LiveDecayPerMinuteBPS = u
	case "seller.pricing.resale_discount_bps":
		cfg.Seller.Pricing.ResaleDiscountBPS = u
	default:
		return fmt.Errorf("unsupported int key: %s", key)
	}
	return nil
}

func adminConfigSetFloat(cfg *Config, key string, v float64) error {
	switch key {
	case "seller.pricing.resale_discount_ratio":
		cfg.Seller.Pricing.ResaleDiscountBPS = uint64(math.Round(v * 10000))
	default:
		return fmt.Errorf("unsupported float key: %s", key)
	}
	return nil
}

func adminConfigSetBool(cfg *Config, key string, v bool) error {
	switch key {
	case "listen.enabled":
		cfg.Listen.Enabled = boolPtr(v)
	case "reachability.auto_announce_enabled":
		cfg.Reachability.AutoAnnounceEnabled = boolPtr(v)
	case "seller.enabled":
		cfg.Seller.Enabled = v
	case "fs_http.strategy_debug_log_enabled":
		cfg.FSHTTP.StrategyDebugLogEnabled = v
	default:
		return fmt.Errorf("unsupported bool key: %s", key)
	}
	return nil
}

func boolPtr(v bool) *bool {
	b := v
	return &b
}

func maskSecretForAdminConfig(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "********"
	}
	return value[:4] + "..." + value[len(value)-4:]
}
