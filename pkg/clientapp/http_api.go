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
	"io/fs"
	"math"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/bsv8/BFTP/pkg/woc"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
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

type saleRecordEntry struct {
	SessionID          string
	SeedHash           string
	ChunkIndex         uint32
	UnitPriceSatPer64K uint64
	AmountSatoshi      uint64
	BuyerGatewayPeerID string
	ReleaseToken       string
}

type gatewayEventEntry struct {
	GatewayPeerID string
	Action        string
	MsgID         string
	SequenceNum   uint32
	PoolID        string
	AmountSatoshi int64
	Payload       any
}

type walletFundFlowEntry struct {
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

func appendTxHistory(db *sql.DB, e txHistoryEntry) {
	if db == nil {
		return
	}
	if strings.TrimSpace(e.GatewayPeerID) == "" {
		e.GatewayPeerID = "unknown"
	}
	if strings.TrimSpace(e.Direction) == "" {
		e.Direction = "info"
	}
	if strings.TrimSpace(e.Purpose) == "" {
		e.Purpose = e.EventType
	}
	_, err := db.Exec(
		`INSERT INTO tx_history(created_at_unix,gateway_peer_id,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		e.GatewayPeerID,
		e.EventType,
		e.Direction,
		e.AmountSatoshi,
		e.Purpose,
		e.Note,
		e.PoolID,
		e.MsgID,
		e.SequenceNum,
		e.CycleIndex,
	)
	if err != nil {
		obs.Error("bitcast-client", "tx_history_append_failed", map[string]any{"error": err.Error(), "event_type": e.EventType})
	}
}

func appendWalletFundFlow(db *sql.DB, e walletFundFlowEntry) {
	if db == nil {
		return
	}
	e.FlowID = strings.TrimSpace(e.FlowID)
	if e.FlowID == "" {
		e.FlowID = "unknown"
	}
	e.FlowType = strings.TrimSpace(e.FlowType)
	if e.FlowType == "" {
		e.FlowType = "unknown"
	}
	e.RefID = strings.TrimSpace(e.RefID)
	e.Stage = strings.TrimSpace(e.Stage)
	if e.Stage == "" {
		e.Stage = "unknown"
	}
	e.Direction = strings.TrimSpace(e.Direction)
	if e.Direction == "" {
		e.Direction = "unknown"
	}
	e.Purpose = strings.TrimSpace(e.Purpose)
	if e.Purpose == "" {
		e.Purpose = "unknown"
	}
	payload := "{}"
	if e.Payload != nil {
		if b, err := json.Marshal(e.Payload); err == nil {
			payload = string(b)
		}
	}
	_, err := db.Exec(
		`INSERT INTO wallet_fund_flows(
			created_at_unix,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		e.FlowID,
		e.FlowType,
		e.RefID,
		e.Stage,
		e.Direction,
		e.Purpose,
		e.AmountSatoshi,
		e.UsedSatoshi,
		e.ReturnedSatoshi,
		strings.TrimSpace(e.RelatedTxID),
		e.Note,
		payload,
	)
	if err != nil {
		obs.Error("bitcast-client", "wallet_fund_flow_append_failed", map[string]any{
			"error":   err.Error(),
			"flow_id": e.FlowID,
			"stage":   e.Stage,
		})
	}
}

func appendWalletLedgerEntry(db *sql.DB, e walletLedgerEntry) {
	if db == nil {
		return
	}
	e.TxID = strings.ToLower(strings.TrimSpace(e.TxID))
	if e.TxID == "" {
		e.TxID = "unknown"
	}
	e.Direction = strings.ToUpper(strings.TrimSpace(e.Direction))
	if e.Direction == "" {
		e.Direction = "UNKNOWN"
	}
	e.Category = strings.ToUpper(strings.TrimSpace(e.Category))
	if e.Category == "" {
		e.Category = "UNKNOWN"
	}
	e.Status = strings.ToUpper(strings.TrimSpace(e.Status))
	if e.Status == "" {
		e.Status = "UNKNOWN"
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	payload := "{}"
	if e.Payload != nil {
		if b, err := json.Marshal(e.Payload); err == nil {
			payload = string(b)
		}
	}
	_, err := db.Exec(
		`INSERT INTO wallet_ledger_entries(
			created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		e.TxID,
		e.Direction,
		e.Category,
		e.AmountSatoshi,
		strings.TrimSpace(e.CounterpartyLabel),
		e.Status,
		e.BlockHeight,
		e.OccurredAtUnix,
		strings.TrimSpace(e.RawRefID),
		e.Note,
		payload,
	)
	if err != nil {
		obs.Error("bitcast-client", "wallet_ledger_entry_append_failed", map[string]any{
			"error":     err.Error(),
			"txid":      e.TxID,
			"direction": e.Direction,
			"category":  e.Category,
		})
	}
}

func appendGatewayEvent(db *sql.DB, e gatewayEventEntry) {
	if db == nil {
		return
	}
	if strings.TrimSpace(e.GatewayPeerID) == "" {
		e.GatewayPeerID = "unknown"
	}
	if strings.TrimSpace(e.Action) == "" {
		e.Action = "unknown"
	}
	payload := "{}"
	if e.Payload != nil {
		if b, err := json.Marshal(e.Payload); err == nil {
			payload = string(b)
		}
	}
	_, err := db.Exec(
		`INSERT INTO gateway_events(created_at_unix,gateway_peer_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json) VALUES(?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		e.GatewayPeerID,
		e.Action,
		e.MsgID,
		e.SequenceNum,
		e.PoolID,
		e.AmountSatoshi,
		payload,
	)
	if err != nil {
		obs.Error("bitcast-client", "gateway_event_append_failed", map[string]any{"error": err.Error(), "action": e.Action})
	}
}

func appendSaleRecord(db *sql.DB, e saleRecordEntry) {
	if db == nil {
		return
	}
	_, err := db.Exec(
		`INSERT INTO sale_records(created_at_unix,session_id,seed_hash,chunk_index,unit_price_sat_per_64k,amount_satoshi,buyer_gateway_peer_id,release_token) VALUES(?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		e.SessionID,
		e.SeedHash,
		e.ChunkIndex,
		e.UnitPriceSatPer64K,
		e.AmountSatoshi,
		e.BuyerGatewayPeerID,
		e.ReleaseToken,
	)
	if err != nil {
		obs.Error("bitcast-client", "sale_record_append_failed", map[string]any{"error": err.Error(), "seed_hash": e.SeedHash})
	}
}

type httpAPIServer struct {
	rt        *Runtime
	cfg       *Config
	db        *sql.DB
	h         host.Host
	gateways  []peer.AddrInfo
	workspace *workspaceManager
	srv       *http.Server
	startedAt time.Time
	jobsMu    sync.RWMutex
	getJobs   map[string]*fileGetJob
	webAssets fs.FS
	rpcTrace  p2prpc.TraceSink

	ledgerSyncMu       sync.Mutex
	ledgerLastSyncUnix int64
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
	GatewayPeerID   string             `json:"gateway_peer_id"`
	Status          string             `json:"status"`
	CancelRequested bool               `json:"cancel_requested,omitempty"`
	StartedAtUnix   int64              `json:"started_at_unix"`
	EndedAtUnix     int64              `json:"ended_at_unix,omitempty"`
	OutputFilePath  string             `json:"output_file_path,omitempty"`
	Error           string             `json:"error,omitempty"`
	Steps           []fileGetStep      `json:"steps"`
	cancel          context.CancelFunc `json:"-"`
}

func newHTTPAPIServer(rt *Runtime, cfg *Config, db *sql.DB, h host.Host, gateways []peer.AddrInfo, workspace *workspaceManager, webAssets fs.FS, trace p2prpc.TraceSink) *httpAPIServer {
	return &httpAPIServer{
		rt:        rt,
		cfg:       cfg,
		db:        db,
		h:         h,
		gateways:  gateways,
		workspace: workspace,
		startedAt: time.Now(),
		getJobs:   map[string]*fileGetJob{},
		webAssets: webAssets,
		rpcTrace:  trace,
	}
}

func (s *httpAPIServer) Start() error {
	mux := http.NewServeMux()
	registerAPI := func(prefix string) {
		mux.HandleFunc(prefix+"/v1/bootstrap/status", s.handleBootstrapStatus)
		mux.HandleFunc(prefix+"/v1/bootstrap/token", s.handleBootstrapToken)
		mux.HandleFunc(prefix+"/v1/info", s.withAuth(s.handleInfo))
		mux.HandleFunc(prefix+"/v1/balance", s.withAuth(s.handleBalance))
		mux.HandleFunc(prefix+"/v1/wallet/summary", s.withAuth(s.handleWalletSummary))
		mux.HandleFunc(prefix+"/v1/wallet/ledger", s.withAuth(s.handleWalletLedger))
		mux.HandleFunc(prefix+"/v1/wallet/ledger/detail", s.withAuth(s.handleWalletLedgerDetail))
		mux.HandleFunc(prefix+"/v1/wallet/fund-flows", s.withAuth(s.handleWalletFundFlows))
		mux.HandleFunc(prefix+"/v1/wallet/fund-flows/detail", s.withAuth(s.handleWalletFundFlowDetail))
		mux.HandleFunc(prefix+"/v1/direct/quotes", s.withAuth(s.handleDirectQuotes))
		mux.HandleFunc(prefix+"/v1/direct/quotes/detail", s.withAuth(s.handleDirectQuoteDetail))
		mux.HandleFunc(prefix+"/v1/direct/deals", s.withAuth(s.handleDirectDeals))
		mux.HandleFunc(prefix+"/v1/direct/deals/detail", s.withAuth(s.handleDirectDealDetail))
		mux.HandleFunc(prefix+"/v1/direct/sessions", s.withAuth(s.handleDirectSessions))
		mux.HandleFunc(prefix+"/v1/direct/sessions/detail", s.withAuth(s.handleDirectSessionDetail))
		mux.HandleFunc(prefix+"/v1/direct/transfer-pools", s.withAuth(s.handleDirectTransferPools))
		mux.HandleFunc(prefix+"/v1/direct/transfer-pools/detail", s.withAuth(s.handleDirectTransferPoolDetail))
		mux.HandleFunc(prefix+"/v1/transactions", s.withAuth(s.handleTransactions))
		mux.HandleFunc(prefix+"/v1/transactions/detail", s.withAuth(s.handleTransactionDetail))
		mux.HandleFunc(prefix+"/v1/sales", s.withAuth(s.handleSales))
		mux.HandleFunc(prefix+"/v1/sales/detail", s.withAuth(s.handleSaleDetail))
		mux.HandleFunc(prefix+"/v1/gateways/events", s.withAuth(s.handleGatewayEvents))
		mux.HandleFunc(prefix+"/v1/gateways/events/detail", s.withAuth(s.handleGatewayEventDetail))
		mux.HandleFunc(prefix+"/v1/files/get-file", s.withAuth(s.handleGetFileStart))
		mux.HandleFunc(prefix+"/v1/files/get-file/job", s.withAuth(s.handleGetFileJob))
		mux.HandleFunc(prefix+"/v1/files/get-file/jobs", s.withAuth(s.handleGetFileJobs))
		mux.HandleFunc(prefix+"/v1/files/get-file/cancel", s.withAuth(s.handleGetFileCancel))
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
		mux.HandleFunc(prefix+"/v1/admin/config", s.withAuth(s.handleAdminConfig))
		mux.HandleFunc(prefix+"/v1/admin/config/schema", s.withAuth(s.handleAdminConfigSchema))
	}
	registerAPI("/api")
	registerAPI("")

	var sub fs.FS
	if s.webAssets != nil {
		var err error
		sub, err = fs.Sub(s.webAssets, "web")
		if err != nil {
			return err
		}
	}
	serveAsset := func(w http.ResponseWriter, r *http.Request, name string) bool {
		if sub == nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
			return false
		}
		b, readErr := fs.ReadFile(sub, name)
		if readErr != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "asset not found"})
			return false
		}
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		if ct := mime.TypeByExtension(filepath.Ext(name)); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
		return true
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" || r.URL.Path == "/api/" || strings.HasPrefix(r.URL.Path, "/api/") {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "api not found"})
			return
		}
		if r.URL.Path == "/" {
			_ = serveAsset(w, r, "index.html")
			return
		}
		name := strings.TrimPrefix(pathClean(r.URL.Path), "/")
		if name == "" || strings.Contains(name, "..") {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
			return
		}
		_ = serveAsset(w, r, name)
	})

	s.srv = &http.Server{
		Addr:              s.cfg.HTTP.ListenAddr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	obs.Important("bitcast-client", "http_api_started", map[string]any{"listen_addr": s.cfg.HTTP.ListenAddr})
	err := s.srv.ListenAndServe()
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
		token := strings.TrimSpace(s.cfg.HTTP.AuthToken)
		if token == "" {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "token is not initialized"})
			return
		}
		if !tokenAuthorized(r, token) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func tokenAuthorized(r *http.Request, token string) bool {
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authz), "bearer ") && strings.TrimSpace(authz[7:]) == token {
		return true
	}
	return strings.TrimSpace(r.Header.Get("X-API-Token")) == token
}

func (s *httpAPIServer) handleBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"needs_bootstrap": strings.TrimSpace(s.cfg.HTTP.AuthToken) == "",
	})
}

func (s *httpAPIServer) handleBootstrapToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil || s.cfg == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	currentToken := strings.TrimSpace(s.cfg.HTTP.AuthToken)
	if currentToken == "" && !requestFromLoopback(r) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "bootstrap is allowed only from loopback"})
		return
	}
	if currentToken != "" && !tokenAuthorized(r, currentToken) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	var req struct {
		AuthToken string `json:"auth_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	token := strings.TrimSpace(req.AuthToken)
	if token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "auth_token is required"})
		return
	}
	cfg := s.rt.Config
	cfg.HTTP.AuthToken = token
	if err := SaveConfigInDB(s.db, cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	s.rt.Config = cfg
	s.cfg.HTTP.AuthToken = token
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                  true,
		"bootstrap_completed": currentToken == "",
		"updated":             currentToken != "",
	})
}

func requestFromLoopback(r *http.Request) bool {
	if r == nil {
		return false
	}
	host := strings.TrimSpace(r.RemoteAddr)
	if host == "" {
		return true
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *httpAPIServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"client_id":           s.cfg.ClientID,
		"peer_id":             s.h.ID().String(),
		"pubkey_hex":          s.cfg.ClientID,
		"seller_enabled":      s.cfg.Seller.Enabled,
		"workspace_dir":       s.cfg.Storage.WorkspaceDir,
		"data_dir":            s.cfg.Storage.DataDir,
		"gateway_count":       len(s.gateways),
		"arbiter_count":       len(s.cfg.Network.Arbiters),
		"rescan_interval_sec": s.cfg.Scan.RescanIntervalSeconds,
		"started_at_unix":     s.startedAt.Unix(),
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
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	s.syncWalletLedgerBestEffort(r.Context())
	var flowCount, txCount, saleCount, gatewayEventCount int64
	var totalIn, totalOut, totalUsed, totalReturned int64
	var ledgerCount, ledgerIn, ledgerOut int64
	if err := s.db.QueryRow(`SELECT COUNT(1),COALESCE(SUM(CASE WHEN amount_satoshi>0 THEN amount_satoshi ELSE 0 END),0),COALESCE(SUM(CASE WHEN amount_satoshi<0 THEN -amount_satoshi ELSE 0 END),0),COALESCE(SUM(used_satoshi),0),COALESCE(SUM(returned_satoshi),0) FROM wallet_fund_flows`).Scan(&flowCount, &totalIn, &totalOut, &totalUsed, &totalReturned); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.db.QueryRow(`SELECT COUNT(1),COALESCE(SUM(CASE WHEN direction='IN' THEN amount_satoshi ELSE 0 END),0),COALESCE(SUM(CASE WHEN direction='OUT' THEN amount_satoshi ELSE 0 END),0) FROM wallet_ledger_entries`).Scan(&ledgerCount, &ledgerIn, &ledgerOut); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM tx_history`).Scan(&txCount); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM sale_records`).Scan(&saleCount); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM gateway_events`).Scan(&gatewayEventCount); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	walletAddr := ""
	onchainBal := int64(0)
	onchainBalErr := ""
	if s != nil && s.rt != nil {
		addr, bal, err := walletAddressAndOnchainBalance(s.rt)
		walletAddr = addr
		onchainBal = int64(bal)
		if err != nil {
			onchainBalErr = err.Error()
		}
	}
	resp := map[string]any{
		"flow_count":               flowCount,
		"tx_event_count":           txCount,
		"sale_count":               saleCount,
		"gateway_event_count":      gatewayEventCount,
		"total_in_satoshi":         totalIn,
		"total_out_satoshi":        totalOut,
		"total_used_satoshi":       totalUsed,
		"total_returned_satoshi":   totalReturned,
		"net_spent_satoshi":        totalUsed - totalReturned,
		"net_amount_delta_satoshi": totalIn - totalOut,
		"ledger_count":             ledgerCount,
		"ledger_total_in_satoshi":  ledgerIn,
		"ledger_total_out_satoshi": ledgerOut,
		"ledger_net_satoshi":       ledgerIn - ledgerOut,
		"wallet_address":           walletAddr,
		"onchain_balance_satoshi":  onchainBal,
		"balance_source":           "onchain_realtime",
	}
	if onchainBalErr != "" {
		resp["onchain_balance_error"] = onchainBalErr
	}
	writeJSON(w, http.StatusOK, resp)
}

func walletAddressAndOnchainBalance(rt *Runtime) (string, uint64, error) {
	if rt == nil || rt.Chain == nil {
		return "", 0, fmt.Errorf("runtime chain not initialized")
	}
	actor, err := buildClientActorFromConfig(rt.Config)
	if err != nil {
		return "", 0, err
	}
	utxos, err := rt.Chain.GetUTXOs(actor.Addr)
	if err != nil {
		return actor.Addr, 0, err
	}
	var sum uint64
	for _, u := range utxos {
		sum += u.Value
	}
	return actor.Addr, sum, nil
}

type walletHistoryClient interface {
	GetAddressHistory(address string) ([]woc.AddressHistoryItem, error)
	GetTxDetail(txid string) (woc.TxDetail, error)
}

func (s *httpAPIServer) syncWalletLedgerBestEffort(ctx context.Context) {
	if s == nil || s.db == nil || s.rt == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ledgerSyncMu.Lock()
	defer s.ledgerSyncMu.Unlock()
	now := time.Now().Unix()
	if now-s.ledgerLastSyncUnix < 15 {
		return
	}
	if err := s.syncWalletLedgerFromChain(ctx); err != nil {
		obs.Error("bitcast-client", "wallet_ledger_sync_failed", map[string]any{"error": err.Error()})
		return
	}
	s.ledgerLastSyncUnix = now
}

func (s *httpAPIServer) syncWalletLedgerFromChain(ctx context.Context) error {
	if s == nil || s.rt == nil || s.db == nil {
		return fmt.Errorf("runtime not initialized")
	}
	hc, ok := s.rt.Chain.(walletHistoryClient)
	if !ok {
		return fmt.Errorf("chain backend does not support history api")
	}
	actor, err := buildClientActorFromConfig(s.rt.Config)
	if err != nil {
		return err
	}
	isMainnet := strings.ToLower(strings.TrimSpace(s.rt.Config.BSV.Network)) == "main"
	clientAddr, err := kmlibs.GetAddressFromPubKey(actor.PubKey, isMainnet)
	if err != nil {
		return err
	}
	lockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return err
	}
	walletLockHex := strings.ToLower(hex.EncodeToString(lockScript.Bytes()))

	history, err := hc.GetAddressHistory(actor.Addr)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return nil
	}
	fundMap, err := buildFundFlowCategoryMap(s.db)
	if err != nil {
		return err
	}
	cache := map[string]woc.TxDetail{}
	for _, it := range history {
		txid := strings.ToLower(strings.TrimSpace(it.TxID))
		if txid == "" {
			continue
		}
		txj, ok := cache[txid]
		if !ok {
			txj, err = hc.GetTxDetail(txid)
			if err != nil {
				continue
			}
			cache[txid] = txj
		}
		inSat, outSat, err := calcWalletTxAmount(txj, walletLockHex, hc, cache)
		if err != nil {
			continue
		}
		net := outSat - inSat
		if net == 0 {
			continue
		}
		direction := "IN"
		amount := net
		if net < 0 {
			direction = "OUT"
			amount = -net
		}
		category := classifyWalletLedgerCategory(net, inSat, fundMap[txid])
		status := "MEMPOOL"
		if it.Height > 0 {
			status = "CONFIRMED"
		}
		occurredAt := time.Now().Unix()
		payload := map[string]any{
			"wallet_input_satoshi":  inSat,
			"wallet_output_satoshi": outSat,
			"net_amount_satoshi":    net,
			"fund_flow_hint":        fundMap[txid],
			"block_height":          it.Height,
		}
		if err := upsertWalletChainRaw(s.db, txid, it.Height, status, occurredAt, inSat, outSat, net, category, payload); err != nil {
			return err
		}
		if err := upsertWalletLedgerByTxID(s.db, walletLedgerEntry{
			TxID:           txid,
			Direction:      direction,
			Category:       category,
			AmountSatoshi:  amount,
			Status:         status,
			BlockHeight:    it.Height,
			OccurredAtUnix: occurredAt,
			RawRefID:       "wallet_chain_tx_raw:" + txid,
			Payload:        payload,
		}); err != nil {
			return err
		}
	}
	return nil
}

func buildFundFlowCategoryMap(db *sql.DB) (map[string][]string, error) {
	out := map[string][]string{}
	rows, err := db.Query(`SELECT related_txid,flow_type,purpose,returned_satoshi FROM wallet_fund_flows WHERE related_txid<>''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var txid, flowType, purpose string
		var returned int64
		if err := rows.Scan(&txid, &flowType, &purpose, &returned); err != nil {
			return nil, err
		}
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			continue
		}
		hints := out[txid]
		if strings.EqualFold(strings.TrimSpace(flowType), "fee_pool") {
			hints = append(hints, "fee_pool")
		}
		if strings.Contains(strings.ToLower(strings.TrimSpace(purpose)), "close") && returned > 0 {
			hints = append(hints, "repayment")
		}
		out[txid] = hints
	}
	return out, nil
}

func calcWalletTxAmount(txj woc.TxDetail, walletLockHex string, hc walletHistoryClient, cache map[string]woc.TxDetail) (int64, int64, error) {
	var walletOutSat int64
	for _, out := range txj.Vout {
		if strings.EqualFold(strings.TrimSpace(out.ScriptPubKey.Hex), walletLockHex) {
			walletOutSat += bsvToSatoshi(out.Value)
		}
	}
	var walletInSat int64
	for _, in := range txj.Vin {
		prevID := strings.ToLower(strings.TrimSpace(in.TxID))
		if prevID == "" {
			continue
		}
		prev, ok := cache[prevID]
		if !ok {
			var err error
			prev, err = hc.GetTxDetail(prevID)
			if err != nil {
				return 0, 0, err
			}
			cache[prevID] = prev
		}
		prevOut, ok := txOutputByN(prev, in.Vout)
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(prevOut.ScriptPubKey.Hex), walletLockHex) {
			walletInSat += bsvToSatoshi(prevOut.Value)
		}
	}
	return walletInSat, walletOutSat, nil
}

func txOutputByN(txj woc.TxDetail, n uint32) (woc.TxOutput, bool) {
	for _, out := range txj.Vout {
		if out.N == n {
			return out, true
		}
	}
	return woc.TxOutput{}, false
}

func bsvToSatoshi(v float64) int64 {
	return int64(math.Round(v * 100_000_000))
}

func classifyWalletLedgerCategory(net, walletInSat int64, hints []string) string {
	for _, h := range hints {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "fee_pool":
			return "FEE_POOL"
		case "repayment":
			if net > 0 {
				return "REPAYMENT"
			}
		}
	}
	if net > 0 && walletInSat > 0 {
		return "CHANGE"
	}
	if net > 0 {
		return "REPAYMENT"
	}
	if net < 0 {
		return "THIRD_PARTY"
	}
	return "UNKNOWN"
}

func upsertWalletChainRaw(db *sql.DB, txid string, blockHeight int64, status string, occurredAt int64, inSat, outSat, net int64, category string, payload any) error {
	rawPayload := "{}"
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			rawPayload = string(b)
		}
	}
	_, err := db.Exec(
		`INSERT INTO wallet_chain_tx_raw(
			txid,block_height,status,occurred_at_unix,wallet_input_satoshi,wallet_output_satoshi,net_amount_satoshi,category,payload_json,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(txid) DO UPDATE SET
			block_height=excluded.block_height,
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			wallet_input_satoshi=excluded.wallet_input_satoshi,
			wallet_output_satoshi=excluded.wallet_output_satoshi,
			net_amount_satoshi=excluded.net_amount_satoshi,
			category=excluded.category,
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		txid, blockHeight, status, occurredAt, inSat, outSat, net, category, rawPayload, time.Now().Unix(),
	)
	return err
}

func upsertWalletLedgerByTxID(db *sql.DB, e walletLedgerEntry) error {
	e.TxID = strings.ToLower(strings.TrimSpace(e.TxID))
	if e.TxID == "" {
		return fmt.Errorf("txid required")
	}
	payload := "{}"
	if e.Payload != nil {
		if b, err := json.Marshal(e.Payload); err == nil {
			payload = string(b)
		}
	}
	var id int64
	err := db.QueryRow(`SELECT id FROM wallet_ledger_entries WHERE txid=? ORDER BY id DESC LIMIT 1`, e.TxID).Scan(&id)
	now := time.Now().Unix()
	if err == nil {
		_, err = db.Exec(
			`UPDATE wallet_ledger_entries SET created_at_unix=?,direction=?,category=?,amount_satoshi=?,counterparty_label=?,status=?,block_height=?,occurred_at_unix=?,raw_ref_id=?,note=?,payload_json=? WHERE id=?`,
			now, strings.ToUpper(strings.TrimSpace(e.Direction)), strings.ToUpper(strings.TrimSpace(e.Category)), e.AmountSatoshi, strings.TrimSpace(e.CounterpartyLabel), strings.ToUpper(strings.TrimSpace(e.Status)), e.BlockHeight, e.OccurredAtUnix, strings.TrimSpace(e.RawRefID), e.Note, payload, id,
		)
		return err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	appendWalletLedgerEntry(db, e)
	return nil
}

func (s *httpAPIServer) handleWalletLedger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	s.syncWalletLedgerBestEffort(r.Context())
	direction := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("direction")))
	category := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("category")))
	status := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))
	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	type argsT struct {
		args  []any
		where string
	}
	build := argsT{args: []any{}}
	if direction != "" {
		build.where += " AND direction=?"
		build.args = append(build.args, direction)
	}
	if category != "" {
		build.where += " AND category=?"
		build.args = append(build.args, category)
	}
	if status != "" {
		build.where += " AND status=?"
		build.args = append(build.args, status)
	}
	if txid != "" {
		build.where += " AND txid=?"
		build.args = append(build.args, txid)
	}
	if q != "" {
		build.where += " AND (txid LIKE ? OR counterparty_label LIKE ? OR note LIKE ? OR raw_ref_id LIKE ?)"
		like := "%" + q + "%"
		build.args = append(build.args, like, like, like, like)
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM wallet_ledger_entries WHERE 1=1"+build.where, build.args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(
		`SELECT id,created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
		FROM wallet_ledger_entries WHERE 1=1`+build.where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
		append(build.args, limit, offset)...,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type item struct {
		ID                int64           `json:"id"`
		CreatedAtUnix     int64           `json:"created_at_unix"`
		TxID              string          `json:"txid"`
		Direction         string          `json:"direction"`
		Category          string          `json:"category"`
		AmountSatoshi     int64           `json:"amount_satoshi"`
		CounterpartyLabel string          `json:"counterparty_label"`
		Status            string          `json:"status"`
		BlockHeight       int64           `json:"block_height"`
		OccurredAtUnix    int64           `json:"occurred_at_unix"`
		RawRefID          string          `json:"raw_ref_id"`
		Note              string          `json:"note"`
		Payload           json.RawMessage `json:"payload"`
	}
	items := make([]item, 0, limit)
	for rows.Next() {
		var it item
		var payload string
		if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.TxID, &it.Direction, &it.Category, &it.AmountSatoshi, &it.CounterpartyLabel, &it.Status, &it.BlockHeight, &it.OccurredAtUnix, &it.RawRefID, &it.Note, &payload); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "limit": limit, "offset": offset, "items": items})
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
	s.syncWalletLedgerBestEffort(r.Context())
	type item struct {
		ID                int64           `json:"id"`
		CreatedAtUnix     int64           `json:"created_at_unix"`
		TxID              string          `json:"txid"`
		Direction         string          `json:"direction"`
		Category          string          `json:"category"`
		AmountSatoshi     int64           `json:"amount_satoshi"`
		CounterpartyLabel string          `json:"counterparty_label"`
		Status            string          `json:"status"`
		BlockHeight       int64           `json:"block_height"`
		OccurredAtUnix    int64           `json:"occurred_at_unix"`
		RawRefID          string          `json:"raw_ref_id"`
		Note              string          `json:"note"`
		Payload           json.RawMessage `json:"payload"`
	}
	var it item
	var payload string
	err := s.db.QueryRow(
		`SELECT id,created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
		FROM wallet_ledger_entries WHERE id=?`, id,
	).Scan(&it.ID, &it.CreatedAtUnix, &it.TxID, &it.Direction, &it.Category, &it.AmountSatoshi, &it.CounterpartyLabel, &it.Status, &it.BlockHeight, &it.OccurredAtUnix, &it.RawRefID, &it.Note, &payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	it.Payload = json.RawMessage(payload)
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
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	type argsT struct {
		args  []any
		where string
	}
	build := argsT{args: []any{}}
	if flowID != "" {
		build.where += " AND flow_id=?"
		build.args = append(build.args, flowID)
	}
	if flowType != "" {
		build.where += " AND flow_type=?"
		build.args = append(build.args, flowType)
	}
	if refID != "" {
		build.where += " AND ref_id=?"
		build.args = append(build.args, refID)
	}
	if stage != "" {
		build.where += " AND stage=?"
		build.args = append(build.args, stage)
	}
	if direction != "" {
		build.where += " AND direction=?"
		build.args = append(build.args, direction)
	}
	if purpose != "" {
		build.where += " AND purpose=?"
		build.args = append(build.args, purpose)
	}
	if relatedTxID != "" {
		build.where += " AND related_txid=?"
		build.args = append(build.args, relatedTxID)
	}
	if q != "" {
		build.where += " AND (flow_id LIKE ? OR ref_id LIKE ? OR note LIKE ? OR related_txid LIKE ?)"
		like := "%" + q + "%"
		build.args = append(build.args, like, like, like, like)
	}

	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM wallet_fund_flows WHERE 1=1"+build.where, build.args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(
		`SELECT id,created_at_unix,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json FROM wallet_fund_flows WHERE 1=1`+build.where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
		append(build.args, limit, offset)...,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type flowItem struct {
		ID              int64           `json:"id"`
		CreatedAtUnix   int64           `json:"created_at_unix"`
		FlowID          string          `json:"flow_id"`
		FlowType        string          `json:"flow_type"`
		RefID           string          `json:"ref_id"`
		Stage           string          `json:"stage"`
		Direction       string          `json:"direction"`
		Purpose         string          `json:"purpose"`
		AmountSatoshi   int64           `json:"amount_satoshi"`
		UsedSatoshi     int64           `json:"used_satoshi"`
		ReturnedSatoshi int64           `json:"returned_satoshi"`
		RelatedTxID     string          `json:"related_txid"`
		Note            string          `json:"note"`
		Payload         json.RawMessage `json:"payload"`
	}
	items := make([]flowItem, 0, limit)
	for rows.Next() {
		var it flowItem
		var payload string
		if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.FlowID, &it.FlowType, &it.RefID, &it.Stage, &it.Direction, &it.Purpose, &it.AmountSatoshi, &it.UsedSatoshi, &it.ReturnedSatoshi, &it.RelatedTxID, &it.Note, &payload); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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
	type flowItem struct {
		ID              int64           `json:"id"`
		CreatedAtUnix   int64           `json:"created_at_unix"`
		FlowID          string          `json:"flow_id"`
		FlowType        string          `json:"flow_type"`
		RefID           string          `json:"ref_id"`
		Stage           string          `json:"stage"`
		Direction       string          `json:"direction"`
		Purpose         string          `json:"purpose"`
		AmountSatoshi   int64           `json:"amount_satoshi"`
		UsedSatoshi     int64           `json:"used_satoshi"`
		ReturnedSatoshi int64           `json:"returned_satoshi"`
		RelatedTxID     string          `json:"related_txid"`
		Note            string          `json:"note"`
		Payload         json.RawMessage `json:"payload"`
	}
	var it flowItem
	var payload string
	err := s.db.QueryRow(`SELECT id,created_at_unix,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json FROM wallet_fund_flows WHERE id=?`, id).
		Scan(&it.ID, &it.CreatedAtUnix, &it.FlowID, &it.FlowType, &it.RefID, &it.Stage, &it.Direction, &it.Purpose, &it.AmountSatoshi, &it.UsedSatoshi, &it.ReturnedSatoshi, &it.RelatedTxID, &it.Note, &payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	it.Payload = json.RawMessage(payload)
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
	sellerPeerID := strings.TrimSpace(r.URL.Query().Get("seller_peer_id"))
	buildWhere := ""
	args := []any{}
	if demandID != "" {
		buildWhere += " AND demand_id=?"
		args = append(args, demandID)
	}
	if sellerPeerID != "" {
		buildWhere += " AND seller_peer_id=?"
		args = append(args, sellerPeerID)
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM direct_quotes WHERE 1=1"+buildWhere, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`SELECT id,demand_id,seller_peer_id,seed_price,chunk_price,expires_at_unix,recommended_file_name,available_chunk_bitmap_hex,seller_arbiter_peer_ids_json,created_at_unix FROM direct_quotes WHERE 1=1`+buildWhere+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type quoteItem struct {
		ID                      int64           `json:"id"`
		DemandID                string          `json:"demand_id"`
		SellerPeerID            string          `json:"seller_peer_id"`
		SeedPrice               uint64          `json:"seed_price"`
		ChunkPrice              uint64          `json:"chunk_price"`
		ExpiresAtUnix           int64           `json:"expires_at_unix"`
		RecommendedFileName     string          `json:"recommended_file_name"`
		AvailableChunkBitmapHex string          `json:"available_chunk_bitmap_hex"`
		SellerArbiterPeerIDs    json.RawMessage `json:"seller_arbiter_peer_ids"`
		CreatedAtUnix           int64           `json:"created_at_unix"`
	}
	items := make([]quoteItem, 0, limit)
	for rows.Next() {
		var it quoteItem
		var arbiterIDs string
		if err := rows.Scan(&it.ID, &it.DemandID, &it.SellerPeerID, &it.SeedPrice, &it.ChunkPrice, &it.ExpiresAtUnix, &it.RecommendedFileName, &it.AvailableChunkBitmapHex, &arbiterIDs, &it.CreatedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		it.SellerArbiterPeerIDs = json.RawMessage(arbiterIDs)
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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
	type quoteItem struct {
		ID                      int64           `json:"id"`
		DemandID                string          `json:"demand_id"`
		SellerPeerID            string          `json:"seller_peer_id"`
		SeedPrice               uint64          `json:"seed_price"`
		ChunkPrice              uint64          `json:"chunk_price"`
		ExpiresAtUnix           int64           `json:"expires_at_unix"`
		RecommendedFileName     string          `json:"recommended_file_name"`
		AvailableChunkBitmapHex string          `json:"available_chunk_bitmap_hex"`
		SellerArbiterPeerIDs    json.RawMessage `json:"seller_arbiter_peer_ids"`
		CreatedAtUnix           int64           `json:"created_at_unix"`
	}
	var it quoteItem
	var arbiterIDs string
	err := s.db.QueryRow(`SELECT id,demand_id,seller_peer_id,seed_price,chunk_price,expires_at_unix,recommended_file_name,available_chunk_bitmap_hex,seller_arbiter_peer_ids_json,created_at_unix FROM direct_quotes WHERE id=?`, id).
		Scan(&it.ID, &it.DemandID, &it.SellerPeerID, &it.SeedPrice, &it.ChunkPrice, &it.ExpiresAtUnix, &it.RecommendedFileName, &it.AvailableChunkBitmapHex, &arbiterIDs, &it.CreatedAtUnix)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	it.SellerArbiterPeerIDs = json.RawMessage(arbiterIDs)
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleDirectDeals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	demandID := strings.TrimSpace(r.URL.Query().Get("demand_id"))
	dealID := strings.TrimSpace(r.URL.Query().Get("deal_id"))
	sellerPeerID := strings.TrimSpace(r.URL.Query().Get("seller_peer_id"))
	buyerPeerID := strings.TrimSpace(r.URL.Query().Get("buyer_peer_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	where := ""
	args := []any{}
	if demandID != "" {
		where += " AND demand_id=?"
		args = append(args, demandID)
	}
	if dealID != "" {
		where += " AND deal_id=?"
		args = append(args, dealID)
	}
	if sellerPeerID != "" {
		where += " AND seller_peer_id=?"
		args = append(args, sellerPeerID)
	}
	if buyerPeerID != "" {
		where += " AND buyer_peer_id=?"
		args = append(args, buyerPeerID)
	}
	if status != "" {
		where += " AND status=?"
		args = append(args, status)
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM direct_deals WHERE 1=1"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`SELECT deal_id,demand_id,buyer_peer_id,seller_peer_id,seed_hash,seed_price,chunk_price,arbiter_peer_id,status,created_at_unix FROM direct_deals WHERE 1=1`+where+` ORDER BY created_at_unix DESC,deal_id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type dealItem struct {
		DealID        string `json:"deal_id"`
		DemandID      string `json:"demand_id"`
		BuyerPeerID   string `json:"buyer_peer_id"`
		SellerPeerID  string `json:"seller_peer_id"`
		SeedHash      string `json:"seed_hash"`
		SeedPrice     uint64 `json:"seed_price"`
		ChunkPrice    uint64 `json:"chunk_price"`
		ArbiterPeerID string `json:"arbiter_peer_id"`
		Status        string `json:"status"`
		CreatedAtUnix int64  `json:"created_at_unix"`
	}
	items := make([]dealItem, 0, limit)
	for rows.Next() {
		var it dealItem
		if err := rows.Scan(&it.DealID, &it.DemandID, &it.BuyerPeerID, &it.SellerPeerID, &it.SeedHash, &it.SeedPrice, &it.ChunkPrice, &it.ArbiterPeerID, &it.Status, &it.CreatedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	})
}

func (s *httpAPIServer) handleDirectDealDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	dealID := strings.TrimSpace(r.URL.Query().Get("deal_id"))
	if dealID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "deal_id is required"})
		return
	}
	type dealItem struct {
		DealID        string `json:"deal_id"`
		DemandID      string `json:"demand_id"`
		BuyerPeerID   string `json:"buyer_peer_id"`
		SellerPeerID  string `json:"seller_peer_id"`
		SeedHash      string `json:"seed_hash"`
		SeedPrice     uint64 `json:"seed_price"`
		ChunkPrice    uint64 `json:"chunk_price"`
		ArbiterPeerID string `json:"arbiter_peer_id"`
		Status        string `json:"status"`
		CreatedAtUnix int64  `json:"created_at_unix"`
	}
	var it dealItem
	err := s.db.QueryRow(`SELECT deal_id,demand_id,buyer_peer_id,seller_peer_id,seed_hash,seed_price,chunk_price,arbiter_peer_id,status,created_at_unix FROM direct_deals WHERE deal_id=?`, dealID).
		Scan(&it.DealID, &it.DemandID, &it.BuyerPeerID, &it.SellerPeerID, &it.SeedHash, &it.SeedPrice, &it.ChunkPrice, &it.ArbiterPeerID, &it.Status, &it.CreatedAtUnix)
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

func (s *httpAPIServer) handleDirectSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	dealID := strings.TrimSpace(r.URL.Query().Get("deal_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	where := ""
	args := []any{}
	if sessionID != "" {
		where += " AND session_id=?"
		args = append(args, sessionID)
	}
	if dealID != "" {
		where += " AND deal_id=?"
		args = append(args, dealID)
	}
	if status != "" {
		where += " AND status=?"
		args = append(args, status)
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM direct_sessions WHERE 1=1"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`SELECT session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix FROM direct_sessions WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,session_id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type sessionItem struct {
		SessionID      string `json:"session_id"`
		DealID         string `json:"deal_id"`
		ChunkPrice     uint64 `json:"chunk_price"`
		PaidChunks     uint32 `json:"paid_chunks"`
		PaidAmount     uint64 `json:"paid_amount"`
		ReleasedChunks uint32 `json:"released_chunks"`
		ReleasedAmount uint64 `json:"released_amount"`
		Status         string `json:"status"`
		CreatedAtUnix  int64  `json:"created_at_unix"`
		UpdatedAtUnix  int64  `json:"updated_at_unix"`
	}
	items := make([]sessionItem, 0, limit)
	for rows.Next() {
		var it sessionItem
		if err := rows.Scan(&it.SessionID, &it.DealID, &it.ChunkPrice, &it.PaidChunks, &it.PaidAmount, &it.ReleasedChunks, &it.ReleasedAmount, &it.Status, &it.CreatedAtUnix, &it.UpdatedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	})
}

func (s *httpAPIServer) handleDirectSessionDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "session_id is required"})
		return
	}
	type sessionItem struct {
		SessionID      string `json:"session_id"`
		DealID         string `json:"deal_id"`
		ChunkPrice     uint64 `json:"chunk_price"`
		PaidChunks     uint32 `json:"paid_chunks"`
		PaidAmount     uint64 `json:"paid_amount"`
		ReleasedChunks uint32 `json:"released_chunks"`
		ReleasedAmount uint64 `json:"released_amount"`
		Status         string `json:"status"`
		CreatedAtUnix  int64  `json:"created_at_unix"`
		UpdatedAtUnix  int64  `json:"updated_at_unix"`
	}
	var it sessionItem
	err := s.db.QueryRow(`SELECT session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix FROM direct_sessions WHERE session_id=?`, sessionID).
		Scan(&it.SessionID, &it.DealID, &it.ChunkPrice, &it.PaidChunks, &it.PaidAmount, &it.ReleasedChunks, &it.ReleasedAmount, &it.Status, &it.CreatedAtUnix, &it.UpdatedAtUnix)
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

func (s *httpAPIServer) handleDirectTransferPools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	dealID := strings.TrimSpace(r.URL.Query().Get("deal_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	sellerPeerID := strings.TrimSpace(r.URL.Query().Get("seller_peer_id"))
	buyerPeerID := strings.TrimSpace(r.URL.Query().Get("buyer_peer_id"))
	arbiterPeerID := strings.TrimSpace(r.URL.Query().Get("arbiter_peer_id"))
	where := ""
	args := []any{}
	if sessionID != "" {
		where += " AND session_id=?"
		args = append(args, sessionID)
	}
	if dealID != "" {
		where += " AND deal_id=?"
		args = append(args, dealID)
	}
	if status != "" {
		where += " AND status=?"
		args = append(args, status)
	}
	if sellerPeerID != "" {
		where += " AND seller_peer_id=?"
		args = append(args, sellerPeerID)
	}
	if buyerPeerID != "" {
		where += " AND buyer_peer_id=?"
		args = append(args, buyerPeerID)
	}
	if arbiterPeerID != "" {
		where += " AND arbiter_peer_id=?"
		args = append(args, arbiterPeerID)
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM direct_transfer_pools WHERE 1=1"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`SELECT session_id,deal_id,buyer_peer_id,seller_peer_id,arbiter_peer_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix FROM direct_transfer_pools WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,session_id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type poolItem struct {
		SessionID        string  `json:"session_id"`
		DealID           string  `json:"deal_id"`
		BuyerPeerID      string  `json:"buyer_peer_id"`
		SellerPeerID     string  `json:"seller_peer_id"`
		ArbiterPeerID    string  `json:"arbiter_peer_id"`
		BuyerPubkeyHex   string  `json:"buyer_pubkey_hex"`
		SellerPubkeyHex  string  `json:"seller_pubkey_hex"`
		ArbiterPubkeyHex string  `json:"arbiter_pubkey_hex"`
		PoolAmount       uint64  `json:"pool_amount"`
		SpendTxFee       uint64  `json:"spend_tx_fee"`
		SequenceNum      uint32  `json:"sequence_num"`
		SellerAmount     uint64  `json:"seller_amount"`
		BuyerAmount      uint64  `json:"buyer_amount"`
		CurrentTxHex     string  `json:"current_tx_hex"`
		BaseTxHex        string  `json:"base_tx_hex"`
		BaseTxID         string  `json:"base_txid"`
		Status           string  `json:"status"`
		FeeRateSatByte   float64 `json:"fee_rate_sat_byte"`
		LockBlocks       uint32  `json:"lock_blocks"`
		CreatedAtUnix    int64   `json:"created_at_unix"`
		UpdatedAtUnix    int64   `json:"updated_at_unix"`
	}
	items := make([]poolItem, 0, limit)
	for rows.Next() {
		var it poolItem
		if err := rows.Scan(
			&it.SessionID, &it.DealID, &it.BuyerPeerID, &it.SellerPeerID, &it.ArbiterPeerID,
			&it.BuyerPubkeyHex, &it.SellerPubkeyHex, &it.ArbiterPubkeyHex, &it.PoolAmount, &it.SpendTxFee,
			&it.SequenceNum, &it.SellerAmount, &it.BuyerAmount, &it.CurrentTxHex, &it.BaseTxHex, &it.BaseTxID,
			&it.Status, &it.FeeRateSatByte, &it.LockBlocks, &it.CreatedAtUnix, &it.UpdatedAtUnix,
		); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	})
}

func (s *httpAPIServer) handleDirectTransferPoolDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "session_id is required"})
		return
	}
	type poolItem struct {
		SessionID        string  `json:"session_id"`
		DealID           string  `json:"deal_id"`
		BuyerPeerID      string  `json:"buyer_peer_id"`
		SellerPeerID     string  `json:"seller_peer_id"`
		ArbiterPeerID    string  `json:"arbiter_peer_id"`
		BuyerPubkeyHex   string  `json:"buyer_pubkey_hex"`
		SellerPubkeyHex  string  `json:"seller_pubkey_hex"`
		ArbiterPubkeyHex string  `json:"arbiter_pubkey_hex"`
		PoolAmount       uint64  `json:"pool_amount"`
		SpendTxFee       uint64  `json:"spend_tx_fee"`
		SequenceNum      uint32  `json:"sequence_num"`
		SellerAmount     uint64  `json:"seller_amount"`
		BuyerAmount      uint64  `json:"buyer_amount"`
		CurrentTxHex     string  `json:"current_tx_hex"`
		BaseTxHex        string  `json:"base_tx_hex"`
		BaseTxID         string  `json:"base_txid"`
		Status           string  `json:"status"`
		FeeRateSatByte   float64 `json:"fee_rate_sat_byte"`
		LockBlocks       uint32  `json:"lock_blocks"`
		CreatedAtUnix    int64   `json:"created_at_unix"`
		UpdatedAtUnix    int64   `json:"updated_at_unix"`
	}
	var it poolItem
	err := s.db.QueryRow(`SELECT session_id,deal_id,buyer_peer_id,seller_peer_id,arbiter_peer_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix FROM direct_transfer_pools WHERE session_id=?`, sessionID).
		Scan(
			&it.SessionID, &it.DealID, &it.BuyerPeerID, &it.SellerPeerID, &it.ArbiterPeerID,
			&it.BuyerPubkeyHex, &it.SellerPubkeyHex, &it.ArbiterPubkeyHex, &it.PoolAmount, &it.SpendTxFee,
			&it.SequenceNum, &it.SellerAmount, &it.BuyerAmount, &it.CurrentTxHex, &it.BaseTxHex, &it.BaseTxID,
			&it.Status, &it.FeeRateSatByte, &it.LockBlocks, &it.CreatedAtUnix, &it.UpdatedAtUnix,
		)
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

	type argsT struct {
		args  []any
		where string
	}
	build := argsT{args: []any{}}
	if eventType != "" {
		build.where += " AND event_type=?"
		build.args = append(build.args, eventType)
	}
	if direction != "" {
		build.where += " AND direction=?"
		build.args = append(build.args, direction)
	}
	if purpose != "" {
		build.where += " AND purpose=?"
		build.args = append(build.args, purpose)
	}
	if q != "" {
		build.where += " AND (note LIKE ? OR msg_id LIKE ? OR gateway_peer_id LIKE ?)"
		like := "%" + q + "%"
		build.args = append(build.args, like, like, like)
	}

	countSQL := "SELECT COUNT(1) FROM tx_history WHERE 1=1" + build.where
	var total int
	if err := s.db.QueryRow(countSQL, build.args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	querySQL := "SELECT id,created_at_unix,gateway_peer_id,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index FROM tx_history WHERE 1=1" + build.where + " ORDER BY id DESC LIMIT ? OFFSET ?"
	qArgs := append(build.args, limit, offset)
	rows, err := s.db.Query(querySQL, qArgs...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()

	type txItem struct {
		ID            int64  `json:"id"`
		CreatedAtUnix int64  `json:"created_at_unix"`
		GatewayPeerID string `json:"gateway_peer_id"`
		EventType     string `json:"event_type"`
		Direction     string `json:"direction"`
		AmountSatoshi int64  `json:"amount_satoshi"`
		Purpose       string `json:"purpose"`
		Note          string `json:"note"`
		PoolID        string `json:"pool_id,omitempty"`
		MsgID         string `json:"msg_id,omitempty"`
		SequenceNum   uint32 `json:"sequence_num,omitempty"`
		CycleIndex    uint32 `json:"cycle_index,omitempty"`
	}
	items := make([]txItem, 0, limit)
	for rows.Next() {
		var it txItem
		if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.GatewayPeerID, &it.EventType, &it.Direction, &it.AmountSatoshi, &it.Purpose, &it.Note, &it.PoolID, &it.MsgID, &it.SequenceNum, &it.CycleIndex); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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
	row := s.db.QueryRow(`SELECT id,created_at_unix,gateway_peer_id,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index FROM tx_history WHERE id=?`, id)
	type txItem struct {
		ID            int64  `json:"id"`
		CreatedAtUnix int64  `json:"created_at_unix"`
		GatewayPeerID string `json:"gateway_peer_id"`
		EventType     string `json:"event_type"`
		Direction     string `json:"direction"`
		AmountSatoshi int64  `json:"amount_satoshi"`
		Purpose       string `json:"purpose"`
		Note          string `json:"note"`
		PoolID        string `json:"pool_id,omitempty"`
		MsgID         string `json:"msg_id,omitempty"`
		SequenceNum   uint32 `json:"sequence_num,omitempty"`
		CycleIndex    uint32 `json:"cycle_index,omitempty"`
	}
	var it txItem
	if err := row.Scan(&it.ID, &it.CreatedAtUnix, &it.GatewayPeerID, &it.EventType, &it.Direction, &it.AmountSatoshi, &it.Purpose, &it.Note, &it.PoolID, &it.MsgID, &it.SequenceNum, &it.CycleIndex); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, it)
}

func (s *httpAPIServer) handleSales(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	seedHash := strings.TrimSpace(r.URL.Query().Get("seed_hash"))

	where := ""
	args := []any{}
	if seedHash != "" {
		where = " WHERE seed_hash=?"
		args = append(args, seedHash)
	}

	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM sale_records"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	rows, err := s.db.Query(
		`SELECT id,created_at_unix,session_id,seed_hash,chunk_index,unit_price_sat_per_64k,amount_satoshi,buyer_gateway_peer_id,release_token
		 FROM sale_records`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
		append(args, limit, offset)...,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type saleItem struct {
		ID                 int64  `json:"id"`
		CreatedAtUnix      int64  `json:"created_at_unix"`
		SessionID          string `json:"session_id"`
		SeedHash           string `json:"seed_hash"`
		ChunkIndex         uint32 `json:"chunk_index"`
		UnitPriceSatPer64K uint64 `json:"unit_price_sat_per_64k"`
		AmountSatoshi      uint64 `json:"amount_satoshi"`
		BuyerGatewayPeerID string `json:"buyer_gateway_peer_id"`
		ReleaseToken       string `json:"release_token"`
	}
	items := make([]saleItem, 0, limit)
	for rows.Next() {
		var it saleItem
		if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.SessionID, &it.SeedHash, &it.ChunkIndex, &it.UnitPriceSatPer64K, &it.AmountSatoshi, &it.BuyerGatewayPeerID, &it.ReleaseToken); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	})
}

func (s *httpAPIServer) handleSaleDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000)
	if id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
		return
	}
	type saleItem struct {
		ID                 int64  `json:"id"`
		CreatedAtUnix      int64  `json:"created_at_unix"`
		SessionID          string `json:"session_id"`
		SeedHash           string `json:"seed_hash"`
		ChunkIndex         uint32 `json:"chunk_index"`
		UnitPriceSatPer64K uint64 `json:"unit_price_sat_per_64k"`
		AmountSatoshi      uint64 `json:"amount_satoshi"`
		BuyerGatewayPeerID string `json:"buyer_gateway_peer_id"`
		ReleaseToken       string `json:"release_token"`
	}
	var it saleItem
	err := s.db.QueryRow(`SELECT id,created_at_unix,session_id,seed_hash,chunk_index,unit_price_sat_per_64k,amount_satoshi,buyer_gateway_peer_id,release_token FROM sale_records WHERE id=?`, id).
		Scan(&it.ID, &it.CreatedAtUnix, &it.SessionID, &it.SeedHash, &it.ChunkIndex, &it.UnitPriceSatPer64K, &it.AmountSatoshi, &it.BuyerGatewayPeerID, &it.ReleaseToken)
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

func (s *httpAPIServer) handleGatewayEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	limit := parseBoundInt(r.URL.Query().Get("limit"), 50, 1, 500)
	offset := parseBoundInt(r.URL.Query().Get("offset"), 0, 0, 1_000_000)
	gatewayPeerID := strings.TrimSpace(r.URL.Query().Get("gateway_peer_id"))
	action := strings.TrimSpace(r.URL.Query().Get("action"))

	where := ""
	args := []any{}
	if gatewayPeerID != "" {
		where += " AND gateway_peer_id=?"
		args = append(args, gatewayPeerID)
	}
	if action != "" {
		where += " AND action=?"
		args = append(args, action)
	}

	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM gateway_events WHERE 1=1"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	rows, err := s.db.Query(`SELECT id,created_at_unix,gateway_peer_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM gateway_events WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type eventItem struct {
		ID            int64           `json:"id"`
		CreatedAtUnix int64           `json:"created_at_unix"`
		GatewayPeerID string          `json:"gateway_peer_id"`
		Action        string          `json:"action"`
		MsgID         string          `json:"msg_id,omitempty"`
		SequenceNum   uint32          `json:"sequence_num,omitempty"`
		PoolID        string          `json:"pool_id,omitempty"`
		AmountSatoshi int64           `json:"amount_satoshi"`
		Payload       json.RawMessage `json:"payload"`
	}
	items := make([]eventItem, 0, limit)
	for rows.Next() {
		var it eventItem
		var payload string
		if err := rows.Scan(&it.ID, &it.CreatedAtUnix, &it.GatewayPeerID, &it.Action, &it.MsgID, &it.SequenceNum, &it.PoolID, &it.AmountSatoshi, &payload); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		it.Payload = json.RawMessage(payload)
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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
	type eventItem struct {
		ID            int64           `json:"id"`
		CreatedAtUnix int64           `json:"created_at_unix"`
		GatewayPeerID string          `json:"gateway_peer_id"`
		Action        string          `json:"action"`
		MsgID         string          `json:"msg_id,omitempty"`
		SequenceNum   uint32          `json:"sequence_num,omitempty"`
		PoolID        string          `json:"pool_id,omitempty"`
		AmountSatoshi int64           `json:"amount_satoshi"`
		Payload       json.RawMessage `json:"payload"`
	}
	var it eventItem
	var payload string
	err := s.db.QueryRow(`SELECT id,created_at_unix,gateway_peer_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json FROM gateway_events WHERE id=?`, id).
		Scan(&it.ID, &it.CreatedAtUnix, &it.GatewayPeerID, &it.Action, &it.MsgID, &it.SequenceNum, &it.PoolID, &it.AmountSatoshi, &payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "record not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	it.Payload = json.RawMessage(payload)
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
		GatewayPeerID string `json:"gateway_peer_id,omitempty"`
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

	where := ""
	args := []any{}
	if pathLike != "" {
		where = " WHERE path LIKE ?"
		args = append(args, "%"+pathLike+"%")
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM workspace_files"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`SELECT path,file_size,mtime_unix,seed_hash,updated_at_unix FROM workspace_files`+where+` ORDER BY updated_at_unix DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type fileItem struct {
		Path          string `json:"path"`
		FileSize      int64  `json:"file_size"`
		MtimeUnix     int64  `json:"mtime_unix"`
		SeedHash      string `json:"seed_hash"`
		UpdatedAtUnix int64  `json:"updated_at_unix"`
	}
	items := make([]fileItem, 0, limit)
	for rows.Next() {
		var it fileItem
		if err := rows.Scan(&it.Path, &it.FileSize, &it.MtimeUnix, &it.SeedHash, &it.UpdatedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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

	where := ""
	args := []any{}
	if seedHash != "" {
		where += " WHERE s.seed_hash=?"
		args = append(args, seedHash)
	} else if seedHashLike != "" {
		where += " WHERE s.seed_hash LIKE ?"
		args = append(args, "%"+seedHashLike+"%")
	}
	var total int
	if err := s.db.QueryRow("SELECT COUNT(1) FROM seeds s"+where, args...).Scan(&total); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	rows, err := s.db.Query(`
		SELECT s.seed_hash,s.seed_file_path,s.chunk_count,s.file_size,s.created_at_unix,
		       COALESCE(p.unit_price_sat_per_64k,0), COALESCE(p.last_buy_unit_price_sat_per_64k,0),
		       COALESCE(p.floor_unit_price_sat_per_64k,0), COALESCE(p.resale_discount_bps,0),
		       COALESCE(p.updated_at_unix,0)
		FROM seeds s
		LEFT JOIN seed_price_state p ON p.seed_hash=s.seed_hash
		`+where+`
		ORDER BY s.created_at_unix DESC
		LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type seedItem struct {
		SeedHash              string `json:"seed_hash"`
		SeedFilePath          string `json:"seed_file_path"`
		ChunkCount            uint32 `json:"chunk_count"`
		FileSize              int64  `json:"file_size"`
		CreatedAtUnix         int64  `json:"created_at_unix"`
		UnitPriceSatPer64K    uint64 `json:"unit_price_sat_per_64k"`
		LastBuyPriceSatPer64K uint64 `json:"last_buy_unit_price_sat_per_64k"`
		FloorPriceSatPer64K   uint64 `json:"floor_unit_price_sat_per_64k"`
		ResaleDiscountBPS     uint64 `json:"resale_discount_bps"`
		PriceUpdatedAtUnix    int64  `json:"price_updated_at_unix"`
	}
	items := make([]seedItem, 0, limit)
	for rows.Next() {
		var it seedItem
		if err := rows.Scan(
			&it.SeedHash,
			&it.SeedFilePath,
			&it.ChunkCount,
			&it.FileSize,
			&it.CreatedAtUnix,
			&it.UnitPriceSatPer64K,
			&it.LastBuyPriceSatPer64K,
			&it.FloorPriceSatPer64K,
			&it.ResaleDiscountBPS,
			&it.PriceUpdatedAtUnix,
		); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
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
	var seedFilePath string
	if err := s.db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(req.SeedHash))).Scan(&seedFilePath); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "seed not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	unit, total, err := upsertSeedPriceState(s.db, strings.ToLower(strings.TrimSpace(req.SeedHash)), req.FloorPriceSatPer64K, req.ResaleDiscountBPS, seedFilePath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                     true,
		"seed_hash":              strings.ToLower(strings.TrimSpace(req.SeedHash)),
		"unit_price_sat_per_64k": unit,
		"seed_price_satoshi":     total,
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
		GatewayPeerID    string `json:"gateway_peer_id,omitempty"`
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
	resp, err := TriggerGatewayPublishLiveDemand(r.Context(), s.rt, PublishLiveDemandParams{
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
	quotes, err := TriggerClientListLiveQuotes(r.Context(), s.rt, demandID)
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
	rows, err := s.db.Query(`SELECT path,seed_hash,updated_at_unix FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix ASC, path ASC`, "%"+string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)+"%")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	recent := make([]LiveSegmentRef, 0, maxLiveWindowSize)
	for rows.Next() {
		var p, seedHash string
		var updatedAt int64
		if err := rows.Scan(&p, &seedHash, &updatedAt); err != nil {
			return nil, nil, err
		}
		segmentBytes, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		data, _, _, err := VerifyLiveSegment(segmentBytes)
		if err != nil {
			continue
		}
		recent = append(recent, LiveSegmentRef{
			SegmentIndex:    data.SegmentIndex,
			SeedHash:        strings.ToLower(strings.TrimSpace(seedHash)),
			PublishedAtUnix: updatedAt,
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
	decision, err := PlanLivePurchase(snap, req.HaveSegmentIndex, LiveBuyerStrategy{
		TargetLagSegments:   s.cfg.Live.Buyer.TargetLagSegments,
		MaxBudgetPerMinute:  s.cfg.Live.Buyer.MaxBudgetPerMinute,
		PreferOlderSegments: s.cfg.Live.Buyer.PreferOlderSegments,
	}, LiveSellerPricing{
		BasePriceSatPer64K:  s.cfg.Seller.Pricing.LiveBasePriceSatPer64K,
		FloorPriceSatPer64K: s.cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
		DecayPerMinuteBPS:   s.cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
	}, time.Now())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stream_id":          strings.ToLower(strings.TrimSpace(req.StreamID)),
		"have_segment_index": req.HaveSegmentIndex,
		"decision":           decision,
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
	st, err := TriggerLiveFollowStart(r.Context(), s.rt, req.StreamURI)
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
	if err := TriggerLiveFollowStop(s.rt, req.StreamID); err != nil {
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
	st, err := TriggerLiveFollowStatus(s.rt, streamID)
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
		type gwResp struct {
			ID      int    `json:"id"`
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
		}
		items := make([]gwResp, len(gateways))
		for i, g := range gateways {
			items[i] = gwResp{ID: i, Addr: g.Addr, Pubkey: g.Pubkey, Enabled: g.Enabled}
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})

	case http.MethodPost:
		// 添加网关
		var req struct {
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
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
		// 验证 pubkey 与 peer_id 匹配
		pidFromPub, err := peerIDFromSecp256k1PubHex(req.Pubkey)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid pubkey: " + err.Error()})
			return
		}
		if ai.ID != pidFromPub {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "pubkey does not match addr peer_id"})
			return
		}
		// 检查重复
		for i, g := range s.rt.Config.Network.Gateways {
			if strings.EqualFold(g.Pubkey, req.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
		}

		node := PeerNode{Enabled: req.Enabled, Addr: req.Addr, Pubkey: req.Pubkey}
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
			Addr    string `json:"addr"`
			Pubkey  string `json:"pubkey"`
			Enabled bool   `json:"enabled"`
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

		node := PeerNode{Enabled: req.Enabled, Addr: req.Addr, Pubkey: req.Pubkey}
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
			writeJSON(w, http.StatusOK, map[string]any{"master_peer_id": "", "has_master": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"master_peer_id": master.String(), "has_master": true})
	case http.MethodPost:
		var req struct {
			MasterPeerID string `json:"master_peer_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		target := strings.TrimSpace(req.MasterPeerID)
		if target == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "master_peer_id is required"})
			return
		}
		targetID, err := peer.Decode(target)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid master_peer_id"})
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
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "gateway peer_id not configured"})
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
				"old_master_peer_id": old.String(),
				"new_master_peer_id": targetID.String(),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"master_peer_id": targetID.String(),
			"changed":        changed,
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
		ID            int    `json:"id"`
		PeerID        string `json:"peer_id,omitempty"`
		Addr          string `json:"addr"`
		Pubkey        string `json:"pubkey"`
		Enabled       bool   `json:"enabled"`
		Connected     bool   `json:"connected"`
		Connectedness string `json:"connectedness"`
		IsMaster      bool   `json:"is_master"`
		InHealthyGWs  bool   `json:"in_healthy_gws"`
		Error         string `json:"error,omitempty"`
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
		"master_peer_id":  master,
		"items":           items,
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
		items := make([]arbResp, len(s.rt.Config.Network.Arbiters))
		for i, a := range s.rt.Config.Network.Arbiters {
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
		for i, a := range s.rt.Config.Network.Arbiters {
			if strings.EqualFold(strings.TrimSpace(a.Pubkey), node.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
			ai, parseErr := parseAddr(strings.TrimSpace(a.Addr))
			if parseErr == nil && ai.ID == newAI.ID {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("addr peer_id already exists at index %d", i)})
				return
			}
		}
		next := s.rt.Config
		next.Network.Arbiters = append(next.Network.Arbiters, node)
		s.rt.Config = next
		if s.cfg != nil {
			*s.cfg = next
		}
		if err := SaveConfigInDB(s.db, next); err != nil {
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
		if id >= len(s.rt.Config.Network.Arbiters) {
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
		for i, a := range s.rt.Config.Network.Arbiters {
			if i == id {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(a.Pubkey), node.Pubkey) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("pubkey already exists at index %d", i)})
				return
			}
			ai, parseErr := parseAddr(strings.TrimSpace(a.Addr))
			if parseErr == nil && ai.ID == newAI.ID {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("addr peer_id already exists at index %d", i)})
				return
			}
		}
		next := s.rt.Config
		next.Network.Arbiters[id] = node
		s.rt.Config = next
		if s.cfg != nil {
			*s.cfg = next
		}
		if err := SaveConfigInDB(s.db, next); err != nil {
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
		if id >= len(s.rt.Config.Network.Arbiters) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "arbiter not found"})
			return
		}
		if s.rt.Config.Network.Arbiters[id].Enabled {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "cannot delete enabled arbiter, please disable it first"})
			return
		}
		next := s.rt.Config
		next.Network.Arbiters = append(next.Network.Arbiters[:id], next.Network.Arbiters[id+1:]...)
		s.rt.Config = next
		if s.cfg != nil {
			*s.cfg = next
		}
		if err := SaveConfigInDB(s.db, next); err != nil {
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
		return PeerNode{}, fmt.Errorf("pubkey does not match addr peer_id")
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
	cfgArbs := s.rt.Config.Network.Arbiters
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
			obs.Error("bitcast-client", "arbiter_connect_failed", map[string]any{"index": i, "peer_id": ai.ID.String(), "error": err.Error()})
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
		PeerID        string `json:"peer_id,omitempty"`
		Addr          string `json:"addr"`
		Pubkey        string `json:"pubkey"`
		Enabled       bool   `json:"enabled"`
		Connected     bool   `json:"connected"`
		Connectedness string `json:"connectedness"`
		InHealthyArbs bool   `json:"in_healthy_arbiters"`
		Error         string `json:"error,omitempty"`
	}
	nodes := s.rt.Config.Network.Arbiters
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
		"total":           len(items),
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
		id := int64(parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000))
		if id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
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
		it, err := s.workspace.UpdateByID(id, req.MaxBytes, req.Enabled)
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
		id := int64(parseBoundInt(r.URL.Query().Get("id"), 0, 0, 1_000_000_000))
		if id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "id is required"})
			return
		}
		if err := s.workspace.DeleteByID(id); err != nil {
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
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "id": id})
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
// - 同步写回 app_config，保证重启后状态一致；
// - DB 持久化失败时回滚内存配置，避免运行态与持久态分叉。
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
		cfg := s.rt.Config
		cfg.FSHTTP.StrategyDebugLogEnabled = req.Enabled
		if err := SaveConfigInDB(s.db, cfg); err != nil {
			s.cfg.FSHTTP.StrategyDebugLogEnabled = oldEnabled
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		s.rt.Config = cfg
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
		var before int64
		if err := s.db.QueryRow(`SELECT COUNT(1) FROM workspace_files WHERE path LIKE ?`, prefix).Scan(&before); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		_ = os.RemoveAll(livePath)
		if _, err := s.db.Exec(`DELETE FROM workspace_files WHERE path LIKE ?`, prefix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if _, err := s.db.Exec(`DELETE FROM static_file_prices WHERE path LIKE ?`, prefix); err != nil {
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
	rows, err := s.db.Query(`SELECT path,file_size,mtime_unix,updated_at_unix FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix DESC,path ASC`, prefix)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type item struct {
		Path          string `json:"path"`
		FileSize      int64  `json:"file_size"`
		MtimeUnix     int64  `json:"mtime_unix"`
		UpdatedAtUnix int64  `json:"updated_at_unix"`
	}
	out := make([]item, 0, 32)
	var totalBytes uint64
	var newest int64
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.Path, &it.FileSize, &it.MtimeUnix, &it.UpdatedAtUnix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
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
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	sep := string(filepath.Separator)
	rows, err := s.db.Query(`SELECT path,file_size,updated_at_unix FROM workspace_files WHERE path LIKE ?`, "%"+sep+"live"+sep+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type acc struct {
		files      int64
		bytes      uint64
		lastUpdate int64
		roots      map[string]struct{}
	}
	m := map[string]*acc{}
	for rows.Next() {
		var path string
		var fileSize int64
		var updated int64
		if err := rows.Scan(&path, &fileSize, &updated); err != nil {
			return nil, err
		}
		streamID, root, ok := extractLiveStreamIDFromPath(path)
		if !ok {
			continue
		}
		it, exists := m[streamID]
		if !exists {
			it = &acc{roots: map[string]struct{}{}}
			m[streamID] = it
		}
		it.files++
		if fileSize > 0 {
			it.bytes += uint64(fileSize)
		}
		if updated > it.lastUpdate {
			it.lastUpdate = updated
		}
		if root != "" {
			it.roots[root] = struct{}{}
		}
	}
	out := make([]adminLiveStreamStat, 0, len(m))
	for streamID, v := range m {
		roots := make([]string, 0, len(v.roots))
		for r := range v.roots {
			roots = append(roots, r)
		}
		sort.Strings(roots)
		liveFolder := filepath.Join(strings.TrimSpace(s.cfg.Storage.WorkspaceDir), "live", streamID)
		out = append(out, adminLiveStreamStat{
			StreamID:         streamID,
			FileCount:        v.files,
			TotalBytes:       v.bytes,
			LastUpdatedUnix:  v.lastUpdate,
			WorkspaceRoots:   roots,
			LiveStreamFolder: liveFolder,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastUpdatedUnix == out[j].LastUpdatedUnix {
			return out[i].StreamID < out[j].StreamID
		}
		return out[i].LastUpdatedUnix > out[j].LastUpdatedUnix
	})
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
			_ = s.db.QueryRow(`SELECT seed_hash FROM workspace_files WHERE path=?`, full).Scan(&node.SeedHash)
			var floor, bps uint64
			var updated int64
			if err := s.db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix FROM static_file_prices WHERE path=?`, full).Scan(&floor, &bps, &updated); err == nil {
				node.FloorPriceSatPer64K = floor
				node.ResaleDiscountBPS = bps
				node.PriceUpdatedAtUnix = updated
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
		prefix := filepath.Clean(abs) + string(filepath.Separator) + "%"
		if _, err := s.db.Exec(`DELETE FROM static_file_prices WHERE path LIKE ?`, prefix); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	} else {
		if err := os.Remove(abs); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if _, err := s.db.Exec(`DELETE FROM static_file_prices WHERE path=?`, abs); err != nil {
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
	now := time.Now().Unix()
	if _, err := s.db.Exec(
		`INSERT INTO static_file_prices(path,floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix) VALUES(?,?,?,?)
		 ON CONFLICT(path) DO UPDATE SET floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,resale_discount_bps=excluded.resale_discount_bps,updated_at_unix=excluded.updated_at_unix`,
		abs, req.FloorPriceSatPer64K, req.ResaleDiscountBPS, now,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var seedHash string
	_ = s.db.QueryRow(`SELECT seed_hash FROM workspace_files WHERE path=?`, abs).Scan(&seedHash)
	var unit, total uint64
	pricingBound := false
	if seedHash != "" {
		var seedPath string
		if err := s.db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, seedHash).Scan(&seedPath); err == nil {
			if v, t, err := upsertSeedPriceState(s.db, seedHash, req.FloorPriceSatPer64K, req.ResaleDiscountBPS, seedPath); err == nil {
				unit, total = v, t
				pricingBound = true
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                           true,
		"path":                         rel,
		"floor_unit_price_sat_per_64k": req.FloorPriceSatPer64K,
		"resale_discount_bps":          req.ResaleDiscountBPS,
		"seed_hash":                    seedHash,
		"pricing_bound":                pricingBound,
		"unit_price_sat_per_64k":       unit,
		"seed_price_satoshi":           total,
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
	var floor uint64
	var bps uint64
	var updated int64
	if err := s.db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix FROM static_file_prices WHERE path=?`, abs).Scan(&floor, &bps, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "price not configured"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var seedHash string
	_ = s.db.QueryRow(`SELECT seed_hash FROM workspace_files WHERE path=?`, abs).Scan(&seedHash)
	writeJSON(w, http.StatusOK, map[string]any{
		"path":                         rel,
		"seed_hash":                    seedHash,
		"floor_unit_price_sat_per_64k": floor,
		"resale_discount_bps":          bps,
		"price_updated_at_unix":        updated,
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
	rows, err := s.db.Query(`SELECT path,floor_unit_price_sat_per_64k,resale_discount_bps FROM static_file_prices WHERE path=? OR path LIKE ?`, fromAbs, filepath.Clean(fromAbs)+string(filepath.Separator)+"%")
	if err != nil {
		return err
	}
	defer rows.Close()
	type row struct {
		path  string
		floor uint64
		bps   uint64
	}
	list := make([]row, 0, 8)
	for rows.Next() {
		var it row
		if err := rows.Scan(&it.path, &it.floor, &it.bps); err != nil {
			return err
		}
		list = append(list, it)
	}
	if len(list) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, it := range list {
		newPath := strings.Replace(it.path, fromAbs, toAbs, 1)
		if _, err := tx.Exec(`DELETE FROM static_file_prices WHERE path=?`, it.path); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO static_file_prices(path,floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix) VALUES(?,?,?,?)`,
			newPath, it.floor, it.bps, time.Now().Unix(),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
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
		{Key: "http.auth_token", Type: adminConfigString, MinLen: 8, MaxLen: 256, Description: "管理 API 鉴权 token"},
		{Key: "fs_http.listen_addr", Type: adminConfigString, MinLen: 3, MaxLen: 128, Description: "文件 HTTP 监听地址"},
		{Key: "listen.enabled", Type: adminConfigBool, Description: "是否启用监听费用池自动循环"},
		{Key: "listen.renew_threshold_seconds", Type: adminConfigInt, MinInt: 1, MaxInt: 86400, Description: "监听续费阈值秒"},
		{Key: "listen.max_auto_renew_amount", Type: adminConfigInt, MinInt: 1, MaxInt: 1 << 40, Description: "监听自动续费上限(最小入池金额)"},
		{Key: "listen.tick_seconds", Type: adminConfigInt, MinInt: 1, MaxInt: 3600, Description: "监听循环调度周期秒"},
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
		writeJSON(w, http.StatusOK, map[string]any{"config": adminConfigSnapshot(s.rt.Config)})
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
		next := s.rt.Config
		ruleMap := adminConfigRuleMap()
		applied := make([]string, 0, len(req.Items))
		for _, it := range req.Items {
			key := strings.TrimSpace(it.Key)
			rule, ok := ruleMap[key]
			if !ok {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unsupported key: " + key})
				return
			}
			if err := adminConfigApplyOne(&next, rule, it.Value); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "key": key})
				return
			}
			applied = append(applied, key)
		}
		// 全局校验兜底，避免单字段校验通过但组合非法。
		if err := validateConfig(&next); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if req.ValidateOnly {
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":            true,
				"validate_only": true,
				"applied_keys":  applied,
				"config":        adminConfigSnapshot(next),
			})
			return
		}
		if err := SaveConfigInDB(s.db, next); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		s.rt.Config = next
		*s.cfg = next
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":            true,
			"validate_only": false,
			"applied_keys":  applied,
			"config":        adminConfigSnapshot(next),
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func adminConfigSnapshot(cfg Config) map[string]any {
	return map[string]any{
		"http.listen_addr":                            cfg.HTTP.ListenAddr,
		"http.auth_token":                             cfg.HTTP.AuthToken,
		"fs_http.listen_addr":                         cfg.FSHTTP.ListenAddr,
		"listen.enabled":                              cfgBool(cfg.Listen.Enabled, true),
		"listen.renew_threshold_seconds":              cfg.Listen.RenewThresholdSeconds,
		"listen.max_auto_renew_amount":                cfg.Listen.MaxAutoRenewAmount,
		"listen.tick_seconds":                         cfg.Listen.TickSeconds,
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
	case "http.auth_token":
		cfg.HTTP.AuthToken = v
	case "fs_http.listen_addr":
		cfg.FSHTTP.ListenAddr = v
	default:
		return fmt.Errorf("unsupported string key: %s", key)
	}
	return nil
}

func adminConfigSetInt(cfg *Config, key string, v int64) error {
	u := uint64(v)
	switch key {
	case "listen.renew_threshold_seconds":
		cfg.Listen.RenewThresholdSeconds = uint32(v)
	case "listen.max_auto_renew_amount":
		cfg.Listen.MaxAutoRenewAmount = u
	case "listen.tick_seconds":
		cfg.Listen.TickSeconds = uint32(v)
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
