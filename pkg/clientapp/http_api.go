package clientapp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/host"
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
	ID             string        `json:"id"`
	SeedHash       string        `json:"seed_hash"`
	ChunkCount     uint32        `json:"chunk_count"`
	GatewayPeerID  string        `json:"gateway_peer_id"`
	Status         string        `json:"status"`
	StartedAtUnix  int64         `json:"started_at_unix"`
	EndedAtUnix    int64         `json:"ended_at_unix,omitempty"`
	OutputFilePath string        `json:"output_file_path,omitempty"`
	Error          string        `json:"error,omitempty"`
	Steps          []fileGetStep `json:"steps"`
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
		mux.HandleFunc(prefix+"/v1/info", s.withAuth(s.handleInfo))
		mux.HandleFunc(prefix+"/v1/balance", s.withAuth(s.handleBalance))
		mux.HandleFunc(prefix+"/v1/transactions", s.withAuth(s.handleTransactions))
		mux.HandleFunc(prefix+"/v1/transactions/detail", s.withAuth(s.handleTransactionDetail))
		mux.HandleFunc(prefix+"/v1/sales", s.withAuth(s.handleSales))
		mux.HandleFunc(prefix+"/v1/sales/detail", s.withAuth(s.handleSaleDetail))
		mux.HandleFunc(prefix+"/v1/gateways/events", s.withAuth(s.handleGatewayEvents))
		mux.HandleFunc(prefix+"/v1/gateways/events/detail", s.withAuth(s.handleGatewayEventDetail))
		mux.HandleFunc(prefix+"/v1/files/get-file", s.withAuth(s.handleGetFileStart))
		mux.HandleFunc(prefix+"/v1/files/get-file/job", s.withAuth(s.handleGetFileJob))
		mux.HandleFunc(prefix+"/v1/files/get-file/jobs", s.withAuth(s.handleGetFileJobs))
		mux.HandleFunc(prefix+"/v1/filehash", s.withAuth(s.handleFileHash))
		mux.HandleFunc(prefix+"/v1/workspace/sync-once", s.withAuth(s.handleWorkspaceSyncOnce))
		mux.HandleFunc(prefix+"/v1/workspace/files", s.withAuth(s.handleWorkspaceFiles))
		mux.HandleFunc(prefix+"/v1/workspace/seeds", s.withAuth(s.handleWorkspaceSeeds))
		mux.HandleFunc(prefix+"/v1/workspace/seeds/price", s.withAuth(s.handleSeedPriceUpdate))
		// 网关管理 API
		mux.HandleFunc(prefix+"/v1/gateways", s.withAuth(s.handleGateways))
		mux.HandleFunc(prefix+"/v1/gateways/master", s.withAuth(s.handleGatewayMaster))
		// 文件系统管理 API
		mux.HandleFunc(prefix+"/v1/admin/workspaces", s.withAuth(s.handleAdminWorkspaces))
		mux.HandleFunc(prefix+"/v1/admin/downloads/resume", s.withAuth(s.handleAdminResumeDownload))
		mux.HandleFunc(prefix+"/v1/admin/fs-http/strategy-debug-log", s.withAuth(s.handleAdminStrategyDebugLog))
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
		if token := strings.TrimSpace(s.cfg.HTTP.AuthToken); token != "" {
			if !tokenAuthorized(r, token) {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
				return
			}
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
	s.jobsMu.Lock()
	s.getJobs[job.ID] = job
	s.jobsMu.Unlock()

	go s.runGetFileJob(job.ID, req.SeedHash, req.ChunkCount, req.GatewayPeerID)
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

func (s *httpAPIServer) runGetFileJob(jobID, seedHash string, chunkCount uint32, gwOverride string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fail := func(msg string) {
		s.jobsMu.Lock()
		if j, ok := s.getJobs[jobID]; ok {
			j.Status = "failed"
			j.Error = msg
			j.EndedAtUnix = time.Now().Unix()
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
		fail(err.Error())
		return
	}

	step := addStep("write_file", map[string]string{
		"selected_file_name": download.FileName,
		"bytes":              fmt.Sprintf("%d", len(download.Transfer.Data)),
	})
	if s.workspace == nil {
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

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
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
