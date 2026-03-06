package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func TestHandleAdminStrategyDebugLog(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = ":memory:"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.FSHTTP.StrategyDebugLogEnabled = false
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigInDB(db, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}

	rt := &Runtime{DB: db, Config: cfg}
	srv := &httpAPIServer{
		rt:  rt,
		cfg: &cfg,
		db:  db,
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/fs-http/strategy-debug-log", nil)
		rec := httptest.NewRecorder()
		srv.handleAdminStrategyDebugLog(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("get status mismatch: got=%d want=%d", rec.Code, http.StatusOK)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode get response: %v", err)
		}
		if got, _ := body["strategy_debug_log_enabled"].(bool); got {
			t.Fatalf("expected disabled on get")
		}
	}

	{
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/fs-http/strategy-debug-log", strings.NewReader(`{"enabled":true}`))
		rec := httptest.NewRecorder()
		srv.handleAdminStrategyDebugLog(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("post status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode post response: %v", err)
		}
		if got, _ := body["strategy_debug_log_enabled"].(bool); !got {
			t.Fatalf("expected enabled in post response")
		}
	}

	if !cfg.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("cfg pointer should be updated")
	}
	if !rt.Config.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("runtime config should be updated")
	}

	var raw string
	if err := db.QueryRow(`SELECT config_toml FROM app_config WHERE id=1`).Scan(&raw); err != nil {
		t.Fatalf("query app config: %v", err)
	}
	loaded, err := ParseConfigTOML([]byte(raw))
	if err != nil {
		t.Fatalf("parse app config: %v", err)
	}
	if !loaded.FSHTTP.StrategyDebugLogEnabled {
		t.Fatalf("db persisted config should be enabled")
	}
}

func TestHandleLiveAPIFlow(t *testing.T) {
	t.Parallel()

	pubHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new publisher host: %v", err)
	}
	defer pubHost.Close()
	subHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new subscriber host: %v", err)
	}
	defer subHost.Close()

	pubCfg := Config{}
	pubCfg.Storage.WorkspaceDir = t.TempDir()
	pubCfg.Storage.DataDir = t.TempDir()
	pubCfg.Index.Backend = "sqlite"
	pubCfg.Index.SQLitePath = ":memory:"
	pubCfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	pubCfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	pubCfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&pubCfg); err != nil {
		t.Fatalf("apply defaults pub: %v", err)
	}
	subCfg := pubCfg
	subCfg.Storage.WorkspaceDir = t.TempDir()
	subCfg.Storage.DataDir = t.TempDir()

	pubRT := &Runtime{Host: pubHost, Config: pubCfg, live: newLiveRuntime()}
	subRT := &Runtime{Host: subHost, Config: subCfg, live: newLiveRuntime()}
	registerLiveHandlers(pubRT)
	registerLiveHandlers(subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)

	pubSrv := &httpAPIServer{rt: pubRT, cfg: &pubCfg}
	subSrv := &httpAPIServer{rt: subRT, cfg: &subCfg}

	streamID := strings.Repeat("ab", 32)

	reqURI := httptest.NewRequest(http.MethodGet, "/api/v1/live/subscribe-uri?stream_id="+streamID, nil)
	recURI := httptest.NewRecorder()
	pubSrv.handleLiveSubscribeURI(recURI, reqURI)
	if recURI.Code != http.StatusOK {
		t.Fatalf("subscribe-uri status: got=%d body=%s", recURI.Code, recURI.Body.String())
	}
	var uriResp map[string]any
	if err := json.Unmarshal(recURI.Body.Bytes(), &uriResp); err != nil {
		t.Fatalf("decode subscribe-uri: %v", err)
	}
	subscribeURI, _ := uriResp["subscribe_uri"].(string)
	if strings.TrimSpace(subscribeURI) == "" {
		t.Fatalf("subscribe_uri missing")
	}

	reqSub := httptest.NewRequest(http.MethodPost, "/api/v1/live/subscribe", strings.NewReader(`{"stream_uri":"`+subscribeURI+`","window":5}`))
	recSub := httptest.NewRecorder()
	subSrv.handleLiveSubscribe(recSub, reqSub)
	if recSub.Code != http.StatusOK {
		t.Fatalf("subscribe status: got=%d body=%s", recSub.Code, recSub.Body.String())
	}

	reqPub := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/latest", strings.NewReader(`{"stream_id":"`+streamID+`","recent_segments":[{"segment_index":7,"seed_hash":"`+strings.Repeat("cd", 32)+`"},{"segment_index":8,"seed_hash":"`+strings.Repeat("ef", 32)+`"}]}`))
	recPub := httptest.NewRecorder()
	pubSrv.handleLivePublishLatest(recPub, reqPub.WithContext(context.Background()))
	if recPub.Code != http.StatusOK {
		t.Fatalf("publish latest status: got=%d body=%s", recPub.Code, recPub.Body.String())
	}

	reqLatest := httptest.NewRequest(http.MethodGet, "/api/v1/live/latest?stream_id="+streamID, nil)
	recLatest := httptest.NewRecorder()
	subSrv.handleLiveLatest(recLatest, reqLatest)
	if recLatest.Code != http.StatusOK {
		t.Fatalf("live latest status: got=%d body=%s", recLatest.Code, recLatest.Body.String())
	}
	var latest LiveSubscriberSnapshot
	if err := json.Unmarshal(recLatest.Body.Bytes(), &latest); err != nil {
		t.Fatalf("decode latest: %v", err)
	}
	if len(latest.RecentSegments) != 2 {
		t.Fatalf("unexpected recent segment count: %d", len(latest.RecentSegments))
	}
	if latest.RecentSegments[1].SegmentIndex != 8 {
		t.Fatalf("unexpected latest segment index: %d", latest.RecentSegments[1].SegmentIndex)
	}

	reqPlan := httptest.NewRequest(http.MethodPost, "/api/v1/live/plan", strings.NewReader(`{"stream_id":"`+streamID+`","have_segment_index":6}`))
	recPlan := httptest.NewRecorder()
	subSrv.handleLivePlan(recPlan, reqPlan)
	if recPlan.Code != http.StatusOK {
		t.Fatalf("live plan status: got=%d body=%s", recPlan.Code, recPlan.Body.String())
	}
}

func TestHandleLivePublishSegmentFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new host: %v", err)
	}
	defer h.Close()

	cfg := Config{}
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = dbPath
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	cfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	workspace := &workspaceManager{cfg: &cfg, db: db, catalog: &sellerCatalog{seeds: map[string]sellerSeed{}}}
	if err := workspace.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}
	rt := &Runtime{Host: h, DB: db, Config: cfg, Workspace: workspace, live: newLiveRuntime()}
	registerLiveHandlers(rt)
	srv := &httpAPIServer{rt: rt, cfg: &cfg, db: db, workspace: workspace}

	req0 := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/segment", strings.NewReader(`{
		"duration_ms": 2000,
		"mime_type": "video/mp2t",
		"media_bytes": "c2VnLTA="
	}`))
	rec0 := httptest.NewRecorder()
	srv.handleLivePublishSegment(rec0, req0)
	if rec0.Code != http.StatusOK {
		t.Fatalf("publish first segment status: got=%d body=%s", rec0.Code, rec0.Body.String())
	}
	var resp0 map[string]any
	if err := json.Unmarshal(rec0.Body.Bytes(), &resp0); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	streamID, _ := resp0["stream_id"].(string)
	seed0, _ := resp0["seed_hash"].(string)
	if !isSeedHashHex(streamID) || !isSeedHashHex(seed0) {
		t.Fatalf("invalid stream_id/seed_hash: stream=%q seed=%q", streamID, seed0)
	}
	if streamID != seed0 {
		t.Fatalf("first stream_id should equal first seed_hash: stream=%s seed=%s", streamID, seed0)
	}

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/segment", strings.NewReader(`{
		"stream_id": "`+streamID+`",
		"duration_ms": 3500,
		"is_discontinuity": true,
		"is_end": true,
		"mime_type": "video/mp2t",
		"init_seed_hash": "`+strings.Repeat("d", 64)+`",
		"playlist_uri_hint": "/custom/live/1.ts",
		"media_bytes": "c2VnLTE="
	}`))
	rec1 := httptest.NewRecorder()
	srv.handleLivePublishSegment(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("publish second segment status: got=%d body=%s", rec1.Code, rec1.Body.String())
	}

	latestReq := httptest.NewRequest(http.MethodGet, "/api/v1/live/latest?stream_id="+streamID, nil)
	latestRec := httptest.NewRecorder()
	srv.handleLiveLatest(latestRec, latestReq)
	if latestRec.Code != http.StatusOK {
		t.Fatalf("live latest status: got=%d body=%s", latestRec.Code, latestRec.Body.String())
	}
	var latest LiveSubscriberSnapshot
	if err := json.Unmarshal(latestRec.Body.Bytes(), &latest); err != nil {
		t.Fatalf("decode latest: %v", err)
	}
	if len(latest.RecentSegments) != 2 {
		t.Fatalf("unexpected recent segment count: %d", len(latest.RecentSegments))
	}

	var outPath string
	if err := db.QueryRow(`SELECT path FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix DESC LIMIT 1`, "%"+streamID+"%").Scan(&outPath); err != nil {
		t.Fatalf("query live segment path: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("live segment output missing: %v", err)
	}
}

func TestHandleLiveFollowFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	pubHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new publisher host: %v", err)
	}
	defer pubHost.Close()
	subHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.NoTransports,
		libp2p.Transport(libp2ptcp.NewTCPTransport),
	)
	if err != nil {
		t.Fatalf("new subscriber host: %v", err)
	}
	defer subHost.Close()

	pubCfg := Config{}
	pubCfg.Storage.WorkspaceDir = t.TempDir()
	pubCfg.Storage.DataDir = t.TempDir()
	pubCfg.Index.Backend = "sqlite"
	pubCfg.Index.SQLitePath = dbPath
	pubCfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	pubCfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	pubCfg.FSHTTP.MaxConcurrentSessions = 4
	if err := ApplyConfigDefaults(&pubCfg); err != nil {
		t.Fatalf("apply defaults pub: %v", err)
	}
	pubCfg.Live.Publish.BroadcastIntervalSec = 1
	subCfg := pubCfg
	subCfg.Storage.WorkspaceDir = t.TempDir()
	subCfg.Storage.DataDir = t.TempDir()
	streamID := strings.Repeat("ab", 32)
	subWorkspace := &workspaceManager{cfg: &subCfg, db: db}
	if err := subWorkspace.EnsureDefaultWorkspace(); err != nil {
		t.Fatalf("ensure default workspace: %v", err)
	}

	pubRT := &Runtime{Host: pubHost, Config: pubCfg, DB: db, live: newLiveRuntime()}
	subRT := &Runtime{Host: subHost, Config: subCfg, DB: db, Workspace: subWorkspace, live: newLiveRuntime()}
	registerLiveHandlers(pubRT)
	registerLiveHandlers(subRT)
	subHost.Peerstore().AddAddrs(pubHost.ID(), pubHost.Addrs(), time.Minute)
	subRT.live.autoBuyFn = func(_ context.Context, _ *Runtime, decision LivePurchaseDecision, _ LiveSubscriberSnapshot) (liveAutoBuyResult, error) {
		outPath, err := subWorkspace.SelectLiveSegmentOutputPath(streamID, decision.TargetSegmentIndex, 1)
		if err != nil {
			return liveAutoBuyResult{}, err
		}
		return liveAutoBuyResult{
			SeedHash:       decision.SeedHash,
			SegmentIndex:   decision.TargetSegmentIndex,
			OutputFilePath: outPath,
		}, nil
	}

	pubSrv := &httpAPIServer{rt: pubRT, cfg: &pubCfg}
	subSrv := &httpAPIServer{rt: subRT, cfg: &subCfg}
	reqURI := httptest.NewRequest(http.MethodGet, "/api/v1/live/subscribe-uri?stream_id="+streamID, nil)
	recURI := httptest.NewRecorder()
	pubSrv.handleLiveSubscribeURI(recURI, reqURI)
	var uriResp map[string]any
	if err := json.Unmarshal(recURI.Body.Bytes(), &uriResp); err != nil {
		t.Fatalf("decode subscribe-uri: %v", err)
	}
	subscribeURI, _ := uriResp["subscribe_uri"].(string)

	reqStart := httptest.NewRequest(http.MethodPost, "/api/v1/live/follow/start", strings.NewReader(`{"stream_uri":"`+subscribeURI+`"}`))
	recStart := httptest.NewRecorder()
	subSrv.handleLiveFollowStart(recStart, reqStart)
	if recStart.Code != http.StatusOK {
		t.Fatalf("follow start status: got=%d body=%s", recStart.Code, recStart.Body.String())
	}

	reqPub := httptest.NewRequest(http.MethodPost, "/api/v1/live/publish/latest", strings.NewReader(`{"stream_id":"`+streamID+`","recent_segments":[{"segment_index":3,"seed_hash":"`+strings.Repeat("cd", 32)+`","published_at_unix":1700000000}]}`))
	recPub := httptest.NewRecorder()
	pubSrv.handleLivePublishLatest(recPub, reqPub)
	if recPub.Code != http.StatusOK {
		t.Fatalf("publish latest status: got=%d body=%s", recPub.Code, recPub.Body.String())
	}

	var st LiveFollowStatus
	deadline := time.Now().Add(3 * time.Second)
	for {
		reqStatus := httptest.NewRequest(http.MethodGet, "/api/v1/live/follow/status?stream_id="+streamID, nil)
		recStatus := httptest.NewRecorder()
		subSrv.handleLiveFollowStatus(recStatus, reqStatus)
		if recStatus.Code == http.StatusOK {
			if err := json.Unmarshal(recStatus.Body.Bytes(), &st); err != nil {
				t.Fatalf("decode status: %v", err)
			}
			if st.HaveSegmentIndex >= 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("follow status did not progress: %+v", st)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st.HaveSegmentIndex < 0 {
		t.Fatalf("expected progressed have_segment_index")
	}
	if st.LastBoughtSeedHash != strings.Repeat("cd", 32) {
		t.Fatalf("unexpected bought seed hash: %s", st.LastBoughtSeedHash)
	}

	reqStop := httptest.NewRequest(http.MethodPost, "/api/v1/live/follow/stop", strings.NewReader(`{"stream_id":"`+streamID+`"}`))
	recStop := httptest.NewRecorder()
	subSrv.handleLiveFollowStop(recStop, reqStop)
	if recStop.Code != http.StatusOK {
		t.Fatalf("follow stop status: got=%d body=%s", recStop.Code, recStop.Body.String())
	}

	subRT.live = newLiveRuntime()
	loaded, err := TriggerLiveFollowStatus(subRT, streamID)
	if err != nil {
		t.Fatalf("load persisted follow status failed: %v", err)
	}
	if loaded.HaveSegmentIndex != st.HaveSegmentIndex {
		t.Fatalf("persisted have_segment_index mismatch: got=%d want=%d", loaded.HaveSegmentIndex, st.HaveSegmentIndex)
	}
	if !strings.Contains(loaded.LastOutputFilePath, string(filepath.Separator)+"live"+string(filepath.Separator)+streamID+string(filepath.Separator)) {
		t.Fatalf("unexpected live segment output path: %s", loaded.LastOutputFilePath)
	}
}
