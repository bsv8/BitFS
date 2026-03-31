package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

type fileHTTPServer struct {
	rt        *Runtime
	cfg       *Config
	db        *sql.DB
	store     *clientDB
	workspace *workspaceManager

	srv *http.Server

	mu       sync.Mutex
	sessions map[string]*fileDownloadSession
}

type fileDownloadSession struct {
	seedHash string
	server   *fileHTTPServer

	mu           sync.Mutex
	cond         *sync.Cond
	started      bool
	status       string
	lastError    string
	partPath     string
	fileSize     uint64
	chunkCount   uint32
	chunkHashes  []string
	completed    map[uint32]bool
	chunkPolicy  fileChunkOrderPolicy
	runtimeState fileDownloadRuntimeState
	paidSats     uint64
	demandID     string
}

type parsedRange struct {
	start int64
	end   int64
	ok    bool
}

type localLiveSegmentFile struct {
	SegmentIndex    uint64
	MediaSequence   uint64
	Path            string
	SeedHash        string
	UpdatedAtUnix   int64
	DurationMs      uint64
	IsDiscontinuity bool
	InitSeedHash    string
	PlaylistURI     string
	IsEnd           bool
}

func newFileHTTPServer(rt *Runtime, cfg *Config, store *clientDB, workspace *workspaceManager) *fileHTTPServer {
	db := (*sql.DB)(nil)
	if store != nil {
		db = store.db
	}
	return &fileHTTPServer{
		rt:        rt,
		cfg:       cfg,
		db:        db,
		store:     store,
		workspace: workspace,
		sessions:  map[string]*fileDownloadSession{},
	}
}

func (s *fileHTTPServer) Start() error {
	return s.startOnListener(nil)
}

func (s *fileHTTPServer) StartOnListener(ln net.Listener) error {
	return s.startOnListener(ln)
}

func (s *fileHTTPServer) startOnListener(ln net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	s.srv = &http.Server{
		Addr:              s.cfg.FSHTTP.ListenAddr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}
	obs.Important("bitcast-client", "fs_http_started", map[string]any{"listen_addr": s.cfg.FSHTTP.ListenAddr})
	var err error
	if ln != nil {
		err = s.srv.Serve(ln)
	} else {
		err = s.srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *fileHTTPServer) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(shutdownCtx)
}

func (s *fileHTTPServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	p := strings.Trim(pathClean(r.URL.Path), "/")
	if p == "" {
		s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	parts := strings.Split(p, "/")
	if len(parts) >= 2 && parts[0] == "live" {
		s.handleLivePlayback(w, r, parts)
		return
	}
	if len(parts) > 2 {
		s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	seed := strings.ToLower(strings.TrimSpace(parts[0]))
	if !isSeedHashHex(seed) {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid seed hash"})
		return
	}
	if len(parts) == 2 {
		if parts[1] != "status" {
			s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
			return
		}
		s.handleStatus(w, r, seed)
		return
	}
	s.handleDownload(w, r, seed)
}

func (s *fileHTTPServer) handleLivePlayback(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) != 3 {
		s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	streamID := strings.ToLower(strings.TrimSpace(parts[1]))
	if !isSeedHashHex(streamID) {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid stream_id"})
		return
	}
	if parts[2] == "playlist.m3u8" {
		s.handleLivePlaylist(w, r, streamID)
		return
	}
	segmentIndex, err := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid segment_index"})
		return
	}
	s.handleLiveSegmentMedia(w, r, streamID, segmentIndex)
}

func (s *fileHTTPServer) handleLivePlaylist(w http.ResponseWriter, r *http.Request, streamID string) {
	segments, err := s.listLocalLiveSegmentFiles(streamID)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if len(segments) == 0 {
		s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "stream not found"})
		return
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	b.WriteString("#EXT-X-PLAYLIST-TYPE:EVENT\n")
	targetDuration := uint64(1)
	for _, seg := range segments {
		secs := durationMsToTargetDuration(seg.DurationMs)
		if secs > targetDuration {
			targetDuration = secs
		}
	}
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))
	firstSequence := segments[0].MediaSequence
	b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", firstSequence))
	lastInitSeedHash := ""
	for _, seg := range segments {
		if seg.IsDiscontinuity {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if seg.InitSeedHash != "" && seg.InitSeedHash != lastInitSeedHash {
			b.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"/%s\"\n", seg.InitSeedHash))
			lastInitSeedHash = seg.InitSeedHash
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%s,\n", formatExtINF(seg.DurationMs)))
		b.WriteString(seg.PlaylistURI)
		b.WriteByte('\n')
	}
	if segments[len(segments)-1].IsEnd {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}

func (s *fileHTTPServer) handleLiveSegmentMedia(w http.ResponseWriter, r *http.Request, streamID string, segmentIndex uint64) {
	path, err := s.findLocalLiveSegmentFile(streamID, segmentIndex)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.writeJSON(w, http.StatusNotFound, map[string]any{"error": "segment not found"})
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	segmentBytes, err := os.ReadFile(path)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	data, mediaBytes, _, err := VerifyLiveSegment(segmentBytes)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if data.SegmentIndex != segmentIndex {
		s.writeJSON(w, http.StatusConflict, map[string]any{"error": "segment index mismatch"})
		return
	}
	if segmentIndex > 0 && !strings.EqualFold(strings.TrimSpace(data.StreamID), streamID) {
		s.writeJSON(w, http.StatusConflict, map[string]any{"error": "stream_id mismatch"})
		return
	}
	contentType := strings.TrimSpace(data.MIMEType)
	if contentType == "" {
		contentType = http.DetectContentType(mediaBytes)
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	s.serveBytes(w, r, mediaBytes, contentType)
}

func (s *fileHTTPServer) handleStatus(w http.ResponseWriter, r *http.Request, seedHash string) {
	if p, size, ok := s.findCompleteLocalFile(seedHash); ok {
		runtimeState := defaultFileDownloadRuntimeState()
		if live, ok := s.liveRuntimeState(seedHash); ok {
			runtimeState = live
		}
		s.writeJSON(w, http.StatusOK, map[string]any{
			"seed_hash":   seedHash,
			"state":       "local",
			"file_path":   p,
			"file_size":   size,
			"local_ready": true,
			"runtime":     runtimeState,
		})
		return
	}
	state, err := dbGetFileDownloadState(r.Context(), fileHTTPStore(s), seedHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.writeJSON(w, http.StatusOK, map[string]any{
				"seed_hash": seedHash,
				"state":     "idle",
				"runtime":   defaultFileDownloadRuntimeState(),
			})
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	runtimeState := decodeFileDownloadRuntimeState(state.StatusJSON)
	if live, ok := s.liveRuntimeState(seedHash); ok {
		runtimeState = live
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"seed_hash":        seedHash,
		"state":            state.Status,
		"file_path":        state.FilePath,
		"file_size":        state.FileSize,
		"chunk_count":      state.ChunkCount,
		"completed_chunks": state.Completed,
		"paid_sats":        state.PaidSats,
		"demand_id":        state.DemandID,
		"error":            state.LastError,
		"runtime":          runtimeState,
		"updated_at_unix":  state.UpdatedAtUnix,
	})
}

func (s *fileHTTPServer) handleDownload(w http.ResponseWriter, r *http.Request, seedHash string) {
	if p, _, ok := s.findCompleteLocalFile(seedHash); ok {
		s.serveLocalFile(w, r, p)
		return
	}
	sess, err := s.ensureSession(seedHash)
	if err != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	pr, err := parseSingleRange(r.Header.Get("Range"))
	if err != nil {
		w.Header().Set("Accept-Ranges", "bytes")
		s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": err.Error()})
		return
	}
	if err := s.serveFromSession(w, r, sess, pr); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "upstream download timeout"})
			return
		}
		if strings.Contains(err.Error(), "range not satisfiable") {
			s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": err.Error()})
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
}

func (s *fileHTTPServer) ensureSession(seedHash string) (*fileDownloadSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[seedHash]; ok {
		return sess, nil
	}
	if s.activeSessionCountLocked() >= int(s.cfg.FSHTTP.MaxConcurrentSessions) {
		return nil, fmt.Errorf("concurrent file sessions limit exceeded")
	}
	sess := &fileDownloadSession{
		seedHash:     seedHash,
		server:       s,
		status:       "queued",
		completed:    map[uint32]bool{},
		partPath:     filepath.Join(s.cfg.Storage.DataDir, "downloads", seedHash+".part"),
		runtimeState: defaultFileDownloadRuntimeState(),
	}
	sess.cond = sync.NewCond(&sess.mu)
	sess.chunkPolicy = newDemandPrefetchBackfillPolicy(demandPrefetchBackfillPolicyConfig{
		PrefetchDistance: s.cfg.FSHTTP.PrefetchDistanceChunks,
		DebugLogEnabled:  s.cfg.FSHTTP.StrategyDebugLogEnabled,
		Logf:             sess.strategyLog,
	})
	sess.refreshChunkPolicySnapshotLocked()
	s.sessions[seedHash] = sess
	go sess.run()
	return sess, nil
}

func (s *fileHTTPServer) activeSessionCountLocked() int {
	n := 0
	for _, sess := range s.sessions {
		sess.mu.Lock()
		st := sess.status
		sess.mu.Unlock()
		if st == "running" || st == "queued" || st == "seed_fetch" {
			n++
		}
	}
	return n
}

func (sess *fileDownloadSession) run() {
	if err := sess.prepareAndDownload(); err != nil {
		sess.mu.Lock()
		sess.status = "failed"
		sess.lastError = err.Error()
		sess.persistStateLocked()
		sess.cond.Broadcast()
		sess.mu.Unlock()
		return
	}
	sess.mu.Lock()
	sess.status = "done"
	sess.lastError = ""
	sess.persistStateLocked()
	sess.cond.Broadcast()
	sess.mu.Unlock()
}

func (sess *fileDownloadSession) prepareAndDownload() error {
	if err := os.MkdirAll(filepath.Dir(sess.partPath), 0o755); err != nil {
		return err
	}
	if err := sess.loadStateFromDB(); err != nil {
		return err
	}
	sess.mu.Lock()
	sess.status = "seed_fetch"
	sess.persistStateLocked()
	sess.mu.Unlock()

	ctx := context.Background()
	gw, err := pickGatewayForBusiness(sess.server.rt, "")
	if err != nil {
		return err
	}
	pub, err := TriggerGatewayPublishDemand(ctx, sess.server.rt, PublishDemandParams{SeedHash: sess.seedHash, ChunkCount: 1, GatewayPeerID: gw.ID.String()})
	if err != nil {
		return err
	}
	sess.mu.Lock()
	sess.demandID = strings.TrimSpace(pub.DemandID)
	sess.persistStateLocked()
	sess.mu.Unlock()

	quotes, err := sess.waitQuotes(ctx, strings.TrimSpace(pub.DemandID))
	if err != nil {
		return err
	}
	if len(quotes) == 0 {
		return fmt.Errorf("no quote under max price")
	}
	workers, meta, seedBytes, err := prepareSpeedPriceWorkersAndSeed(speedPriceBootstrapParams{
		Ctx:      ctx,
		Buyer:    sess.server.rt,
		Quotes:   quotes,
		SeedHash: sess.seedHash,
		OnQuoteRejected: func(q DirectQuoteItem, err error) {
			fields := map[string]any{
				"seller_pubkey_hex": q.SellerPeerID,
				"reason":            "arbiter_unavailable",
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			sess.strategyLog("bootstrap_quote_rejected", fields)
		},
		OnQuoteAccepted: func(q DirectQuoteItem, arbiterPeerID string) {
			sess.strategyLog("bootstrap_quote_accepted", map[string]any{
				"seller_pubkey_hex":  q.SellerPeerID,
				"arbiter_pubkey_hex": arbiterPeerID,
				"chunk_price":        q.ChunkPrice,
				"seed_price":         q.SeedPrice,
			})
		},
		OnSeedProbeFail: func(w *transferSellerWorker, reason string, err error) {
			fields := map[string]any{
				"seller_pubkey_hex": w.quote.SellerPeerID,
				"reason":            reason,
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			sess.strategyLog("bootstrap_seed_probe_failed", fields)
		},
		OnSeedProbeOK: func(w *transferSellerWorker, meta seedV1Meta) {
			sess.strategyLog("bootstrap_seed_probe_ok", map[string]any{
				"seller_pubkey_hex": w.quote.SellerPeerID,
				"chunk_count":       meta.ChunkCount,
				"file_size":         meta.FileSize,
			})
		},
	})
	if err != nil {
		return err
	}
	if err := sess.initMeta(meta); err != nil {
		_ = closeTransferWorkers(context.Background(), workers)
		return err
	}

	f, err := os.OpenFile(sess.partPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = closeTransferWorkers(context.Background(), workers)
		return err
	}
	defer f.Close()
	if err := f.Truncate(int64(meta.FileSize)); err != nil {
		_ = closeTransferWorkers(context.Background(), workers)
		return err
	}

	pending := make(map[uint32]bool, meta.ChunkCount)
	sess.mu.Lock()
	for i := uint32(0); i < meta.ChunkCount; i++ {
		if !sess.completed[i] {
			pending[i] = true
		}
	}
	sess.mu.Unlock()
	if len(pending) > 0 {
		_, err := runSpeedPriceChunkScheduler(speedPriceChunkSchedulerParams{
			Ctx:             ctx,
			Workers:         workers,
			ChunkHashes:     meta.ChunkHashes,
			Pending:         pending,
			MaxRetries:      defaultChunkRetryMax,
			Strategy:        buildTransferStrategy(TransferStrategySmart),
			StrategyName:    TransferStrategySmart,
			SelectChunk:     sess.nextPendingChunk,
			OnState:         sess.updateStrategyRuntimeState,
			DebugLogEnabled: sess.server.cfg.FSHTTP.StrategyDebugLogEnabled,
			Logf:            sess.strategyLog,
			OnChunk: func(chunkIndex uint32, chunk []byte, w *transferSellerWorker, elapsed time.Duration) (uint64, error) {
				off := int64(chunkIndex) * seedBlockSize
				writeN := len(chunk)
				maxN := int64(meta.FileSize) - off
				if maxN < int64(writeN) {
					writeN = int(maxN)
				}
				if writeN < 0 {
					writeN = 0
				}
				if writeN > 0 {
					if _, err := f.WriteAt(chunk[:writeN], off); err != nil {
						return 0, err
					}
					if err := f.Sync(); err != nil {
						return 0, err
					}
				}
				sess.markChunkDone(chunkIndex, w.quote.SellerPeerID, w.quote.ChunkPrice)
				return uint64(writeN), nil
			},
		})
		if err != nil {
			_ = closeTransferWorkers(context.Background(), workers)
			return err
		}
	}
	if err := closeTransferWorkers(context.Background(), workers); err != nil {
		return err
	}

	outName := pickRecommendedFileName(quotes, sess.seedHash)
	if outName == "" {
		outName = sess.seedHash + ".bin"
	}
	outPath, err := sess.server.workspace.SelectOutputPath(outName, meta.FileSize)
	if err != nil {
		return err
	}
	if err := copyFile(sess.partPath, outPath); err != nil {
		return err
	}
	if _, err := sess.server.workspace.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              outPath,
		Seed:                  seedBytes,
		AvailableChunkIndexes: contiguousChunkIndexes(meta.ChunkCount),
		RecommendedFileName:   outName,
		MIMEHint:              pickRecommendedMIMEHint(quotes),
	}); err != nil {
		return err
	}
	return nil
}

func (sess *fileDownloadSession) waitQuotes(ctx context.Context, demandID string) ([]DirectQuoteItem, error) {
	deadline := time.Now().Add(time.Duration(sess.server.cfg.FSHTTP.QuoteWaitSeconds) * time.Second)
	poll := time.Duration(sess.server.cfg.FSHTTP.QuotePollSeconds) * time.Second
	if poll <= 0 {
		poll = 2 * time.Second
	}
	maxChunk := sess.server.cfg.FSHTTP.MaxChunkPriceSatPer64K
	for {
		quotes, err := TriggerClientListDirectQuotes(ctx, sess.server.rt, demandID)
		if err != nil {
			return nil, err
		}
		filtered := make([]DirectQuoteItem, 0, len(quotes))
		for _, q := range quotes {
			if maxChunk > 0 && q.ChunkPrice > maxChunk {
				continue
			}
			filtered = append(filtered, q)
		}
		if len(filtered) > 0 {
			return filtered, nil
		}
		if time.Now().After(deadline) {
			return nil, context.DeadlineExceeded
		}
		time.Sleep(poll)
	}
}

func (sess *fileDownloadSession) initMeta(meta seedV1Meta) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.fileSize = meta.FileSize
	sess.chunkCount = meta.ChunkCount
	sess.chunkHashes = append([]string(nil), meta.ChunkHashes...)
	sess.status = "running"
	sess.refreshChunkPolicySnapshotLocked()
	sess.persistStateLocked()
	sess.cond.Broadcast()
	return nil
}

func (sess *fileDownloadSession) loadStateFromDB() error {
	state, err := dbGetFileDownloadStateNoUpdated(context.Background(), fileHTTPStore(sess.server), sess.seedHash)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return dbEnsureFileDownloadQueued(context.Background(), fileHTTPStore(sess.server), sess.seedHash, sess.partPath, encodeFileDownloadRuntimeState(sess.runtimeState))
	}
	sess.mu.Lock()
	sess.partPath = strings.TrimSpace(state.FilePath)
	if sess.partPath == "" {
		sess.partPath = filepath.Join(sess.server.cfg.Storage.DataDir, "downloads", sess.seedHash+".part")
	}
	sess.fileSize = state.FileSize
	sess.chunkCount = state.ChunkCount
	sess.status = state.Status
	sess.demandID = state.DemandID
	sess.lastError = state.LastError
	sess.paidSats = state.PaidSats
	sess.runtimeState = decodeFileDownloadRuntimeState(state.StatusJSON)
	sess.refreshChunkPolicySnapshotLocked()
	sess.mu.Unlock()

	rows, err := dbListFileDownloadChunks(context.Background(), fileHTTPStore(sess.server), sess.seedHash)
	if err != nil {
		return err
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, row := range rows {
		if row.Status == "done" {
			sess.completed[row.ChunkIndex] = true
		}
	}
	return nil
}

func (sess *fileDownloadSession) persistStateLocked() {
	completed := uint32(len(sess.completed))
	sess.runtimeState.UpdatedAtUnix = time.Now().Unix()
	if sess.server == nil || fileHTTPStore(sess.server) == nil {
		return
	}
	_ = dbUpsertFileDownloadState(context.Background(), fileHTTPStore(sess.server), sess.seedHash, fileDownloadStateRow{
		FilePath:   sess.partPath,
		FileSize:   sess.fileSize,
		ChunkCount: sess.chunkCount,
		Completed:  completed,
		PaidSats:   sess.paidSats,
		Status:     sess.status,
		DemandID:   sess.demandID,
		LastError:  sess.lastError,
		StatusJSON: encodeFileDownloadRuntimeState(sess.runtimeState),
	})
}

func (sess *fileDownloadSession) markChunkDone(idx uint32, sellerPeerID string, price uint64) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.completed[idx] = true
	sess.paidSats += price
	if sess.server == nil || fileHTTPStore(sess.server) == nil {
		return
	}
	_ = dbUpsertFileDownloadChunkDone(context.Background(), fileHTTPStore(sess.server), sess.seedHash, idx, sellerPeerID, price)
	sess.persistStateLocked()
	sess.cond.Broadcast()
}

func (sess *fileDownloadSession) nextPendingChunk(pending map[uint32]bool, inflight map[uint32]bool) (uint32, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(pending) == 0 || sess.chunkPolicy == nil {
		return 0, false
	}
	ch, ok := sess.chunkPolicy.SelectNext(pending, inflight, sess.completed, sess.chunkCount)
	if ok {
		sess.refreshChunkPolicySnapshotLocked()
		sess.persistStateLocked()
	}
	return ch, ok
}

func (s *fileHTTPServer) serveFromSession(w http.ResponseWriter, r *http.Request, sess *fileDownloadSession, pr parsedRange) error {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.cfg.FSHTTP.DownloadWaitTimeoutSeconds)*time.Second)
	defer cancel()

	if err := sess.waitMeta(ctx); err != nil {
		return err
	}

	sess.mu.Lock()
	fileSize := sess.fileSize
	chunkCount := sess.chunkCount
	sess.mu.Unlock()
	if fileSize == 0 || chunkCount == 0 {
		return fmt.Errorf("file metadata unavailable")
	}
	start := int64(0)
	end := int64(fileSize) - 1
	if pr.ok {
		if pr.start >= int64(fileSize) {
			return fmt.Errorf("range not satisfiable")
		}
		start = pr.start
		if pr.end < int64(fileSize) {
			end = pr.end
		}
		if end < start {
			return fmt.Errorf("range not satisfiable")
		}
	}
	startChunk := uint32(start / seedBlockSize)
	endChunk := uint32(end / seedBlockSize)
	sess.addWantedRange(startChunk, endChunk)
	defer sess.removeWantedRange(startChunk, endChunk)

	if err := sess.waitChunkDone(ctx, startChunk); err != nil {
		return err
	}

	f, err := os.Open(sess.partPath)
	if err != nil {
		return err
	}
	defer f.Close()

	w.Header().Set("Accept-Ranges", "bytes")
	if pr.ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatUint(fileSize, 10))
		w.WriteHeader(http.StatusOK)
	}

	buf := make([]byte, 32*1024)
	cur := start
	for cur <= end {
		chunk := uint32(cur / seedBlockSize)
		if err := sess.waitChunkDone(r.Context(), chunk); err != nil {
			return err
		}
		chunkEnd := (int64(chunk)+1)*seedBlockSize - 1
		if chunkEnd > end {
			chunkEnd = end
		}
		if _, err := f.Seek(cur, io.SeekStart); err != nil {
			return err
		}
		remain := chunkEnd - cur + 1
		for remain > 0 {
			n := int64(len(buf))
			if remain < n {
				n = remain
			}
			m, err := io.ReadFull(f, buf[:n])
			if err != nil {
				return err
			}
			if _, err := w.Write(buf[:m]); err != nil {
				return err
			}
			remain -= int64(m)
			cur += int64(m)
		}
	}
	return nil
}

func (sess *fileDownloadSession) waitMeta(ctx context.Context) error {
	for {
		sess.mu.Lock()
		ready := sess.chunkCount > 0 && sess.fileSize > 0
		failed := sess.status == "failed"
		lastErr := sess.lastError
		sess.mu.Unlock()
		if ready {
			return nil
		}
		if failed {
			return errors.New(lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(120 * time.Millisecond):
		}
	}
}

func (sess *fileDownloadSession) waitChunkDone(ctx context.Context, idx uint32) error {
	for {
		sess.mu.Lock()
		done := sess.completed[idx]
		failed := sess.status == "failed"
		lastErr := sess.lastError
		sess.mu.Unlock()
		if done {
			return nil
		}
		if failed {
			return errors.New(lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(120 * time.Millisecond):
		}
	}
}

func (sess *fileDownloadSession) addWantedRange(start, end uint32) {
	sess.mu.Lock()
	if sess.chunkPolicy != nil {
		sess.chunkPolicy.AddDemand(start, end)
		sess.refreshChunkPolicySnapshotLocked()
		sess.persistStateLocked()
	}
	sess.cond.Broadcast()
	sess.mu.Unlock()
}

func (sess *fileDownloadSession) removeWantedRange(start, end uint32) {
	sess.mu.Lock()
	if sess.chunkPolicy != nil {
		sess.chunkPolicy.RemoveDemand(start, end)
		sess.refreshChunkPolicySnapshotLocked()
		sess.persistStateLocked()
	}
	sess.mu.Unlock()
}

func (s *fileHTTPServer) findCompleteLocalFile(seedHash string) (string, uint64, bool) {
	p, _, err := dbFindLatestWorkspaceFileBySeedHash(context.Background(), fileHTTPStore(s), seedHash)
	if err != nil {
		return "", 0, false
	}
	st, statErr := os.Stat(p)
	if statErr != nil || !st.Mode().IsRegular() {
		return "", 0, false
	}
	return p, uint64(st.Size()), true
}

func (s *fileHTTPServer) serveLocalFile(w http.ResponseWriter, r *http.Request, path string) {
	pr, err := parseSingleRange(r.Header.Get("Range"))
	if err != nil {
		w.Header().Set("Accept-Ranges", "bytes")
		s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": err.Error()})
		return
	}
	f, err := os.Open(path)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	size := st.Size()
	if size <= 0 {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		return
	}
	start := int64(0)
	end := size - 1
	if pr.ok {
		if pr.start >= size {
			s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": "range not satisfiable"})
			return
		}
		start = pr.start
		if pr.end < size {
			end = pr.end
		}
		if end < start {
			s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": "range not satisfiable"})
			return
		}
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	if pr.ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return
	}
	_, _ = io.CopyN(w, f, end-start+1)
}

func (s *fileHTTPServer) serveBytes(w http.ResponseWriter, r *http.Request, data []byte, contentType string) {
	pr, err := parseSingleRange(r.Header.Get("Range"))
	if err != nil {
		w.Header().Set("Accept-Ranges", "bytes")
		s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": err.Error()})
		return
	}
	size := int64(len(data))
	if size <= 0 {
		if strings.TrimSpace(contentType) != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	start := int64(0)
	end := size - 1
	if pr.ok {
		if pr.start >= size {
			s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": "range not satisfiable"})
			return
		}
		start = pr.start
		if pr.end < size {
			end = pr.end
		}
		if end < start {
			s.writeJSON(w, http.StatusRequestedRangeNotSatisfiable, map[string]any{"error": "range not satisfiable"})
			return
		}
	}
	w.Header().Set("Accept-Ranges", "bytes")
	if strings.TrimSpace(contentType) != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if pr.ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(data[int(start) : int(end)+1])
}

func (s *fileHTTPServer) listLocalLiveSegmentFiles(streamID string) ([]localLiveSegmentFile, error) {
	rows, err := dbListLiveSegmentWorkspaceEntries(context.Background(), fileHTTPStore(s), streamID)
	if err != nil {
		return nil, err
	}
	out := make([]localLiveSegmentFile, 0, 16)
	for _, row := range rows {
		if st, err := os.Stat(row.Path); err != nil || !st.Mode().IsRegular() {
			continue
		}
		segmentBytes, err := os.ReadFile(row.Path)
		if err != nil {
			continue
		}
		data, _, _, err := VerifyLiveSegment(segmentBytes)
		if err != nil {
			continue
		}
		if data.SegmentIndex > 0 && !strings.EqualFold(data.StreamID, streamID) {
			continue
		}
		playlistURI := strings.TrimSpace(data.PlaylistURIHint)
		if playlistURI == "" {
			playlistURI = fmt.Sprintf("/live/%s/%d", streamID, data.SegmentIndex)
		}
		mediaSequence := data.MediaSequence
		if mediaSequence == 0 && data.SegmentIndex > 0 {
			mediaSequence = data.SegmentIndex
		}
		out = append(out, localLiveSegmentFile{
			SegmentIndex:    data.SegmentIndex,
			MediaSequence:   mediaSequence,
			Path:            row.Path,
			SeedHash:        strings.ToLower(strings.TrimSpace(row.SeedHash)),
			UpdatedAtUnix:   row.UpdatedAtUnix,
			DurationMs:      data.DurationMs,
			IsDiscontinuity: data.IsDiscontinuity,
			InitSeedHash:    data.InitSeedHash,
			PlaylistURI:     playlistURI,
			IsEnd:           data.IsEnd,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MediaSequence == out[j].MediaSequence {
			return out[i].SegmentIndex < out[j].SegmentIndex
		}
		return out[i].MediaSequence < out[j].MediaSequence
	})
	return out, nil
}

func (s *fileHTTPServer) findLocalLiveSegmentFile(streamID string, segmentIndex uint64) (string, error) {
	items, err := s.listLocalLiveSegmentFiles(streamID)
	if err != nil {
		return "", err
	}
	for _, it := range items {
		if it.SegmentIndex == segmentIndex {
			return it.Path, nil
		}
	}
	return "", sql.ErrNoRows
}

func formatExtINF(durationMs uint64) string {
	if durationMs == 0 {
		return "2.000"
	}
	return fmt.Sprintf("%.3f", float64(durationMs)/1000.0)
}

func durationMsToTargetDuration(durationMs uint64) uint64 {
	if durationMs == 0 {
		return 2
	}
	secs := durationMs / 1000
	if durationMs%1000 != 0 {
		secs++
	}
	if secs == 0 {
		return 1
	}
	return secs
}

func parseSingleRange(raw string) (parsedRange, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return parsedRange{}, nil
	}
	if !strings.HasPrefix(raw, "bytes=") {
		return parsedRange{}, fmt.Errorf("invalid range format")
	}
	body := strings.TrimSpace(strings.TrimPrefix(raw, "bytes="))
	if strings.Contains(body, ",") {
		return parsedRange{}, fmt.Errorf("multiple ranges are not supported")
	}
	parts := strings.Split(body, "-")
	if len(parts) != 2 {
		return parsedRange{}, fmt.Errorf("invalid range format")
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return parsedRange{}, fmt.Errorf("open-ended ranges are not supported")
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || start < 0 {
		return parsedRange{}, fmt.Errorf("invalid range start")
	}
	end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || end < 0 || end < start {
		return parsedRange{}, fmt.Errorf("invalid range end")
	}
	return parsedRange{start: start, end: end, ok: true}, nil
}

func isSeedHashHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func (s *fileHTTPServer) liveRuntimeState(seedHash string) (fileDownloadRuntimeState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[seedHash]
	if !ok {
		return fileDownloadRuntimeState{}, false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.runtimeState, true
}

func (sess *fileDownloadSession) updateStrategyRuntimeState(state speedPriceSchedulerState) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.runtimeState.Strategy = state
	sess.refreshChunkPolicySnapshotLocked()
	sess.runtimeState.UpdatedAtUnix = time.Now().Unix()
	sess.persistStateLocked()
}

func (sess *fileDownloadSession) refreshChunkPolicySnapshotLocked() {
	if sess.chunkPolicy == nil {
		return
	}
	sess.runtimeState.ChunkPolicy = sess.chunkPolicy.Snapshot()
	sess.runtimeState.UpdatedAtUnix = time.Now().Unix()
}

func (sess *fileDownloadSession) strategyLog(event string, fields map[string]any) {
	if sess == nil || sess.server == nil || sess.server.cfg == nil {
		return
	}
	if !sess.server.cfg.FSHTTP.StrategyDebugLogEnabled {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["seed_hash"] = sess.seedHash
	if strings.TrimSpace(sess.demandID) != "" {
		fields["demand_id"] = sess.demandID
	}
	obs.Info("bitcast-client", "fs_strategy_"+event, fields)
}

func (s *fileHTTPServer) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *fileHTTPServer) Resume(seedHash string, full bool) error {
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if !isSeedHashHex(seedHash) {
		return fmt.Errorf("invalid seed hash")
	}
	sess, err := s.ensureSession(seedHash)
	if err != nil {
		return err
	}
	if full {
		sess.mu.Lock()
		if sess.chunkPolicy != nil {
			sess.chunkPolicy.SetFocus(0)
			sess.refreshChunkPolicySnapshotLocked()
			sess.persistStateLocked()
		}
		sess.mu.Unlock()
	}
	return nil
}
