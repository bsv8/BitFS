package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type getFilePlanRequest struct {
	SeedHashes    []string `json:"seed_hashes"`
	GatewayPeerID string   `json:"gateway_pubkey_hex,omitempty"`
	ResourceKind  string   `json:"resource_kind,omitempty"`
	RootSeedHash  string   `json:"root_seed_hash,omitempty"`
}

type getFileEnsureRequest struct {
	SeedHash         string `json:"seed_hash"`
	GatewayPeerID    string `json:"gateway_pubkey_hex,omitempty"`
	MaxTotalPriceSat uint64 `json:"max_total_price_sat,omitempty"`
}

type staticResourcePlanItem struct {
	SeedHash            string `json:"seed_hash"`
	Status              string `json:"status"`
	Reason              string `json:"reason,omitempty"`
	LocalReady          bool   `json:"local_ready"`
	RecommendedFileName string `json:"recommended_file_name,omitempty"`
	MIMEHint            string `json:"mime_hint,omitempty"`
	FileSize            uint64 `json:"file_size,omitempty"`
	ChunkCount          uint32 `json:"chunk_count,omitempty"`
	SeedPrice           uint64 `json:"seed_price,omitempty"`
	ChunkPrice          uint64 `json:"chunk_price_sat_per_64k,omitempty"`
	EstimatedTotalSat   uint64 `json:"estimated_total_sat,omitempty"`
}

type staticResourceStatus struct {
	staticResourcePlanItem
	OutputFilePath string `json:"output_file_path,omitempty"`
	ContentURI     string `json:"content_uri,omitempty"`
}

// 设计说明：
// - `plan` 负责把“浏览器资源请求”提前变成价格与可用性判断；
// - 这里优先走“网关原生批量 demand 发布”，让 HTML 初始静态资源能一次性询价；
// - sellers 仍然按单 demand 递交 direct quote，因此后端选价逻辑不需要被推翻；
// - plan 的职责是给浏览器预算闸门做决策，不在这里掺进下载落盘。
func (s *httpAPIServer) handleGetFilePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req getFilePlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	hashes, err := normalizeSeedHashList(req.SeedHashes)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	items, _, err := s.planStaticSeedResources(r.Context(), hashes, req.GatewayPeerID)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":          items,
		"total":          len(items),
		"resource_kind":  strings.TrimSpace(req.ResourceKind),
		"root_seed_hash": strings.ToLower(strings.TrimSpace(req.RootSeedHash)),
	})
}

func (s *httpAPIServer) handleGetFileContent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	seedHash := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("seed_hash")))
	if !isSeedHashHex(seedHash) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid seed hash"})
		return
	}
	if path, _, ok := findCompleteLocalFileBySeedHash(s.store, seedHash); ok {
		serveSeedContentFile(w, r, path, mimeHintBySeedHash(s.store, seedHash))
		return
	}
	items, quotes, err := s.planStaticSeedResources(r.Context(), []string{seedHash}, r.URL.Query().Get("gateway_pubkey_hex"))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	item := staticResourcePlanItem{SeedHash: seedHash, Status: "quote_unavailable", Reason: "plan item missing"}
	if len(items) > 0 {
		item = items[0]
	}
	quote, _ := quotes[seedHash]
	if quote == nil || item.Status != "quoted" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "resource quote not ready", "status": item.Status, "reason": item.Reason})
		return
	}
	if maxTotalRaw := strings.TrimSpace(r.URL.Query().Get("max_total_price_sat")); maxTotalRaw != "" {
		maxTotal, parseErr := strconv.ParseUint(maxTotalRaw, 10, 64)
		if parseErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid max_total_price_sat"})
			return
		}
		if maxTotal > 0 && item.EstimatedTotalSat > maxTotal {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":               "resource price exceeded approved threshold",
				"seed_hash":           seedHash,
				"estimated_total_sat": item.EstimatedTotalSat,
				"approved_total_sat":  maxTotal,
			})
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	path, err := s.downloadStaticSeedToWorkspace(ctx, seedHash, *quote)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	serveSeedContentFile(w, r, path, item.MIMEHint)
}

func (s *httpAPIServer) handleGetFileStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	seedHash := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("seed_hash")))
	if !isSeedHashHex(seedHash) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid seed hash"})
		return
	}
	status, err := s.buildStaticResourceStatus(r.Context(), seedHash, r.URL.Query().Get("gateway_pubkey_hex"))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *httpAPIServer) handleGetFileEnsure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req getFileEnsureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if !isSeedHashHex(seedHash) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid seed hash"})
		return
	}
	if path, _, ok := findCompleteLocalFileBySeedHash(s.store, seedHash); ok {
		status := buildLocalStaticResourceStatus(s.store, seedHash, path)
		writeJSON(w, http.StatusOK, status)
		return
	}
	items, quotes, err := s.planStaticSeedResources(r.Context(), []string{seedHash}, req.GatewayPeerID)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	item := staticResourcePlanItem{SeedHash: seedHash, Status: "quote_unavailable", Reason: "plan item missing"}
	if len(items) > 0 {
		item = items[0]
	}
	quote, _ := quotes[seedHash]
	if quote == nil || item.Status != "quoted" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "resource quote not ready", "status": item.Status, "reason": item.Reason})
		return
	}
	if req.MaxTotalPriceSat > 0 && item.EstimatedTotalSat > req.MaxTotalPriceSat {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":               "resource price exceeded approved threshold",
			"seed_hash":           seedHash,
			"estimated_total_sat": item.EstimatedTotalSat,
			"approved_total_sat":  req.MaxTotalPriceSat,
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	path, err := s.downloadStaticSeedToWorkspace(ctx, seedHash, *quote)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	status := buildLocalStaticResourceStatus(s.store, seedHash, path)
	status.EstimatedTotalSat = item.EstimatedTotalSat
	status.SeedPrice = item.SeedPrice
	status.ChunkPrice = item.ChunkPrice
	writeJSON(w, http.StatusOK, status)
}

func (s *httpAPIServer) planStaticSeedResources(ctx context.Context, seedHashes []string, gwOverride string) ([]staticResourcePlanItem, map[string]*DirectQuoteItem, error) {
	hashes, err := normalizeSeedHashList(seedHashes)
	if err != nil {
		return nil, nil, err
	}
	itemsBySeedHash := make(map[string]staticResourcePlanItem, len(hashes))
	quotes := make(map[string]*DirectQuoteItem, len(hashes))
	missing := make([]PublishDemandBatchItem, 0, len(hashes))
	missingOrder := make([]string, 0, len(hashes))
	for _, seedHash := range hashes {
		if path, size, ok := findCompleteLocalFileBySeedHash(s.store, seedHash); ok {
			recommendedName, mimeHint := localSeedMetadataBySeedHash(s.store, seedHash, path)
			item := staticResourcePlanItem{
				SeedHash:            seedHash,
				Status:              "local",
				LocalReady:          true,
				FileSize:            size,
				RecommendedFileName: recommendedName,
				MIMEHint:            mimeHint,
			}
			if chunkCount, ok := loadSeedChunkCount(s.store, seedHash); ok {
				item.ChunkCount = chunkCount
			}
			itemsBySeedHash[seedHash] = item
			continue
		}
		missing = append(missing, PublishDemandBatchItem{SeedHash: seedHash, ChunkCount: 1})
		missingOrder = append(missingOrder, seedHash)
	}
	if len(missing) == 0 {
		return collectPlanItemsByOrder(hashes, itemsBySeedHash), quotes, nil
	}
	if s == nil || s.rt == nil {
		for _, missingSeedHash := range missingOrder {
			itemsBySeedHash[missingSeedHash] = staticResourcePlanItem{
				SeedHash: missingSeedHash,
				Status:   "runtime_unavailable",
				Reason:   "client runtime not initialized",
			}
		}
		return collectPlanItemsByOrder(hashes, itemsBySeedHash), quotes, nil
	}
	publishCtx, cancel := context.WithTimeout(ctx, filePlanTimeout(s.cfg))
	defer cancel()
	pub, err := TriggerGatewayPublishDemandBatch(publishCtx, s.store, s.rt, PublishDemandBatchParams{
		Items:         missing,
		GatewayPeerID: strings.TrimSpace(gwOverride),
	})
	if err != nil {
		return nil, nil, err
	}
	demandBySeedHash := make(map[string]string, len(pub.Items))
	for _, item := range pub.Items {
		if item == nil {
			continue
		}
		demandBySeedHash[strings.ToLower(strings.TrimSpace(item.SeedHash))] = strings.TrimSpace(item.DemandID)
	}
	bestQuotes, reasons, timedOut, err := waitBestStaticQuotesBySeedHash(
		publishCtx,
		s.store,
		s.rt,
		demandBySeedHash,
		filePlanQuoteRetry(s.cfg),
		filePlanQuoteInterval(s.cfg),
	)
	if err != nil {
		return nil, nil, err
	}
	for _, seedHash := range missingOrder {
		if best, ok := bestQuotes[seedHash]; ok {
			bestCopy := best
			quotes[seedHash] = &bestCopy
			itemsBySeedHash[seedHash] = staticResourcePlanItem{
				SeedHash:            seedHash,
				Status:              "quoted",
				LocalReady:          false,
				RecommendedFileName: sanitizeRecommendedFileName(best.RecommendedFileName),
				MIMEHint:            sanitizeMIMEHint(best.MIMEHint),
				FileSize:            best.FileSize,
				ChunkCount:          best.ChunkCount,
				SeedPrice:           best.SeedPrice,
				ChunkPrice:          best.ChunkPrice,
				EstimatedTotalSat:   estimateStaticResourceTotal(best),
			}
			continue
		}
		status := "quote_unavailable"
		reason := reasons[seedHash]
		if timedOut[seedHash] {
			status = "quote_timeout"
			if strings.TrimSpace(reason) == "" {
				reason = "wait direct quotes timeout"
			}
		}
		if strings.TrimSpace(reason) == "" {
			reason = "no complete quote with chunk metadata"
		}
		itemsBySeedHash[seedHash] = staticResourcePlanItem{
			SeedHash: seedHash,
			Status:   status,
			Reason:   reason,
		}
	}
	return collectPlanItemsByOrder(hashes, itemsBySeedHash), quotes, nil
}

func (s *httpAPIServer) buildStaticResourceStatus(ctx context.Context, seedHash string, gwOverride string) (staticResourceStatus, error) {
	if path, _, ok := findCompleteLocalFileBySeedHash(s.store, seedHash); ok {
		return buildLocalStaticResourceStatus(s.store, seedHash, path), nil
	}
	items, _, err := s.planStaticSeedResources(ctx, []string{seedHash}, gwOverride)
	if err != nil {
		return staticResourceStatus{}, err
	}
	if len(items) == 0 {
		return staticResourceStatus{
			staticResourcePlanItem: staticResourcePlanItem{
				SeedHash: seedHash,
				Status:   "quote_unavailable",
				Reason:   "plan item missing",
			},
			ContentURI: "bitfs://" + seedHash,
		}, nil
	}
	return staticResourceStatus{
		staticResourcePlanItem: items[0],
		ContentURI:             "bitfs://" + seedHash,
	}, nil
}

func (s *httpAPIServer) downloadStaticSeedToWorkspace(ctx context.Context, seedHash string, quote DirectQuoteItem) (string, error) {
	if path, _, ok := findCompleteLocalFileBySeedHash(s.store, seedHash); ok {
		return path, nil
	}
	if s == nil || s.rt == nil || s.workspace == nil {
		return "", fmt.Errorf("workspace runtime not initialized")
	}
	workers, meta, seedBytes, err := prepareSpeedPriceWorkersAndSeed(speedPriceBootstrapParams{
		Ctx:      ctx,
		Buyer:    s.rt,
		Store:    s.store,
		Quotes:   []DirectQuoteItem{quote},
		SeedHash: seedHash,
	})
	if err != nil {
		return "", err
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer closeCancel()
	defer func() { _ = closeTransferWorkers(closeCtx, workers) }()

	if meta.ChunkCount == 0 {
		return "", fmt.Errorf("seed has zero chunks")
	}
	if missing := missingChunkCoverage(meta.ChunkCount, workers); len(missing) > 0 {
		return "", fmt.Errorf("selected quote does not cover all chunks")
	}
	chunks := make([][]byte, meta.ChunkCount)
	pending := make(map[uint32]bool, meta.ChunkCount)
	for i := uint32(0); i < meta.ChunkCount; i++ {
		pending[i] = true
	}
	_, err = runSpeedPriceChunkScheduler(speedPriceChunkSchedulerParams{
		Ctx:          ctx,
		Workers:      workers,
		ChunkHashes:  append([]string(nil), meta.ChunkHashes...),
		Pending:      pending,
		MaxRetries:   defaultChunkRetryMax,
		Strategy:     buildTransferStrategy(TransferStrategySmart),
		StrategyName: TransferStrategySmart,
		SelectChunk:  defaultSelectChunk,
		OnChunk: func(chunkIndex uint32, chunk []byte, _ *transferSellerWorker, _ time.Duration) (uint64, error) {
			if chunkIndex >= uint32(len(chunks)) {
				return 0, fmt.Errorf("chunk index out of range")
			}
			if chunks[chunkIndex] == nil {
				chunks[chunkIndex] = append([]byte(nil), chunk...)
				return uint64(len(chunk)), nil
			}
			return 0, nil
		},
	})
	if err != nil {
		return "", err
	}
	data := make([]byte, 0, int(meta.ChunkCount)*int(seedBlockSize))
	for i := 0; i < int(meta.ChunkCount); i++ {
		if chunks[i] == nil {
			return "", fmt.Errorf("missing chunk index=%d", i)
		}
		data = append(data, chunks[i]...)
	}
	if uint64(len(data)) > meta.FileSize {
		data = data[:meta.FileSize]
	}
	outName := sanitizeRecommendedFileName(quote.RecommendedFileName)
	if outName == "" {
		outName = seedHash + ".bin"
	}
	outPath, err := s.workspace.SelectOutputPath(outName, meta.FileSize)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(outPath, data, 0o644); err != nil {
		return "", err
	}
	if _, err := s.workspace.RegisterDownloadedFile(registerDownloadedFileParams{
		FilePath:              outPath,
		Seed:                  seedBytes,
		AvailableChunkIndexes: contiguousChunkIndexes(meta.ChunkCount),
		RecommendedFileName:   quote.RecommendedFileName,
		MIMEHint:              quote.MIMEHint,
	}); err != nil {
		return "", err
	}
	return outPath, nil
}

func normalizeSeedHashList(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("seed_hashes is required")
	}
	out := make([]string, 0, len(raw))
	seen := map[string]struct{}{}
	for _, item := range raw {
		seedHash := strings.ToLower(strings.TrimSpace(item))
		if !isSeedHashHex(seedHash) {
			return nil, fmt.Errorf("invalid seed hash: %s", strings.TrimSpace(item))
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, seedHash)
	}
	return out, nil
}

func selectBestStaticQuote(quotes []DirectQuoteItem) (DirectQuoteItem, bool, string) {
	filtered := make([]DirectQuoteItem, 0, len(quotes))
	for _, q := range quotes {
		if q.ChunkCount == 0 {
			continue
		}
		if len(q.AvailableChunkIndexes) > 0 {
			indexes := normalizeChunkIndexes(q.AvailableChunkIndexes, q.ChunkCount)
			if len(indexes) < int(q.ChunkCount) {
				continue
			}
		}
		filtered = append(filtered, q)
	}
	if len(filtered) == 0 {
		return DirectQuoteItem{}, false, "no complete quote with chunk metadata"
	}
	sort.Slice(filtered, func(i, j int) bool {
		ti := estimateStaticResourceTotal(filtered[i])
		tj := estimateStaticResourceTotal(filtered[j])
		if ti == tj {
			if filtered[i].ChunkPrice == filtered[j].ChunkPrice {
				return filtered[i].SeedPrice < filtered[j].SeedPrice
			}
			return filtered[i].ChunkPrice < filtered[j].ChunkPrice
		}
		return ti < tj
	})
	best := filtered[0]
	return best, true, ""
}

func estimateStaticResourceTotal(q DirectQuoteItem) uint64 {
	return q.SeedPrice + q.ChunkPrice*uint64(q.ChunkCount)
}

func findCompleteLocalFileBySeedHash(store *clientDB, seedHash string) (string, uint64, bool) {
	if store == nil {
		return "", 0, false
	}
	path, _, err := dbFindLatestWorkspaceFileBySeedHash(context.Background(), store, seedHash)
	if err != nil {
		return "", 0, false
	}
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		return "", 0, false
	}
	return path, uint64(st.Size()), true
}

func loadSeedChunkCount(store *clientDB, seedHash string) (uint32, bool) {
	if store == nil {
		return 0, false
	}
	return dbGetSeedChunkCount(context.Background(), store, seedHash)
}

func localSeedMetadataBySeedHash(store *clientDB, seedHash string, path string) (string, string) {
	recommendedName := dbRecommendedFileNameBySeedHash(context.Background(), store, seedHash)
	if recommendedName == "" {
		recommendedName = sanitizeRecommendedFileName(filepath.Base(path))
	}
	mimeHint := dbMimeHintBySeedHash(context.Background(), store, seedHash)
	if mimeHint == "" {
		mimeHint = sanitizeMIMEHint(guessContentType(path, nil))
	}
	return recommendedName, mimeHint
}

func serveSeedContentFile(w http.ResponseWriter, r *http.Request, path string, mimeHint string) {
	f, err := os.Open(path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	contentType := sanitizeMIMEHint(mimeHint)
	if contentType == "" {
		contentType = guessContentType(path, head[:n])
	}
	w.Header().Set("Content-Type", contentType)
	http.ServeContent(w, r, filepath.Base(path), st.ModTime(), f)
}

func guessContentType(name string, head []byte) string {
	ext := strings.TrimSpace(filepath.Ext(strings.TrimSpace(name)))
	if ext != "" {
		if ct := mime.TypeByExtension(ext); strings.TrimSpace(ct) != "" {
			return ct
		}
	}
	if len(head) > 0 {
		if ct := guessContentTypeFromTextHead(head); ct != "" {
			return ct
		}
		return http.DetectContentType(head)
	}
	return "application/octet-stream"
}

// 设计说明：
//   - BitFS 浏览器里的静态资源通常会被改写成“纯 seed hash 文件名”，天然没有扩展名；
//   - 仅靠 filepath.Ext() 或 http.DetectContentType()，JS/CSS 往往会退化成 text/plain；
//   - 这里补一层轻量文本探测，让首页这类 extensionless web 资源仍然能回正确 MIME；
//   - 判断顺序必须先偏向 JavaScript，再偏向 CSS，因为现代打包后的压缩 JS 会同时含有 `{}` 和 `:`，
//     如果 CSS 规则过早命中，就会把模块脚本误发成样式表，页面只剩背景却没有正文。
func guessContentTypeFromTextHead(head []byte) string {
	trimmed := strings.TrimSpace(string(head))
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "<!doctype html"), strings.HasPrefix(lower, "<html"):
		return "text/html; charset=utf-8"
	case strings.HasPrefix(lower, "<?xml"), strings.Contains(lower, "<svg"):
		return "image/svg+xml"
	case json.Valid([]byte(trimmed)):
		if strings.HasPrefix(lower, "{") || strings.HasPrefix(lower, "[") {
			return "application/json; charset=utf-8"
		}
	case looksLikeJavaScript(trimmed):
		return "application/javascript; charset=utf-8"
	case looksLikeCSS(lower):
		return "text/css; charset=utf-8"
	}
	return ""
}

func looksLikeCSS(lower string) bool {
	if lower == "" {
		return false
	}
	if looksLikeJavaScript(lower) {
		return false
	}
	if strings.HasPrefix(lower, ":root") || strings.HasPrefix(lower, "@media") || strings.HasPrefix(lower, "@import") {
		return true
	}
	if !strings.Contains(lower, "{") || !strings.Contains(lower, "}") {
		return false
	}
	if strings.Contains(lower, "function") || strings.Contains(lower, "=>") ||
		strings.Contains(lower, "const ") || strings.Contains(lower, "let ") ||
		strings.Contains(lower, "var ") || strings.Contains(lower, "return ") ||
		strings.Contains(lower, "document.") || strings.Contains(lower, "window.") {
		return false
	}
	return strings.Contains(lower, ":") && strings.Contains(lower, ";")
}

func looksLikeJavaScript(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	if lower == "" {
		return false
	}
	prefixes := []string{
		"(function", "(()=>", "const ", "let ", "var ", "function ",
		"import ", "export ", "\"use strict\"", "'use strict'",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return strings.Contains(lower, "=>") ||
		strings.Contains(lower, "document.") ||
		strings.Contains(lower, "window.") ||
		strings.Contains(lower, "createelement(") ||
		strings.Contains(lower, "queryselector(")
}

func filePlanTimeout(cfg *Config) time.Duration {
	if cfg != nil && cfg.FSHTTP.QuoteWaitSeconds > 0 {
		return time.Duration(cfg.FSHTTP.QuoteWaitSeconds+4) * time.Second
	}
	return 12 * time.Second
}

func filePlanQuoteInterval(cfg *Config) time.Duration {
	if cfg != nil && cfg.FSHTTP.QuotePollSeconds > 0 {
		return time.Duration(cfg.FSHTTP.QuotePollSeconds) * time.Second
	}
	return 2 * time.Second
}

func filePlanQuoteRetry(cfg *Config) int {
	wait := filePlanTimeout(cfg)
	poll := filePlanQuoteInterval(cfg)
	if poll <= 0 {
		poll = 2 * time.Second
	}
	retry := int(wait / poll)
	if retry < 2 {
		retry = 2
	}
	return retry
}

func waitBestStaticQuotesBySeedHash(ctx context.Context, store *clientDB, rt *Runtime, demandBySeedHash map[string]string, maxRetry int, interval time.Duration) (map[string]DirectQuoteItem, map[string]string, map[string]bool, error) {
	if len(demandBySeedHash) == 0 {
		return map[string]DirectQuoteItem{}, map[string]string{}, map[string]bool{}, nil
	}
	if maxRetry <= 0 {
		maxRetry = 1
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	bestQuotes := make(map[string]DirectQuoteItem, len(demandBySeedHash))
	reasons := make(map[string]string, len(demandBySeedHash))
	timedOut := make(map[string]bool, len(demandBySeedHash))
	pending := make(map[string]string, len(demandBySeedHash))
	for seedHash, demandID := range demandBySeedHash {
		pending[seedHash] = demandID
	}
	for attempt := 0; attempt < maxRetry; attempt++ {
		for seedHash, demandID := range pending {
			quotes, err := TriggerClientListDirectQuotes(ctx, store, demandID)
			if err != nil {
				return nil, nil, nil, err
			}
			best, ok, reason := selectBestStaticQuote(quotes)
			if ok {
				bestQuotes[seedHash] = best
				delete(pending, seedHash)
				delete(reasons, seedHash)
				continue
			}
			reasons[seedHash] = reason
		}
		if len(pending) == 0 {
			return bestQuotes, reasons, timedOut, nil
		}
		if attempt == maxRetry-1 {
			break
		}
		select {
		case <-ctx.Done():
			for seedHash := range pending {
				timedOut[seedHash] = true
			}
			if ctx.Err() == context.DeadlineExceeded {
				return bestQuotes, reasons, timedOut, nil
			}
			return nil, nil, nil, ctx.Err()
		case <-time.After(interval):
		}
	}
	for seedHash := range pending {
		timedOut[seedHash] = true
	}
	return bestQuotes, reasons, timedOut, nil
}

func collectPlanItemsByOrder(seedHashes []string, itemsBySeedHash map[string]staticResourcePlanItem) []staticResourcePlanItem {
	items := make([]staticResourcePlanItem, 0, len(seedHashes))
	for _, seedHash := range seedHashes {
		if item, ok := itemsBySeedHash[seedHash]; ok {
			items = append(items, item)
		}
	}
	return items
}

func buildLocalStaticResourceStatus(store *clientDB, seedHash string, path string) staticResourceStatus {
	path = strings.TrimSpace(path)
	recommendedName, mimeHint := localSeedMetadataBySeedHash(store, seedHash, path)
	status := staticResourceStatus{
		staticResourcePlanItem: staticResourcePlanItem{
			SeedHash:            strings.ToLower(strings.TrimSpace(seedHash)),
			Status:              "local",
			LocalReady:          true,
			RecommendedFileName: recommendedName,
			MIMEHint:            mimeHint,
		},
		OutputFilePath: path,
		ContentURI:     "bitfs://" + strings.ToLower(strings.TrimSpace(seedHash)),
	}
	if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() {
		status.FileSize = uint64(st.Size())
	}
	if chunkCount, ok := loadSeedChunkCount(store, seedHash); ok {
		status.ChunkCount = chunkCount
	}
	return status
}
