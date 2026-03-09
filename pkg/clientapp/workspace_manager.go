package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type workspaceManager struct {
	cfg     *Config
	db      *sql.DB
	catalog *sellerCatalog
	mu      sync.Mutex
}

type workspaceItem struct {
	ID            int64  `json:"id"`
	Path          string `json:"path"`
	MaxBytes      uint64 `json:"max_bytes"`
	Enabled       bool   `json:"enabled"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

type registerDownloadedFileParams struct {
	FilePath              string
	Seed                  []byte
	AvailableChunkIndexes []uint32
}

type liveCacheStreamStat struct {
	StreamID            string
	TotalBytes          uint64
	NewestUpdatedAtUnix int64
	Paths               []string
	WorkspaceDirs       []string
}

func (m *workspaceManager) EnsureDefaultWorkspace() error {
	if m == nil || m.db == nil || m.cfg == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	abs, err := filepath.Abs(strings.TrimSpace(m.cfg.Storage.WorkspaceDir))
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	_, err = m.db.Exec(
		`INSERT INTO workspaces(path,max_bytes,enabled,created_at_unix,updated_at_unix)
		 VALUES(?,?,1,?,?)
		 ON CONFLICT(path) DO UPDATE SET enabled=1,updated_at_unix=excluded.updated_at_unix`,
		abs, int64(0), now, now,
	)
	return err
}

func (m *workspaceManager) List() ([]workspaceItem, error) {
	if m == nil || m.db == nil {
		return nil, fmt.Errorf("workspace manager not initialized")
	}
	rows, err := m.db.Query(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]workspaceItem, 0, 8)
	for rows.Next() {
		var it workspaceItem
		var enabled int64
		if err := rows.Scan(&it.ID, &it.Path, &it.MaxBytes, &enabled, &it.CreatedAtUnix, &it.UpdatedAtUnix); err != nil {
			return nil, err
		}
		it.Enabled = enabled != 0
		out = append(out, it)
	}
	return out, nil
}

func (m *workspaceManager) Add(path string, maxBytes uint64) (workspaceItem, error) {
	if m == nil || m.db == nil {
		return workspaceItem{}, fmt.Errorf("workspace manager not initialized")
	}
	abs, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return workspaceItem{}, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return workspaceItem{}, err
	}
	if !st.IsDir() {
		return workspaceItem{}, fmt.Errorf("workspace path is not directory")
	}
	now := time.Now().Unix()
	_, err = m.db.Exec(
		`INSERT INTO workspaces(path,max_bytes,enabled,created_at_unix,updated_at_unix)
		 VALUES(?,?,1,?,?)
		 ON CONFLICT(path) DO UPDATE SET max_bytes=excluded.max_bytes,enabled=1,updated_at_unix=excluded.updated_at_unix`,
		abs, maxBytes, now, now,
	)
	if err != nil {
		return workspaceItem{}, err
	}
	var it workspaceItem
	var enabled int64
	err = m.db.QueryRow(
		`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces WHERE path=?`,
		abs,
	).Scan(&it.ID, &it.Path, &it.MaxBytes, &enabled, &it.CreatedAtUnix, &it.UpdatedAtUnix)
	if err != nil {
		return workspaceItem{}, err
	}
	it.Enabled = enabled != 0
	return it, nil
}

func (m *workspaceManager) DeleteByID(id int64) error {
	if m == nil || m.db == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	if id <= 0 {
		return fmt.Errorf("invalid workspace id")
	}
	var path string
	if err := m.db.QueryRow(`SELECT path FROM workspaces WHERE id=?`, id).Scan(&path); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("workspace not found")
		}
		return err
	}
	if _, err := m.db.Exec(`DELETE FROM workspaces WHERE id=?`, id); err != nil {
		return err
	}
	path = filepath.Clean(strings.TrimSpace(path))
	if path != "" {
		_, _ = m.db.Exec(`DELETE FROM workspace_files WHERE path=? OR path LIKE ?`, path, path+string(filepath.Separator)+"%")
	}
	return m.cleanupOrphanSeedState()
}

func (m *workspaceManager) UpdateByID(id int64, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if m == nil || m.db == nil {
		return workspaceItem{}, fmt.Errorf("workspace manager not initialized")
	}
	if id <= 0 {
		return workspaceItem{}, fmt.Errorf("invalid workspace id")
	}
	if maxBytes == nil && enabled == nil {
		return workspaceItem{}, fmt.Errorf("no fields to update")
	}

	var cur workspaceItem
	var curEnabled int64
	if err := m.db.QueryRow(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces WHERE id=?`, id).
		Scan(&cur.ID, &cur.Path, &cur.MaxBytes, &curEnabled, &cur.CreatedAtUnix, &cur.UpdatedAtUnix); err != nil {
		if err == sql.ErrNoRows {
			return workspaceItem{}, fmt.Errorf("workspace not found")
		}
		return workspaceItem{}, err
	}
	cur.Enabled = curEnabled != 0

	nextMaxBytes := cur.MaxBytes
	if maxBytes != nil {
		nextMaxBytes = *maxBytes
	}
	nextEnabled := cur.Enabled
	if enabled != nil {
		nextEnabled = *enabled
	}
	enabledValue := int64(0)
	if nextEnabled {
		enabledValue = 1
	}

	now := time.Now().Unix()
	if _, err := m.db.Exec(`UPDATE workspaces SET max_bytes=?,enabled=?,updated_at_unix=? WHERE id=?`, nextMaxBytes, enabledValue, now, id); err != nil {
		return workspaceItem{}, err
	}

	var out workspaceItem
	var outEnabled int64
	if err := m.db.QueryRow(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces WHERE id=?`, id).
		Scan(&out.ID, &out.Path, &out.MaxBytes, &outEnabled, &out.CreatedAtUnix, &out.UpdatedAtUnix); err != nil {
		return workspaceItem{}, err
	}
	out.Enabled = outEnabled != 0
	return out, nil
}

func (m *workspaceManager) SelectOutputPath(fileName string, fileSize uint64) (string, error) {
	return m.selectOutputPath("", fileName, fileSize)
}

func (m *workspaceManager) SelectLiveSegmentOutputPath(streamID string, segmentIndex uint64, fileSize uint64) (string, error) {
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return "", fmt.Errorf("invalid stream_id")
	}
	name := fmt.Sprintf("%06d.seg", segmentIndex)
	return m.selectOutputPath(filepath.Join("live", streamID), name, fileSize)
}

func (m *workspaceManager) selectOutputPath(relDir string, fileName string, fileSize uint64) (string, error) {
	items, err := m.List()
	if err != nil {
		return "", err
	}
	name := sanitizeRecommendedFileName(fileName)
	if name == "" {
		return "", fmt.Errorf("invalid output file name")
	}
	relDir = strings.TrimSpace(relDir)
	if relDir != "" {
		relDir = filepath.Clean(relDir)
		if filepath.IsAbs(relDir) || relDir == "." || strings.HasPrefix(relDir, "..") {
			return "", fmt.Errorf("invalid output relative dir")
		}
	}
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		free, ferr := freeBytesUnderPath(it.Path)
		if ferr != nil {
			continue
		}
		if it.MaxBytes > 0 {
			var used uint64
			_ = m.db.QueryRow(`SELECT COALESCE(SUM(file_size),0) FROM workspace_files WHERE path=? OR path LIKE ?`, it.Path, it.Path+string(filepath.Separator)+"%").Scan(&used)
			if used+fileSize > it.MaxBytes {
				continue
			}
		}
		if free < fileSize {
			continue
		}
		if relDir != "" {
			return filepath.Join(it.Path, relDir, name), nil
		}
		return filepath.Join(it.Path, name), nil
	}
	return "", fmt.Errorf("no workspace has enough capacity")
}

func (m *workspaceManager) SyncOnce(ctx context.Context) (map[string]sellerSeed, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seeds, err := scanAndSyncWorkspace(ctx, m.cfg, m.db)
	if err != nil {
		return nil, err
	}
	if m.catalog != nil {
		m.catalog.Replace(seeds)
	}
	return seeds, nil
}

func (m *workspaceManager) ValidateLiveCacheCapacity(maxBytes uint64) error {
	if m == nil || m.db == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	if maxBytes == 0 {
		return nil
	}
	items, err := m.List()
	if err != nil {
		return err
	}
	var total uint64
	hasBounded := false
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		if it.MaxBytes == 0 {
			return nil
		}
		total += it.MaxBytes
		hasBounded = true
	}
	if hasBounded && maxBytes > total {
		return fmt.Errorf("live cache max bytes exceeds total workspace capacity")
	}
	return nil
}

// EnforceLiveCacheLimit 按整条直播淘汰缓存。
// 设计说明：
// - 直播段以 workspace/live/<stream_id>/ 存放；
// - 淘汰时不删单段，直接删整条流，避免播放器拿到断裂时间线；
// - 删除顺序按“最近更新时间最老”的流优先。
func (m *workspaceManager) EnforceLiveCacheLimit(maxBytes uint64) error {
	if m == nil || m.db == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	if maxBytes == 0 {
		return nil
	}
	if err := m.ValidateLiveCacheCapacity(maxBytes); err != nil {
		return err
	}
	streams, totalBytes, err := m.listLiveCacheStreams()
	if err != nil {
		return err
	}
	if totalBytes <= maxBytes {
		return nil
	}
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].NewestUpdatedAtUnix == streams[j].NewestUpdatedAtUnix {
			return streams[i].StreamID < streams[j].StreamID
		}
		return streams[i].NewestUpdatedAtUnix < streams[j].NewestUpdatedAtUnix
	})
	for _, st := range streams {
		if totalBytes <= maxBytes {
			break
		}
		if err := m.deleteLiveStreamCache(st); err != nil {
			return err
		}
		if totalBytes > st.TotalBytes {
			totalBytes -= st.TotalBytes
		} else {
			totalBytes = 0
		}
	}
	return m.cleanupOrphanSeedState()
}

func (m *workspaceManager) listLiveCacheStreams() ([]liveCacheStreamStat, uint64, error) {
	items, err := m.List()
	if err != nil {
		return nil, 0, err
	}
	rows, err := m.db.Query(`SELECT path,file_size,updated_at_unix FROM workspace_files`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	streams := map[string]*liveCacheStreamStat{}
	var total uint64
	for rows.Next() {
		var absPath string
		var fileSize uint64
		var updatedAt int64
		if err := rows.Scan(&absPath, &fileSize, &updatedAt); err != nil {
			return nil, 0, err
		}
		streamID, workspaceDir, ok := classifyLiveWorkspacePath(items, absPath)
		if !ok {
			continue
		}
		st, exists := streams[streamID]
		if !exists {
			st = &liveCacheStreamStat{StreamID: streamID}
			streams[streamID] = st
		}
		st.TotalBytes += fileSize
		if updatedAt > st.NewestUpdatedAtUnix {
			st.NewestUpdatedAtUnix = updatedAt
		}
		st.Paths = append(st.Paths, absPath)
		if !containsString(st.WorkspaceDirs, workspaceDir) {
			st.WorkspaceDirs = append(st.WorkspaceDirs, workspaceDir)
		}
		total += fileSize
	}
	out := make([]liveCacheStreamStat, 0, len(streams))
	for _, st := range streams {
		out = append(out, *st)
	}
	return out, total, nil
}

func classifyLiveWorkspacePath(items []workspaceItem, absPath string) (string, string, bool) {
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		root := filepath.Clean(strings.TrimSpace(it.Path))
		prefix := root + string(filepath.Separator)
		if !strings.HasPrefix(absPath, prefix) {
			continue
		}
		rel := strings.TrimPrefix(absPath, prefix)
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) < 3 || parts[0] != "live" {
			continue
		}
		streamID := strings.ToLower(strings.TrimSpace(parts[1]))
		if !isSeedHashHex(streamID) {
			continue
		}
		return streamID, root, true
	}
	return "", "", false
}

func (m *workspaceManager) deleteLiveStreamCache(st liveCacheStreamStat) error {
	for _, dir := range st.WorkspaceDirs {
		if err := os.RemoveAll(filepath.Join(dir, "live", st.StreamID)); err != nil {
			return err
		}
	}
	if _, err := m.db.Exec(`DELETE FROM workspace_files WHERE path LIKE ?`, "%"+string(filepath.Separator)+"live"+string(filepath.Separator)+st.StreamID+string(filepath.Separator)+"%"); err != nil {
		return err
	}
	return nil
}

func (m *workspaceManager) cleanupOrphanSeedState() error {
	if _, err := m.db.Exec(`DELETE FROM seeds WHERE seed_hash NOT IN (SELECT DISTINCT seed_hash FROM workspace_files)`); err != nil {
		return err
	}
	if _, err := m.db.Exec(`DELETE FROM seed_price_state WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := m.db.Exec(`DELETE FROM seed_available_chunks WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := m.db.Exec(`DELETE FROM file_downloads WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := m.db.Exec(`DELETE FROM file_download_chunks WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	return nil
}

func containsString(items []string, want string) bool {
	for _, it := range items {
		if it == want {
			return true
		}
	}
	return false
}

// RegisterDownloadedFile 直接把“已下载文件 + 对应 seed”写入索引与种子目录，不走全盘扫描。
// 设计说明：
// - 下载流程产出的 seed 是独立事实来源，不能再从当前文件反推 seed；
// - 支持 partial 文件：workspace_files 记录当前文件大小，seeds 记录完整种子元信息。
func (m *workspaceManager) RegisterDownloadedFile(p registerDownloadedFileParams) (sellerSeed, error) {
	if m == nil || m.db == nil || m.cfg == nil {
		return sellerSeed{}, fmt.Errorf("workspace manager not initialized")
	}
	abs, err := filepath.Abs(strings.TrimSpace(p.FilePath))
	if err != nil {
		return sellerSeed{}, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return sellerSeed{}, err
	}
	if !st.Mode().IsRegular() {
		return sellerSeed{}, fmt.Errorf("downloaded file is not regular")
	}
	meta, err := parseSeedV1(p.Seed)
	if err != nil {
		return sellerSeed{}, err
	}
	seedHash := strings.ToLower(strings.TrimSpace(meta.SeedHashHex))
	if seedHash == "" {
		return sellerSeed{}, fmt.Errorf("invalid seed hash")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	seedsDir := filepath.Join(m.cfg.Storage.DataDir, "seeds")
	if err := os.MkdirAll(seedsDir, 0o755); err != nil {
		return sellerSeed{}, err
	}
	seedPath := filepath.Join(seedsDir, seedHash+".bse")
	if err := writeIfChanged(seedPath, p.Seed); err != nil {
		return sellerSeed{}, err
	}

	now := time.Now().Unix()
	if _, err := m.db.Exec(
		`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,seed_locked,updated_at_unix)
		 VALUES(?,?,?,?,?,?)
		 ON CONFLICT(path) DO UPDATE SET
		 file_size=excluded.file_size,
		 mtime_unix=excluded.mtime_unix,
		 seed_hash=excluded.seed_hash,
		 seed_locked=excluded.seed_locked,
		 updated_at_unix=excluded.updated_at_unix`,
		abs, st.Size(), st.ModTime().Unix(), seedHash, 1, now,
	); err != nil {
		return sellerSeed{}, err
	}
	if _, err := m.db.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,created_at_unix)
		 VALUES(?,?,?,?,?)
		 ON CONFLICT(seed_hash) DO UPDATE SET
		 seed_file_path=excluded.seed_file_path,
		 chunk_count=excluded.chunk_count,
		 file_size=excluded.file_size`,
		seedHash, seedPath, meta.ChunkCount, meta.FileSize, now,
	); err != nil {
		return sellerSeed{}, err
	}
	available := normalizeChunkIndexes(p.AvailableChunkIndexes, meta.ChunkCount)
	if len(available) == 0 {
		haveCount := uint32(ceilDiv(uint64(st.Size()), seedBlockSize))
		if haveCount > meta.ChunkCount {
			haveCount = meta.ChunkCount
		}
		available = contiguousChunkIndexes(haveCount)
	}
	existing := make([]uint32, 0, len(available))
	rows, err := m.db.Query(`SELECT chunk_index FROM seed_available_chunks WHERE seed_hash=? ORDER BY chunk_index ASC`, seedHash)
	if err != nil {
		return sellerSeed{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var idx uint32
		if err := rows.Scan(&idx); err != nil {
			return sellerSeed{}, err
		}
		existing = append(existing, idx)
	}
	if len(existing) > 0 {
		available = normalizeChunkIndexes(append(existing, available...), meta.ChunkCount)
	}
	if err := replaceSeedAvailableChunks(m.db, seedHash, available); err != nil {
		return sellerSeed{}, err
	}

	unit, total, err := upsertSeedPriceState(
		m.db,
		seedHash,
		m.cfg.Seller.Pricing.FloorPriceSatPer64K,
		m.cfg.Seller.Pricing.ResaleDiscountBPS,
		seedPath,
	)
	if err != nil {
		return sellerSeed{}, err
	}
	seed := sellerSeed{
		SeedHash:   seedHash,
		ChunkCount: meta.ChunkCount,
		ChunkPrice: unit,
		SeedPrice:  total,
	}
	if m.catalog != nil {
		m.catalog.Upsert(seed)
	}
	return seed, nil
}
