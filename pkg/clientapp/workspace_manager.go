package clientapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

type workspaceManager struct {
	ctx     context.Context
	cfg     *Config
	db      sqlConn
	store   *clientDB
	catalog *sellerCatalog
	mu      sync.Mutex
}

type workspaceItem struct {
	WorkspacePath string `json:"workspace_path"`
	MaxBytes      uint64 `json:"max_bytes"`
	Enabled       bool   `json:"enabled"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type registerDownloadedFileParams struct {
	FilePath              string
	Seed                  []byte
	AvailableChunkIndexes []uint32
	RecommendedFileName   string
	MIMEHint              string
}

type liveCacheStreamStat struct {
	StreamID            string
	TotalBytes          uint64
	NewestUpdatedAtUnix int64
	Paths               []string
	WorkspaceDirs       []string
}

func (m *workspaceManager) EnsureDefaultWorkspace() error {
	if m == nil || m.cfg == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	store := workspaceStore(m)
	if store == nil {
		return fmt.Errorf("workspace manager not initialized")
	}
	if m.ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	return dbEnsureDefaultWorkspace(m.ctx, store, m.cfg.Storage.WorkspaceDir)
}

func (m *workspaceManager) List() ([]workspaceItem, error) {
	return m.ListWithContext(m.ctx)
}

func (m *workspaceManager) ListWithContext(ctx context.Context) ([]workspaceItem, error) {
	meta := workspaceCommandMetaFromContext(ctx, workspaceCommandTypeList)
	startAt := time.Now()
	walletMode := m != nil && m.cfg != nil && strings.TrimSpace(m.cfg.Storage.WorkspaceDir) == ""
	obs.Business("bitcast-client", "evt_trigger_workspace_list_begin", map[string]any{
		"wallet_mode": walletMode,
	})
	if walletMode {
		items := []workspaceItem{}
		m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_list", "workspace_list", true, "applied", "", "", map[string]any{}, map[string]any{
			"items": items,
			"total": len(items),
		}, time.Since(startAt))
		obs.Business("bitcast-client", "evt_trigger_workspace_list_end", map[string]any{
			"wallet_mode": walletMode,
			"total":       0,
		})
		return items, nil
	}
	store := workspaceStore(m)
	if store == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, "workspace:default", "workspace_list", "workspace_list", err, map[string]any{})
		return nil, err
	}
	runCtx := resolveWorkspaceCtx(ctx, m.ctx)
	if runCtx == nil {
		err := workspaceRejected("ctx_required", "ctx is required")
		m.auditWorkspaceRejected(ctx, meta, "workspace:default", "workspace_list", "workspace_list", err, map[string]any{})
		return nil, err
	}
	items, err := dbListWorkspaces(runCtx, store)
	if err != nil {
		m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_list", "workspace_list", true, "failed", "workspace_list_failed", err.Error(), map[string]any{}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_list_failed", map[string]any{
			"wallet_mode": walletMode,
			"error":       err.Error(),
		})
		return nil, err
	}
	m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_list", "workspace_list", true, "applied", "", "", map[string]any{}, map[string]any{
		"items": items,
		"total": len(items),
	}, time.Since(startAt))
	obs.Business("bitcast-client", "evt_trigger_workspace_list_end", map[string]any{
		"wallet_mode": walletMode,
		"total":       len(items),
	})
	return items, nil
}

func (m *workspaceManager) Add(path string, maxBytes uint64) (workspaceItem, error) {
	return m.AddWithContext(m.ctx, path, maxBytes)
}

func (m *workspaceManager) AddWithContext(ctx context.Context, path string, maxBytes uint64) (workspaceItem, error) {
	meta := workspaceCommandMetaFromContext(ctx, workspaceCommandTypeAdd)
	startAt := time.Now()
	path = strings.TrimSpace(path)
	walletMode := m != nil && m.cfg != nil && strings.TrimSpace(m.cfg.Storage.WorkspaceDir) == ""
	obs.Business("bitcast-client", "evt_trigger_workspace_add_begin", map[string]any{
		"workspace_path": path,
		"max_bytes":      maxBytes,
		"wallet_mode":    walletMode,
	})
	if path == "" {
		err := workspaceRejected("path_required", "path is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", err, map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		})
		return workspaceItem{}, err
	}
	if walletMode {
		err := fmt.Errorf("workspace_dir is not configured")
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", true, "failed", "workspace_add_failed", err.Error(), map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_add_failed", map[string]any{
			"workspace_path": path,
			"max_bytes":      maxBytes,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return workspaceItem{}, err
	}
	store := workspaceStore(m)
	if store == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", err, map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		})
		return workspaceItem{}, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", true, "failed", "workspace_add_failed", err.Error(), map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		}, map[string]any{}, time.Since(startAt))
		return workspaceItem{}, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", true, "failed", "workspace_add_failed", err.Error(), map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		}, map[string]any{}, time.Since(startAt))
		return workspaceItem{}, err
	}
	if !st.IsDir() {
		err = fmt.Errorf("workspace path is not directory")
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", true, "failed", "workspace_add_failed", err.Error(), map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		}, map[string]any{}, time.Since(startAt))
		return workspaceItem{}, err
	}
	runCtx := resolveWorkspaceCtx(ctx, m.ctx)
	if runCtx == nil {
		err := workspaceRejected("ctx_required", "ctx is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_add", path), "workspace_add", "workspace_add", err, map[string]any{
			"path":      path,
			"max_bytes": maxBytes,
		})
		return workspaceItem{}, err
	}
	item, err := dbAddWorkspace(runCtx, store, abs, maxBytes)
	if err != nil {
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", abs), "workspace_add", "workspace_add", true, "failed", "workspace_add_failed", err.Error(), map[string]any{
			"path":      abs,
			"max_bytes": maxBytes,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_add_failed", map[string]any{
			"workspace_path": abs,
			"max_bytes":      maxBytes,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return workspaceItem{}, err
	}
	m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_add", abs), "workspace_add", "workspace_add", true, "applied", "", "", map[string]any{
		"path":      abs,
		"max_bytes": maxBytes,
	}, map[string]any{"workspace": item}, time.Since(startAt))
	obs.Business("bitcast-client", "evt_trigger_workspace_add_end", map[string]any{
		"workspace_path": abs,
		"max_bytes":      maxBytes,
		"wallet_mode":    walletMode,
	})
	return item, nil
}

func (m *workspaceManager) DeleteByPath(workspacePath string) error {
	return m.DeleteByPathWithContext(m.ctx, workspacePath)
}

func (m *workspaceManager) DeleteByPathWithContext(ctx context.Context, workspacePath string) error {
	meta := workspaceCommandMetaFromContext(ctx, workspaceCommandTypeDelete)
	startAt := time.Now()
	workspacePath = strings.TrimSpace(workspacePath)
	walletMode := m != nil && m.cfg != nil && strings.TrimSpace(m.cfg.Storage.WorkspaceDir) == ""
	obs.Business("bitcast-client", "evt_trigger_workspace_delete_begin", map[string]any{
		"workspace_path": workspacePath,
		"wallet_mode":    walletMode,
	})
	if workspacePath == "" {
		err := workspaceRejected("workspace_path_required", "workspace path is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return err
	}
	if walletMode {
		err := fmt.Errorf("workspace_dir is not configured")
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", true, "failed", "workspace_delete_failed", err.Error(), map[string]any{
			"workspace_path": workspacePath,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_delete_failed", map[string]any{
			"workspace_path": workspacePath,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return err
	}
	store := workspaceStore(m)
	if store == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return err
	}
	runCtx := resolveWorkspaceCtx(ctx, m.ctx)
	if runCtx == nil {
		err := workspaceRejected("ctx_required", "ctx is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return err
	}
	if err := dbDeleteWorkspaceByPath(runCtx, store, workspacePath); err != nil {
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", true, "failed", "workspace_delete_failed", err.Error(), map[string]any{
			"workspace_path": workspacePath,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_delete_failed", map[string]any{
			"workspace_path": workspacePath,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return err
	}
	m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_delete", workspacePath), "workspace_delete", "workspace_delete", true, "applied", "", "", map[string]any{
		"workspace_path": workspacePath,
	}, map[string]any{}, time.Since(startAt))
	obs.Business("bitcast-client", "evt_trigger_workspace_delete_end", map[string]any{
		"workspace_path": workspacePath,
		"wallet_mode":    walletMode,
	})
	return nil
}

func (m *workspaceManager) UpdateByPath(workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	return m.UpdateByPathWithContext(m.ctx, workspacePath, maxBytes, enabled)
}

func (m *workspaceManager) UpdateByPathWithContext(ctx context.Context, workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	meta := workspaceCommandMetaFromContext(ctx, workspaceCommandTypeUpdate)
	startAt := time.Now()
	workspacePath = strings.TrimSpace(workspacePath)
	walletMode := m != nil && m.cfg != nil && strings.TrimSpace(m.cfg.Storage.WorkspaceDir) == ""
	obs.Business("bitcast-client", "evt_trigger_workspace_update_begin", map[string]any{
		"workspace_path": workspacePath,
		"wallet_mode":    walletMode,
		"max_bytes": func() any {
			if maxBytes == nil {
				return nil
			}
			return *maxBytes
		}(),
		"enabled": func() any {
			if enabled == nil {
				return nil
			}
			return *enabled
		}(),
	})
	if workspacePath == "" {
		err := workspaceRejected("workspace_path_required", "workspace path is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return workspaceItem{}, err
	}
	if maxBytes == nil && enabled == nil {
		err := workspaceRejected("no_fields_to_update", "no fields to update")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return workspaceItem{}, err
	}
	if walletMode {
		err := fmt.Errorf("workspace_dir is not configured")
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", true, "failed", "workspace_update_failed", err.Error(), map[string]any{
			"workspace_path": workspacePath,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_update_failed", map[string]any{
			"workspace_path": workspacePath,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return workspaceItem{}, err
	}
	store := workspaceStore(m)
	if store == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return workspaceItem{}, err
	}
	runCtx := resolveWorkspaceCtx(ctx, m.ctx)
	if runCtx == nil {
		err := workspaceRejected("ctx_required", "ctx is required")
		m.auditWorkspaceRejected(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", err, map[string]any{
			"workspace_path": workspacePath,
		})
		return workspaceItem{}, err
	}
	item, err := dbUpdateWorkspaceByPath(runCtx, store, workspacePath, maxBytes, enabled)
	if err != nil {
		m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", true, "failed", "workspace_update_failed", err.Error(), map[string]any{
			"workspace_path": workspacePath,
		}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_update_failed", map[string]any{
			"workspace_path": workspacePath,
			"wallet_mode":    walletMode,
			"error":          err.Error(),
		})
		return workspaceItem{}, err
	}
	m.auditWorkspaceCommand(ctx, meta, workspaceAggregateID("workspace_update", workspacePath), "workspace_update", "workspace_update", true, "applied", "", "", map[string]any{
		"workspace_path": workspacePath,
	}, map[string]any{"workspace": item}, time.Since(startAt))
	obs.Business("bitcast-client", "evt_trigger_workspace_update_end", map[string]any{
		"workspace_path": workspacePath,
		"wallet_mode":    walletMode,
	})
	return item, nil
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
		free, ferr := freeBytesUnderPath(it.WorkspacePath)
		if ferr != nil {
			continue
		}
		if it.MaxBytes > 0 {
			used, _ := dbWorkspaceUsedBytes(m.ctx, workspaceStore(m), it.WorkspacePath)
			if used+fileSize > it.MaxBytes {
				continue
			}
		}
		if free < fileSize {
			continue
		}
		if relDir != "" {
			return filepath.Join(it.WorkspacePath, relDir, name), nil
		}
		return filepath.Join(it.WorkspacePath, name), nil
	}
	return "", fmt.Errorf("no workspace has enough capacity")
}

func (m *workspaceManager) SyncOnce(ctx context.Context) (map[string]sellerSeed, error) {
	meta := workspaceCommandMetaFromContext(ctx, workspaceCommandTypeSync)
	startAt := time.Now()
	walletMode := m != nil && m.cfg != nil && strings.TrimSpace(m.cfg.Storage.WorkspaceDir) == ""
	obs.Business("bitcast-client", "evt_trigger_workspace_sync_once_begin", map[string]any{
		"wallet_mode": walletMode,
	})
	if walletMode {
		out := map[string]sellerSeed{}
		m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_sync", "workspace_sync", true, "applied", "", "", map[string]any{}, map[string]any{
			"seed_count": 0,
		}, time.Since(startAt))
		obs.Business("bitcast-client", "evt_trigger_workspace_sync_once_end", map[string]any{
			"wallet_mode": walletMode,
			"seed_count":  0,
		})
		return out, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m == nil || m.cfg == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, "workspace:default", "workspace_sync", "workspace_sync", err, map[string]any{})
		return nil, err
	}
	store := workspaceStore(m)
	if store == nil {
		err := workspaceRejected("workspace_not_initialized", "workspace manager not initialized")
		m.auditWorkspaceRejected(ctx, meta, "workspace:default", "workspace_sync", "workspace_sync", err, map[string]any{})
		return nil, err
	}
	if m.catalog != nil {
		m.catalog.IncDBQueries(1)
	}
	biz_seeds, err := dbScanAndSyncWorkspace(ctx, store, *m.cfg)
	if err != nil {
		if m.catalog != nil {
			m.catalog.MarkStale()
		}
		m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_sync", "workspace_sync", true, "failed", "workspace_sync_failed", err.Error(), map[string]any{}, map[string]any{}, time.Since(startAt))
		obs.Error("bitcast-client", "evt_trigger_workspace_sync_once_failed", map[string]any{
			"wallet_mode": walletMode,
			"error":       err.Error(),
		})
		return nil, err
	}
	if m.catalog != nil {
		m.catalog.Replace(biz_seeds)
	}
	m.auditWorkspaceCommand(ctx, meta, "workspace:default", "workspace_sync", "workspace_sync", true, "applied", "", "", map[string]any{}, map[string]any{
		"seed_count": len(biz_seeds),
	}, time.Since(startAt))
	obs.Business("bitcast-client", "evt_trigger_workspace_sync_once_end", map[string]any{
		"wallet_mode": walletMode,
		"seed_count":  len(biz_seeds),
	})
	return biz_seeds, nil
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
	rows, err := dbListLiveCacheFiles(m.ctx, workspaceStore(m))
	if err != nil {
		return nil, 0, err
	}
	streams := map[string]*liveCacheStreamStat{}
	var total uint64
	for _, row := range rows {
		fullPath := workspacePathJoin(row.WorkspacePath, row.FilePath)
		streamID, workspaceDir, ok := classifyLiveWorkspacePath(items, fullPath)
		if !ok {
			continue
		}
		st, exists := streams[streamID]
		if !exists {
			st = &liveCacheStreamStat{StreamID: streamID}
			streams[streamID] = st
		}
		if info, err := os.Stat(fullPath); err == nil {
			st.TotalBytes += uint64(info.Size())
			total += uint64(info.Size())
			if info.ModTime().Unix() > st.NewestUpdatedAtUnix {
				st.NewestUpdatedAtUnix = info.ModTime().Unix()
			}
		}
		st.Paths = append(st.Paths, fullPath)
		if !containsString(st.WorkspaceDirs, workspaceDir) {
			st.WorkspaceDirs = append(st.WorkspaceDirs, workspaceDir)
		}
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
		root := filepath.Clean(strings.TrimSpace(it.WorkspacePath))
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
	return dbDeleteLiveStreamCacheRows(m.ctx, workspaceStore(m), st.StreamID)
}

func (m *workspaceManager) cleanupOrphanSeedState() error {
	return dbCleanupOrphanSeedState(m.ctx, workspaceStore(m))
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
// - 支持 partial 文件：biz_workspace_files 记录当前文件大小，biz_seeds 记录完整种子元信息。
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
	recommendedName := sanitizeRecommendedFileName(p.RecommendedFileName)
	if recommendedName == "" {
		recommendedName = sanitizeRecommendedFileName(filepath.Base(abs))
	}
	mimeHint := sanitizeMIMEHint(p.MIMEHint)

	m.mu.Lock()
	defer m.mu.Unlock()

	seedsDir := filepath.Join(m.cfg.Storage.DataDir, "biz_seeds")
	if err := os.MkdirAll(seedsDir, 0o755); err != nil {
		return sellerSeed{}, err
	}
	seedPath := filepath.Join(seedsDir, seedHash+".bse")
	if err := writeIfChanged(seedPath, p.Seed); err != nil {
		return sellerSeed{}, err
	}

	if err := dbUpsertDownloadedFile(m.ctx, workspaceStore(m), abs, seedHash, seedPath, meta.ChunkCount, meta.FileSize, recommendedName, mimeHint, true); err != nil {
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
	if err := dbReplaceSeedChunkSupply(m.ctx, workspaceStore(m), seedHash, available); err != nil {
		return sellerSeed{}, err
	}
	seed := sellerSeed{
		SeedHash:            seedHash,
		FileSize:            meta.FileSize,
		ChunkCount:          meta.ChunkCount,
		RecommendedFileName: recommendedName,
		MIMEHint:            mimeHint,
	}
	if m.catalog != nil {
		m.catalog.Upsert(seed)
	}
	return seed, nil
}
