package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseeds"
)

type liveWorkspaceEntry struct {
	Path          string
	SeedHash      string
	FileSize      int64
	MtimeUnix     int64
	UpdatedAtUnix int64
}

type liveStreamStatsItem struct {
	StreamID        string
	FileCount       int64
	TotalBytes      uint64
	LastUpdatedUnix int64
	WorkspaceRoots  []string
}

func dbGetSeedFilePathByHash(ctx context.Context, store *clientDB, seedHash string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		row, err := root.BizSeeds.Query().
			Where(bizseeds.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", sql.ErrNoRows
			}
			return "", err
		}
		return row.SeedFilePath, nil
	})
}

func dbListLiveWorkspaceEntries(ctx context.Context, store *clientDB, pattern string, includeMeta bool) ([]liveWorkspaceEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	needle := strings.Trim(pattern, "%")
	needle = strings.ReplaceAll(needle, "/", string(filepath.Separator))
	page, err := store.ListWorkspaceFiles(ctx, -1, 0, "")
	if err != nil {
		return nil, err
	}
	out := make([]liveWorkspaceEntry, 0, len(page.Items))
	for _, row := range page.Items {
		absPath := workspacePathJoin(row.WorkspacePath, row.FilePath)
		fullPath := filepath.Clean(absPath)
		if needle != "" && !strings.Contains(filepath.ToSlash(fullPath), filepath.ToSlash(needle)) {
			continue
		}
		it := liveWorkspaceEntry{Path: fullPath, SeedHash: row.SeedHash}
		if includeMeta {
			if st, err := os.Stat(fullPath); err == nil {
				it.FileSize = st.Size()
				it.MtimeUnix = st.ModTime().Unix()
				it.UpdatedAtUnix = it.MtimeUnix
			}
		}
		out = append(out, it)
	}
	return out, nil
}

func dbListLiveSegmentWorkspaceEntries(ctx context.Context, store *clientDB, streamID string) ([]liveWorkspaceEntry, error) {
	segmentID := strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(segmentID) {
		return nil, fmt.Errorf("invalid stream_id")
	}
	return dbListLiveWorkspaceEntries(ctx, store, "%"+string(filepath.Separator)+"live"+string(filepath.Separator)+segmentID+string(filepath.Separator)+"%", true)
}

// dbDeleteLiveStreamWorkspaceRows 只做直播流定向清理。
// 设计说明：
// - 只删除 live/<stream_id>/ 下的 biz_workspace_files 行；
// - 同事务执行 orphan 清理，避免遗留孤儿 seed 状态；
// - 不触发全量 workspace 扫描。
func dbDeleteLiveStreamWorkspaceRows(ctx context.Context, store *clientDB, streamID string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return store.DeleteLiveStreamWorkspaceRows(ctx, streamID)
}

func dbListLiveStreamStats(ctx context.Context, store *clientDB) ([]liveStreamStatsItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	sep := string(filepath.Separator)
	rows, err := dbListLiveWorkspaceEntries(ctx, store, "%"+sep+"live"+sep+"%", true)
	if err != nil {
		return nil, err
	}
	type acc struct {
		files      int64
		bytes      uint64
		lastUpdate int64
		roots      map[string]struct{}
	}
	m := map[string]*acc{}
	for _, row := range rows {
		streamID, root, ok := extractLiveStreamIDFromPath(row.Path)
		if !ok {
			continue
		}
		it := m[streamID]
		if it == nil {
			it = &acc{roots: map[string]struct{}{}}
			m[streamID] = it
		}
		it.files++
		if row.FileSize > 0 {
			it.bytes += uint64(row.FileSize)
		}
		if row.MtimeUnix > it.lastUpdate {
			it.lastUpdate = row.MtimeUnix
		}
		if root != "" {
			it.roots[root] = struct{}{}
		}
	}
	out := make([]liveStreamStatsItem, 0, len(m))
	for streamID, v := range m {
		roots := make([]string, 0, len(v.roots))
		for r := range v.roots {
			roots = append(roots, r)
		}
		sort.Strings(roots)
		out = append(out, liveStreamStatsItem{
			StreamID:        streamID,
			FileCount:       v.files,
			TotalBytes:      v.bytes,
			LastUpdatedUnix: v.lastUpdate,
			WorkspaceRoots:  roots,
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
