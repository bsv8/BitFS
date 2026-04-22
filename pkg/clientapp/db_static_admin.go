package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]liveWorkspaceEntry, error) {
		rows, err := root.BizWorkspaceFiles.Query().
			Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]liveWorkspaceEntry, 0, len(rows))
		for _, row := range rows {
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
	})
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
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return 0, fmt.Errorf("invalid stream_id")
	}
	return writeEntValue(ctx, store, func(tx EntWriteRoot) (int64, error) {
		pathPrefix := "live/" + streamID + "/"
		q := tx.BizWorkspaceFiles.Query().Where(bizworkspacefiles.FilePathHasPrefix(pathPrefix))
		before, err := q.Count(ctx)
		if err != nil {
			return 0, err
		}
		if _, err := tx.BizWorkspaceFiles.Delete().Where(bizworkspacefiles.FilePathHasPrefix(pathPrefix)).Exec(ctx); err != nil {
			return 0, err
		}
		return int64(before), nil
	})
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
