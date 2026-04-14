package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedpricingpolicy"
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

type staticFilePriceRecord struct {
	FloorPriceSatPer64K uint64
	ResaleDiscountBPS   uint64
	UpdatedAtUnix       int64
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		row, err := tx.BizSeeds.Query().
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
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) ([]liveWorkspaceEntry, error) {
		rows, err := tx.BizWorkspaceFiles.Query().
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

func dbDeleteLiveStreamWorkspaceRows(ctx context.Context, store *clientDB, prefix string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	trimmed := strings.TrimSuffix(strings.TrimSpace(prefix), "%")
	trimmed = filepath.Clean(trimmed)
	streamID := filepath.Base(strings.TrimRight(trimmed, string(filepath.Separator)))
	if !isSeedHashHex(streamID) {
		return 0, fmt.Errorf("invalid stream prefix")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (int64, error) {
		q := tx.BizWorkspaceFiles.Query().Where(bizworkspacefiles.FilePathHasPrefix("live/" + streamID + "/"))
		before, err := q.Count(ctx)
		if err != nil {
			return 0, err
		}
		if _, err := tx.BizWorkspaceFiles.Delete().Where(bizworkspacefiles.FilePathHasPrefix("live/" + streamID + "/")).Exec(ctx); err != nil {
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

func dbGetWorkspaceFileSeedHash(ctx context.Context, store *clientDB, path string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	abs, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	roots, err := dbListWorkspaceRoots(ctx, store)
	if err != nil {
		return "", err
	}
	resolved, ok := resolveWorkspaceRelativePath(abs, roots)
	if !ok {
		return "", nil
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		row, err := tx.BizWorkspaceFiles.Query().
			Where(
				bizworkspacefiles.WorkspacePathEQ(resolved.WorkspacePath),
				bizworkspacefiles.FilePathEQ(resolved.FilePath),
			).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return "", nil
			}
			return "", err
		}
		return row.SeedHash, nil
	})
}

func dbGetStaticFilePrice(ctx context.Context, store *clientDB, path string) (staticFilePriceRecord, error) {
	if store == nil {
		return staticFilePriceRecord{}, fmt.Errorf("client db is nil")
	}
	seedHash, err := dbGetWorkspaceFileSeedHash(ctx, store, path)
	if err != nil || seedHash == "" {
		return staticFilePriceRecord{}, err
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (staticFilePriceRecord, error) {
		row, err := tx.BizSeedPricingPolicy.Query().
			Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return staticFilePriceRecord{}, sql.ErrNoRows
			}
			return staticFilePriceRecord{}, err
		}
		return staticFilePriceRecord{
			FloorPriceSatPer64K: uint64(row.FloorUnitPriceSatPer64k),
			ResaleDiscountBPS:   uint64(row.ResaleDiscountBps),
			UpdatedAtUnix:       row.UpdatedAtUnix,
		}, nil
	})
}

func dbUpsertStaticFilePrice(ctx context.Context, store *clientDB, path string, floor uint64, bps uint64) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	seedHash, err := dbGetWorkspaceFileSeedHash(ctx, store, path)
	if err != nil || seedHash == "" {
		if err == nil {
			err = fmt.Errorf("seed not found")
		}
		return 0, err
	}
	now := time.Now().Unix()
	if err := dbUpsertSeedPricingPolicy(ctx, store, seedHash, floor, bps, "user", now); err != nil {
		return 0, err
	}
	return now, nil
}

func dbBindStaticPriceToSeed2(ctx context.Context, store *clientDB, path string, floor uint64, bps uint64) (string, uint64, uint64, bool, error) {
	if store == nil {
		return "", 0, 0, false, fmt.Errorf("client db is nil")
	}
	seedHash, err := dbGetWorkspaceFileSeedHash(ctx, store, path)
	if err != nil {
		return "", 0, 0, false, err
	}
	if seedHash == "" {
		return "", 0, 0, false, nil
	}
	now := time.Now().Unix()
	if err := dbUpsertSeedPricingPolicy(ctx, store, seedHash, floor, bps, "user", now); err != nil {
		return "", 0, 0, false, err
	}
	var chunkCount uint32
	if _, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (struct{}, error) {
		row, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return struct{}{}, sql.ErrNoRows
			}
			return struct{}{}, err
		}
		chunkCount = uint32(row.ChunkCount)
		return struct{}{}, nil
	}); err != nil {
		return "", 0, 0, false, err
	}
	seedPrice := floor * uint64(chunkCount)
	return seedHash, floor, seedPrice, true, nil
}

func dbRewriteStaticPricePaths(ctx context.Context, store *clientDB, fromAbs string, toAbs string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return nil
}
