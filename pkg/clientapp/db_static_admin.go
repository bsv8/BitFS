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
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedFilePath string
		err := QueryRowContext(ctx, db, `SELECT seed_file_path FROM biz_seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&seedFilePath)
		if err != nil {
			return "", err
		}
		return seedFilePath, nil
	})
}

func dbListLiveWorkspaceEntries(ctx context.Context, store *clientDB, pattern string, includeMeta bool) ([]liveWorkspaceEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	needle := strings.Trim(pattern, "%")
	needle = strings.ReplaceAll(needle, "/", string(filepath.Separator))
	return clientDBValue(ctx, store, func(db *sql.DB) ([]liveWorkspaceEntry, error) {
		rows, err := QueryContext(ctx, db, `SELECT workspace_path,file_path,seed_hash FROM biz_workspace_files ORDER BY workspace_path ASC,file_path ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]liveWorkspaceEntry, 0, 32)
		for rows.Next() {
			var workspacePath, filePath, seedHash string
			if err := rows.Scan(&workspacePath, &filePath, &seedHash); err != nil {
				return nil, err
			}
			absPath := workspacePathJoin(workspacePath, filePath)
			fullPath := filepath.Clean(absPath)
			if needle != "" && !strings.Contains(filepath.ToSlash(fullPath), filepath.ToSlash(needle)) {
				continue
			}
			it := liveWorkspaceEntry{Path: fullPath, SeedHash: seedHash}
			if includeMeta {
				if st, err := os.Stat(fullPath); err == nil {
					it.FileSize = st.Size()
					it.MtimeUnix = st.ModTime().Unix()
					it.UpdatedAtUnix = it.MtimeUnix
				}
			}
			out = append(out, it)
		}
		if err := rows.Err(); err != nil {
			return nil, err
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
	return clientDBTxValue(ctx, store, func(tx *sql.Tx) (int64, error) {
		var before int64
		if err := QueryRowContext(ctx, tx, `SELECT COUNT(1) FROM biz_workspace_files WHERE file_path LIKE ?`, "live/"+streamID+"/%").Scan(&before); err != nil {
			return 0, err
		}
		if _, err := ExecContext(ctx, tx, `DELETE FROM biz_workspace_files WHERE file_path LIKE ?`, "live/"+streamID+"/%"); err != nil {
			return 0, err
		}
		return before, nil
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
	roots, err := dbListWorkspaceRoots(store.db)
	if err != nil {
		return "", err
	}
	resolved, ok := resolveWorkspaceRelativePath(abs, roots)
	if !ok {
		return "", nil
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedHash string
		err := QueryRowContext(ctx, db, `SELECT seed_hash FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, resolved.WorkspacePath, resolved.FilePath).Scan(&seedHash)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return seedHash, nil
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
	return clientDBValue(ctx, store, func(db *sql.DB) (staticFilePriceRecord, error) {
		var out staticFilePriceRecord
		err := QueryRowContext(ctx, db, `SELECT floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix FROM biz_seed_pricing_policy WHERE seed_hash=?`, seedHash).
			Scan(&out.FloorPriceSatPer64K, &out.ResaleDiscountBPS, &out.UpdatedAtUnix)
		if err != nil {
			if err == sql.ErrNoRows {
				return staticFilePriceRecord{}, err
			}
			return staticFilePriceRecord{}, err
		}
		return out, nil
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
	if err := dbUpsertSeedPricingPolicy(store.db, seedHash, floor, bps, "user", now); err != nil {
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
	if err := dbUpsertSeedPricingPolicy(store.db, seedHash, floor, bps, "user", now); err != nil {
		return "", 0, 0, false, err
	}
	var chunkCount uint32
	if err := QueryRowContext(ctx, store.db, `SELECT chunk_count FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&chunkCount); err != nil {
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
