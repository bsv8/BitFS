package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type liveWorkspaceEntry struct {
	Path          string
	SeedHash      string
	UpdatedAtUnix int64
	FileSize      int64
	MtimeUnix     int64
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
		err := db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, strings.ToLower(strings.TrimSpace(seedHash))).Scan(&seedFilePath)
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
	return clientDBValue(ctx, store, func(db *sql.DB) ([]liveWorkspaceEntry, error) {
		query := `SELECT path,seed_hash,updated_at_unix FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix ASC, path ASC`
		if includeMeta {
			query = `SELECT path,seed_hash,updated_at_unix,file_size,mtime_unix FROM workspace_files WHERE path LIKE ? ORDER BY updated_at_unix DESC,path ASC`
		}
		rows, err := db.Query(query, pattern)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]liveWorkspaceEntry, 0, 32)
		for rows.Next() {
			var it liveWorkspaceEntry
			if includeMeta {
				if err := rows.Scan(&it.Path, &it.SeedHash, &it.UpdatedAtUnix, &it.FileSize, &it.MtimeUnix); err != nil {
					return nil, err
				}
			} else {
				if err := rows.Scan(&it.Path, &it.SeedHash, &it.UpdatedAtUnix); err != nil {
					return nil, err
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
	return clientDBTxValue(ctx, store, func(tx *sql.Tx) (int64, error) {
		var before int64
		if err := tx.QueryRow(`SELECT COUNT(1) FROM workspace_files WHERE path LIKE ?`, prefix).Scan(&before); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM workspace_files WHERE path LIKE ?`, prefix); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM static_file_prices WHERE path LIKE ?`, prefix); err != nil {
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
		if row.UpdatedAtUnix > it.lastUpdate {
			it.lastUpdate = row.UpdatedAtUnix
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
	return clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var seedHash string
		err := db.QueryRow(`SELECT seed_hash FROM workspace_files WHERE path=?`, path).Scan(&seedHash)
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
	return clientDBValue(ctx, store, func(db *sql.DB) (staticFilePriceRecord, error) {
		var out staticFilePriceRecord
		err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix FROM static_file_prices WHERE path=?`, path).
			Scan(&out.FloorPriceSatPer64K, &out.ResaleDiscountBPS, &out.UpdatedAtUnix)
		if err != nil {
			return staticFilePriceRecord{}, err
		}
		return out, nil
	})
}

func dbDeleteStaticPriceByPrefix(ctx context.Context, store *clientDB, prefix string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`DELETE FROM static_file_prices WHERE path LIKE ?`, prefix)
		return err
	})
}

func dbDeleteStaticPriceByPath(ctx context.Context, store *clientDB, path string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`DELETE FROM static_file_prices WHERE path=?`, path)
		return err
	})
}

func dbUpsertStaticFilePrice(ctx context.Context, store *clientDB, path string, floor uint64, bps uint64) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		now := time.Now().Unix()
		_, err := db.Exec(
			`INSERT INTO static_file_prices(path,floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix) VALUES(?,?,?,?)
			 ON CONFLICT(path) DO UPDATE SET floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,resale_discount_bps=excluded.resale_discount_bps,updated_at_unix=excluded.updated_at_unix`,
			path, floor, bps, now,
		)
		if err != nil {
			return 0, err
		}
		return now, nil
	})
}

func dbBindStaticPriceToSeed2(ctx context.Context, store *clientDB, path string, floor uint64, bps uint64) (string, uint64, uint64, bool, error) {
	if store == nil {
		return "", 0, 0, false, fmt.Errorf("client db is nil")
	}
	type result struct {
		seed  string
		unit  uint64
		total uint64
		bound bool
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (result, error) {
		var out result
		_ = db.QueryRow(`SELECT seed_hash FROM workspace_files WHERE path=?`, path).Scan(&out.seed)
		if out.seed != "" {
			var seedPath string
			if err := db.QueryRow(`SELECT seed_file_path FROM seeds WHERE seed_hash=?`, out.seed).Scan(&seedPath); err == nil {
				unit, total, err := upsertSeedPriceState(db, out.seed, floor, bps, seedPath)
				if err == nil {
					out.unit = unit
					out.total = total
					out.bound = true
				}
			}
		}
		return out, nil
	})
	if err != nil {
		return "", 0, 0, false, err
	}
	return out.seed, out.unit, out.total, out.bound, nil
}

func dbRewriteStaticPricePaths(ctx context.Context, store *clientDB, fromAbs string, toAbs string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT path,floor_unit_price_sat_per_64k,resale_discount_bps FROM static_file_prices WHERE path=? OR path LIKE ?`, fromAbs, filepath.Clean(fromAbs)+string(filepath.Separator)+"%")
		if err != nil {
			return err
		}
		defer rows.Close()
		type row struct {
			path  string
			floor uint64
			bps   uint64
		}
		list := make([]row, 0, 8)
		for rows.Next() {
			var it row
			if err := rows.Scan(&it.path, &it.floor, &it.bps); err != nil {
				return err
			}
			list = append(list, it)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, it := range list {
			newPath := strings.Replace(it.path, fromAbs, toAbs, 1)
			if _, err := tx.Exec(`DELETE FROM static_file_prices WHERE path=?`, it.path); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO static_file_prices(path,floor_unit_price_sat_per_64k,resale_discount_bps,updated_at_unix) VALUES(?,?,?,?)`, newPath, it.floor, it.bps, time.Now().Unix()); err != nil {
				return err
			}
		}
		return nil
	})
}
