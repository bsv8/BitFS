package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type workspaceFilesPage struct {
	Total int
	Items []workspaceFileItem
}

type workspaceFileItem struct {
	Path          string `json:"path"`
	FileSize      int64  `json:"file_size"`
	MtimeUnix     int64  `json:"mtime_unix"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

type workspaceSeedsPage struct {
	Total int
	Items []workspaceSeedItem
}

type workspaceSeedItem struct {
	SeedHash              string `json:"seed_hash"`
	SeedFilePath          string `json:"seed_file_path"`
	ChunkCount            uint32 `json:"chunk_count"`
	FileSize              int64  `json:"file_size"`
	CreatedAtUnix         int64  `json:"created_at_unix"`
	UnitPriceSatPer64K    uint64 `json:"unit_price_sat_per_64k"`
	LastBuyPriceSatPer64K uint64 `json:"last_buy_unit_price_sat_per_64k"`
	FloorPriceSatPer64K   uint64 `json:"floor_unit_price_sat_per_64k"`
	ResaleDiscountBPS     uint64 `json:"resale_discount_bps"`
	PriceUpdatedAtUnix    int64  `json:"price_updated_at_unix"`
}

type workspaceFileRow struct {
	Path          string
	FileSize      uint64
	UpdatedAtUnix int64
}

func workspaceStore(m *workspaceManager) *clientDB {
	if m == nil {
		return nil
	}
	if m.store != nil {
		return m.store
	}
	return newClientDB(m.db, nil)
}

func dbEnsureDefaultWorkspace(ctx context.Context, store *clientDB, workspaceDir string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	abs, err := filepath.Abs(strings.TrimSpace(workspaceDir))
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO workspaces(path,max_bytes,enabled,created_at_unix,updated_at_unix)
			 VALUES(?,?,1,?,?)
			 ON CONFLICT(path) DO UPDATE SET enabled=1,updated_at_unix=excluded.updated_at_unix`,
			abs, int64(0), now, now,
		)
		return err
	})
}

func dbListWorkspaces(ctx context.Context, store *clientDB) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]workspaceItem, error) {
		rows, err := db.Query(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces ORDER BY id ASC`)
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
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbAddWorkspace(ctx context.Context, store *clientDB, absPath string, maxBytes uint64) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	now := time.Now().Unix()
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceItem, error) {
		if _, err := db.Exec(
			`INSERT INTO workspaces(path,max_bytes,enabled,created_at_unix,updated_at_unix)
			 VALUES(?,?,1,?,?)
			 ON CONFLICT(path) DO UPDATE SET max_bytes=excluded.max_bytes,enabled=1,updated_at_unix=excluded.updated_at_unix`,
			absPath, maxBytes, now, now,
		); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByPath(db, absPath)
	})
}

func dbDeleteWorkspaceByID(ctx context.Context, store *clientDB, id int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		var path string
		if err := tx.QueryRow(`SELECT path FROM workspaces WHERE id=?`, id).Scan(&path); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("workspace not found")
			}
			return err
		}
		if _, err := tx.Exec(`DELETE FROM workspaces WHERE id=?`, id); err != nil {
			return err
		}
		path = filepath.Clean(strings.TrimSpace(path))
		if path != "" {
			if _, err := tx.Exec(`DELETE FROM workspace_files WHERE path=? OR path LIKE ?`, path, path+string(filepath.Separator)+"%"); err != nil {
				return err
			}
		}
		return dbCleanupOrphanSeedStateTx(tx)
	})
}

func dbUpdateWorkspaceByID(ctx context.Context, store *clientDB, id int64, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceItem, error) {
		cur, err := dbLoadWorkspaceByID(db, id)
		if err != nil {
			if err == sql.ErrNoRows {
				return workspaceItem{}, fmt.Errorf("workspace not found")
			}
			return workspaceItem{}, err
		}
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
		if _, err := db.Exec(`UPDATE workspaces SET max_bytes=?,enabled=?,updated_at_unix=? WHERE id=?`, nextMaxBytes, enabledValue, now, id); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByID(db, id)
	})
}

func dbWorkspaceUsedBytes(ctx context.Context, store *clientDB, rootPath string) (uint64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	rootPath = filepath.Clean(strings.TrimSpace(rootPath))
	return clientDBValue(ctx, store, func(db *sql.DB) (uint64, error) {
		var used uint64
		if err := db.QueryRow(`SELECT COALESCE(SUM(file_size),0) FROM workspace_files WHERE path=? OR path LIKE ?`, rootPath, rootPath+string(filepath.Separator)+"%").Scan(&used); err != nil {
			return 0, err
		}
		return used, nil
	})
}

func dbListLiveCacheFiles(ctx context.Context, store *clientDB) ([]workspaceFileRow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]workspaceFileRow, error) {
		rows, err := db.Query(`SELECT path,file_size,updated_at_unix FROM workspace_files`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]workspaceFileRow, 0, 64)
		for rows.Next() {
			var it workspaceFileRow
			if err := rows.Scan(&it.Path, &it.FileSize, &it.UpdatedAtUnix); err != nil {
				return nil, err
			}
			out = append(out, it)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbDeleteLiveStreamCacheRows(ctx context.Context, store *clientDB, streamID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	pattern := "%" + string(filepath.Separator) + "live" + string(filepath.Separator) + streamID + string(filepath.Separator) + "%"
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`DELETE FROM workspace_files WHERE path LIKE ?`, pattern)
		return err
	})
}

func dbCleanupOrphanSeedState(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, dbCleanupOrphanSeedStateTx)
}

func dbCleanupOrphanSeedStateTx(tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM seeds WHERE seed_hash NOT IN (SELECT DISTINCT seed_hash FROM workspace_files)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM seed_price_state WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM seed_available_chunks WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM file_downloads WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM file_download_chunks WHERE seed_hash NOT IN (SELECT seed_hash FROM seeds)`); err != nil {
		return err
	}
	return nil
}

func dbMergeSeedAvailableChunks(ctx context.Context, store *clientDB, seedHash string, incoming []uint32, chunkCount uint32) ([]uint32, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBTxValue(ctx, store, func(tx *sql.Tx) ([]uint32, error) {
		existing := make([]uint32, 0, len(incoming))
		rows, err := tx.Query(`SELECT chunk_index FROM seed_available_chunks WHERE seed_hash=? ORDER BY chunk_index ASC`, seedHash)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var idx uint32
			if err := rows.Scan(&idx); err != nil {
				return nil, err
			}
			existing = append(existing, idx)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		merged := normalizeChunkIndexes(append(existing, incoming...), chunkCount)
		if err := replaceSeedAvailableChunksTx(tx, seedHash, merged); err != nil {
			return nil, err
		}
		return merged, nil
	})
}

func dbUpsertDownloadedFile(ctx context.Context, store *clientDB, absPath string, fileSize int64, mtimeUnix int64, seedHash string, seedPath string, chunkCount uint32, fullFileSize uint64, recommendedName string, mimeHint string, seedLocked bool) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	now := time.Now().Unix()
	lockedValue := int64(0)
	if seedLocked {
		lockedValue = 1
	}
	return store.Do(ctx, func(db *sql.DB) error {
		if _, err := db.Exec(
			`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,seed_locked,updated_at_unix)
			 VALUES(?,?,?,?,?,?)
			 ON CONFLICT(path) DO UPDATE SET
			 file_size=excluded.file_size,
			 mtime_unix=excluded.mtime_unix,
			 seed_hash=excluded.seed_hash,
			 seed_locked=excluded.seed_locked,
			 updated_at_unix=excluded.updated_at_unix`,
			absPath, fileSize, mtimeUnix, seedHash, lockedValue, now,
		); err != nil {
			return err
		}
		_, err := db.Exec(
			`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint,created_at_unix)
			 VALUES(?,?,?,?,?,?,?)
			 ON CONFLICT(seed_hash) DO UPDATE SET
			 seed_file_path=excluded.seed_file_path,
			 chunk_count=excluded.chunk_count,
			 file_size=excluded.file_size,
			 recommended_file_name=excluded.recommended_file_name,
			 mime_hint=excluded.mime_hint`,
			seedHash, seedPath, chunkCount, fullFileSize, recommendedName, mimeHint, now,
		)
		return err
	})
}

func dbListWorkspaceFiles(ctx context.Context, store *clientDB, limit int, offset int, pathLike string) (workspaceFilesPage, error) {
	if store == nil {
		return workspaceFilesPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceFilesPage, error) {
		where := ""
		args := []any{}
		if pathLike != "" {
			where = " WHERE path LIKE ?"
			args = append(args, "%"+pathLike+"%")
		}
		var out workspaceFilesPage
		if err := db.QueryRow("SELECT COUNT(1) FROM workspace_files"+where, args...).Scan(&out.Total); err != nil {
			return workspaceFilesPage{}, err
		}
		rows, err := db.Query(`SELECT path,file_size,mtime_unix,seed_hash,updated_at_unix FROM workspace_files`+where+` ORDER BY updated_at_unix DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
		if err != nil {
			return workspaceFilesPage{}, err
		}
		defer rows.Close()
		out.Items = make([]workspaceFileItem, 0, limit)
		for rows.Next() {
			var it workspaceFileItem
			if err := rows.Scan(&it.Path, &it.FileSize, &it.MtimeUnix, &it.SeedHash, &it.UpdatedAtUnix); err != nil {
				return workspaceFilesPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return workspaceFilesPage{}, err
		}
		return out, nil
	})
}

func dbListWorkspaceSeeds(ctx context.Context, store *clientDB, limit int, offset int, seedHash string, seedHashLike string) (workspaceSeedsPage, error) {
	if store == nil {
		return workspaceSeedsPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceSeedsPage, error) {
		where := ""
		args := []any{}
		if seedHash != "" {
			where += " WHERE s.seed_hash=?"
			args = append(args, seedHash)
		} else if seedHashLike != "" {
			where += " WHERE s.seed_hash LIKE ?"
			args = append(args, "%"+seedHashLike+"%")
		}
		var out workspaceSeedsPage
		if err := db.QueryRow("SELECT COUNT(1) FROM seeds s"+where, args...).Scan(&out.Total); err != nil {
			return workspaceSeedsPage{}, err
		}
		rows, err := db.Query(`
			SELECT s.seed_hash,s.seed_file_path,s.chunk_count,s.file_size,s.created_at_unix,
			       COALESCE(p.unit_price_sat_per_64k,0), COALESCE(p.last_buy_unit_price_sat_per_64k,0),
			       COALESCE(p.floor_unit_price_sat_per_64k,0), COALESCE(p.resale_discount_bps,0),
			       COALESCE(p.updated_at_unix,0)
			FROM seeds s
			LEFT JOIN seed_price_state p ON p.seed_hash=s.seed_hash
			`+where+`
			ORDER BY s.created_at_unix DESC
			LIMIT ? OFFSET ?`, append(args, limit, offset)...)
		if err != nil {
			return workspaceSeedsPage{}, err
		}
		defer rows.Close()
		out.Items = make([]workspaceSeedItem, 0, limit)
		for rows.Next() {
			var it workspaceSeedItem
			if err := rows.Scan(
				&it.SeedHash,
				&it.SeedFilePath,
				&it.ChunkCount,
				&it.FileSize,
				&it.CreatedAtUnix,
				&it.UnitPriceSatPer64K,
				&it.LastBuyPriceSatPer64K,
				&it.FloorPriceSatPer64K,
				&it.ResaleDiscountBPS,
				&it.PriceUpdatedAtUnix,
			); err != nil {
				return workspaceSeedsPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return workspaceSeedsPage{}, err
		}
		return out, nil
	})
}

func dbLoadWorkspaceByID(db *sql.DB, id int64) (workspaceItem, error) {
	var out workspaceItem
	var enabled int64
	err := db.QueryRow(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces WHERE id=?`, id).
		Scan(&out.ID, &out.Path, &out.MaxBytes, &enabled, &out.CreatedAtUnix, &out.UpdatedAtUnix)
	if err != nil {
		return workspaceItem{}, err
	}
	out.Enabled = enabled != 0
	return out, nil
}

func dbLoadWorkspaceByPath(db *sql.DB, absPath string) (workspaceItem, error) {
	var out workspaceItem
	var enabled int64
	err := db.QueryRow(`SELECT id,path,max_bytes,enabled,created_at_unix,updated_at_unix FROM workspaces WHERE path=?`, absPath).
		Scan(&out.ID, &out.Path, &out.MaxBytes, &enabled, &out.CreatedAtUnix, &out.UpdatedAtUnix)
	if err != nil {
		return workspaceItem{}, err
	}
	out.Enabled = enabled != 0
	return out, nil
}

func replaceSeedAvailableChunksTx(tx *sql.Tx, seedHash string, indexes []uint32) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	indexes = normalizeChunkIndexes(indexes, 0)
	if _, err := tx.Exec(`DELETE FROM seed_available_chunks WHERE seed_hash=?`, seedHash); err != nil {
		return err
	}
	if len(indexes) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO seed_available_chunks(seed_hash,chunk_index,updated_at_unix) VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	now := time.Now().Unix()
	for _, idx := range indexes {
		if _, err := stmt.Exec(seedHash, idx, now); err != nil {
			return err
		}
	}
	return nil
}
