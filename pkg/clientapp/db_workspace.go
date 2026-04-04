package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type workspaceFilesPage struct {
	Total int
	Items []workspaceFileItem
}

type workspaceFileItem struct {
	WorkspacePath string `json:"workspace_path"`
	FilePath      string `json:"file_path"`
	SeedHash      string `json:"seed_hash"`
	SeedLocked    bool   `json:"seed_locked"`
}

type workspaceSeedsPage struct {
	Total int
	Items []workspaceSeedItem
}

type workspaceSeedItem struct {
	SeedHash            string `json:"seed_hash"`
	SeedFilePath        string `json:"seed_file_path"`
	ChunkCount          uint32 `json:"chunk_count"`
	FileSize            int64  `json:"file_size"`
	FloorPriceSatPer64K uint64 `json:"floor_unit_price_sat_per_64k"`
	ResaleDiscountBPS   uint64 `json:"resale_discount_bps"`
	PricingSource       string `json:"pricing_source"`
	PriceUpdatedAtUnix  int64  `json:"price_updated_at_unix"`
}

type workspaceFileRow struct {
	WorkspacePath string
	FilePath      string
	SeedHash      string
	SeedLocked    bool
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
	abs, err := normalizeWorkspacePath(workspaceDir)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix)
			 VALUES(?,1,?,?)
			 ON CONFLICT(workspace_path) DO UPDATE SET enabled=1`,
			abs, int64(0), now,
		)
		return err
	})
}

func dbListWorkspaces(ctx context.Context, store *clientDB) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]workspaceItem, error) {
		rows, err := db.Query(`SELECT workspace_path,max_bytes,enabled,created_at_unix FROM biz_workspaces ORDER BY workspace_path ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]workspaceItem, 0, 8)
		for rows.Next() {
			var it workspaceItem
			var enabled int64
			if err := rows.Scan(&it.WorkspacePath, &it.MaxBytes, &enabled, &it.CreatedAtUnix); err != nil {
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
	absPath, err := normalizeWorkspacePath(absPath)
	if err != nil {
		return workspaceItem{}, err
	}
	now := time.Now().Unix()
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceItem, error) {
		if _, err := db.Exec(
			`INSERT INTO biz_workspaces(workspace_path,max_bytes,enabled,created_at_unix)
			 VALUES(?, ?, 1, ?)
			 ON CONFLICT(workspace_path) DO UPDATE SET max_bytes=excluded.max_bytes,enabled=1`,
			absPath, maxBytes, now,
		); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByPath(db, absPath)
	})
}

func dbDeleteWorkspaceByPath(ctx context.Context, store *clientDB, workspacePath string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	workspacePath, err := normalizeWorkspacePath(workspacePath)
	if err != nil {
		return err
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		var path string
		if err := tx.QueryRow(`SELECT workspace_path FROM biz_workspaces WHERE workspace_path=?`, workspacePath).Scan(&path); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("workspace not found")
			}
			return err
		}
		if _, err := tx.Exec(`DELETE FROM biz_workspaces WHERE workspace_path=?`, workspacePath); err != nil {
			return err
		}
		path = filepath.Clean(strings.TrimSpace(path))
		if path != "" {
			if _, err := tx.Exec(`DELETE FROM biz_workspace_files WHERE workspace_path=?`, path); err != nil {
				return err
			}
		}
		return dbCleanupOrphanSeedStateTx(tx)
	})
}

func dbUpdateWorkspaceByPath(ctx context.Context, store *clientDB, workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	workspacePath, err := normalizeWorkspacePath(workspacePath)
	if err != nil {
		return workspaceItem{}, err
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceItem, error) {
		cur, err := dbLoadWorkspaceByPath(db, workspacePath)
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
		if _, err := db.Exec(`UPDATE biz_workspaces SET max_bytes=?,enabled=? WHERE workspace_path=?`, nextMaxBytes, enabledValue, workspacePath); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByPath(db, workspacePath)
	})
}

func dbWorkspaceUsedBytes(ctx context.Context, store *clientDB, rootPath string) (uint64, error) {
	rootPath = filepath.Clean(strings.TrimSpace(rootPath))
	if rootPath == "" {
		return 0, fmt.Errorf("workspace path is empty")
	}
	var used uint64
	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		used += uint64(st.Size())
		return nil
	})
	return used, err
}

func dbListLiveCacheFiles(ctx context.Context, store *clientDB) ([]workspaceFileRow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]workspaceFileRow, error) {
		rows, err := db.Query(`SELECT workspace_path,file_path,seed_hash,seed_locked FROM biz_workspace_files`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]workspaceFileRow, 0, 64)
		for rows.Next() {
			var it workspaceFileRow
			var locked int64
			if err := rows.Scan(&it.WorkspacePath, &it.FilePath, &it.SeedHash, &locked); err != nil {
				return nil, err
			}
			it.SeedLocked = locked != 0
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`DELETE FROM biz_workspace_files WHERE file_path LIKE ?`, "live/"+strings.ToLower(strings.TrimSpace(streamID))+"/%")
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
	if _, err := tx.Exec(`DELETE FROM biz_seeds WHERE seed_hash NOT IN (SELECT DISTINCT seed_hash FROM biz_workspace_files)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM biz_seed_pricing_policy WHERE seed_hash NOT IN (SELECT seed_hash FROM biz_seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM biz_seed_chunk_supply WHERE seed_hash NOT IN (SELECT seed_hash FROM biz_seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM proc_file_downloads WHERE seed_hash NOT IN (SELECT seed_hash FROM biz_seeds)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM proc_file_download_chunks WHERE seed_hash NOT IN (SELECT seed_hash FROM biz_seeds)`); err != nil {
		return err
	}
	return nil
}

func dbUpsertDownloadedFile(ctx context.Context, store *clientDB, absPath string, seedHash string, seedPath string, chunkCount uint32, fullFileSize uint64, recommendedName string, mimeHint string, seedLocked bool) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	lockedValue := int64(0)
	if seedLocked {
		lockedValue = 1
	}
	return store.Do(ctx, func(db *sql.DB) error {
		roots, err := dbListWorkspaceRoots(db)
		if err != nil {
			return err
		}
		resolved, ok := resolveWorkspaceRelativePath(absPath, roots)
		if !ok {
			return fmt.Errorf("output path is outside registered biz_workspaces")
		}
		if _, err := db.Exec(
			`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked)
			 VALUES(?,?,?,?)
			 ON CONFLICT(workspace_path,file_path) DO UPDATE SET
			 seed_hash=excluded.seed_hash,
			 seed_locked=excluded.seed_locked`,
			resolved.WorkspacePath, resolved.FilePath, seedHash, lockedValue,
		); err != nil {
			return err
		}
		_, err = db.Exec(
			`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint)
			 VALUES(?,?,?,?,?,?)
			 ON CONFLICT(seed_hash) DO UPDATE SET
			 chunk_count=excluded.chunk_count,
			 file_size=excluded.file_size,
			 seed_file_path=excluded.seed_file_path,
			 recommended_file_name=excluded.recommended_file_name,
			 mime_hint=excluded.mime_hint`,
			seedHash, chunkCount, fullFileSize, seedPath, recommendedName, mimeHint,
		)
		return err
	})
}

func dbListWorkspaceRoots(db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`SELECT workspace_path FROM biz_workspaces WHERE enabled=1 ORDER BY workspace_path ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var root string
		if err := rows.Scan(&root); err != nil {
			return nil, err
		}
		if root, err = normalizeWorkspacePath(root); err == nil && root != "" {
			out = append(out, root)
		}
	}
	return out, rows.Err()
}

func dbListWorkspaceFiles(ctx context.Context, store *clientDB, limit int, offset int, pathLike string) (workspaceFilesPage, error) {
	if store == nil {
		return workspaceFilesPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (workspaceFilesPage, error) {
		where := ""
		args := []any{}
		if pathLike != "" {
			where = " WHERE workspace_path LIKE ? OR file_path LIKE ?"
			args = append(args, "%"+pathLike+"%")
			args = append(args, "%"+pathLike+"%")
		}
		var out workspaceFilesPage
		if err := db.QueryRow("SELECT COUNT(1) FROM biz_workspace_files"+where, args...).Scan(&out.Total); err != nil {
			return workspaceFilesPage{}, err
		}
		rows, err := db.Query(`SELECT workspace_path,file_path,seed_hash,seed_locked FROM biz_workspace_files`+where+` ORDER BY workspace_path ASC,file_path ASC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
		if err != nil {
			return workspaceFilesPage{}, err
		}
		defer rows.Close()
		out.Items = make([]workspaceFileItem, 0, limit)
		for rows.Next() {
			var it workspaceFileItem
			var locked int64
			if err := rows.Scan(&it.WorkspacePath, &it.FilePath, &it.SeedHash, &locked); err != nil {
				return workspaceFilesPage{}, err
			}
			it.SeedLocked = locked != 0
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
		if err := db.QueryRow("SELECT COUNT(1) FROM biz_seeds s"+where, args...).Scan(&out.Total); err != nil {
			return workspaceSeedsPage{}, err
		}
		rows, err := db.Query(`
			SELECT s.seed_hash,s.seed_file_path,s.chunk_count,s.file_size,
			       COALESCE(p.floor_unit_price_sat_per_64k,0),
			       COALESCE(p.resale_discount_bps,0),
			       COALESCE(p.pricing_source,'system'),
			       COALESCE(p.updated_at_unix,0)
			FROM biz_seeds s
			LEFT JOIN biz_seed_pricing_policy p ON p.seed_hash=s.seed_hash
			`+where+`
			ORDER BY s.seed_hash ASC
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
				&it.FloorPriceSatPer64K,
				&it.ResaleDiscountBPS,
				&it.PricingSource,
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

func dbLoadWorkspaceByPath(db *sql.DB, absPath string) (workspaceItem, error) {
	var out workspaceItem
	var enabled int64
	err := db.QueryRow(`SELECT workspace_path,max_bytes,enabled,created_at_unix FROM biz_workspaces WHERE workspace_path=?`, absPath).
		Scan(&out.WorkspacePath, &out.MaxBytes, &enabled, &out.CreatedAtUnix)
	if err != nil {
		return workspaceItem{}, err
	}
	out.Enabled = enabled != 0
	return out, nil
}
