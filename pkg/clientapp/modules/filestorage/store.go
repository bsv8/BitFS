package filestorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type workspaceItem struct {
	WorkspacePath string `json:"workspace_path"`
	MaxBytes      uint64 `json:"max_bytes"`
	Enabled       bool   `json:"enabled"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type workspaceFileItem struct {
	WorkspacePath string `json:"workspace_path"`
	FilePath      string `json:"file_path"`
	SeedHash      string `json:"seed_hash"`
	SeedLocked    bool   `json:"seed_locked"`
}

func dbListWorkspaces(ctx context.Context, store moduleapi.Store) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	var out []workspaceItem
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `SELECT workspace_path,enabled,max_bytes,created_at_unix FROM biz_workspaces ORDER BY workspace_path ASC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row workspaceItem
			var enabled int64
			var maxBytes int64
			if err := rows.Scan(&row.WorkspacePath, &enabled, &maxBytes, &row.CreatedAtUnix); err != nil {
				return err
			}
			row.Enabled = enabled != 0
			row.MaxBytes = uint64(maxBytes)
			out = append(out, row)
		}
		return rows.Err()
	})
	return out, err
}

func dbUpsertWorkspace(ctx context.Context, store moduleapi.Store, absPath string, maxBytes uint64, enabled bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("store is nil")
	}
	absPath, err := normalizeRootPath(absPath)
	if err != nil {
		return workspaceItem{}, err
	}
	item := workspaceItem{WorkspacePath: absPath, MaxBytes: maxBytes, Enabled: enabled}
	err = store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO biz_workspaces(workspace_path, enabled, max_bytes, created_at_unix)
			VALUES(?,?,?,strftime('%s','now'))
			ON CONFLICT(workspace_path) DO UPDATE SET enabled=excluded.enabled, max_bytes=excluded.max_bytes`,
			absPath, boolToInt64(enabled), int64(maxBytes))
		if err != nil {
			return err
		}
		return nil
	})
	return item, err
}

func dbDeleteWorkspace(ctx context.Context, store moduleapi.Store, workspacePath string) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return err
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM biz_workspace_files WHERE workspace_path=?`, workspacePath)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM biz_workspaces WHERE workspace_path=?`, workspacePath)
		return err
	})
}

func dbUpdateWorkspace(ctx context.Context, store moduleapi.Store, workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("store is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return workspaceItem{}, err
	}
	var out workspaceItem
	err = store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		row, err := loadWorkspaceRowTx(ctx, tx, workspacePath)
		if err != nil {
			return err
		}
		if maxBytes != nil {
			row.MaxBytes = *maxBytes
		}
		if enabled != nil {
			row.Enabled = *enabled
		}
		_, err = tx.ExecContext(ctx, `UPDATE biz_workspaces SET enabled=?, max_bytes=? WHERE workspace_path=?`, boolToInt64(row.Enabled), int64(row.MaxBytes), row.WorkspacePath)
		if err != nil {
			return err
		}
		out = row
		return nil
	})
	return out, err
}

func loadWorkspaceRowTx(ctx context.Context, tx moduleapi.WriteTx, workspacePath string) (workspaceItem, error) {
	var row workspaceItem
	var enabled int64
	var maxBytes int64
	err := tx.QueryRowContext(ctx, `SELECT workspace_path,enabled,max_bytes,created_at_unix FROM biz_workspaces WHERE workspace_path=?`, workspacePath).Scan(&row.WorkspacePath, &enabled, &maxBytes, &row.CreatedAtUnix)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workspaceItem{}, fmt.Errorf("workspace not found")
		}
		return workspaceItem{}, err
	}
	row.Enabled = enabled != 0
	row.MaxBytes = uint64(maxBytes)
	return row, nil
}

func dbListWorkspaceFiles(ctx context.Context, store moduleapi.Store, limit, offset int, pathLike string) ([]workspaceFileItem, int, error) {
	if store == nil {
		return nil, 0, fmt.Errorf("store is nil")
	}
	var items []workspaceFileItem
	var total int
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		where := ""
		args := []any{}
		if strings.TrimSpace(pathLike) != "" {
			where = " WHERE workspace_path LIKE ? OR file_path LIKE ?"
			args = append(args, "%"+pathLike+"%", "%"+pathLike+"%")
		}
		row := rc.QueryRowContext(ctx, `SELECT COUNT(1) FROM biz_workspace_files`+where, args...)
		if err := row.Scan(&total); err != nil {
			return err
		}
		q := `SELECT workspace_path,file_path,seed_hash,seed_locked FROM biz_workspace_files` + where + ` ORDER BY workspace_path ASC,file_path ASC`
		if limit > 0 {
			q += ` LIMIT ? OFFSET ?`
			args = append(args, limit, offset)
		}
		rows, err := rc.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row workspaceFileItem
			var locked int64
			if err := rows.Scan(&row.WorkspacePath, &row.FilePath, &row.SeedHash, &locked); err != nil {
				return err
			}
			row.SeedLocked = locked != 0
			items = append(items, row)
		}
		return rows.Err()
	})
	return items, total, err
}

func dbUpsertWorkspaceFileTx(ctx context.Context, tx moduleapi.WriteTx, workspacePath, filePath, seedHash string, locked bool) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return err
	}
	filePath, err = normalizeRelFilePath(filePath)
	if err != nil {
		return err
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO biz_workspace_files(workspace_path, file_path, seed_hash, seed_locked)
		VALUES(?,?,?,?)
		ON CONFLICT(workspace_path, file_path) DO UPDATE SET seed_hash=excluded.seed_hash, seed_locked=excluded.seed_locked`,
		workspacePath, filePath, seedHash, boolToInt64(locked))
	return err
}

func dbDeleteWorkspaceFileTx(ctx context.Context, tx moduleapi.WriteTx, workspacePath, filePath string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return err
	}
	filePath, err = normalizeRelFilePath(filePath)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, workspacePath, filePath)
	return err
}

func dbListWorkspaceRoots(ctx context.Context, store moduleapi.Store) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	var out []string
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `SELECT workspace_path FROM biz_workspaces WHERE enabled=1 ORDER BY workspace_path ASC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				return err
			}
			if root, err := normalizeRootPath(p); err == nil {
				out = append(out, root)
			}
		}
		return rows.Err()
	})
	return out, err
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func normalizeSeedHashHex(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if len(s) != 64 {
		return ""
	}
	for _, ch := range s {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		return ""
	}
	return s
}
