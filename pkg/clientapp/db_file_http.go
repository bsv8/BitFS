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

type fileDownloadStateRow struct {
	FilePath      string
	FileSize      uint64
	ChunkCount    uint32
	Completed     uint32
	PaidSats      uint64
	Status        string
	DemandID      string
	LastError     string
	StatusJSON    string
	UpdatedAtUnix int64
}

type fileDownloadChunkRow struct {
	ChunkIndex uint32
	Status     string
}

func fileHTTPStore(s *fileHTTPServer) *clientDB {
	if s == nil {
		return nil
	}
	if s.store != nil {
		return s.store
	}
	// 仅兼容旧测试夹具：fileHTTPServer 测试有 db 直塞场景。
	if s.db != nil {
		return &clientDB{db: s.db}
	}
	return nil
}

func dbGetFileDownloadState(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (fileDownloadStateRow, error) {
		var out fileDownloadStateRow
		err := QueryRowContext(ctx, db, `SELECT file_path,file_size,chunk_count,completed_chunks,paid_sats,status,demand_id,last_error,status_json,updated_at_unix FROM proc_file_downloads WHERE seed_hash=?`, seedHash).
			Scan(&out.FilePath, &out.FileSize, &out.ChunkCount, &out.Completed, &out.PaidSats, &out.Status, &out.DemandID, &out.LastError, &out.StatusJSON, &out.UpdatedAtUnix)
		if err != nil {
			return fileDownloadStateRow{}, err
		}
		return out, nil
	})
}

func dbGetFileDownloadStateNoUpdated(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (fileDownloadStateRow, error) {
		var out fileDownloadStateRow
		err := QueryRowContext(ctx, db, `SELECT file_path,file_size,chunk_count,completed_chunks,paid_sats,status,demand_id,last_error,status_json FROM proc_file_downloads WHERE seed_hash=?`, seedHash).
			Scan(&out.FilePath, &out.FileSize, &out.ChunkCount, &out.Completed, &out.PaidSats, &out.Status, &out.DemandID, &out.LastError, &out.StatusJSON)
		if err != nil {
			return fileDownloadStateRow{}, err
		}
		return out, nil
	})
}

func dbEnsureFileDownloadQueued(ctx context.Context, store *clientDB, seedHash string, partPath string, statusJSON string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		_, err := ExecContext(ctx, db, `INSERT INTO proc_file_downloads(seed_hash,file_path,file_size,chunk_count,completed_chunks,paid_sats,status,demand_id,last_error,status_json,created_at_unix,updated_at_unix) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			seedHash, partPath, 0, 0, 0, 0, "queued", "", "", statusJSON, now, now)
		return err
	})
}

func dbListFileDownloadChunks(ctx context.Context, store *clientDB, seedHash string) ([]fileDownloadChunkRow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]fileDownloadChunkRow, error) {
		rows, err := QueryContext(ctx, db, `SELECT chunk_index,status FROM proc_file_download_chunks WHERE seed_hash=?`, seedHash)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]fileDownloadChunkRow, 0, 16)
		for rows.Next() {
			var it fileDownloadChunkRow
			if err := rows.Scan(&it.ChunkIndex, &it.Status); err != nil {
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

func dbUpsertFileDownloadState(ctx context.Context, store *clientDB, seedHash string, row fileDownloadStateRow) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		_, err := ExecContext(ctx, db, `INSERT INTO proc_file_downloads(seed_hash,file_path,file_size,chunk_count,completed_chunks,paid_sats,status,demand_id,last_error,status_json,created_at_unix,updated_at_unix)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(seed_hash) DO UPDATE SET
				file_path=excluded.file_path,
				file_size=excluded.file_size,
				chunk_count=excluded.chunk_count,
				completed_chunks=excluded.completed_chunks,
				paid_sats=excluded.paid_sats,
				status=excluded.status,
				demand_id=excluded.demand_id,
				last_error=excluded.last_error,
				status_json=excluded.status_json,
				updated_at_unix=excluded.updated_at_unix`,
			seedHash, row.FilePath, row.FileSize, row.ChunkCount, row.Completed, row.PaidSats, row.Status, row.DemandID, row.LastError, row.StatusJSON, now, now)
		return err
	})
}

func dbUpsertFileDownloadChunkDone(ctx context.Context, store *clientDB, seedHash string, idx uint32, sellerPeerID string, price uint64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		now := time.Now().Unix()
		_, err := ExecContext(ctx, db, `INSERT INTO proc_file_download_chunks(seed_hash,chunk_index,status,seller_pubkey_hex,price_sats,updated_at_unix)
			VALUES(?,?,?,?,?,?)
			ON CONFLICT(seed_hash,chunk_index) DO UPDATE SET
				status=excluded.status,
				seller_pubkey_hex=excluded.seller_pubkey_hex,
				price_sats=excluded.price_sats,
				updated_at_unix=excluded.updated_at_unix`,
			seedHash, idx, "done", sellerPeerID, price, now)
		return err
	})
}

func dbFindLatestWorkspaceFileBySeedHash(ctx context.Context, store *clientDB, seedHash string) (string, uint64, error) {
	if store == nil {
		return "", 0, fmt.Errorf("client db is nil")
	}
	type result struct {
		path string
		size uint64
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (result, error) {
		var out result
		var workspacePath, filePath string
		if err := QueryRowContext(ctx, db, `SELECT workspace_path,file_path FROM biz_workspace_files WHERE seed_hash=? ORDER BY workspace_path ASC,file_path ASC LIMIT 1`, seedHash).Scan(&workspacePath, &filePath); err != nil {
			return out, err
		}
		out.path = workspacePathJoin(workspacePath, filePath)
		if st, err := os.Stat(out.path); err == nil {
			out.size = uint64(st.Size())
		}
		return out, nil
	})
	if err != nil {
		return "", 0, err
	}
	return out.path, out.size, nil
}

func dbListLiveSegmentWorkspaceEntries(ctx context.Context, store *clientDB, streamID string) ([]liveWorkspaceEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	pattern := "%" + string(filepath.Separator) + "live" + string(filepath.Separator) + strings.ToLower(strings.TrimSpace(streamID)) + string(filepath.Separator) + "%"
	return dbListLiveWorkspaceEntries(ctx, store, pattern, false)
}
