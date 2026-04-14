package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procfiledownloadchunks"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procfiledownloads"
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
		return clientDBFromDB(s.db)
	}
	return nil
}

func dbGetFileDownloadState(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (fileDownloadStateRow, error) {
		var out fileDownloadStateRow
		row, err := tx.ProcFileDownloads.Query().
			Where(procfiledownloads.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fileDownloadStateRow{}, sql.ErrNoRows
			}
			return fileDownloadStateRow{}, err
		}
		out.FilePath = row.FilePath
		out.FileSize = uint64(row.FileSize)
		out.ChunkCount = uint32(row.ChunkCount)
		out.Completed = uint32(row.CompletedChunks)
		out.PaidSats = uint64(row.PaidSats)
		out.Status = row.Status
		out.DemandID = row.DemandID
		out.LastError = row.LastError
		out.StatusJSON = row.StatusJSON
		out.UpdatedAtUnix = row.UpdatedAtUnix
		return out, nil
	})
}

func dbGetFileDownloadStateNoUpdated(ctx context.Context, store *clientDB, seedHash string) (fileDownloadStateRow, error) {
	if store == nil {
		return fileDownloadStateRow{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (fileDownloadStateRow, error) {
		var out fileDownloadStateRow
		row, err := tx.ProcFileDownloads.Query().
			Where(procfiledownloads.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fileDownloadStateRow{}, sql.ErrNoRows
			}
			return fileDownloadStateRow{}, err
		}
		out.FilePath = row.FilePath
		out.FileSize = uint64(row.FileSize)
		out.ChunkCount = uint32(row.ChunkCount)
		out.Completed = uint32(row.CompletedChunks)
		out.PaidSats = uint64(row.PaidSats)
		out.Status = row.Status
		out.DemandID = row.DemandID
		out.LastError = row.LastError
		out.StatusJSON = row.StatusJSON
		return out, nil
	})
}

func dbEnsureFileDownloadQueued(ctx context.Context, store *clientDB, seedHash string, partPath string, statusJSON string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		now := time.Now().Unix()
		existing, err := tx.ProcFileDownloads.Query().
			Where(procfiledownloads.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetFilePath(partPath).
				SetFileSize(0).
				SetChunkCount(0).
				SetCompletedChunks(0).
				SetPaidSats(0).
				SetStatus("queued").
				SetDemandID("").
				SetLastError("").
				SetStatusJSON(statusJSON).
				SetCreatedAtUnix(now).
				SetUpdatedAtUnix(now).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcFileDownloads.Create().
			SetSeedHash(strings.ToLower(strings.TrimSpace(seedHash))).
			SetFilePath(partPath).
			SetFileSize(0).
			SetChunkCount(0).
			SetCompletedChunks(0).
			SetPaidSats(0).
			SetStatus("queued").
			SetDemandID("").
			SetLastError("").
			SetStatusJSON(statusJSON).
			SetCreatedAtUnix(now).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

func dbListFileDownloadChunks(ctx context.Context, store *clientDB, seedHash string) ([]fileDownloadChunkRow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) ([]fileDownloadChunkRow, error) {
		rows, err := tx.ProcFileDownloadChunks.Query().
			Where(procfiledownloadchunks.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Order(procfiledownloadchunks.ByChunkIndex()).
			All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]fileDownloadChunkRow, 0, len(rows))
		for _, row := range rows {
			out = append(out, fileDownloadChunkRow{
				ChunkIndex: uint32(row.ChunkIndex),
				Status:     row.Status,
			})
		}
		return out, nil
	})
}

func dbUpsertFileDownloadState(ctx context.Context, store *clientDB, seedHash string, row fileDownloadStateRow) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		now := time.Now().Unix()
		existing, err := tx.ProcFileDownloads.Query().
			Where(procfiledownloads.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetFilePath(row.FilePath).
				SetFileSize(int64(row.FileSize)).
				SetChunkCount(int64(row.ChunkCount)).
				SetCompletedChunks(int64(row.Completed)).
				SetPaidSats(int64(row.PaidSats)).
				SetStatus(row.Status).
				SetDemandID(row.DemandID).
				SetLastError(row.LastError).
				SetStatusJSON(row.StatusJSON).
				SetCreatedAtUnix(now).
				SetUpdatedAtUnix(now).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcFileDownloads.Create().
			SetSeedHash(strings.ToLower(strings.TrimSpace(seedHash))).
			SetFilePath(row.FilePath).
			SetFileSize(int64(row.FileSize)).
			SetChunkCount(int64(row.ChunkCount)).
			SetCompletedChunks(int64(row.Completed)).
			SetPaidSats(int64(row.PaidSats)).
			SetStatus(row.Status).
			SetDemandID(row.DemandID).
			SetLastError(row.LastError).
			SetStatusJSON(row.StatusJSON).
			SetCreatedAtUnix(now).
			SetUpdatedAtUnix(now).
			Save(ctx)
		return err
	})
}

func dbUpsertFileDownloadChunkDone(ctx context.Context, store *clientDB, seedHash string, idx uint32, sellerPeerID string, price uint64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		now := time.Now().Unix()
		existing, err := tx.ProcFileDownloadChunks.Query().
			Where(
				procfiledownloadchunks.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash))),
				procfiledownloadchunks.ChunkIndexEQ(int64(idx)),
			).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetStatus("done").
				SetSellerPubkeyHex(sellerPeerID).
				SetPriceSats(int64(price)).
				SetUpdatedAtUnix(now).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.ProcFileDownloadChunks.Create().
			SetSeedHash(strings.ToLower(strings.TrimSpace(seedHash))).
			SetChunkIndex(int64(idx)).
			SetStatus("done").
			SetSellerPubkeyHex(sellerPeerID).
			SetPriceSats(int64(price)).
			SetUpdatedAtUnix(now).
			Save(ctx)
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
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (result, error) {
		var out result
		row, err := tx.BizWorkspaceFiles.Query().
			Where(bizworkspacefiles.SeedHashEQ(strings.ToLower(strings.TrimSpace(seedHash)))).
			Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).
			First(ctx)
		if err != nil {
			return out, err
		}
		out.path = workspacePathJoin(row.WorkspacePath, row.FilePath)
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
