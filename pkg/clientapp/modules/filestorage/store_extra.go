package filestorage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen/bizworkspacefiles"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen/procfiledownloadchunks"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen/procfiledownloads"
)

// FileDownloadState 是下载状态的模块内壳。
type FileDownloadState struct {
	FilePath     string
	FileSize     uint64
	ChunkCount   uint32
	Completed    uint32
	PaidSats     uint64
	Status       string
	DemandID     string
	LastError    string
	StatusJSON   string
	UpdatedAtUnix int64
}

// FileDownloadChunk 是下载分块状态的模块内壳。
type FileDownloadChunk struct {
	ChunkIndex uint32
	Status     string
}

func IsNotFound(err error) bool {
	return gen.IsNotFound(err)
}

func FindLatestWorkspaceFileBySeedHash(ctx context.Context, root DBReadRoot, seedHash string) (string, uint64, error) {
	if root == nil {
		return "", 0, fmt.Errorf("workspace root is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return "", 0, fmt.Errorf("seed_hash is required")
	}
	page, _, err := DBListWorkspaceFiles(ctx, root, -1, 0, "")
	if err != nil {
		return "", 0, err
	}
	for _, row := range page {
		if !strings.EqualFold(strings.TrimSpace(row.SeedHash), seedHash) {
			continue
		}
		absPath := filepath.Join(row.WorkspacePath, filepath.FromSlash(row.FilePath))
		st, statErr := os.Stat(absPath)
		if statErr != nil || !st.Mode().IsRegular() {
			continue
		}
		return absPath, uint64(st.Size()), nil
	}
	return "", 0, sql.ErrNoRows
}

func CollectActiveSeedHashes(ctx context.Context, root DBReadRoot) (map[string]struct{}, error) {
	if root == nil {
		return nil, fmt.Errorf("workspace root is nil")
	}
	active := map[string]struct{}{}
	workspaceRows, _, err := DBListWorkspaceFiles(ctx, root, -1, 0, "")
	if err != nil {
		return nil, err
	}
	for _, row := range workspaceRows {
		if seed := normalizeSeedHashHex(row.SeedHash); seed != "" {
			active[seed] = struct{}{}
		}
	}
	downloadRows, err := root.ProcFileDownloadsQuery().All(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range downloadRows {
		if seed := normalizeSeedHashHex(row.SeedHash); seed != "" {
			active[seed] = struct{}{}
		}
	}
	chunkRows, err := root.ProcFileDownloadChunksQuery().All(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range chunkRows {
		if seed := normalizeSeedHashHex(row.SeedHash); seed != "" {
			active[seed] = struct{}{}
		}
	}
	return active, nil
}

func GetFileDownloadState(ctx context.Context, root DBReadRoot, seedHash string, includeUpdated bool) (FileDownloadState, error) {
	if root == nil {
		return FileDownloadState{}, fmt.Errorf("workspace root is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return FileDownloadState{}, fmt.Errorf("seed_hash is required")
	}
	row, err := root.ProcFileDownloadsQuery().
		Where(procfiledownloads.SeedHashEQ(seedHash)).
		Only(ctx)
	if err != nil {
		if IsNotFound(err) {
			return FileDownloadState{}, sql.ErrNoRows
		}
		return FileDownloadState{}, err
	}
	out := FileDownloadState{
		FilePath:   row.FilePath,
		FileSize:   uint64(row.FileSize),
		ChunkCount: uint32(row.ChunkCount),
		Completed:  uint32(row.CompletedChunks),
		PaidSats:   uint64(row.PaidSats),
		Status:     row.Status,
		DemandID:   row.DemandID,
		LastError:  row.LastError,
		StatusJSON: row.StatusJSON,
	}
	if includeUpdated {
		out.UpdatedAtUnix = row.UpdatedAtUnix
	}
	return out, nil
}

func EnsureFileDownloadQueued(ctx context.Context, root DBWriteRoot, seedHash string, partPath string, statusJSON string) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
	}
	now := time.Now().Unix()
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	existing, err := root.ProcFileDownloadsClient().Query().
		Where(procfiledownloads.SeedHashEQ(seedHash)).
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
	if !IsNotFound(err) {
		return err
	}
	_, err = root.ProcFileDownloadsClient().Create().
		SetSeedHash(seedHash).
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

func ListFileDownloadChunks(ctx context.Context, root DBReadRoot, seedHash string) ([]FileDownloadChunk, error) {
	if root == nil {
		return nil, fmt.Errorf("workspace root is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	rows, err := root.ProcFileDownloadChunksQuery().
		Where(procfiledownloadchunks.SeedHashEQ(seedHash)).
		Order(procfiledownloadchunks.ByChunkIndex()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]FileDownloadChunk, 0, len(rows))
	for _, row := range rows {
		out = append(out, FileDownloadChunk{ChunkIndex: uint32(row.ChunkIndex), Status: row.Status})
	}
	return out, nil
}

func UpsertFileDownloadState(ctx context.Context, root DBWriteRoot, seedHash string, row FileDownloadState) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
	}
	now := time.Now().Unix()
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	existing, err := root.ProcFileDownloadsClient().Query().
		Where(procfiledownloads.SeedHashEQ(seedHash)).
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
	if !IsNotFound(err) {
		return err
	}
	_, err = root.ProcFileDownloadsClient().Create().
		SetSeedHash(seedHash).
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

func UpsertFileDownloadChunkDone(ctx context.Context, root DBWriteRoot, seedHash string, idx uint32, sellerPeerID string, price uint64) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
	}
	now := time.Now().Unix()
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	existing, err := root.ProcFileDownloadChunksClient().Query().
		Where(
			procfiledownloadchunks.SeedHashEQ(seedHash),
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
	if !IsNotFound(err) {
		return err
	}
	_, err = root.ProcFileDownloadChunksClient().Create().
		SetSeedHash(seedHash).
		SetChunkIndex(int64(idx)).
		SetStatus("done").
		SetSellerPubkeyHex(sellerPeerID).
		SetPriceSats(int64(price)).
		SetUpdatedAtUnix(now).
		Save(ctx)
	return err
}

func DeleteLiveStreamWorkspaceRows(ctx context.Context, root DBWriteRoot, streamID string) (int64, error) {
	if root == nil {
		return 0, fmt.Errorf("workspace root is nil")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if normalizeSeedHashHex(streamID) == "" {
		return 0, fmt.Errorf("invalid stream_id")
	}
	pathPrefix := "live/" + streamID + "/"
	count, err := root.BizWorkspaceFilesClient().Query().Where(bizworkspacefiles.FilePathHasPrefix(pathPrefix)).Count(ctx)
	if err != nil {
		return 0, err
	}
	if _, err := root.BizWorkspaceFilesClient().Delete().Where(bizworkspacefiles.FilePathHasPrefix(pathPrefix)).Exec(ctx); err != nil {
		return 0, err
	}
	return int64(count), nil
}
