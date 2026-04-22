package clientapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspaces"
)

type fileStorageRuntimeAdapter struct {
	store     *clientDB
	seedStore moduleapi.SeedStorage
}

func newFileStorageRuntimeAdapter(store *clientDB, seedStore moduleapi.SeedStorage) *fileStorageRuntimeAdapter {
	if store == nil {
		return nil
	}
	return &fileStorageRuntimeAdapter{store: store, seedStore: seedStore}
}

func (a *fileStorageRuntimeAdapter) SelectOutputPath(ctx context.Context, fileName string, fileSize uint64) (string, error) {
	if a == nil || a.store == nil {
		return "", fmt.Errorf("file storage is not initialized")
	}
	return downloadFileWorkspaceSelectOutputPath(ctx, a.store, fileName, fileSize)
}

func (a *fileStorageRuntimeAdapter) SelectLiveSegmentOutputPath(ctx context.Context, streamID string, segmentIndex uint64, fileSize uint64) (string, error) {
	if a == nil || a.store == nil {
		return "", fmt.Errorf("file storage is not initialized")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if !isSeedHashHex(streamID) {
		return "", fmt.Errorf("invalid stream_id")
	}
	name := fmt.Sprintf("%06d.seg", segmentIndex)
	return a.selectOutputPathInDir(ctx, filepath.Join("live", streamID), name, fileSize)
}

func (a *fileStorageRuntimeAdapter) RegisterDownloadedFile(ctx context.Context, p registerDownloadedFileParams) (sellerSeed, error) {
	if a == nil || a.store == nil {
		return sellerSeed{}, fmt.Errorf("file storage is not initialized")
	}
	if a.seedStore == nil {
		return sellerSeed{}, fmt.Errorf("seed storage is not initialized")
	}
	seedRec, err := a.seedStore.SaveSeed(ctx, moduleapi.SeedSaveInput{
		SeedBytes:           append([]byte(nil), p.Seed...),
		ChunkHashes:         nil,
		ChunkCount:          0,
		RecommendedFileName: p.RecommendedFileName,
		MimeHint:            p.MIMEHint,
	})
	if err != nil {
		return sellerSeed{}, err
	}
	if err := dbRegisterWorkspaceFileOnly(ctx, a.store, p.FilePath, seedRec.SeedHash, false); err != nil {
		return sellerSeed{}, err
	}
	return sellerSeed{
		SeedHash:            seedRec.SeedHash,
		FileSize:            seedRec.FileSize,
		ChunkCount:          seedRec.ChunkCount,
		ChunkHashes:         append([]string(nil), seedRec.ChunkHashes...),
		RecommendedFileName: seedRec.RecommendedFileName,
		MIMEHint:            seedRec.MimeHint,
	}, nil
}

func (a *fileStorageRuntimeAdapter) EnforceLiveCacheLimit(ctx context.Context, maxBytes uint64) error {
	if a == nil || a.store == nil {
		return fmt.Errorf("file storage is not initialized")
	}
	if maxBytes == 0 {
		return nil
	}
	streams, totalBytes, err := a.listLiveCacheStreams(ctx)
	if err != nil {
		return err
	}
	if totalBytes <= maxBytes {
		return nil
	}
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].NewestUpdatedAtUnix == streams[j].NewestUpdatedAtUnix {
			return streams[i].StreamID < streams[j].StreamID
		}
		return streams[i].NewestUpdatedAtUnix < streams[j].NewestUpdatedAtUnix
	})
	for _, st := range streams {
		if totalBytes <= maxBytes {
			break
		}
		if err := a.deleteLiveStreamCache(ctx, st); err != nil {
			return err
		}
		if totalBytes > st.TotalBytes {
			totalBytes -= st.TotalBytes
		} else {
			totalBytes = 0
		}
	}
	return nil
}

func (a *fileStorageRuntimeAdapter) selectOutputPathInDir(ctx context.Context, relDir string, fileName string, fileSize uint64) (string, error) {
	items, err := a.listWorkspaceRows(ctx)
	if err != nil {
		return "", err
	}
	name := sanitizeRecommendedFileName(fileName)
	if name == "" {
		return "", fmt.Errorf("invalid output file name")
	}
	relDir = strings.TrimSpace(relDir)
	if relDir != "" {
		relDir = filepath.Clean(relDir)
		if filepath.IsAbs(relDir) || relDir == "." || strings.HasPrefix(relDir, "..") {
			return "", fmt.Errorf("invalid output relative dir")
		}
	}
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		free, ferr := freeBytesUnderPath(it.WorkspacePath)
		if ferr != nil {
			continue
		}
		if it.MaxBytes > 0 {
			used, _ := dbWorkspaceUsedBytes(ctx, a.store, it.WorkspacePath)
			if used+fileSize > it.MaxBytes {
				continue
			}
		}
		if free < fileSize {
			continue
		}
		if relDir != "" {
			return filepath.Join(it.WorkspacePath, relDir, name), nil
		}
		return filepath.Join(it.WorkspacePath, name), nil
	}
	return "", fmt.Errorf("no workspace has enough capacity")
}

func (a *fileStorageRuntimeAdapter) listLiveCacheStreams(ctx context.Context) ([]liveCacheStreamStat, uint64, error) {
	items, err := a.listWorkspaceRows(ctx)
	if err != nil {
		return nil, 0, err
	}
	rows, err := dbListLiveCacheFiles(ctx, a.store)
	if err != nil {
		return nil, 0, err
	}
	streams := map[string]*liveCacheStreamStat{}
	var total uint64
	for _, row := range rows {
		fullPath := workspacePathJoin(row.WorkspacePath, row.FilePath)
		streamID, workspaceDir, ok := classifyLiveWorkspacePathAdapter(items, fullPath)
		if !ok {
			continue
		}
		st := streams[streamID]
		if st == nil {
			st = &liveCacheStreamStat{StreamID: streamID}
			streams[streamID] = st
		}
		st.Paths = append(st.Paths, fullPath)
		st.WorkspaceDirs = append(st.WorkspaceDirs, workspaceDir)
		total += uint64(len(fullPath))
	}
	out := make([]liveCacheStreamStat, 0, len(streams))
	for _, st := range streams {
		out = append(out, *st)
	}
	return out, total, nil
}

func (a *fileStorageRuntimeAdapter) deleteLiveStreamCache(ctx context.Context, st liveCacheStreamStat) error {
	paths := append([]string(nil), st.Paths...)
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	if _, err := dbDeleteLiveStreamWorkspaceRows(ctx, a.store, st.StreamID); err != nil {
		return err
	}
	if a.seedStore != nil {
		return a.seedStore.CleanupOrphanSeeds(ctx)
	}
	return nil
}

func (a *fileStorageRuntimeAdapter) listWorkspaceRows(ctx context.Context) ([]workspaceItem, error) {
	if a == nil || a.store == nil {
		return nil, fmt.Errorf("file storage is not initialized")
	}
	var out []workspaceItem
	err := a.store.ReadEnt(ctx, func(root EntReadRoot) error {
		var rows []struct {
			WorkspacePath string `json:"workspace_path,omitempty"`
			Enabled       int64  `json:"enabled,omitempty"`
			MaxBytes      int64  `json:"max_bytes,omitempty"`
			CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
		}
		err := root.BizWorkspaces.Query().
			Order(bizworkspaces.ByWorkspacePath()).
			Select(
				bizworkspaces.FieldWorkspacePath,
				bizworkspaces.FieldEnabled,
				bizworkspaces.FieldMaxBytes,
				bizworkspaces.FieldCreatedAtUnix,
			).
			Scan(ctx, &rows)
		if err != nil {
			return err
		}
		for _, row := range rows {
			item := workspaceItem{
				WorkspacePath: row.WorkspacePath,
				Enabled:       row.Enabled != 0,
				MaxBytes:      uint64(row.MaxBytes),
				CreatedAtUnix: row.CreatedAtUnix,
			}
			out = append(out, item)
		}
		return nil
	})
	return out, err
}

func classifyLiveWorkspacePathAdapter(items []workspaceItem, absPath string) (string, string, bool) {
	absPath = filepath.Clean(strings.TrimSpace(absPath))
	if absPath == "" {
		return "", "", false
	}
	for _, it := range items {
		root := filepath.Clean(strings.TrimSpace(it.WorkspacePath))
		if root == "" {
			continue
		}
		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(filepath.Clean(rel))
		if rel == "." || rel == "" || strings.HasPrefix(rel, "../") {
			continue
		}
		if !strings.HasPrefix(rel, "live/") {
			continue
		}
		parts := strings.Split(rel, "/")
		if len(parts) < 3 || len(parts[1]) != 64 {
			continue
		}
		return parts[1], filepath.Join(root, "live", parts[1]), true
	}
	return "", "", false
}
