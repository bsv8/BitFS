package clientapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
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

func dbListWorkspaces(ctx context.Context, store *clientDB) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	rows, err := store.ListWorkspaces(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]workspaceItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, workspaceItem{
			WorkspacePath: row.WorkspacePath,
			MaxBytes:      row.MaxBytes,
			Enabled:       row.Enabled,
			CreatedAtUnix: row.CreatedAtUnix,
		})
	}
	return out, nil
}

func dbAddWorkspace(ctx context.Context, store *clientDB, absPath string, maxBytes uint64) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	absPath, err := normalizeWorkspacePath(absPath)
	if err != nil {
		return workspaceItem{}, err
	}
	row, err := store.UpsertWorkspace(ctx, absPath, maxBytes, true)
	if err != nil {
		return workspaceItem{}, err
	}
	return workspaceItem{
		WorkspacePath: row.WorkspacePath,
		MaxBytes:      row.MaxBytes,
		Enabled:       row.Enabled,
		CreatedAtUnix: row.CreatedAtUnix,
	}, nil
}

func dbDeleteWorkspaceByPath(ctx context.Context, store *clientDB, workspacePath string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	workspacePath, err := normalizeWorkspacePath(workspacePath)
	if err != nil {
		return err
	}
	return store.DeleteWorkspace(ctx, workspacePath)
}

func dbUpdateWorkspaceByPath(ctx context.Context, store *clientDB, workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	workspacePath, err := normalizeWorkspacePath(workspacePath)
	if err != nil {
		return workspaceItem{}, err
	}
	row, err := store.UpdateWorkspace(ctx, workspacePath, maxBytes, enabled)
	if err != nil {
		return workspaceItem{}, err
	}
	return workspaceItem{
		WorkspacePath: row.WorkspacePath,
		MaxBytes:      row.MaxBytes,
		Enabled:       row.Enabled,
		CreatedAtUnix: row.CreatedAtUnix,
	}, nil
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
	page, err := store.ListWorkspaceFiles(ctx, -1, 0, "")
	if err != nil {
		return nil, err
	}
	out := make([]workspaceFileRow, 0, len(page.Items))
	for _, row := range page.Items {
		out = append(out, workspaceFileRow{
			WorkspacePath: row.WorkspacePath,
			FilePath:      row.FilePath,
			SeedHash:      row.SeedHash,
			SeedLocked:    row.SeedLocked,
		})
	}
	return out, nil
}

func dbDeleteLiveStreamCacheRows(ctx context.Context, store *clientDB, streamID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	streamID = strings.ToLower(strings.TrimSpace(streamID))
	if streamID == "" {
		return nil
	}
	rows, err := store.ListWorkspaceFiles(ctx, -1, 0, "live/"+streamID+"/")
	if err != nil {
		return err
	}
	for _, row := range rows.Items {
		if !strings.HasPrefix(strings.ToLower(row.FilePath), "live/"+streamID+"/") {
			continue
		}
		if err := store.DeleteWorkspaceFile(ctx, row.WorkspacePath, row.FilePath); err != nil {
			return err
		}
	}
	return nil
}

func dbListWorkspaceRoots(ctx context.Context, store *clientDB) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return store.ListWorkspaceRoots(ctx)
}

func dbListWorkspaceRootsTx(ctx context.Context, store *clientDB) ([]string, error) {
	return dbListWorkspaceRoots(ctx, store)
}

func dbListWorkspaceFiles(ctx context.Context, store *clientDB, limit int, offset int, pathLike string) (workspaceFilesPage, error) {
	if store == nil {
		return workspaceFilesPage{}, fmt.Errorf("client db is nil")
	}
	page, err := store.ListWorkspaceFiles(ctx, limit, offset, pathLike)
	if err != nil {
		return workspaceFilesPage{}, err
	}
	out := workspaceFilesPage{Total: page.Total, Items: make([]workspaceFileItem, 0, len(page.Items))}
	for _, row := range page.Items {
		out.Items = append(out.Items, workspaceFileItem{
			WorkspacePath: row.WorkspacePath,
			FilePath:      row.FilePath,
			SeedHash:      row.SeedHash,
			SeedLocked:    row.SeedLocked,
		})
	}
	return out, nil
}

func dbListWorkspaceSeeds(ctx context.Context, store *clientDB, limit int, offset int, seedHash string, seedHashLike string) (workspaceSeedsPage, error) {
	if store == nil {
		return workspaceSeedsPage{}, fmt.Errorf("client db is nil")
	}
	page, err := store.ListWorkspaceFiles(ctx, -1, 0, "")
	if err != nil {
		return workspaceSeedsPage{}, err
	}
	seedSet := make(map[string]struct{}, len(page.Items))
	for _, row := range page.Items {
		seed := normalizeSeedHashHex(row.SeedHash)
		if seed == "" {
			continue
		}
		seedSet[seed] = struct{}{}
	}
	hashes := make([]string, 0, len(seedSet))
	for h := range seedSet {
		if seedHash != "" && h != normalizeSeedHashHex(seedHash) {
			continue
		}
		if seedHashLike != "" && !strings.Contains(strings.ToLower(h), strings.ToLower(strings.TrimSpace(seedHashLike))) {
			continue
		}
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	if offset < 0 {
		offset = 0
	}
	if offset > len(hashes) {
		offset = len(hashes)
	}
	hashes = hashes[offset:]
	if limit >= 0 && limit < len(hashes) {
		hashes = hashes[:limit]
	}
	out := workspaceSeedsPage{Total: len(hashes), Items: make([]workspaceSeedItem, 0, len(hashes))}
	for _, h := range hashes {
		record, ok, err := store.LoadSeedSnapshot(ctx, h)
		if err != nil {
			return workspaceSeedsPage{}, err
		}
		if !ok {
			continue
		}
		item := workspaceSeedItem{
			SeedHash:           record.SeedHash,
			SeedFilePath:       record.SeedFilePath,
			ChunkCount:         record.ChunkCount,
			FileSize:           int64(record.FileSize),
			PricingSource:      record.PricingSource,
			PriceUpdatedAtUnix: record.PriceUpdatedAtUnix,
		}
		item.FloorPriceSatPer64K = record.FloorPriceSatPer64K
		item.ResaleDiscountBPS = record.ResaleDiscountBPS
		out.Items = append(out.Items, item)
	}
	return out, nil
}

func dbLoadWorkspaceByPathTx(ctx context.Context, store *clientDB, absPath string) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("client db is nil")
	}
	absPath, err := normalizeWorkspacePath(absPath)
	if err != nil {
		return workspaceItem{}, err
	}
	rows, err := store.ListWorkspaces(ctx)
	if err != nil {
		return workspaceItem{}, err
	}
	for _, row := range rows {
		if row.WorkspacePath == absPath {
			return workspaceItem{
				WorkspacePath: row.WorkspacePath,
				MaxBytes:      row.MaxBytes,
				Enabled:       row.Enabled,
				CreatedAtUnix: row.CreatedAtUnix,
			}, nil
		}
	}
	return workspaceItem{}, fmt.Errorf("workspace not found")
}

func dbUpsertWorkspaceFileTx(ctx context.Context, store *clientDB, workspacePath string, filePath string, seedHash string, lockedValue int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.UpsertWorkspaceFile(ctx, workspacePath, filePath, seedHash, lockedValue != 0)
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func policyValueOrZero[T any](policy *moduleapi.SeedRecord, fn func(*moduleapi.SeedRecord) T) T {
	var zero T
	if policy == nil || fn == nil {
		return zero
	}
	return fn(policy)
}

func policyStringOrDefault(policy *moduleapi.SeedRecord, fn func(*moduleapi.SeedRecord) string, fallback string) string {
	if policy == nil || fn == nil {
		return fallback
	}
	out := strings.TrimSpace(fn(policy))
	if out == "" {
		return fallback
	}
	return out
}

func normalizeSeedHashes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		seedHash := normalizeSeedHashHex(raw)
		if seedHash == "" {
			continue
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, seedHash)
	}
	return out
}
