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
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedchunksupply"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedpricingpolicy"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspaces"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procfiledownloadchunks"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procfiledownloads"
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
	return m.store
}

func dbEnsureDefaultWorkspace(ctx context.Context, store *clientDB, workspaceDir string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	workspaceDir = strings.TrimSpace(workspaceDir)
	if workspaceDir == "" {
		// 设计说明：
		// - 空 workspace_dir 表示钱包模式；
		// - 启动时不创建默认 workspace 记录。
		return nil
	}
	abs, err := normalizeWorkspacePath(workspaceDir)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		if _, err := dbLoadWorkspaceByPathTx(ctx, tx, abs); err != nil {
			if err != sql.ErrNoRows {
				return err
			}
			_, err = tx.BizWorkspaces.Create().
				SetWorkspacePath(abs).
				SetEnabled(1).
				SetMaxBytes(0).
				SetCreatedAtUnix(now).
				Save(ctx)
			return err
		}
		_, err := tx.BizWorkspaces.Update().
			Where(bizworkspaces.WorkspacePathEQ(abs)).
			SetEnabled(1).
			Save(ctx)
		return err
	})
}

func dbListWorkspaces(ctx context.Context, store *clientDB) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]workspaceItem, error) {
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
			return nil, err
		}
		out := make([]workspaceItem, 0, len(rows))
		for _, row := range rows {
			out = append(out, workspaceItem{
				WorkspacePath: row.WorkspacePath,
				Enabled:       row.Enabled != 0,
				MaxBytes:      uint64(row.MaxBytes),
				CreatedAtUnix: row.CreatedAtUnix,
			})
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
	return writeEntValue(ctx, store, func(tx EntWriteRoot) (workspaceItem, error) {
		if _, err := dbLoadWorkspaceByPathTx(ctx, tx, absPath); err != nil {
			if err != sql.ErrNoRows {
				return workspaceItem{}, err
			}
			if _, err := tx.BizWorkspaces.Create().
				SetWorkspacePath(absPath).
				SetMaxBytes(int64(maxBytes)).
				SetEnabled(1).
				SetCreatedAtUnix(now).
				Save(ctx); err != nil {
				return workspaceItem{}, err
			}
			return dbLoadWorkspaceByPathTx(ctx, tx, absPath)
		}
		if _, err := tx.BizWorkspaces.Update().
			Where(bizworkspaces.WorkspacePathEQ(absPath)).
			SetMaxBytes(int64(maxBytes)).
			SetEnabled(1).
			Save(ctx); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByPathTx(ctx, tx, absPath)
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		cur, err := dbLoadWorkspaceByPathTx(ctx, tx, workspacePath)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("workspace not found")
			}
			return err
		}
		if _, err := tx.BizWorkspaceFiles.Delete().Where(bizworkspacefiles.WorkspacePathEQ(cur.WorkspacePath)).Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.BizWorkspaces.Delete().Where(bizworkspaces.WorkspacePathEQ(cur.WorkspacePath)).Exec(ctx); err != nil {
			return err
		}
		return dbCleanupOrphanSeedStateTx(ctx, tx)
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
	return writeEntValue(ctx, store, func(tx EntWriteRoot) (workspaceItem, error) {
		cur, err := dbLoadWorkspaceByPathTx(ctx, tx, workspacePath)
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
		if _, err := tx.BizWorkspaces.Update().
			Where(bizworkspaces.WorkspacePathEQ(workspacePath)).
			SetMaxBytes(int64(nextMaxBytes)).
			SetEnabled(boolToInt64(nextEnabled)).
			Save(ctx); err != nil {
			return workspaceItem{}, err
		}
		return dbLoadWorkspaceByPathTx(ctx, tx, workspacePath)
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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]workspaceFileRow, error) {
		rows, err := root.BizWorkspaceFiles.Query().Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).All(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]workspaceFileRow, 0, len(rows))
		for _, row := range rows {
			out = append(out, workspaceFileRow{
				WorkspacePath: row.WorkspacePath,
				FilePath:      row.FilePath,
				SeedHash:      row.SeedHash,
				SeedLocked:    row.SeedLocked != 0,
			})
		}
		return out, nil
	})
}

func dbDeleteLiveStreamCacheRows(ctx context.Context, store *clientDB, streamID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	like := "live/" + strings.ToLower(strings.TrimSpace(streamID)) + "/%"
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.BizWorkspaceFiles.Delete().Where(bizworkspacefiles.FilePathHasPrefix(strings.TrimSuffix(like, "%"))).Exec(ctx)
		return err
	})
}

func dbCleanupOrphanSeedState(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbCleanupOrphanSeedStateTx(ctx, tx)
	})
}

func dbCleanupOrphanSeedStateTx(ctx context.Context, tx EntWriteRoot) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	activeSeedHashes, err := dbListActiveSeedHashesTx(ctx, tx)
	if err != nil {
		return err
	}
	if err := deleteSeedScopedOrphansTx(ctx, tx, activeSeedHashes); err != nil {
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		roots, err := dbListWorkspaceRootsTx(ctx, tx)
		if err != nil {
			return err
		}
		resolved, ok := resolveWorkspaceRelativePath(absPath, roots)
		if !ok {
			return fmt.Errorf("output path is outside registered biz_workspaces")
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			return fmt.Errorf("seed_hash required")
		}
		if err := dbUpsertBizSeedTx(ctx, tx, seedHash, chunkCount, fullFileSize, seedPath, recommendedName, mimeHint); err != nil {
			return err
		}
		return dbUpsertWorkspaceFileTx(ctx, tx, resolved.WorkspacePath, resolved.FilePath, seedHash, lockedValue)
	})
}

func dbListWorkspaceRoots(ctx context.Context, store *clientDB) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]string, error) {
		var rows []struct {
			WorkspacePath string `json:"workspace_path,omitempty"`
		}
		err := root.BizWorkspaces.Query().
			Where(bizworkspaces.EnabledEQ(1)).
			Order(bizworkspaces.ByWorkspacePath()).
			Select(bizworkspaces.FieldWorkspacePath).
			Scan(ctx, &rows)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			if root, err := normalizeWorkspacePath(row.WorkspacePath); err == nil && root != "" {
				out = append(out, root)
			}
		}
		return out, nil
	})
}

func dbListWorkspaceRootsTx(ctx context.Context, tx EntWriteRoot) ([]string, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	var rows []struct {
		WorkspacePath string `json:"workspace_path,omitempty"`
	}
	err := tx.BizWorkspaces.Query().
		Where(bizworkspaces.EnabledEQ(1)).
		Order(bizworkspaces.ByWorkspacePath()).
		Select(bizworkspaces.FieldWorkspacePath).
		Scan(ctx, &rows)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if root, err := normalizeWorkspacePath(row.WorkspacePath); err == nil && root != "" {
			out = append(out, root)
		}
	}
	return out, nil
}

func dbListWorkspaceFiles(ctx context.Context, store *clientDB, limit int, offset int, pathLike string) (workspaceFilesPage, error) {
	if store == nil {
		return workspaceFilesPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (workspaceFilesPage, error) {
		query := root.BizWorkspaceFiles.Query()
		if pathLike != "" {
			query = query.Where(bizworkspacefiles.Or(
				bizworkspacefiles.WorkspacePathContains(pathLike),
				bizworkspacefiles.FilePathContains(pathLike),
			))
		}
		var out workspaceFilesPage
		var err error
		out.Total, err = query.Clone().Count(ctx)
		if err != nil {
			return workspaceFilesPage{}, err
		}
		rows, err := query.Clone().Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).Limit(limit).Offset(offset).All(ctx)
		if err != nil {
			return workspaceFilesPage{}, err
		}
		out.Items = make([]workspaceFileItem, 0, len(rows))
		for _, row := range rows {
			out.Items = append(out.Items, workspaceFileItem{
				WorkspacePath: row.WorkspacePath,
				FilePath:      row.FilePath,
				SeedHash:      row.SeedHash,
				SeedLocked:    row.SeedLocked != 0,
			})
		}
		return out, nil
	})
}

func dbListWorkspaceSeeds(ctx context.Context, store *clientDB, limit int, offset int, seedHash string, seedHashLike string) (workspaceSeedsPage, error) {
	if store == nil {
		return workspaceSeedsPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (workspaceSeedsPage, error) {
		var out workspaceSeedsPage
		q := root.BizSeeds.Query()
		switch {
		case seedHash != "":
			q = q.Where(bizseeds.SeedHashEQ(normalizeSeedHashHex(seedHash)))
		case seedHashLike != "":
			q = q.Where(bizseeds.SeedHashContains(strings.ToLower(strings.TrimSpace(seedHashLike))))
		}
		total, err := q.Clone().Count(ctx)
		if err != nil {
			return workspaceSeedsPage{}, err
		}
		out.Total = total
		rows, err := q.Clone().Order(bizseeds.BySeedHash()).Limit(limit).Offset(offset).All(ctx)
		if err != nil {
			return workspaceSeedsPage{}, err
		}
		out.Items = make([]workspaceSeedItem, 0, len(rows))
		for _, seed := range rows {
			item := workspaceSeedItem{
				SeedHash:      seed.SeedHash,
				SeedFilePath:  seed.SeedFilePath,
				ChunkCount:    uint32(seed.ChunkCount),
				FileSize:      seed.FileSize,
				PricingSource: "system",
			}
			policy, err := root.BizSeedPricingPolicy.Query().Where(bizseedpricingpolicy.SeedHashEQ(seed.SeedHash)).Only(ctx)
			if err != nil {
				if !gen.IsNotFound(err) {
					return workspaceSeedsPage{}, err
				}
			} else {
				item.FloorPriceSatPer64K = uint64(policy.FloorUnitPriceSatPer64k)
				item.ResaleDiscountBPS = uint64(policy.ResaleDiscountBps)
				if strings.TrimSpace(policy.PricingSource) != "" {
					item.PricingSource = policy.PricingSource
				}
				item.PriceUpdatedAtUnix = policy.UpdatedAtUnix
			}
			out.Items = append(out.Items, item)
		}
		return out, nil
	})
}

func dbLoadWorkspaceByPathTx(ctx context.Context, tx EntWriteRoot, absPath string) (workspaceItem, error) {
	if tx == nil {
		return workspaceItem{}, fmt.Errorf("tx is nil")
	}
	absPath, err := normalizeWorkspacePath(absPath)
	if err != nil {
		return workspaceItem{}, err
	}
	var rows []struct {
		WorkspacePath string `json:"workspace_path,omitempty"`
		Enabled       int64  `json:"enabled,omitempty"`
		MaxBytes      int64  `json:"max_bytes,omitempty"`
		CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
	}
	err = tx.BizWorkspaces.Query().
		Where(bizworkspaces.WorkspacePathEQ(absPath)).
		Limit(1).
		Select(
			bizworkspaces.FieldWorkspacePath,
			bizworkspaces.FieldEnabled,
			bizworkspaces.FieldMaxBytes,
			bizworkspaces.FieldCreatedAtUnix,
		).
		Scan(ctx, &rows)
	if err != nil {
		if gen.IsNotFound(err) {
			return workspaceItem{}, sql.ErrNoRows
		}
		return workspaceItem{}, err
	}
	if len(rows) == 0 {
		return workspaceItem{}, sql.ErrNoRows
	}
	row := rows[0]
	return workspaceItem{
		WorkspacePath: row.WorkspacePath,
		Enabled:       row.Enabled != 0,
		MaxBytes:      uint64(row.MaxBytes),
		CreatedAtUnix: row.CreatedAtUnix,
	}, nil
}

func dbUpsertBizSeedTx(ctx context.Context, tx EntWriteRoot, seedHash string, chunkCount uint32, fullFileSize uint64, seedPath string, recommendedName string, mimeHint string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	seed, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.BizSeeds.Create().
			SetSeedHash(seedHash).
			SetChunkCount(int64(chunkCount)).
			SetFileSize(int64(fullFileSize)).
			SetSeedFilePath(seedPath).
			SetRecommendedFileName(recommendedName).
			SetMimeHint(mimeHint).
			Save(ctx)
		return err
	}
	_, err = seed.Update().
		SetChunkCount(int64(chunkCount)).
		SetFileSize(int64(fullFileSize)).
		SetSeedFilePath(seedPath).
		SetRecommendedFileName(recommendedName).
		SetMimeHint(mimeHint).
		Save(ctx)
	return err
}

func dbUpsertBizSeedClient(ctx context.Context, client *gen.BizSeedsClient, seedHash string, chunkCount uint32, fullFileSize uint64, seedPath string, recommendedName string, mimeHint string) error {
	if client == nil {
		return fmt.Errorf("biz_seeds client is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	seed, err := client.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = client.Create().
			SetSeedHash(seedHash).
			SetChunkCount(int64(chunkCount)).
			SetFileSize(int64(fullFileSize)).
			SetSeedFilePath(seedPath).
			SetRecommendedFileName(recommendedName).
			SetMimeHint(mimeHint).
			Save(ctx)
		return err
	}
	_, err = seed.Update().
		SetChunkCount(int64(chunkCount)).
		SetFileSize(int64(fullFileSize)).
		SetSeedFilePath(seedPath).
		SetRecommendedFileName(recommendedName).
		SetMimeHint(mimeHint).
		Save(ctx)
	return err
}

func dbUpsertWorkspaceFileTx(ctx context.Context, tx EntWriteRoot, workspacePath string, filePath string, seedHash string, lockedValue int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	workspacePath, err := normalizeWorkspacePath(workspacePath)
	if err != nil {
		return err
	}
	filePath, err = normalizeWorkspaceFilePath(filePath)
	if err != nil {
		return err
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	row, err := tx.BizWorkspaceFiles.Query().Where(
		bizworkspacefiles.WorkspacePathEQ(workspacePath),
		bizworkspacefiles.FilePathEQ(filePath),
	).Only(ctx)
	if err != nil {
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.BizWorkspaceFiles.Create().
			SetWorkspacePath(workspacePath).
			SetFilePath(filePath).
			SetSeedHash(seedHash).
			SetSeedLocked(lockedValue).
			Save(ctx)
		return err
	}
	_, err = row.Update().
		SetSeedHash(seedHash).
		SetSeedLocked(lockedValue).
		Save(ctx)
	return err
}

func dbListActiveSeedHashesTx(ctx context.Context, tx EntWriteRoot) ([]string, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	var rows []struct {
		SeedHash string `json:"seed_hash,omitempty"`
	}
	if err := tx.BizWorkspaceFiles.Query().Unique(true).Select("seed_hash").Scan(ctx, &rows); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seedHash := normalizeSeedHashHex(row.SeedHash)
		if seedHash == "" {
			continue
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, seedHash)
	}
	return out, nil
}

func deleteSeedScopedOrphansTx(ctx context.Context, tx EntWriteRoot, activeSeedHashes []string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	activeSeedHashes = normalizeSeedHashes(activeSeedHashes)
	if len(activeSeedHashes) == 0 {
		if _, err := tx.BizSeeds.Delete().Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.BizSeedPricingPolicy.Delete().Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.BizSeedChunkSupply.Delete().Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.ProcFileDownloads.Delete().Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.ProcFileDownloadChunks.Delete().Exec(ctx); err != nil {
			return err
		}
		return nil
	}
	if _, err := tx.BizSeeds.Delete().Where(bizseeds.SeedHashNotIn(activeSeedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := tx.BizSeedPricingPolicy.Delete().Where(bizseedpricingpolicy.SeedHashNotIn(activeSeedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := tx.BizSeedChunkSupply.Delete().Where(bizseedchunksupply.SeedHashNotIn(activeSeedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := tx.ProcFileDownloads.Delete().Where(procfiledownloads.SeedHashNotIn(activeSeedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := tx.ProcFileDownloadChunks.Delete().Where(procfiledownloadchunks.SeedHashNotIn(activeSeedHashes...)).Exec(ctx); err != nil {
		return err
	}
	return nil
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func policyValueOrZero[T any](policy *gen.BizSeedPricingPolicy, fn func(*gen.BizSeedPricingPolicy) T) T {
	var zero T
	if policy == nil || fn == nil {
		return zero
	}
	return fn(policy)
}

func policyStringOrDefault(policy *gen.BizSeedPricingPolicy, fn func(*gen.BizSeedPricingPolicy) string, fallback string) string {
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
