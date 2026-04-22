package filestorage

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen/bizworkspacefiles"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/filestorage/storedb/gen/bizworkspaces"
)

// 设计说明：
// - 这里放文件存储箱子的 DB 入口本体；
// - 外层 store 只负责把能力传进来，不再承载一堆实现细节。

func DBListWorkspaces(ctx context.Context, root DBReadRoot) ([]moduleapi.WorkspaceItem, error) {
	if root == nil {
		return nil, fmt.Errorf("workspace root is nil")
	}
	rows, err := root.BizWorkspacesQuery().
		Order(bizworkspaces.ByWorkspacePath()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]moduleapi.WorkspaceItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, moduleapi.WorkspaceItem{
			WorkspacePath: row.WorkspacePath,
			MaxBytes:      uint64(row.MaxBytes),
			Enabled:       row.Enabled != 0,
			CreatedAtUnix: row.CreatedAtUnix,
		})
	}
	return out, nil
}

func DBUpsertWorkspace(ctx context.Context, root DBWriteRoot, absPath string, maxBytes uint64, enabled bool) (moduleapi.WorkspaceItem, error) {
	if root == nil {
		return moduleapi.WorkspaceItem{}, fmt.Errorf("workspace root is nil")
	}
	workspacePath, err := normalizeRootPath(absPath)
	if err != nil {
		return moduleapi.WorkspaceItem{}, err
	}
	now := time.Now().Unix()
	row, err := root.BizWorkspacesClient().Query().Where(bizworkspaces.WorkspacePathEQ(workspacePath)).Only(ctx)
	if err == nil {
		_, err = row.Update().
			SetMaxBytes(int64(maxBytes)).
			SetEnabled(boolToInt64DB(enabled)).
			Save(ctx)
		if err != nil {
			return moduleapi.WorkspaceItem{}, err
		}
		return moduleapi.WorkspaceItem{
			WorkspacePath: workspacePath,
			MaxBytes:      maxBytes,
			Enabled:       enabled,
			CreatedAtUnix: row.CreatedAtUnix,
		}, nil
	}
	if !gen.IsNotFound(err) {
		return moduleapi.WorkspaceItem{}, err
	}
	if _, err := root.BizWorkspacesClient().Create().
		SetWorkspacePath(workspacePath).
		SetMaxBytes(int64(maxBytes)).
		SetEnabled(boolToInt64DB(enabled)).
		SetCreatedAtUnix(now).
		Save(ctx); err != nil {
		return moduleapi.WorkspaceItem{}, err
	}
	return moduleapi.WorkspaceItem{
		WorkspacePath: workspacePath,
		MaxBytes:      maxBytes,
		Enabled:       enabled,
		CreatedAtUnix: now,
	}, nil
}

func DBDeleteWorkspace(ctx context.Context, root DBWriteRoot, workspacePath string) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return err
	}
	if _, err := root.BizWorkspaceFilesClient().Delete().Where(bizworkspacefiles.WorkspacePathEQ(workspacePath)).Exec(ctx); err != nil {
		return err
	}
	if _, err := root.BizWorkspacesClient().Delete().Where(bizworkspaces.WorkspacePathEQ(workspacePath)).Exec(ctx); err != nil {
		return err
	}
	return nil
}

func DBUpdateWorkspace(ctx context.Context, root DBWriteRoot, workspacePath string, maxBytes *uint64, enabled *bool) (moduleapi.WorkspaceItem, error) {
	if root == nil {
		return moduleapi.WorkspaceItem{}, fmt.Errorf("workspace root is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return moduleapi.WorkspaceItem{}, err
	}
	cur, err := root.BizWorkspacesClient().Query().Where(bizworkspaces.WorkspacePathEQ(workspacePath)).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return moduleapi.WorkspaceItem{}, fmt.Errorf("workspace not found")
		}
		return moduleapi.WorkspaceItem{}, err
	}
	nextMaxBytes := uint64(cur.MaxBytes)
	if maxBytes != nil {
		nextMaxBytes = *maxBytes
	}
	nextEnabled := cur.Enabled != 0
	if enabled != nil {
		nextEnabled = *enabled
	}
	if _, err := cur.Update().
		SetMaxBytes(int64(nextMaxBytes)).
		SetEnabled(boolToInt64DB(nextEnabled)).
		Save(ctx); err != nil {
		return moduleapi.WorkspaceItem{}, err
	}
	return moduleapi.WorkspaceItem{
		WorkspacePath: workspacePath,
		MaxBytes:      nextMaxBytes,
		Enabled:       nextEnabled,
		CreatedAtUnix: cur.CreatedAtUnix,
	}, nil
}

func DBListWorkspaceFiles(ctx context.Context, root DBReadRoot, limit, offset int, pathLike string) ([]moduleapi.WorkspaceFileItem, int, error) {
	if root == nil {
		return nil, 0, fmt.Errorf("workspace root is nil")
	}
	query := root.BizWorkspaceFilesQuery()
	if pathLike != "" {
		query = query.Where(bizworkspacefiles.Or(
			bizworkspacefiles.WorkspacePathContains(pathLike),
			bizworkspacefiles.FilePathContains(pathLike),
		))
	}
	total, err := query.Clone().Count(ctx)
	if err != nil {
		return nil, 0, err
	}
	rows, err := query.Clone().Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).Limit(limit).Offset(offset).All(ctx)
	if err != nil {
		return nil, 0, err
	}
	out := make([]moduleapi.WorkspaceFileItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, moduleapi.WorkspaceFileItem{
			WorkspacePath: row.WorkspacePath,
			FilePath:      row.FilePath,
			SeedHash:      row.SeedHash,
			SeedLocked:    row.SeedLocked != 0,
		})
	}
	return out, total, nil
}

func DBListWorkspaceRoots(ctx context.Context, root DBReadRoot) ([]string, error) {
	if root == nil {
		return nil, fmt.Errorf("workspace root is nil")
	}
	rows, err := root.BizWorkspacesQuery().
		Where(bizworkspaces.EnabledEQ(1)).
		Order(bizworkspaces.ByWorkspacePath()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if p, err := normalizeRootPath(row.WorkspacePath); err == nil && p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

func DBUpsertWorkspaceFile(ctx context.Context, root DBWriteRoot, workspacePath, filePath, seedHash string, locked bool) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
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
		return fmt.Errorf("seed_hash required")
	}
	row, err := root.BizWorkspaceFilesClient().Query().Where(
		bizworkspacefiles.WorkspacePathEQ(workspacePath),
		bizworkspacefiles.FilePathEQ(filePath),
	).Only(ctx)
	if err != nil {
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = root.BizWorkspaceFilesClient().Create().
			SetWorkspacePath(workspacePath).
			SetFilePath(filePath).
			SetSeedHash(seedHash).
			SetSeedLocked(boolToInt64DB(locked)).
			Save(ctx)
		return err
	}
	_, err = row.Update().
		SetSeedHash(seedHash).
		SetSeedLocked(boolToInt64DB(locked)).
		Save(ctx)
	return err
}

func DBDeleteWorkspaceFile(ctx context.Context, root DBWriteRoot, workspacePath, filePath string) error {
	if root == nil {
		return fmt.Errorf("workspace root is nil")
	}
	workspacePath, err := normalizeRootPath(workspacePath)
	if err != nil {
		return err
	}
	filePath, err = normalizeRelFilePath(filePath)
	if err != nil {
		return err
	}
	_, err = root.BizWorkspaceFilesClient().Delete().Where(
		bizworkspacefiles.WorkspacePathEQ(workspacePath),
		bizworkspacefiles.FilePathEQ(filePath),
	).Exec(ctx)
	return err
}

func DBGetWorkspaceFileSeedHashByAbsPath(ctx context.Context, root DBReadRoot, absPath string) (string, error) {
	if root == nil {
		return "", fmt.Errorf("workspace root is nil")
	}
	roots, err := DBListWorkspaceRoots(ctx, root)
	if err != nil {
		return "", err
	}
	rootPath, rel, ok := resolveWorkspaceRelativePath(absPath, roots)
	if !ok {
		return "", fmt.Errorf("file path is outside workspace")
	}
	row, err := root.BizWorkspaceFilesQuery().
		Where(
			bizworkspacefiles.WorkspacePathEQ(rootPath),
			bizworkspacefiles.FilePathEQ(rel),
		).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return normalizeSeedHashHex(row.SeedHash), nil
}

func boolToInt64DB(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
