package filestorage

import (
	"context"
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type workspaceItem = moduleapi.WorkspaceItem
type workspaceFileItem = moduleapi.WorkspaceFileItem
type workspaceFilesPage = moduleapi.WorkspaceFilesPage

func dbListWorkspaces(ctx context.Context, store moduleapi.WorkspaceStore) ([]workspaceItem, error) {
	if store == nil {
		return nil, fmt.Errorf("workspace store is nil")
	}
	return store.ListWorkspaces(ctx)
}

func dbUpsertWorkspace(ctx context.Context, store moduleapi.WorkspaceStore, absPath string, maxBytes uint64, enabled bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("workspace store is nil")
	}
	return store.UpsertWorkspace(ctx, absPath, maxBytes, enabled)
}

func dbDeleteWorkspace(ctx context.Context, store moduleapi.WorkspaceStore, workspacePath string) error {
	if store == nil {
		return fmt.Errorf("workspace store is nil")
	}
	return store.DeleteWorkspace(ctx, workspacePath)
}

func dbUpdateWorkspace(ctx context.Context, store moduleapi.WorkspaceStore, workspacePath string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if store == nil {
		return workspaceItem{}, fmt.Errorf("workspace store is nil")
	}
	return store.UpdateWorkspace(ctx, workspacePath, maxBytes, enabled)
}

func dbListWorkspaceFiles(ctx context.Context, store moduleapi.WorkspaceStore, limit, offset int, pathLike string) ([]workspaceFileItem, int, error) {
	if store == nil {
		return nil, 0, fmt.Errorf("workspace store is nil")
	}
	page, err := store.ListWorkspaceFiles(ctx, limit, offset, pathLike)
	if err != nil {
		return nil, 0, err
	}
	return page.Items, page.Total, nil
}

func dbListWorkspaceRoots(ctx context.Context, store moduleapi.WorkspaceStore) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("workspace store is nil")
	}
	return store.ListWorkspaceRoots(ctx)
}
