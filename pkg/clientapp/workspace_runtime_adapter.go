package clientapp

import (
	"context"
	"fmt"
)

// workspaceRuntime 是运行时给管理层的最小工作区能力。
// 设计说明：
// - 只暴露管理动作，不暴露底层表；
// - 具体读写仍走 WorkspaceStore 和 SeedStore 能力；
// - 这样 managedclient 还可以保持“工作区控制面”的外壳语义。
type workspaceRuntime interface {
	ListWithContext(context.Context) ([]workspaceItem, error)
	AddWithContext(context.Context, string, uint64) (workspaceItem, error)
	UpdateByPathWithContext(context.Context, string, *uint64, *bool) (workspaceItem, error)
	DeleteByPathWithContext(context.Context, string) error
	SyncOnce(context.Context) ([]workspaceSeedItem, error)
}

type workspaceRuntimeAdapter struct {
	store *clientDB
}

func newWorkspaceRuntimeAdapter(store *clientDB) workspaceRuntime {
	if store == nil {
		return nil
	}
	return &workspaceRuntimeAdapter{store: store}
}

func (a *workspaceRuntimeAdapter) ListWithContext(ctx context.Context) ([]workspaceItem, error) {
	if a == nil || a.store == nil {
		return nil, fmt.Errorf("workspace store is nil")
	}
	return dbListWorkspaces(ctx, a.store)
}

func (a *workspaceRuntimeAdapter) AddWithContext(ctx context.Context, path string, maxBytes uint64) (workspaceItem, error) {
	if a == nil || a.store == nil {
		return workspaceItem{}, fmt.Errorf("workspace store is nil")
	}
	return dbAddWorkspace(ctx, a.store, path, maxBytes)
}

func (a *workspaceRuntimeAdapter) UpdateByPathWithContext(ctx context.Context, path string, maxBytes *uint64, enabled *bool) (workspaceItem, error) {
	if a == nil || a.store == nil {
		return workspaceItem{}, fmt.Errorf("workspace store is nil")
	}
	return dbUpdateWorkspaceByPath(ctx, a.store, path, maxBytes, enabled)
}

func (a *workspaceRuntimeAdapter) DeleteByPathWithContext(ctx context.Context, path string) error {
	if a == nil || a.store == nil {
		return fmt.Errorf("workspace store is nil")
	}
	return dbDeleteWorkspaceByPath(ctx, a.store, path)
}

func (a *workspaceRuntimeAdapter) SyncOnce(ctx context.Context) ([]workspaceSeedItem, error) {
	if a == nil || a.store == nil {
		return nil, fmt.Errorf("workspace sync capability is nil")
	}
	page, err := dbListWorkspaceSeeds(ctx, a.store, -1, 0, "", "")
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}
