package clientapp

import (
	"fmt"
	"sort"
	"strings"
)

// WorkspaceItem 直接复用工作区记录结构，避免再造一层和数据库不一致的字段。
type WorkspaceItem = workspaceItem

// WorkspaceListResult 是 workspace.list 的统一返回。
type WorkspaceListResult struct {
	Items []WorkspaceItem `json:"items"`
	Total int             `json:"total"`
}

// WorkspaceAddRequest 是 workspace.add 的输入。
type WorkspaceAddRequest struct {
	Path     string `json:"path"`
	MaxBytes uint64 `json:"max_bytes"`
}

// WorkspaceAddResult 是 workspace.add 的统一返回。
type WorkspaceAddResult struct {
	Workspace       WorkspaceItem           `json:"workspace,omitempty"`
	Sync            WorkspaceSyncOnceResult `json:"sync,omitempty"`
	MutationApplied bool                    `json:"mutation_applied"`
	NeedSync        bool                    `json:"need_sync,omitempty"`
}

// WorkspaceUpdateRequest 是 workspace.update 的输入。
type WorkspaceUpdateRequest struct {
	WorkspacePath string  `json:"workspace_path"`
	MaxBytes      *uint64 `json:"max_bytes,omitempty"`
	Enabled       *bool   `json:"enabled,omitempty"`
}

// WorkspaceUpdateResult 是 workspace.update 的统一返回。
type WorkspaceUpdateResult struct {
	Workspace       WorkspaceItem           `json:"workspace,omitempty"`
	Sync            WorkspaceSyncOnceResult `json:"sync,omitempty"`
	MutationApplied bool                    `json:"mutation_applied"`
	NeedSync        bool                    `json:"need_sync,omitempty"`
}

// WorkspaceDeleteRequest 是 workspace.delete 的输入。
type WorkspaceDeleteRequest struct {
	WorkspacePath string `json:"workspace_path"`
}

// WorkspaceDeleteResult 是 workspace.delete 的统一返回。
type WorkspaceDeleteResult struct {
	WorkspacePath   string                  `json:"workspace_path,omitempty"`
	Sync            WorkspaceSyncOnceResult `json:"sync,omitempty"`
	MutationApplied bool                    `json:"mutation_applied"`
	NeedSync        bool                    `json:"need_sync,omitempty"`
}

// WorkspaceSyncOnceResult 是 workspace.sync_once 的统一返回。
type WorkspaceSyncOnceResult struct {
	SeedCount int             `json:"seed_count"`
	Seeds     []WorkspaceSeed `json:"biz_seeds,omitempty"`
}

// WorkspaceSyncResult 保留旧名字，只做薄别名，避免把旧调用面一起打散。
type WorkspaceSyncResult = WorkspaceSyncOnceResult

func workspaceToResultSeed(seed sellerSeed, chunkPrice uint64) WorkspaceSeed {
	return WorkspaceSeed{
		SeedHash:   strings.ToLower(strings.TrimSpace(seed.SeedHash)),
		ChunkCount: seed.ChunkCount,
		SeedPrice:  chunkPrice * uint64(seed.ChunkCount),
		ChunkPrice: chunkPrice,
	}
}

func workspaceSyncResultFromSeeds(chunkPrice uint64, bizSeeds map[string]sellerSeed) WorkspaceSyncOnceResult {
	out := make([]WorkspaceSeed, 0, len(bizSeeds))
	for _, seed := range bizSeeds {
		out = append(out, workspaceToResultSeed(seed, chunkPrice))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SeedHash < out[j].SeedHash })
	return WorkspaceSyncOnceResult{
		SeedCount: len(out),
		Seeds:     out,
	}
}

func workspaceWalletMode(cfg configSnapshotter) bool {
	if cfg == nil {
		return false
	}
	return strings.TrimSpace(cfg.ConfigSnapshot().Storage.WorkspaceDir) == ""
}

func workspaceManagerNotReadyErr() error {
	return fmt.Errorf("workspace manager not initialized")
}

func workspaceChunkPriceSatPer64K(cfg configSnapshotter) uint64 {
	if cfg == nil {
		return 0
	}
	return cfg.ConfigSnapshot().Seller.Pricing.FloorPriceSatPer64K
}

func workspaceAggregateID(commandType, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "workspace:default"
	}
	switch strings.TrimSpace(commandType) {
	case "workspace_add", "workspace_update", "workspace_delete":
		return "workspace:" + value
	default:
		return "workspace:default"
	}
}
